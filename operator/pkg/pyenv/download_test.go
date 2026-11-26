// SPDX-License-Identifier: Apache-2.0

package pyenv

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// noWaits makes the retries of a test immediate.
func noWaits(t *testing.T) {
	t.Helper()
	saved := downloadRetryWaits
	downloadRetryWaits = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { downloadRetryWaits = saved })
}

// nameDidNotResolve is what the HTTP client returns when the resolver
// answers with an error and not with "no such name".
func nameDidNotResolve(rawURL string) error {
	return &neturl.Error{Op: "Get", URL: rawURL, Err: &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{
		Err: "server misbehaving", Name: "release-assets.example.test", Server: "10.43.0.10:53", IsTemporary: true,
	}}}
}

// chain serves a whole pin chain and returns its pin.
func chain(t *testing.T) (served map[string][]byte, pin string) {
	t.Helper()
	archive := uvArchive(t, []byte("#!uv"))
	const uvURL = "https://example.test/uv-x86_64-unknown-linux-gnu.tar.gz"
	lock := []byte("uv@0.12.19 amd64 " + sha(archive) + " " + uvURL + "\n")
	sums := []byte(sha(lock) + "  upstream.lock\n")
	return map[string][]byte{"https://base.test/SHA256SUMS": sums, "https://base.test/upstream.lock": lock, uvURL: archive}, sha(sums)
}

// A name that does not resolve for an instant must not leave a container
// without its Python: the agent exits when uv cannot be fetched, and nothing
// starts it again (local suite, 2026-10-13).
func TestDownloadThatFailsOnTheNetworkIsTriedAgain(t *testing.T) {
	noWaits(t)
	served, pin := chain(t)
	failures := map[string]int{}
	got, err := FetchUV(context.Background(), Fetch{
		Base: "https://base.test/", SumsSHA256: pin, Arch: "amd64", Dir: t.TempDir(),
		Get: func(_ context.Context, url string) ([]byte, error) {
			if strings.HasSuffix(url, ".tar.gz") && failures[url] < 2 {
				failures[url]++
				return nil, nameDidNotResolve(url)
			}
			return served[url], nil
		},
	})
	if err != nil {
		t.Fatalf("two failures of the network ended the fetch: %v", err)
	}
	if body, _ := os.ReadFile(got); string(body) != "#!uv" {
		t.Fatalf("uv = %q", body)
	}
}

// The tries are bounded: a network that stays down ends the fetch with the
// network's own error.
func TestDownloadGivesUpAfterItsWaits(t *testing.T) {
	noWaits(t)
	_, pin := chain(t)
	calls := 0
	_, err := FetchUV(context.Background(), Fetch{
		Base: "https://base.test/", SumsSHA256: pin, Arch: "amd64", Dir: t.TempDir(),
		Get: func(_ context.Context, url string) ([]byte, error) {
			calls++
			return nil, nameDidNotResolve(url)
		},
	})
	if err == nil || !strings.Contains(err.Error(), "server misbehaving") {
		t.Fatalf("err = %v, want the resolver's", err)
	}
	if want := len(downloadRetryWaits) + 1; calls != want {
		t.Fatalf("tried %d times, want %d", calls, want)
	}
}

// A release host's outage lasts more than an instant: GitHub answered uv with
// 504 for 24 s (full suite, 2026-11-05), and the agent exits when uv cannot be
// fetched. The waits cover what the host installer's downloads cover.
func TestDownloadWaitsOutAMomentsOutage(t *testing.T) {
	var waited time.Duration
	for _, wait := range downloadRetryWaits {
		waited += wait
	}
	if waited < 2*time.Minute {
		t.Fatalf("the tries of a download span %s, want at least 2m", waited)
	}
}

// A server that answers 500 or 429 (a release host's outage, its rate
// limit) is a moment's: final at once, it stopped the agent, and an attached
// container dropped off the hub (R4 ronda 4, M2).
func TestAServerErrorOrARateLimitIsTriedAgain(t *testing.T) {
	noWaits(t)
	for _, code := range []int{http.StatusInternalServerError, http.StatusTooManyRequests, http.StatusRequestTimeout} {
		served, pin := chain(t)
		calls := 0
		_, err := FetchUV(context.Background(), Fetch{
			Base: "https://base.test/", SumsSHA256: pin, Arch: "amd64", Dir: t.TempDir(),
			Get: func(_ context.Context, url string) ([]byte, error) {
				if strings.HasSuffix(url, "/SHA256SUMS") {
					calls++
					if calls == 1 {
						return nil, &statusError{code: code, status: http.StatusText(code)}
					}
				}
				return served[url], nil
			},
		})
		if err != nil || calls != 2 {
			t.Errorf("%d: fetch = %v after %d asks, want it asked again", code, err, calls)
		}
	}
}

// An answer is final: a file that is not there, a name that does not exist
// and a file that is not the pinned one are not asked for again.
func TestDownloadThatWasAnsweredIsNotTriedAgain(t *testing.T) {
	noWaits(t)
	served, pin := chain(t)
	for name, answer := range map[string]func(url string) ([]byte, error){
		"not found": func(string) ([]byte, error) {
			return nil, &statusError{code: http.StatusNotFound, status: "404 Not Found"}
		},
		"forbidden": func(string) ([]byte, error) {
			return nil, &statusError{code: http.StatusForbidden, status: "403 Forbidden"}
		},
		"no such name": func(url string) ([]byte, error) {
			return nil, &neturl.Error{Op: "Get", URL: url, Err: &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "base.test", IsNotFound: true}}}
		},
		"another file": func(string) ([]byte, error) { return []byte("not the pinned one"), nil },
	} {
		calls := 0
		_, err := FetchUV(context.Background(), Fetch{
			Base: "https://base.test/", SumsSHA256: pin, Arch: "amd64", Dir: t.TempDir(),
			Get: func(_ context.Context, url string) ([]byte, error) {
				if strings.HasSuffix(url, "/SHA256SUMS") {
					calls++
					return answer(url)
				}
				return served[url], nil
			},
		})
		if err == nil {
			t.Fatalf("%s: fetched", name)
		}
		if calls != 1 {
			t.Errorf("%s: asked %d times, want 1 (%v)", name, calls, err)
		}
	}
}

// A fetch that is called off does not sit out its wait.
func TestDownloadCalledOffStopsWaiting(t *testing.T) {
	saved := downloadRetryWaits
	downloadRetryWaits = []time.Duration{time.Hour}
	t.Cleanup(func() { downloadRetryWaits = saved })
	_, pin := chain(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := FetchUV(ctx, Fetch{
			Base: "https://base.test/", SumsSHA256: pin, Arch: "amd64", Dir: t.TempDir(),
			Get: func(_ context.Context, url string) ([]byte, error) {
				cancel()
				return nil, nameDidNotResolve(url)
			},
		})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("fetched")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("still waiting after it was called off")
	}
}

// Over HTTP: a server that is briefly unavailable is asked again, and one
// that says the file is not there is not.
func TestHTTPDownloadRetriesAnUnavailableServer(t *testing.T) {
	noWaits(t)
	served, pin := chain(t)
	var sumsAsked, missingAsked atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/SHA256SUMS":
			if sumsAsked.Add(1) <= 2 {
				http.Error(w, "busy", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write(served["https://base.test/SHA256SUMS"])
		default:
			missingAsked.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, err := FetchUV(context.Background(), Fetch{Base: server.URL, SumsSHA256: pin, Arch: "amd64", Dir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("err = %v, want the lock's 404 after SHA256SUMS was served", err)
	}
	if got := sumsAsked.Load(); got != 3 {
		t.Errorf("SHA256SUMS asked %d times, want 3", got)
	}
	if got := missingAsked.Load(); got != 1 {
		t.Errorf("the missing file asked %d times, want 1", got)
	}
}

// A presigned URL's query is a credential, and so is the one a server
// redirects to. The HTTP client's own error carries the whole URL: none of it
// may reach the error, which ends up in the container's log.
func TestHTTPDownloadErrorNamesNoQuery(t *testing.T) {
	noWaits(t)
	gone := httptest.NewServer(http.NotFoundHandler())
	goneURL := gone.URL
	gone.Close()

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, goneURL+"/asset?sig=REDIRECTSECRET", http.StatusFound)
	}))
	defer redirect.Close()

	for name, base := range map[string]string{"direct": goneURL, "redirected": redirect.URL} {
		_, err := FetchUV(context.Background(), Fetch{
			ArtifactURLs: map[string]string{"SHA256SUMS": base + "/p/SHA256SUMS?X-Amz-Signature=SECRET"},
			SumsSHA256:   strings.Repeat("0", 64), Arch: "amd64", Dir: t.TempDir(),
		})
		if err == nil {
			t.Fatalf("%s: fetched from a server that is gone", name)
		}
		if strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%s: the error carries a URL's query: %v", name, err)
		}
		var urlErr *neturl.Error
		if !errors.As(err, &urlErr) {
			t.Errorf("%s: the client's error is no longer in the chain: %v", name, err)
		}
		if !strings.Contains(err.Error(), "/p/SHA256SUMS") {
			t.Errorf("%s: the error does not name the file: %v", name, err)
		}
	}
}
