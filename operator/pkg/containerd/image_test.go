// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

func TestNormalizeImageRef(t *testing.T) {
	cases := map[string]string{
		"python:3.13.14-slim-bookworm":                   "docker.io/library/python:3.13.14-slim-bookworm",
		"docker.io/library/python:3.13.14-slim-bookworm": "docker.io/library/python:3.13.14-slim-bookworm",
		"takenking9879/ray-train:2.53.0":                 "docker.io/takenking9879/ray-train:2.53.0",
		"ghcr.io/org/app:1":                              "ghcr.io/org/app:1",
		"localhost:5000/app:dev":                         "localhost:5000/app:dev",
		"":                                               "",
	}
	for in, want := range cases {
		if got := NormalizeImageRef(in); got != want {
			t.Errorf("NormalizeImageRef(%q) = %q, want %q", in, got, want)
		}
	}
}

type scriptedRunner struct {
	commands []string
	infoErr  error
	pullErr  error
}

func (r *scriptedRunner) Run(_ context.Context, cmd string) (string, error) {
	r.commands = append(r.commands, cmd)
	switch {
	case strings.Contains(cmd, "images ls"):
		if r.infoErr != nil {
			return "", r.infoErr
		}
		return "docker.io/library/python:3.13-slim\n", nil
	case strings.Contains(cmd, "images pull"):
		return "", r.pullErr
	default:
		return "", nil
	}
}

func TestImageExistsMatchesQuietListing(t *testing.T) {
	r := &scriptedRunner{}
	ok, err := ImageExists(context.Background(), r, "sudo ", "python:3.13-slim")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("quiet listing must match the normalized ref")
	}
}

func TestImageExistsCommandNotFoundIsError(t *testing.T) {
	r := &scriptedRunner{infoErr: errors.New("sudo: ctr: command not found")}
	ok, err := ImageExists(context.Background(), r, "sudo ", "python:3.13-slim")
	if err == nil {
		t.Fatal("missing ctr binary must not look like a cache miss")
	}
	if ok {
		t.Fatal("command-not-found must not report the image present")
	}
}

func TestEnsureImageSkipsPullWhenPresent(t *testing.T) {
	r := &scriptedRunner{}
	pulled, err := EnsureImage(context.Background(), r, "sudo ", "python:3.13-slim")
	if err != nil {
		t.Fatal(err)
	}
	if pulled {
		t.Fatal("cached image must not pull")
	}
	if len(r.commands) != 1 || !strings.Contains(r.commands[0], "images ls -q") {
		t.Fatalf("commands = %v, want a single images ls -q", r.commands)
	}
}

func TestEnsureImagePullsWhenAbsent(t *testing.T) {
	r := &scriptedRunner{infoErr: errors.New("ctr: image not found")}
	pulled, err := EnsureImage(context.Background(), r, "sudo ", "python:3.13-slim")
	if err != nil {
		t.Fatal(err)
	}
	if !pulled {
		t.Fatal("missing image must pull")
	}
	if len(r.commands) != 2 {
		t.Fatalf("commands = %v, want info then pull", r.commands)
	}
	if !strings.Contains(r.commands[1], "images pull 'docker.io/library/python:3.13-slim'") {
		t.Fatalf("pull command = %q", r.commands[1])
	}
	if strings.Contains(r.commands[1], "linux/amd64") {
		t.Fatalf("ctr pull hardcoded linux/amd64: %s", r.commands[1])
	}
	if strings.Contains(r.commands[1], "oauth2accesstoken") {
		t.Fatalf("docker hub pull must not use GCE oauth: %q", r.commands[1])
	}
}

// Off Google Cloud there is no metadata server: a gcr.io or Artifact
// Registry image pulls anonymously, as a public one must. The pull command
// used to curl the GCE metadata itself and pass an empty token, which a
// public image answers with 401. On GCP the token
// comes from GARAuth, through the private user file.
func TestOffGCPPullOfGCRIsAnonymous(t *testing.T) {
	for _, ref := range []string{"gcr.io/distroless/static:nonroot", "us-central1-docker.pkg.dev/zenthros/zenthros/playground-runtime:1.0.0"} {
		r := &scriptedRunner{infoErr: errors.New("ctr: image not found")}
		auth := GARAuth{TokenURL: "http://127.0.0.1:1/token"}
		if _, err := EnsureImageWith(context.Background(), r, "", ref, auth); err != nil {
			t.Fatalf("%s: %v", ref, err)
		}
		cmd := r.commands[len(r.commands)-1]
		if strings.Contains(cmd, "metadata.google.internal") || strings.Contains(cmd, "--user") || !strings.Contains(cmd, "images pull '"+ref+"'") {
			t.Fatalf("off-GCP pull of %s = %q, want an anonymous pull", ref, cmd)
		}
	}
}

// On Google Cloud the service account's token logs the pull in, through the
// private user file: it never appears in the command an error would carry.
func TestOnGCPPullUsesTheServiceAccountToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(writer, `{"access_token":"ya29.token"}`)
	}))
	t.Cleanup(server.Close)
	r := &scriptedRunner{infoErr: errors.New("ctr: image not found")}
	ref := "us-central1-docker.pkg.dev/zenthros/zenthros/playground-runtime:1.0.0"
	if _, err := EnsureImageWith(context.Background(), r, "", ref, GARAuth{TokenURL: server.URL, Client: server.Client()}); err != nil {
		t.Fatal(err)
	}
	cmd := r.commands[len(r.commands)-1]
	if !strings.Contains(cmd, `--user "$(cat '`) || strings.Contains(cmd, "ya29.token") || strings.Contains(cmd, "metadata.google.internal") {
		t.Fatalf("on-GCP pull = %q, want the login from the user file", cmd)
	}
}

// flakyPull fails the first pulls with errs, in order, then succeeds.
type flakyPull struct {
	scriptedRunner
	errs  []error
	pulls int
}

func (r *flakyPull) Run(ctx context.Context, cmd string) (string, error) {
	if !strings.Contains(cmd, "images pull") {
		return r.scriptedRunner.Run(ctx, cmd)
	}
	r.pulls++
	if r.pulls <= len(r.errs) {
		return "", r.errs[r.pulls-1]
	}
	return "", nil
}

// One failed name lookup must not fail a job: the registry's name did not
// resolve for an instant and the job ended SUBMISSION_FAILED
// (TestSharedMachineContainerJobsAreIsolated, local suite 2026-10-12, the
// machine's resolver answering "server misbehaving"). A pull that fails on
// the network is tried again a few times; one the registry refused is not.
func TestEnsureImageTriesAgainWhenTheNetworkFailsThePull(t *testing.T) {
	waits := pullRetryWaits
	pullRetryWaits = []time.Duration{0, 0, 0}
	t.Cleanup(func() { pullRetryWaits = waits })
	absent := scriptedRunner{infoErr: errors.New("ctr: image not found")}
	lookup := errors.New(`ctr: failed to resolve image: failed to authorize: failed to fetch anonymous token: Get "https://auth.docker.io/token": dial tcp: lookup auth.docker.io on 10.43.0.10:53: server misbehaving`)

	r := &flakyPull{scriptedRunner: absent, errs: []error{lookup, errors.New("read tcp 10.0.0.1:443: i/o timeout")}}
	if pulled, err := EnsureImage(context.Background(), r, "sudo ", "python:3.13-slim"); err != nil || !pulled {
		t.Fatalf("pulled=%v err=%v after two network failures and a good pull", pulled, err)
	}
	if r.pulls != 3 {
		t.Fatalf("%d pulls, want 3", r.pulls)
	}

	// The network stays down: the last error is the job's, after one try
	// more than there are waits.
	r = &flakyPull{scriptedRunner: absent, errs: []error{lookup, lookup, lookup, lookup, lookup}}
	if _, err := EnsureImage(context.Background(), r, "sudo ", "python:3.13-slim"); err == nil || !strings.Contains(err.Error(), "server misbehaving") {
		t.Fatalf("err = %v, want the lookup failure", err)
	}
	if r.pulls != 4 {
		t.Fatalf("%d pulls with the network down, want 4", r.pulls)
	}

	// The registry's own answer is final: asking again changes nothing.
	for _, refusal := range []string{
		"ctr: failed to resolve image: pull access denied, repository does not exist or may require authorization",
		"ctr: failed to resolve image: docker.io/library/nope:1: not found",
		"unexpected status from HEAD request: 401 Unauthorized",
		"unexpected status from HEAD request: 429 Too Many Requests",
		"dial tcp: lookup regisry.example.com: no such host",
	} {
		r = &flakyPull{scriptedRunner: absent, errs: []error{errors.New(refusal)}}
		if _, err := EnsureImage(context.Background(), r, "sudo ", "python:3.13-slim"); err == nil || r.pulls != 1 {
			t.Fatalf("%q: err=%v after %d pulls, want it final at the first", refusal, err, r.pulls)
		}
	}
}

// A job that is cancelled while a pull waits to try again stops waiting.
func TestEnsureImageStopsRetryingWhenCancelled(t *testing.T) {
	waits := pullRetryWaits
	pullRetryWaits = []time.Duration{time.Hour}
	t.Cleanup(func() { pullRetryWaits = waits })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &flakyPull{scriptedRunner: scriptedRunner{infoErr: errors.New("ctr: image not found")},
		errs: []error{errors.New("dial tcp 1.2.3.4:443: connection refused"), errors.New("never asked")}}
	if _, err := EnsureImage(ctx, r, "sudo ", "python:3.13-slim"); err == nil || r.pulls != 1 {
		t.Fatalf("err=%v after %d pulls, want the first failure and no wait", err, r.pulls)
	}
}

// A pull's failure carries its retry class to the controller (c4-A7): the
// registry's refusal fails the job at once, its 429 and the network's
// failure are retried within the submit budget. Unmarked, every pull
// failure crossed the wire as a refusal of the job.
func TestAPullFailureCarriesItsRetryClass(t *testing.T) {
	waits := pullRetryWaits
	pullRetryWaits = nil
	t.Cleanup(func() { pullRetryWaits = waits })
	absent := scriptedRunner{infoErr: errors.New("ctr: image not found")}
	for _, tc := range []struct {
		answer string
		class  error
	}{
		{"ctr: failed to resolve image: docker.io/library/nope:1: not found", backend.ErrImagePull},
		{"unexpected status from HEAD request: 401 Unauthorized", backend.ErrImagePull},
		{"unexpected status from HEAD request: 429 Too Many Requests", backend.ErrRateLimited},
		{"toomanyrequests: You have reached your unauthenticated pull rate limit", backend.ErrRateLimited},
		{"dial tcp 1.2.3.4:443: i/o timeout", backend.ErrDestinationUnavailable},
		// A registry's own failure is as passing as its gateway's.
		{"unexpected status from GET request: 500 Internal Server Error", backend.ErrDestinationUnavailable},
		// A connection cut a way nobody listed is the network too: only the
		// registry's refusals are final (R4 ronda 3, G1).
		{"read tcp 10.0.0.2:51234->1.2.3.4:443: read: connection timed out", backend.ErrDestinationUnavailable},
		{"http2: client connection lost", backend.ErrDestinationUnavailable},
		{"failed to copy: EOF", backend.ErrDestinationUnavailable},
		{"unexpected status from GET request: 403 Forbidden", backend.ErrImagePull},
		{"failed to resolve reference \"ghcr.io/acme/app:v9\": ghcr.io/acme/app:v9: not found", backend.ErrImagePull},
		{"no match for platform in manifest: not found", backend.ErrImagePull},
		// A blob of the local content store that another pull's cleanup
		// took is this machine's moment, not the registry's answer
		// (R4 ronda 4, P1).
		{"ctr: failed to copy: content digest sha256:4f4fb700ef54461cfa02571ae0db9a0dc1e0cdb5577484a6d75e68dc38e8acc1: not found", backend.ErrDestinationUnavailable},
		// So is a snapshot of the local store the unpack needs (final
		// review of Ola J).
		{"ctr: failed to prepare extraction snapshot \"extract-1\": parent snapshot sha256:9f2a does not exist: not found", backend.ErrDestinationUnavailable},
	} {
		r := &flakyPull{scriptedRunner: absent, errs: []error{errors.New(tc.answer)}}
		_, err := EnsureImage(context.Background(), r, "sudo ", "python:3.13-slim")
		if !errors.Is(err, tc.class) || !strings.Contains(err.Error(), tc.answer) {
			t.Fatalf("%q: err = %v, want %v with the registry's text", tc.answer, err, tc.class)
		}
		// The same answer through a kubelet (containerd's words, in the
		// pod's waiting state) fails a Kubernetes job only when it is final
		// here: one decision for both (backend.PullRefused).
		kubelet := `ImagePullBackOff Back-off pulling image "python:3.13-slim": ErrImagePull: rpc error: code = Unknown desc = ` + tc.answer
		if final := backend.DiagnosticReasonCode(kubelet) == v1.ReasonImagePullFailed; final != errors.Is(tc.class, backend.ErrImagePull) {
			t.Errorf("%q: a pod's pull fails the job = %v, a machine's = %v", tc.answer, final, errors.Is(tc.class, backend.ErrImagePull))
		}
	}
}
