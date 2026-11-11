// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type blobs struct {
	mu      sync.Mutex
	body    map[string][]byte
	gets    map[string]int
	delay   time.Duration
	active  atomic.Int32
	peak    atomic.Int32
	failing map[string]int
}

func newBlobs() *blobs {
	return &blobs{body: map[string][]byte{}, gets: map[string]int{}, failing: map[string]int{}}
}

func (b *blobs) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := b.active.Add(1)
	defer b.active.Add(-1)
	for {
		peak := b.peak.Load()
		if now <= peak || b.peak.CompareAndSwap(peak, now) {
			break
		}
	}
	time.Sleep(b.delay)
	b.mu.Lock()
	b.gets[r.URL.Path]++
	body, ok := b.body[r.URL.Path]
	code := b.failing[r.URL.Path]
	b.mu.Unlock()
	if code != 0 {
		w.WriteHeader(code)
		fmt.Fprint(w, "<Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>")
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	_, _ = w.Write(body)
}

func newManager(t *testing.T) *Manager {
	t.Helper()
	return &Manager{Root: t.TempDir(), Backoff: time.Millisecond}
}

func statusOf(t *testing.T, m *Manager, key string, items []Item) []Status {
	t.Helper()
	m.Prepare(key, items, 4)
	m.Wait(key)
	return m.Prepare(key, items, 4)
}

// Items download at once up to the Prewarm's concurrency, never more.
func TestPrewarmItemsRunConcurrentlyWithLimit(t *testing.T) {
	b := newBlobs()
	b.delay = 150 * time.Millisecond
	srv := httptest.NewServer(b)
	defer srv.Close()
	var items []Item
	for i := range 6 {
		body := []byte(fmt.Sprintf("model shard %d", i))
		path := fmt.Sprintf("/shard-%d", i)
		b.body[path] = body
		items = append(items, Item{Ref: "artifact:" + path, URI: srv.URL + path, SHA256: digest(body), Path: "model" + path})
	}
	m := newManager(t)
	m.Prepare("llama/1", items, 3)
	m.Wait("llama/1")
	// Three in flight at once is the overlap, measured by the server; a
	// wall-clock bound failed under the full suite's load (3.3s for 6x150ms).
	if peak := b.peak.Load(); peak != 3 {
		t.Fatalf("peak concurrent downloads %d, want 3", peak)
	}
	for _, s := range m.Prepare("llama/1", items, 3) {
		if s.Phase != PhaseReady {
			t.Fatalf("%s: %s %s", s.Ref, s.Phase, s.Message)
		}
	}
}

// An artifact is stored once by its digest: another Prewarm, or another
// generation, naming the same digest finds it without downloading it, and
// both names reach the same bytes.
func TestArtifactCacheIsContentAddressedAndIdempotent(t *testing.T) {
	b := newBlobs()
	srv := httptest.NewServer(b)
	defer srv.Close()
	body := []byte("weights")
	b.body["/w.bin"] = body
	m := newManager(t)
	first := Item{Ref: "artifact:w", URI: srv.URL + "/w.bin?X-Amz-Signature=secret", SHA256: digest(body), Path: "models/w.bin"}
	if s := statusOf(t, m, "a/1", []Item{first}); s[0].Phase != PhaseReady || s[0].Bytes != int64(len(body)) {
		t.Fatalf("first: %+v", s)
	}
	again := first
	again.Path = "other/w.bin"
	if s := statusOf(t, m, "b/1", []Item{again}); s[0].Phase != PhaseReady {
		t.Fatalf("second: %+v", s)
	}
	if s := statusOf(t, m, "a/2", []Item{first}); s[0].Phase != PhaseReady {
		t.Fatalf("new generation: %+v", s)
	}
	if n := b.gets["/w.bin"]; n != 1 {
		t.Fatalf("downloaded %d times, want once", n)
	}
	for _, name := range []string{"models/w.bin", "other/w.bin"} {
		got, err := os.ReadFile(filepath.Join(m.PathsDir(), name))
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s: %q %v", name, got, err)
		}
	}
	if _, artifacts := m.Summary(); len(artifacts) != 1 || artifacts[0] != digest(body) {
		t.Fatalf("summary %v", artifacts)
	}
}

// A zstd artifact is verified against the bytes published and stored
// decompressed.
func TestArtifactZstdVerifiedThenDecompressed(t *testing.T) {
	b := newBlobs()
	srv := httptest.NewServer(b)
	defer srv.Close()
	plain := bytes.Repeat([]byte("wheel "), 1000)
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	packed := enc.EncodeAll(plain, nil)
	b.body["/wheels.tar.zst"] = packed
	m := newManager(t)
	s := statusOf(t, m, "w/1", []Item{{Ref: "artifact:wheels", URI: srv.URL + "/wheels.tar.zst", SHA256: digest(packed), Path: "wheels.tar"}})
	if s[0].Phase != PhaseReady {
		t.Fatalf("%+v", s)
	}
	got, _ := os.ReadFile(filepath.Join(m.PathsDir(), "wheels.tar"))
	if !bytes.Equal(got, plain) {
		t.Fatalf("stored %d bytes, want the %d decompressed", len(got), len(plain))
	}
}

// Failures carry the destination's words: the server's status and body, a
// digest that does not match. A presigned query never appears.
func TestPrewarmErrorVerbatimInStatus(t *testing.T) {
	b := newBlobs()
	srv := httptest.NewServer(b)
	defer srv.Close()
	b.body["/ok"] = []byte("ok")
	b.failing["/denied"] = http.StatusForbidden
	m := newManager(t)
	m.Pull = func(_ context.Context, ref string) (bool, error) {
		return false, fmt.Errorf("ctr images pull %s: failed to resolve reference: unexpected status from HEAD request: 403 Forbidden", ref)
	}
	wrong := strings.Repeat("0", 64)
	s := statusOf(t, m, "e/1", []Item{
		{Ref: "artifact:denied", URI: srv.URL + "/denied?X-Amz-Signature=secret", SHA256: digest([]byte("x")), Path: "d"},
		{Ref: "artifact:bad", URI: srv.URL + "/ok", SHA256: wrong, Path: "b"},
		{Ref: "image:private", Image: "registry.example/private:1"},
	})
	want := map[string]string{
		"artifact:denied": "GET " + srv.URL + "/denied: 403 Forbidden: <Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>",
		"artifact:bad":    "sha256 mismatch for " + srv.URL + "/ok: got " + digest([]byte("ok")) + ", want " + wrong,
		"image:private":   "ctr images pull registry.example/private:1: failed to resolve reference: unexpected status from HEAD request: 403 Forbidden",
	}
	for _, st := range s {
		if st.Phase != PhaseFailed || st.Message != want[st.Ref] {
			t.Fatalf("%s: %s %q\nwant %q", st.Ref, st.Phase, st.Message, want[st.Ref])
		}
		if strings.Contains(st.Message, "secret") {
			t.Fatalf("%s leaks the query: %s", st.Ref, st.Message)
		}
	}
	if _, err := os.Stat(m.ArtifactPath(wrong)); err == nil {
		t.Fatal("a mismatched download was kept")
	}
}

// A URI the HTTP client cannot even parse fails with an error that quotes
// it whole, query and all: a presigned URL's signature reached the
// Prewarm's status.
func TestAnUnparsableURIKeepsItsQueryOutOfTheStatus(t *testing.T) {
	m := newManager(t)
	s := statusOf(t, m, "e/bad-uri", []Item{
		{Ref: "artifact:bad-uri", URI: "https://bucket.example/key?X-Amz-Signature=secret\x7f", SHA256: digest([]byte("x")), Path: "u"},
	})
	if len(s) != 1 || s[0].Phase != PhaseFailed {
		t.Fatalf("status = %+v, want one failed item", s)
	}
	if strings.Contains(s[0].Message, "secret") {
		t.Fatalf("the status leaks the URI's query: %s", s[0].Message)
	}
}

// Over the limit, the least recently used artifacts go first, and an
// artifact of a Prewarm not released is never evicted.
func TestLRUKeepsPinnedItems(t *testing.T) {
	b := newBlobs()
	srv := httptest.NewServer(b)
	defer srv.Close()
	clock := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	m := newManager(t)
	m.Now = func() time.Time { clock = clock.Add(time.Minute); return clock }
	item := func(name string) Item {
		body := bytes.Repeat([]byte(name), 100)
		b.body["/"+name] = body
		return Item{Ref: "artifact:" + name, URI: srv.URL + "/" + name, SHA256: digest(body), Path: name}
	}
	pinned, old, recent := item("p"), item("o"), item("r")
	statusOf(t, m, "keep/1", []Item{pinned})
	statusOf(t, m, "gone/1", []Item{old})
	m.Release("gone/1")
	for _, it := range []Item{pinned, old} {
		_ = os.Chtimes(m.ArtifactPath(it.SHA256), clock.Add(-time.Hour), clock.Add(-time.Hour))
	}
	m.MaxBytes = 200
	statusOf(t, m, "new/1", []Item{recent})
	if _, err := os.Stat(m.ArtifactPath(pinned.SHA256)); err != nil {
		t.Fatal("evicted a pinned artifact")
	}
	if _, err := os.Stat(m.ArtifactPath(old.SHA256)); err == nil {
		t.Fatal("kept the least recently used unpinned artifact over the limit")
	}
	if _, err := os.Stat(m.ArtifactPath(recent.SHA256)); err != nil {
		t.Fatal("evicted the artifact just warmed")
	}
}

// An artifact path cannot leave the cache's paths directory.
func TestArtifactPathConfined(t *testing.T) {
	m := newManager(t)
	s := statusOf(t, m, "x/1", []Item{{Ref: "artifact:x", URI: "http://127.0.0.1:1/x", SHA256: digest([]byte("x")), Path: "../../etc/cron.d/x"}})
	if s[0].Phase != PhaseFailed || s[0].Message != `artifact path "../../etc/cron.d/x" must be a relative path without ..` {
		t.Fatalf("%+v", s)
	}
}

// A download that failed transiently (the connection, a 5xx, a 429) is
// tried again; a 4xx is not, and after the last try the failure keeps the
// server's words, prefixed with the tries.
// E2E TestPrewarmArtifactThenJobReadsIt (a Service without endpoints yet
// refuses the first connection).
func TestPrewarmRetriesOnlyTransientFailures(t *testing.T) {
	b := newBlobs()
	srv := httptest.NewServer(b)
	defer srv.Close()
	body := []byte("weights")
	// Distinct bytes: the cache is content-addressed, so a retry of one
	// would find another's download.
	downBody, goneBody, lateBody := []byte("down weights"), []byte("gone weights"), []byte("late weights")
	b.body["/flaky"] = body
	b.body["/late"] = lateBody
	// A 408 is the server's timeout, asked again as by every other caller
	// (fault.TransientHTTP; R4 ronda 5, M3).
	b.failing["/late"] = http.StatusRequestTimeout
	b.body["/down"] = downBody
	b.body["/gone"] = goneBody
	b.failing["/flaky"] = http.StatusServiceUnavailable
	b.failing["/down"] = http.StatusBadGateway
	b.failing["/gone"] = http.StatusForbidden
	m := newManager(t)
	m.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		b.mu.Lock()
		for _, path := range []string{"/flaky", "/late"} {
			if r.URL.Path == path && b.gets[path] == 1 {
				delete(b.failing, path)
			}
		}
		b.mu.Unlock()
		return http.DefaultTransport.RoundTrip(r)
	})}
	s := statusOf(t, m, "r/1", []Item{
		{Ref: "artifact:flaky", URI: srv.URL + "/flaky", SHA256: digest(body), Path: "flaky"},
		{Ref: "artifact:down", URI: srv.URL + "/down", SHA256: digest(downBody), Path: "down"},
		{Ref: "artifact:gone", URI: srv.URL + "/gone", SHA256: digest(goneBody), Path: "gone"},
		{Ref: "artifact:late", URI: srv.URL + "/late", SHA256: digest(lateBody), Path: "late"},
		{Ref: "artifact:refused", URI: "http://127.0.0.1:1/x", SHA256: digest([]byte("refused")), Path: "refused"},
	})
	got := map[string]Status{}
	for _, st := range s {
		got[st.Ref] = st
	}
	if got["artifact:flaky"].Phase != PhaseReady || b.gets["/flaky"] != 2 {
		t.Fatalf("flaky: %+v after %d gets", got["artifact:flaky"], b.gets["/flaky"])
	}
	if got["artifact:late"].Phase != PhaseReady || b.gets["/late"] != 2 {
		t.Fatalf("a 408 was not tried again: %+v after %d gets", got["artifact:late"], b.gets["/late"])
	}
	down := "after 3 attempts: GET " + srv.URL + "/down: 502 Bad Gateway: <Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>"
	if got["artifact:down"].Message != down || b.gets["/down"] != 3 {
		t.Fatalf("down: %+v after %d gets", got["artifact:down"], b.gets["/down"])
	}
	if got["artifact:gone"].Phase != PhaseFailed || b.gets["/gone"] != 1 || strings.HasPrefix(got["artifact:gone"].Message, "after") {
		t.Fatalf("a 403 was tried again: %+v after %d gets", got["artifact:gone"], b.gets["/gone"])
	}
	if msg := got["artifact:refused"].Message; !strings.HasPrefix(msg, `after 3 attempts: GET http://127.0.0.1:1/x: Get "http://127.0.0.1:1/x": dial tcp`) {
		t.Fatalf("refused: %q", msg)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A download is cut at the cache's size, counted after decompression: an
// artifact larger than the whole cache could never be kept, and a zstd bomb
// filled the disk before eviction ran (12#8). Nothing of it stays.
func TestADownloadLargerThanTheCacheStopsAtItsSize(t *testing.T) {
	b := newBlobs()
	srv := httptest.NewServer(b)
	defer srv.Close()
	plain := bytes.Repeat([]byte("x"), 10_000)
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	packed := enc.EncodeAll(plain, nil)
	b.body["/big"] = plain
	b.body["/bomb.zst"] = packed
	m := newManager(t)
	m.MaxBytes = 1_000
	s := statusOf(t, m, "big/1", []Item{
		{Ref: "artifact:big", URI: srv.URL + "/big", SHA256: digest(plain), Path: "big"},
		{Ref: "artifact:bomb", URI: srv.URL + "/bomb.zst", SHA256: digest(packed), Path: "bomb"},
	})
	for _, st := range s {
		if st.Phase != PhaseFailed || !strings.Contains(st.Message, "larger than the cache") {
			t.Fatalf("%+v, want failed as larger than the cache", st)
		}
	}
	left, _ := os.ReadDir(filepath.Join(m.Root, "tmp"))
	if len(left) != 0 {
		t.Fatalf("partial downloads left: %v", left)
	}
}

// A released Prewarm's artifacts go down to the cache's size at once, not
// at the next run (12#8).
func TestReleaseEvictsDownToTheCachesSize(t *testing.T) {
	b := newBlobs()
	srv := httptest.NewServer(b)
	defer srv.Close()
	m := newManager(t)
	var items []Item
	for _, name := range []string{"a", "b"} {
		body := bytes.Repeat([]byte(name), 100)
		b.body["/"+name] = body
		items = append(items, Item{Ref: "artifact:" + name, URI: srv.URL + "/" + name, SHA256: digest(body), Path: name})
	}
	m.MaxBytes = 150
	statusOf(t, m, "both/1", items)
	if kept := len(m.entries()); kept != 2 {
		t.Fatalf("%d artifacts of a Prewarm not released, want both kept", kept)
	}
	m.Release("both/1")
	if kept := len(m.entries()); kept != 1 {
		t.Fatalf("%d artifacts after release, want 1 (150 bytes hold one of 100)", kept)
	}
}

// cutReader is a body the network cut mid-way.
type cutReader struct{ rest io.Reader }

func (c *cutReader) Read(p []byte) (int, error) {
	n, err := c.rest.Read(p)
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// A zstd artifact whose body the network cut mid-way is tried again, as a
// plain one is: the cut is the network's, not a broken artifact (R4 ronda 4,
// M5).
func TestAZstdDownloadCutMidwayIsTriedAgain(t *testing.T) {
	raw := bytes.Repeat([]byte("weights "), 4096)
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	packed := enc.EncodeAll(raw, nil)
	gets := 0
	m := newManager(t)
	m.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gets++
		var body io.Reader = bytes.NewReader(packed)
		if gets == 1 {
			body = &cutReader{bytes.NewReader(packed[:len(packed)/2])}
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(body), Request: r}, nil
	})}
	s := statusOf(t, m, "z/1", []Item{{Ref: "artifact:w", URI: "http://blobs.test/w.tar.zst", SHA256: digest(packed), Path: "w.tar"}})
	if s[0].Phase != PhaseReady || gets != 2 {
		t.Fatalf("%+v after %d gets, want Ready after a second", s[0], gets)
	}
}
