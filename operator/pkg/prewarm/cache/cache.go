// SPDX-License-Identifier: Apache-2.0

// Package cache is a host's warm cache: the images and artifacts a Prewarm
// puts on a machine before jobs need them. Artifacts are content addressed
// (sha256/<digest>) and named for jobs under paths/; images are the
// container runtime's own content store, reached through a puller.
//
// Prepare is idempotent: the first call with a key starts the work in the
// background and every call reports each item's progress. Errors are the
// destination's own words (a registry's 403, a server's body, a digest that
// does not match), with a presigned URL's query never repeated.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/urlredact"
)

// Phases of an item.
const (
	PhasePending = "Pending"
	PhaseRunning = "Running"
	PhaseReady   = "Ready"
	PhaseFailed  = "Failed"
)

// DefaultRoot is where a VM agent keeps its cache.
const DefaultRoot = "/var/lib/zenthros/cache"

// MaxSummary bounds the images and artifacts a host reports.
const MaxSummary = 64

// maxBodyInError bounds how much of a failed response's body is kept.
const maxBodyInError = 1024

// Item is one thing to warm. Exactly one of Image, URI and Toolchain is set.
type Item struct {
	Ref    string
	Image  string
	URI    string
	SHA256 string
	Path   string

	Toolchain         string
	ToolchainVersion  string
	ToolchainPackages []string
}

// Status is one item's progress.
type Status struct {
	Ref            string
	Phase          string
	Bytes          int64
	DurationMillis int64
	Message        string
}

// ImagePuller puts an image in the host's container runtime. pulled is false
// when the image was already there.
type ImagePuller func(ctx context.Context, ref string) (pulled bool, err error)

// ToolchainInstaller installs a host toolchain and its packages.
type ToolchainInstaller func(ctx context.Context, name, version string, packages []string) error

// Manager is a host's cache.
type Manager struct {
	Root string
	// MaxBytes bounds the artifacts kept; 0 keeps everything. Artifacts of
	// a Prewarm not released are never evicted.
	MaxBytes int64
	// Pull warms an image; nil refuses image items.
	Pull ImagePuller
	// Install installs a toolchain; nil refuses toolchain items.
	Install ToolchainInstaller
	HTTP    *http.Client
	Now     func() time.Time
	// Attempts bounds tries of a download that failed transiently (the
	// connection, a 5xx, a 429); 0 is 3. Backoff is the first wait between
	// them, doubled each time; 0 is 2s. A 4xx or a digest mismatch is never
	// tried again: it would fail the same way.
	Attempts int
	Backoff  time.Duration

	mu     sync.Mutex
	runs   map[string]*run
	images []string
}

type run struct {
	key    string
	items  []Item
	status []Status
	cancel context.CancelFunc
	done   chan struct{}
}

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Manager) client() *http.Client {
	if m.HTTP != nil {
		return m.HTTP
	}
	return http.DefaultClient
}

// name is the Prewarm a key belongs to: keys are "<name>/<generation>".
func name(key string) string {
	n, _, _ := strings.Cut(key, "/")
	return n
}

// Prepare starts key's items, or reports on them when key already runs. A
// new key for the same Prewarm replaces the old run.
func (m *Manager) Prepare(key string, items []Item, concurrency int) []Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runs == nil {
		m.runs = map[string]*run{}
	}
	if r, ok := m.runs[name(key)]; ok {
		if r.key == key {
			return append([]Status(nil), r.status...)
		}
		r.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &run{key: key, items: items, cancel: cancel, done: make(chan struct{})}
	for _, item := range items {
		r.status = append(r.status, Status{Ref: item.Ref, Phase: PhasePending})
	}
	m.runs[name(key)] = r
	go m.execute(ctx, r, min(max(concurrency, 1), 16))
	return append([]Status(nil), r.status...)
}

// Wait blocks until key's run finishes (tests).
func (m *Manager) Wait(key string) {
	m.mu.Lock()
	r, ok := m.runs[name(key)]
	m.mu.Unlock()
	if ok && r.key == key {
		<-r.done
	}
}

// Release stops key's work and unpins its artifacts, which the cache then
// evicts down to MaxBytes: a released artifact kept the disk until the next
// run (12#8).
func (m *Manager) Release(key string) {
	m.mu.Lock()
	if r, ok := m.runs[name(key)]; ok {
		r.cancel()
		delete(m.runs, name(key))
	}
	m.mu.Unlock()
	m.evict()
}

func (m *Manager) execute(ctx context.Context, r *run, concurrency int) {
	defer close(r.done)
	slots := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := range r.items {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				m.set(r, i, Status{Phase: PhaseFailed, Message: "prewarm replaced or released"})
				return
			}
			defer func() { <-slots }()
			start := m.now()
			m.set(r, i, Status{Phase: PhaseRunning})
			bytes, err := m.warmWithRetry(ctx, r.items[i])
			took := m.now().Sub(start).Milliseconds()
			if err != nil {
				m.set(r, i, Status{Phase: PhaseFailed, DurationMillis: took, Message: err.Error()})
				return
			}
			m.set(r, i, Status{Phase: PhaseReady, Bytes: bytes, DurationMillis: took})
		}(i)
	}
	wg.Wait()
	m.evict()
}

// transient is a failure worth another try: the connection, or a server
// that said to come back (5xx, 429). Its text is the cause's.
type transient struct{ err error }

// bodyReader keeps the error reading an answer's body met: a cut there is
// the network's, where an error of the decompressor alone is the artifact's.
type bodyReader struct {
	r   io.Reader
	err error
}

func (b *bodyReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		b.err = err
	}
	return n, err
}

func (t transient) Error() string { return t.err.Error() }
func (t transient) Unwrap() error { return t.err }

// warmWithRetry warms an item, trying a transient failure again with
// backoff. The last failure keeps its own words, prefixed with how many
// tries it took.
func (m *Manager) warmWithRetry(ctx context.Context, item Item) (int64, error) {
	attempts, wait := m.Attempts, m.Backoff
	if attempts <= 0 {
		attempts = 3
	}
	if wait <= 0 {
		wait = 2 * time.Second
	}
	for try := 1; ; try++ {
		bytes, err := m.warm(ctx, item)
		var t transient
		if err == nil || !errors.As(err, &t) {
			return bytes, err
		}
		if try == attempts {
			return 0, fmt.Errorf("after %d attempts: %w", attempts, err)
		}
		select {
		case <-ctx.Done():
			return 0, err
		case <-time.After(wait):
		}
		wait *= 2
	}
}

func (m *Manager) set(r *run, i int, s Status) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.Ref = r.status[i].Ref
	r.status[i] = s
}

func (m *Manager) warm(ctx context.Context, item Item) (int64, error) {
	switch {
	case item.Image != "":
		if m.Pull == nil {
			return 0, errors.New("this backend has no container runtime to warm images in")
		}
		if _, err := m.Pull(ctx, item.Image); err != nil {
			return 0, err
		}
		m.NoteImage(item.Image)
		return 0, nil
	case item.URI != "":
		return m.fetch(ctx, item)
	case item.Toolchain != "":
		if m.Install == nil {
			return 0, errors.New("this backend cannot install host toolchains; put them in the job's image")
		}
		return 0, m.Install(ctx, item.Toolchain, item.ToolchainVersion, item.ToolchainPackages)
	default:
		return 0, fmt.Errorf("item %s has nothing to warm", item.Ref)
	}
}

// NoteImage records an image as the most recently used (a job pulled it, or
// a Prewarm did).
func (m *Manager) NoteImage(ref string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.images[:0]
	for _, image := range m.images {
		if image != ref {
			kept = append(kept, image)
		}
	}
	m.images = append(kept, ref)
	if len(m.images) > MaxSummary {
		m.images = m.images[len(m.images)-MaxSummary:]
	}
}

// ForgetImage drops an image the machine removed from what it reports warm.
func (m *Manager) ForgetImage(ref string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.images[:0]
	for _, image := range m.images {
		if image != ref {
			kept = append(kept, image)
		}
	}
	m.images = kept
}

// ArtifactPath is where the artifact with digest lives.
func (m *Manager) ArtifactPath(digest string) string {
	return filepath.Join(m.Root, "sha256", digest)
}

// PathsDir is where jobs find artifacts by the names Prewarms gave them.
func (m *Manager) PathsDir() string {
	return filepath.Join(m.Root, "paths")
}

// linkPath is the job-facing name for an artifact, confined to PathsDir.
func (m *Manager) linkPath(path string) (string, error) {
	clean := filepath.Clean("/" + path)
	if path == "" || clean == "/" || strings.Contains(path, "..") {
		return "", fmt.Errorf("artifact path %q must be a relative path without ..", path)
	}
	return filepath.Join(m.PathsDir(), clean), nil
}

func (m *Manager) fetch(ctx context.Context, item Item) (int64, error) {
	if !digestPattern.MatchString(item.SHA256) {
		return 0, fmt.Errorf("artifact sha256 %q is not 64 lowercase hex characters", item.SHA256)
	}
	link, err := m.linkPath(item.Path)
	if err != nil {
		return 0, err
	}
	final := m.ArtifactPath(item.SHA256)
	if info, err := os.Stat(final); err == nil {
		now := m.now()
		_ = os.Chtimes(final, now, now)
		return info.Size(), m.link(final, link)
	}
	if err := os.MkdirAll(filepath.Join(m.Root, "tmp"), 0o755); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, item.URI, nil)
	if err != nil {
		return 0, fmt.Errorf("GET %s: %w", urlredact.URL(item.URI), urlredact.Error(err))
	}
	resp, err := m.client().Do(req)
	if err != nil {
		return 0, transient{fmt.Errorf("GET %s: %w", urlredact.URL(item.URI), urlredact.Error(err))}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyInError))
		err := fmt.Errorf("GET %s: %s: %s", urlredact.URL(item.URI), resp.Status, strings.TrimSpace(string(body)))
		if fault.TransientHTTP(resp.StatusCode) {
			return 0, transient{err}
		}
		return 0, err
	}
	tmp, err := os.CreateTemp(filepath.Join(m.Root, "tmp"), item.SHA256+"-*")
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmp.Name())
	hash := sha256.New()
	body := &bodyReader{r: resp.Body}
	raw := io.TeeReader(body, hash)
	out := m.capped(tmp)
	if strings.HasSuffix(urlredact.URL(item.URI), ".zst") {
		dec, err := zstd.NewReader(raw)
		if err != nil {
			tmp.Close()
			return 0, err
		}
		_, err = io.Copy(out, dec)
		dec.Close()
		if err != nil {
			tmp.Close()
			err = fmt.Errorf("decompress %s: %w", urlredact.URL(item.URI), err)
			if body.err != nil && !errors.Is(err, errTooLarge) {
				// The body was cut: the network's, as for a plain one.
				return 0, transient{err}
			}
			return 0, err
		}
		// Hash what the zstd reader left unread (the frame's tail).
		_, _ = io.Copy(io.Discard, raw)
	} else if _, err := io.Copy(out, raw); err != nil {
		tmp.Close()
		if errors.Is(err, errTooLarge) {
			return 0, fmt.Errorf("GET %s: %w", urlredact.URL(item.URI), err)
		}
		return 0, transient{fmt.Errorf("GET %s: %w", urlredact.URL(item.URI), err)}
	}
	if err := tmp.Close(); err != nil {
		return 0, err
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != item.SHA256 {
		return 0, fmt.Errorf("sha256 mismatch for %s: got %s, want %s", urlredact.URL(item.URI), got, item.SHA256)
	}
	if err := os.Chmod(tmp.Name(), 0o444); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		return 0, err
	}
	info, err := os.Stat(final)
	if err != nil {
		return 0, err
	}
	return info.Size(), m.link(final, link)
}

// link names target at link, replacing whatever the name pointed to.
func (m *Manager) link(target, link string) error {
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		return err
	}
	tmp := link + ".new"
	_ = os.Remove(tmp)
	if err := os.Symlink(target, tmp); err != nil {
		return err
	}
	return os.Rename(tmp, link)
}

// pinned are the digests of every run not released.
func (m *Manager) pinned() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]bool{}
	for _, r := range m.runs {
		for _, item := range r.items {
			if item.SHA256 != "" {
				out[item.SHA256] = true
			}
		}
	}
	return out
}

type entry struct {
	digest string
	size   int64
	used   time.Time
}

func (m *Manager) entries() []entry {
	dir := filepath.Join(m.Root, "sha256")
	list, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []entry
	for _, e := range list {
		if !digestPattern.MatchString(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, entry{digest: e.Name(), size: info.Size(), used: info.ModTime()})
	}
	return out
}

// errTooLarge is an artifact larger than the whole cache: it could never
// be kept, and writing it would fill the disk first (12#8).
var errTooLarge = errors.New("artifact is larger than the cache's MaxBytes")

// capped bounds what one download writes to MaxBytes (none when 0), counted
// after decompression.
func (m *Manager) capped(w io.Writer) io.Writer {
	if m.MaxBytes <= 0 {
		return w
	}
	return &capWriter{w: w, left: m.MaxBytes}
}

type capWriter struct {
	w    io.Writer
	left int64
}

func (c *capWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > c.left {
		return 0, errTooLarge
	}
	c.left -= int64(len(p))
	return c.w.Write(p)
}

// evict removes the least recently used artifacts, none pinned, until the
// cache fits MaxBytes.
func (m *Manager) evict() {
	if m.MaxBytes <= 0 {
		return
	}
	entries := m.entries()
	var total int64
	for _, e := range entries {
		total += e.size
	}
	sort.Slice(entries, func(a, b int) bool { return entries[a].used.Before(entries[b].used) })
	pinned := m.pinned()
	for _, e := range entries {
		if total <= m.MaxBytes {
			return
		}
		if pinned[e.digest] {
			continue
		}
		if os.Remove(m.ArtifactPath(e.digest)) == nil {
			total -= e.size
		}
	}
}

// Summary is what the host reports for scheduling: its most recently used
// images and artifact digests, most recent first, at most MaxSummary each.
func (m *Manager) Summary() (images, artifacts []string) {
	m.mu.Lock()
	for i := len(m.images) - 1; i >= 0; i-- {
		images = append(images, m.images[i])
	}
	m.mu.Unlock()
	entries := m.entries()
	sort.Slice(entries, func(a, b int) bool {
		if entries[a].used.Equal(entries[b].used) {
			return entries[a].digest < entries[b].digest
		}
		return entries[a].used.After(entries[b].used)
	})
	for i, e := range entries {
		if i == MaxSummary {
			break
		}
		artifacts = append(artifacts, e.digest)
	}
	return images, artifacts
}
