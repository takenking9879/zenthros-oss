// SPDX-License-Identifier: Apache-2.0

package logpersist

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/klauspost/compress/zstd"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/objectput"
)

// fakeLog is a job's log file as an executor reads it: a cursor is the
// byte after a chunk, a part below floor is freed, and a follow read ends
// once the job ended and its last byte is sent.
type fakeLog struct {
	mu    sync.Mutex
	data  []byte
	floor int64
	ended bool
	grew  chan struct{}
	// breakAfter closes the next follow read after that many bytes, and
	// frees the file up to freeOnBreak then.
	breakAfter  int
	freeOnBreak int64
	// refuse is how many reads fail before one opens (a pod not there
	// yet); refuseRest fails every read that does not follow (its pod gone).
	refuse     int
	refuseRest bool
	reads      []backend.LogRequest
}

func newFakeLog() *fakeLog { return &fakeLog{grew: make(chan struct{})} }

func (f *fakeLog) write(b []byte) {
	f.mu.Lock()
	f.data = append(f.data, b...)
	close(f.grew)
	f.grew = make(chan struct{})
	f.mu.Unlock()
}

func (f *fakeLog) end() {
	f.mu.Lock()
	f.ended = true
	close(f.grew)
	f.grew = make(chan struct{})
	f.mu.Unlock()
}

func (f *fakeLog) free(to int64) {
	f.mu.Lock()
	f.floor = to
	f.mu.Unlock()
}

func (f *fakeLog) Ended(context.Context, string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ended
}

func (f *fakeLog) Logs(ctx context.Context, _ string, req backend.LogRequest) (<-chan backend.LogChunk, error) {
	f.mu.Lock()
	f.reads = append(f.reads, req)
	switch {
	case f.refuse > 0:
		f.refuse--
		f.mu.Unlock()
		return nil, errors.New("no pods yet")
	case f.refuseRest && !req.Follow:
		f.mu.Unlock()
		return nil, errors.New("the pod is gone")
	}
	var pos, omitted, gap int64
	switch {
	case req.After != "":
		pos, _ = strconv.ParseInt(req.After, 10, 64)
		if pos < f.floor {
			gap, pos = f.floor-pos, f.floor
		}
	case req.TailLines == 0:
		pos = int64(len(f.data))
	default:
		omitted, pos = f.floor, f.floor
	}
	cut := f.breakAfter
	if req.Follow {
		f.breakAfter = 0
	}
	f.mu.Unlock()
	out := make(chan backend.LogChunk)
	go func() {
		defer close(out)
		sent := 0
		first := true
		for {
			f.mu.Lock()
			data, ended, grew := f.data, f.ended, f.grew
			f.mu.Unlock()
			if int64(len(data)) > pos {
				n := min(int64(len(data))-pos, 8<<10)
				chunk := backend.LogChunk{Data: append([]byte(nil), data[pos:pos+n]...), Cursor: strconv.FormatInt(pos+n, 10)}
				if first {
					chunk.Omitted, chunk.Gap, first = omitted, gap, false
				}
				select {
				case out <- chunk:
				case <-ctx.Done():
					return
				}
				pos += n
				sent += int(n)
				if req.Follow && cut > 0 && sent >= cut {
					f.mu.Lock()
					f.floor = max(f.floor, f.freeOnBreak)
					f.mu.Unlock()
					return
				}
				continue
			}
			if !req.Follow || ended {
				return
			}
			select {
			case <-grew:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// fakeStore is a bucket: an object is written once, and fail says what a
// put gets before it is tried.
type fakeStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	order   []string
	fail    func(key string) error
	// keepThenFail keeps the object and still answers this error: a
	// write the agent never heard back from.
	keepThenFail func(key string) error
}

func newFakeStore() *fakeStore { return &fakeStore{objects: map[string][]byte{}} }

func (f *fakeStore) Key(parts ...string) string {
	return strings.Join(append([]string{"logs"}, parts...), "/")
}

func (f *fakeStore) Put(_ context.Context, key, _ string, body []byte) error {
	f.mu.Lock()
	fail, keep := f.fail, f.keepThenFail
	f.mu.Unlock()
	if fail != nil {
		if err := fail(key); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if have, ok := f.objects[key]; ok {
		if !bytes.Equal(have, body) {
			return objectput.ErrDifferent
		}
		return nil
	}
	f.objects[key] = append([]byte(nil), body...)
	f.order = append(f.order, key)
	if keep != nil {
		return keep(key)
	}
	return nil
}

// List and Get read the bucket back (Find, Read): an object's time is when
// it came, in the order they came.
func (f *fakeStore) List(_ context.Context, prefix string) ([]objectput.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var listed []objectput.Object
	for i, key := range f.order {
		if _, ok := f.objects[key]; ok && strings.HasPrefix(key, prefix) {
			listed = append(listed, objectput.Object{Key: key, Modified: time.Unix(int64(i), 0)})
		}
	}
	return listed, nil
}

func (f *fakeStore) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[key]
	if !ok {
		return nil, fault.Wrap(fault.Absent, "get", fmt.Errorf("%s: no such key", key))
	}
	return append([]byte(nil), object...), nil
}

func (f *fakeStore) setFail(fail func(string) error) {
	f.mu.Lock()
	f.fail = fail
	f.mu.Unlock()
}

// end is the job's END.json, once it is in.
func (f *fakeStore) end(job Job) (End, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, ok := f.objects[f.Key(job.Prefix(), endName)]
	var end End
	if ok {
		_ = json.Unmarshal(raw, &end)
	}
	return end, ok
}

// text is the log the bucket holds: its objects in order, unpacked.
func (f *fakeStore) text(t *testing.T, job Job) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	var keys []string
	for key := range f.objects {
		if strings.HasSuffix(key, objectSuffix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var out []byte
	for i, key := range keys {
		if want := f.Key(job.Prefix(), ObjectName(int64(i))); key != want {
			t.Fatalf("object %d is %s, want %s", i, key, want)
		}
		raw, err := dec.DecodeAll(f.objects[key], nil)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		out = append(out, raw...)
	}
	return string(out)
}

// Each object ends with a skippable frame that says where the log goes on
// after it (the cursor a reader asks the machine after), how much of the
// log the objects hold through it, and the SHA-256 of the frames before
// it: logs(to=) checks each object and completes from the machine. Any
// zstd reader skips it.
func TestEachObjectSaysWhereTheLogGoesOn(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		src.write(lines(i*100, 100))
		time.Sleep(5 * time.Millisecond)
	}
	src.end()
	end := waitEnd(t, store, job)
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	var through int64
	for i := int64(0); i < end.Objects; i++ {
		store.mu.Lock()
		object := store.objects[store.Key(job.Prefix(), ObjectName(i))]
		store.mu.Unlock()
		frames, tail, err := ReadTrailer(object)
		if err != nil {
			t.Fatalf("object %d: %v", i, err)
		}
		sum := sha256.Sum256(frames)
		raw, err := dec.DecodeAll(object, nil)
		if err != nil {
			t.Fatalf("object %d with its trailer: %v", i, err)
		}
		through += int64(len(raw))
		// The fake's cursor is the byte after a chunk.
		if tail.SHA256 != hex.EncodeToString(sum[:]) || tail.Bytes != through || tail.Cursor != strconv.FormatInt(through, 10) {
			t.Fatalf("object %d says %+v; it holds %d bytes through it and its frames' SHA-256 is %x", i, tail, through, sum)
		}
	}
	if _, _, err := ReadTrailer([]byte("not an object")); err == nil {
		t.Fatal("an object without its trailer is taken")
	}
}

// A handle is one segment of its key, whatever it holds: a pod's
// namespace/name stays under its attempt, where logs(to=) looks.
func TestAHandleIsOneSegmentOfItsKey(t *testing.T) {
	if got := testJob("job:ns/pod").Prefix(); got != "train/u-1/g3-a1/job:ns%2Fpod" {
		t.Fatalf("prefix %q", got)
	}
}

func testJob(handle string) Job {
	return Job{ID: "j1", Name: "train", UID: "u-1", Generation: 3, Attempt: 1, Handle: handle,
		Logs: v1.LogsSpec{Destination: "s3://b/logs"}}
}

// newTestSaver is a Saver with small objects and short waits.
func newTestSaver(t *testing.T, dir string, src Source, store Store) *Saver {
	t.Helper()
	s, err := New(Config{Dir: dir, Source: src, Logger: logr.Discard(),
		Open: func(context.Context, Job) (Store, error) { return store, nil }})
	if err != nil {
		t.Fatal(err)
	}
	s.pieceBytes, s.objectBytes, s.objectAge, s.hurriedAge = 4<<10, 16<<10, 200*time.Millisecond, 50*time.Millisecond
	s.reopen = 10 * time.Millisecond
	s.backoff = func(int) time.Duration { return 5 * time.Millisecond }
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s.Drain(ctx)
	})
	return s
}

// lines is n numbered lines from from.
func lines(from, n int) []byte {
	var b bytes.Buffer
	for i := from; i < from+n; i++ {
		fmt.Fprintf(&b, "epoch %06d loss=%.6f some words to make it longer\n", i, 1/float64(i+1))
	}
	return b.Bytes()
}

func waitEnd(t *testing.T, store *fakeStore, job Job) End {
	t.Helper()
	var end End
	testutil.Eventually(t, "END.json", func() bool {
		var ok bool
		end, ok = store.end(job)
		return ok
	})
	return end
}

// A log is saved whole, in order, as the job writes it, and END.json comes
// last, once every object is in: complete. The queue is gone after it, and
// the file is no longer held for the bucket.
func TestALogIsSavedWholeAndThenItsEnd(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	job := testJob("h1")
	src.write(lines(0, 100))
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	if !Saving(job.Handle) {
		t.Fatal("the log's file is not held while it is read for its bucket")
	}
	var want []byte
	want = append(want, lines(0, 100)...)
	for i := 1; i < 40; i++ {
		chunk := lines(i*100, 100)
		want = append(want, chunk...)
		src.write(chunk)
		time.Sleep(5 * time.Millisecond)
	}
	src.end()
	end := waitEnd(t, store, job)
	if got := store.text(t, job); got != string(want) {
		t.Fatalf("the bucket holds %d bytes, want %d", len(got), len(want))
	}
	if end.Status != StatusComplete || end.Bytes != int64(len(want)) || end.Objects < 2 {
		t.Fatalf("END.json %+v", end)
	}
	store.mu.Lock()
	last := store.order[len(store.order)-1]
	store.mu.Unlock()
	if !strings.HasSuffix(last, endName) {
		t.Fatalf("the last object written is %s: END.json goes after every object", last)
	}
	testutil.Eventually(t, "the queue gone and the file let go", func() bool {
		_, err := os.Stat(filepath.Join(s.cfg.Dir, job.Handle))
		return os.IsNotExist(err) && !Saving(job.Handle)
	})
	if p := s.Problem(job.Handle); p != nil {
		t.Fatalf("a complete log has a problem: %+v", p)
	}
}

// A bucket that is down holds nothing back: the log is read on at its own
// pace into the queue, and once the bucket is back it gets all of it.
func TestABucketThatIsDownHoldsNothingBack(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	down := errors.New("connection refused")
	store.setFail(func(string) error { return down })
	s := newTestSaver(t, t.TempDir(), src, store)
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	var want []byte
	for i := range 20 {
		chunk := lines(i*200, 200)
		want = append(want, chunk...)
		src.write(chunk)
	}
	src.end()
	// The whole log read and sealed while the bucket refuses.
	testutil.Eventually(t, "the log read to its end into the queue", func() bool {
		j := s.jobs[job.Handle]
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.st.Ended && j.st.Saved == int64(len(want))
	})
	if p := s.Problem(job.Handle); p != nil {
		t.Fatalf("a bucket that is down for now is reported as %+v", p)
	}
	store.setFail(nil)
	if end := waitEnd(t, store, job); end.Status != StatusComplete {
		t.Fatalf("END.json %+v", end)
	}
	if got := store.text(t, job); got != string(want) {
		t.Fatalf("the bucket holds %d bytes, want %d", len(got), len(want))
	}
}

// A queue that fills stops the reader, which says so; it goes on from where
// it was once there is room, and what the file freed meanwhile is a gap
// that END.json and the reason say. Nothing is read twice.
func TestAFullQueueStopsTheReaderAndWhatWasFreedIsAGap(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	store.setFail(func(string) error { return errors.New("connection refused") })
	s := newTestSaver(t, t.TempDir(), src, store)
	s.jobQueue = 40 << 10
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	first := lines(0, 20000) // ~1 MiB, ~4x the queue packed
	src.write(first)
	testutil.Eventually(t, "the reader stopped by a full queue", func() bool {
		p := s.Problem(job.Handle)
		return p != nil && p.Code == v1.ReasonLogsIncomplete && strings.Contains(p.Detail, "wait on the machine")
	})
	j := s.jobs[job.Handle]
	j.mu.Lock()
	read := j.st.Saved
	j.mu.Unlock()
	if read >= int64(len(first)) {
		t.Fatalf("the reader read all %d bytes past a full queue", read)
	}
	// The file frees past where the reader stopped, then the bucket comes back.
	freed := read + 100000
	src.free(freed)
	src.end()
	store.setFail(nil)
	end := waitEnd(t, store, job)
	got := store.text(t, job)
	if end.Status != StatusGap || end.Gap <= 0 {
		t.Fatalf("END.json %+v after a part was freed", end)
	}
	want := string(first[:len(got)-len(first[freed:])]) + string(first[freed:])
	if got != want || int64(len(got))+end.Gap != int64(len(first)) {
		t.Fatalf("the bucket holds %d bytes and a gap of %d of %d", len(got), end.Gap, len(first))
	}
	if p := s.Problem(job.Handle); p == nil || p.Code != v1.ReasonLogsIncomplete {
		t.Fatalf("a log with a gap: %+v", p)
	}
}

// A credential the bucket refuses ends the saving at the first object, with
// LOGS_NOT_SAVED: no END.json will come, and the queue goes.
func TestARefusedCredentialStopsTheSavingForGood(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	puts := 0
	store.setFail(func(string) error {
		puts++
		return fault.FromHTTP(403, true, errors.New("AccessDenied"))
	})
	dir := t.TempDir()
	s := newTestSaver(t, dir, src, store)
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	src.write(lines(0, 10))
	started := time.Now()
	testutil.Eventually(t, "LOGS_NOT_SAVED", func() bool {
		p := s.Problem(job.Handle)
		return p != nil && p.Code == v1.ReasonLogsNotSaved && strings.Contains(p.Detail, "AccessDenied")
	})
	if waited := time.Since(started); waited > time.Second {
		t.Fatalf("the refusal took %s: it shows with the first object (objectAge)", waited)
	}
	src.write(lines(10, 1000))
	src.end()
	time.Sleep(300 * time.Millisecond)
	if _, ok := store.end(job); ok || puts != 1 {
		t.Fatalf("puts %d after a refusal, END.json %v", puts, ok)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, job.Handle))
	for _, e := range entries {
		if e.Name() != stateName {
			t.Fatalf("%s left in the queue of a refused log", e.Name())
		}
	}
}

// A restarted agent sends what the last one sealed, an object it sent
// without hearing back among them, and reads on from right after its last
// object: every byte once. An object written after its last state is
// dropped and read again.
func TestARestartedAgentSendsWhatWasSealedAndReadsOn(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	dir := t.TempDir()
	unheard := errors.New("connection reset")
	store.mu.Lock()
	store.keepThenFail = func(key string) error {
		if strings.HasSuffix(key, ObjectName(1)) {
			return unheard
		}
		return nil
	}
	store.mu.Unlock()
	store.setFail(func(key string) error {
		if strings.HasSuffix(key, ObjectName(2)) {
			return errors.New("connection refused")
		}
		return nil
	})
	first := newTestSaver(t, dir, src, store)
	job := testJob("ns/pod")
	if err := first.Start(job); err != nil {
		t.Fatal(err)
	}
	var want []byte
	// Each write an object of its own: sealed by its age.
	for i := range 6 {
		chunk := lines(i*300, 300)
		want = append(want, chunk...)
		src.write(chunk)
		time.Sleep(250 * time.Millisecond)
	}
	testutil.Eventually(t, "objects sealed past the one the bucket refuses", func() bool {
		j := first.jobs[job.Handle]
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.st.Next > 3
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	first.Drain(ctx)
	cancel()
	// An object a crash left after the last state.
	queue := filepath.Join(dir, "ns%2Fpod")
	if err := os.WriteFile(filepath.Join(queue, ObjectName(99)), []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.keepThenFail = nil
	store.mu.Unlock()
	store.setFail(nil)
	for i := 6; i < 11; i++ {
		chunk := lines(i*300, 300)
		want = append(want, chunk...)
		src.write(chunk)
	}
	src.end()
	second := newTestSaver(t, dir, src, store)
	second.Resume()
	if end := waitEnd(t, store, job); end.Status != StatusComplete || end.Bytes != int64(len(want)) {
		t.Fatalf("END.json %+v, want complete with %d bytes", end, len(want))
	}
	if got := store.text(t, job); got != string(want) {
		t.Fatalf("the bucket holds %d bytes, want %d once each", len(got), len(want))
	}
	src.mu.Lock()
	last := src.reads[len(src.reads)-1]
	src.mu.Unlock()
	if last.After == "" {
		t.Fatalf("the restarted agent read from %+v, not after its last object", last)
	}
}

// END.json says complete only when nothing was lost: a part freed before
// the reader got to it is a gap, counted.
func TestEndIsCompleteOnlyWithNothingLost(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	src.write(lines(0, 1000))
	src.free(8192)
	src.end()
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	end := waitEnd(t, store, job)
	if end.Status != StatusGap || end.Gap != 8192 {
		t.Fatalf("END.json %+v, want a gap of 8192", end)
	}
	if got := store.text(t, job); got != string(lines(0, 1000)[8192:]) {
		t.Fatalf("the bucket holds %d bytes", len(got))
	}
}

// A log past logs.maxBytes is saved to it, and END.json and the reason say
// the rest is not.
func TestALogPastItsMaxIsCutAndSaysSo(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	limit := int64(1 << 20)
	job := testJob("h1")
	job.Logs.MaxBytes = &limit
	log := lines(0, 40000)
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	src.write(log)
	end := waitEnd(t, store, job)
	if end.Status != StatusTruncated || end.Bytes != limit {
		t.Fatalf("END.json %+v", end)
	}
	if got := store.text(t, job); got != string(log[:limit]) {
		t.Fatalf("the bucket holds %d bytes, want the first %d", len(got), limit)
	}
	if p := s.Problem(job.Handle); p == nil || p.Code != v1.ReasonLogsIncomplete || !strings.Contains(p.Detail, "maxBytes") {
		t.Fatalf("reason %+v", p)
	}
}

// The end is the executor's word: a read that broke just before it leaves
// the rest of the log, which is read once more, to its end.
func TestAReadThatBrokeBeforeTheEndIsReadToIt(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	log := lines(0, 3000)
	src.write(log)
	src.breakAfter = 40 << 10
	src.end()
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	if end := waitEnd(t, store, job); end.Status != StatusComplete {
		t.Fatalf("END.json %+v", end)
	}
	if got := store.text(t, job); got != string(log) {
		t.Fatalf("the bucket holds %d bytes, want %d", len(got), len(log))
	}
}

// A Spot notice seals the open object now, and then every 2 s.
func TestASpotNoticeSealsNow(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	s.objectAge = time.Hour
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	src.write(lines(0, 5))
	time.Sleep(100 * time.Millisecond)
	s.Hurry()
	key := store.Key(job.Prefix(), ObjectName(0))
	testutil.Eventually(t, "the first object, sealed by the notice", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		_, ok := store.objects[key]
		return ok
	})
	src.write(lines(5, 5))
	testutil.Eventually(t, "the next one, sealed within the hurried window", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		_, ok := store.objects[store.Key(job.Prefix(), ObjectName(1))]
		return ok
	})
}

// objects is each of the run's objects in the bucket: its log, unpacked,
// and its trailer.
func (f *fakeStore) objectsOf(t *testing.T, job Job, n int64) ([]string, []Trailer) {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	var texts []string
	var trailers []Trailer
	for i := range n {
		f.mu.Lock()
		object := f.objects[f.Key(job.Prefix(), ObjectName(i))]
		f.mu.Unlock()
		frames, said, err := ReadTrailer(object)
		if err != nil {
			t.Fatalf("object %d: %v", i, err)
		}
		raw, err := dec.DecodeAll(frames, nil)
		if err != nil {
			t.Fatalf("object %d: %v", i, err)
		}
		texts, trailers = append(texts, string(raw)), append(trailers, said)
	}
	return texts, trailers
}

// END.json goes after every object, the last one too: the log's end is
// recorded with its last object, so an uploader that looks between the two
// never writes END.json without it (it removed the queue, and the object
// with it, while END.json said complete).
func TestEndNeverGoesBeforeTheLastObject(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	s.objectAge = time.Hour // one object, sealed at the end
	job := testJob("h1")
	early, looked := false, make(chan struct{})
	s.atEnd = func() {
		// The uploader woken as the end is known, before the last object
		// is sealed: it finds nothing to end.
		defer close(looked)
		s.mu.Lock()
		j := s.jobs[job.Handle]
		s.mu.Unlock()
		signal(j.sealed)
		time.Sleep(200 * time.Millisecond)
		_, early = store.end(job)
	}
	log := lines(0, 100)
	src.write(log)
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	src.end()
	end := waitEnd(t, store, job)
	<-looked
	if early {
		t.Fatal("END.json went before the log's last object")
	}
	if end.Status != StatusComplete || end.Objects != 1 || store.text(t, job) != string(log) {
		t.Fatalf("END.json %+v over %d bytes, want complete over the %d of the log", end, len(store.text(t, job)), len(log))
	}
}

// A log that cannot be read yet (a pod not started: "no pods yet") is saved
// from its first byte once it can be: the first read that gives a chunk
// opens it, not the first one asked for, which went on from then.
func TestALogNotThereYetIsSavedFromItsFirstByte(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	src.refuse = 2
	s := newTestSaver(t, t.TempDir(), src, store)
	job := testJob("h1")
	log := lines(0, 100)
	src.write(log)
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, "the log read once it could be", func() bool {
		s.mu.Lock()
		j := s.jobs[job.Handle]
		s.mu.Unlock()
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.st.Saved == int64(len(log))
	})
	src.end()
	if end := waitEnd(t, store, job); end.Status != StatusComplete || store.text(t, job) != string(log) {
		t.Fatalf("END.json %+v over %d bytes, want complete over the %d of the log", end, len(store.text(t, job)), len(log))
	}
}

// The rest of a read that broke just before the job's end, when it cannot
// be read again (its pod gone), is a part missing: END.json never says
// complete without it.
func TestAnEndThatCannotBeReadAgainIsAGap(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	log := lines(0, 3000)
	src.write(log)
	src.breakAfter = 40 << 10
	src.refuseRest = true
	src.end()
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	end := waitEnd(t, store, job)
	if end.Status != StatusGap || end.Gap != backend.Uncounted {
		t.Fatalf("END.json %+v after the end could not be read, want a gap no one counted", end)
	}
	if p := s.Problem(job.Handle); p == nil || p.Code != v1.ReasonLogsIncomplete {
		t.Fatalf("reason %+v", p)
	}
}

// A part missing starts an object, whose trailer counts it with the parts
// missing before it: a reader of the bucket says it where it is.
func TestAPartMissingStartsAnObjectThatSaysIt(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	s := newTestSaver(t, t.TempDir(), src, store)
	log := lines(0, 3000)
	src.write(log)
	// The read breaks after 40 KiB (5 chunks of 8 KiB), and the file
	// frees up to 60 KiB before it is opened again.
	src.breakAfter, src.freeOnBreak = 40<<10, 60<<10
	src.end()
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	end := waitEnd(t, store, job)
	if end.Status != StatusGap || end.Gap != 20<<10 {
		t.Fatalf("END.json %+v, want a gap of %d", end, 20<<10)
	}
	texts, trailers := store.objectsOf(t, job, end.Objects)
	var before strings.Builder
	for i, said := range trailers {
		if said.Gap == 0 {
			before.WriteString(texts[i])
			continue
		}
		if before.String() != string(log[:40<<10]) || !strings.HasPrefix(texts[i], string(log[60<<10:60<<10+100])) {
			t.Fatalf("object %d says the gap after %d bytes; it starts %q", i, before.Len(), texts[i][:40])
		}
		for _, later := range trailers[i:] {
			if later.Gap != 20<<10 {
				t.Fatalf("the trailers after the gap say %+v", later)
			}
		}
		return
	}
	t.Fatal("no object says the gap")
}

// A log its bucket refused keeps only its reason across an agent's
// restart, until the job's Cancel forgets it: then its state goes, and the
// agent holds nothing of it (it was resumed, and kept, after every restart).
func TestARefusedLogIsForgottenAfterARestart(t *testing.T) {
	src, store := newFakeLog(), newFakeStore()
	store.setFail(func(string) error { return fault.FromHTTP(403, true, errors.New("AccessDenied")) })
	dir := t.TempDir()
	first := newTestSaver(t, dir, src, store)
	job := testJob("h1")
	if err := first.Start(job); err != nil {
		t.Fatal(err)
	}
	src.write(lines(0, 10))
	testutil.Eventually(t, "LOGS_NOT_SAVED", func() bool {
		p := first.Problem(job.Handle)
		return p != nil && p.Code == v1.ReasonLogsNotSaved
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	first.Drain(ctx)

	second := newTestSaver(t, dir, src, store)
	second.Resume()
	if p := second.Problem(job.Handle); p == nil || p.Code != v1.ReasonLogsNotSaved {
		t.Fatalf("after a restart the reason is %+v", p)
	}
	second.Forget(job.Handle)
	second.mu.Lock()
	_, kept := second.jobs[job.Handle]
	second.mu.Unlock()
	if _, err := os.Stat(filepath.Join(dir, job.Handle)); kept || !os.IsNotExist(err) {
		t.Fatalf("a refused log forgotten: kept %v, its state %v", kept, err)
	}
}
