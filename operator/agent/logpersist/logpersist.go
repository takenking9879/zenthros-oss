// SPDX-License-Identifier: Apache-2.0

// Package logpersist saves a job's whole log to a bucket of the user's as the
// job writes it (spec.workload.template.logs; benchmarks/loglab PLAN.md §3).
//
// One reader per job reads its log from the first byte, as any reader does
// (the executor's Logs), at the pace of the log and never of the bucket. It
// packs up to 128 KiB of the log in a zstd frame of its own, and seals the
// frames into an object at ~1 MiB or 30 s (2 s after a Spot notice): a fast
// job seals every few seconds, a slow one every 30. A sealed object waits in
// the queue, on the machine's disk (on a Kubernetes agent, its volume), and
// an uploader sends each as it is, once (pkg/objectput), at most two at a time
// for the agent. The queue holds 256 MiB of a job and 5 % of its disk: past
// that the reader stops and goes on from where it was, and a part the log's
// file freed meanwhile is a gap. Last, once every object is confirmed, comes
// END.json, which says complete only when nothing was lost. A restarted agent
// sends what was sealed and reads on from right after the last object sealed:
// what it keeps is the queue and one state file a job.
//
// Each object ends with a zstd skippable frame (Trailer), which any zstd
// reader skips: where the log goes on after the object, how much of it the
// objects hold through it, and the SHA-256 of its frames. A reader of the
// bucket checks each object by it, and asks the machine for the rest after
// the last one's cursor.
//
// Saving never fails the job. What keeps the log from being saved whole is
// the job's reason: LOGS_NOT_SAVED when nothing more will be (the bucket
// refused it), LOGS_INCOMPLETE when its copy misses a part.
package logpersist

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/fault"
)

const (
	// pieceBytes is the most of the log one frame packs.
	pieceBytes = 128 << 10
	// objectBytes and objectAge seal an object: ~1 MiB packed, or 30 s
	// after its first byte; hurriedAge after a Spot notice.
	objectBytes = 1 << 20
	objectAge   = 30 * time.Second
	hurriedAge  = 2 * time.Second
	// jobQueue is the most of one job's objects that wait on the machine.
	jobQueue = 256 << 20
	// diskShare is the most of the disk every job's queue takes: 1/20.
	diskShare = 20
	// puts is how many objects an agent sends at once.
	puts = 2
	// maxBackoff bounds the wait between two tries of a send.
	maxBackoff = time.Minute
	// reopen is the wait before a read that broke is opened again.
	reopen = time.Second
)

// The objects' and the end's names, and END.json's statuses.
const (
	objectSuffix = ".log.zst"
	endName      = "END.json"
	stateName    = "state.json"

	StatusComplete  = "complete"
	StatusTruncated = "truncated"
	StatusGap       = "gap"
)

// ObjectName is the name of a log's object seq under its prefix.
func ObjectName(seq int64) string { return fmt.Sprintf("%08d%s", seq, objectSuffix) }

// Job is one run of a job whose log is saved: where its objects go, and how a
// restarted agent reads its log again.
type Job struct {
	ID        string `json:"id"`
	Namespace string `json:"namespace,omitempty"`
	// Name and UID are the ZenthrosJob's; Generation its lease's
	// (v1.LeaseGeneration) and Attempt its run (Status.Run), as its spec
	// says them.
	Name       string      `json:"name"`
	UID        string      `json:"uid"`
	Generation int64       `json:"generation"`
	Attempt    int32       `json:"attempt"`
	Handle     string      `json:"handle"`
	Logs       v1.LogsSpec `json:"logs"`
}

// Prefix is where the run's objects go under the destination:
// <job>/<uid>/g<generation>-a<attempt>/<handle>.
func (j Job) Prefix() string {
	return path.Join(j.Name, j.UID, fmt.Sprintf("g%d-a%d", j.Generation, j.Attempt), url.PathEscape(j.Handle))
}

// End is END.json: written once every object is in the bucket.
type End struct {
	// Status is complete, truncated (past logs.maxBytes) or gap (a part
	// never saved).
	Status string `json:"status"`
	// Objects are 00000000.log.zst up to Objects-1.
	Objects int64 `json:"objects"`
	// Bytes is how much of the log they hold, unpacked.
	Bytes int64 `json:"bytes"`
	// Gap is how many bytes of the log never came, -1 when some could
	// not be counted.
	Gap       int64 `json:"gap,omitempty"`
	Truncated bool  `json:"truncated,omitempty"`
}

// trailerMagic opens the skippable frame each object ends with (the zstd
// format keeps 0x184D2A50-0x184D2A5F for frames a decoder skips).
const trailerMagic = 0x184D2A5E

// Trailer is what an object says of itself in its last frame.
type Trailer struct {
	// Cursor is where the log goes on after the object, as its machine's
	// reader names it (a read After it).
	Cursor string `json:"cursor,omitempty"`
	// Bytes is how much of the log the objects hold through this one,
	// unpacked.
	Bytes int64 `json:"bytes"`
	// Gap is how much of the log never came through this object: it grows
	// only right before an object, since a part missing starts one, so a
	// reader says it where it is. backend.Uncounted once a part could not
	// be counted.
	Gap int64 `json:"gap,omitempty"`
	// SHA256 is the hex SHA-256 of the object's frames before the trailer.
	SHA256 string `json:"sha256"`
}

// AppendTrailer ends frames with t, its SHA-256 the frames'.
func AppendTrailer(frames []byte, t Trailer) []byte {
	sum := sha256.Sum256(frames)
	t.SHA256 = hex.EncodeToString(sum[:])
	raw, err := json.Marshal(t)
	if err != nil {
		panic(err) // a struct of strings and a number
	}
	frames = binary.LittleEndian.AppendUint32(frames, trailerMagic)
	frames = binary.LittleEndian.AppendUint32(frames, uint32(len(raw)))
	return append(frames, raw...)
}

// ReadTrailer splits an object into its frames and its trailer, checked:
// an object that does not end with one, or whose frames are not the bytes
// it vouches for, is refused.
func ReadTrailer(object []byte) ([]byte, Trailer, error) {
	var t Trailer
	// The trailer is the object's last frame: 8 bytes of header, then at most
	// a cursor, a number and a hash.
	for at := len(object) - 9; at >= 0 && at >= len(object)-8-4096; at-- {
		if binary.LittleEndian.Uint32(object[at:]) != trailerMagic || int(binary.LittleEndian.Uint32(object[at+4:])) != len(object)-at-8 {
			continue
		}
		if err := json.Unmarshal(object[at+8:], &t); err != nil {
			return nil, t, fmt.Errorf("the object's trailer: %w", err)
		}
		frames := object[:at]
		sum := sha256.Sum256(frames)
		if hex.EncodeToString(sum[:]) != t.SHA256 {
			return nil, t, errors.New("the object is not the bytes its trailer vouches for")
		}
		return frames, t, nil
	}
	return nil, t, errors.New("the object has no trailer: not one Zenthros sealed whole")
}

// Store is the bucket objects go to (objectput.Store).
type Store interface {
	Put(ctx context.Context, key, contentType string, body []byte) error
	Key(parts ...string) string
}

// Source is the agent's executor: it reads a job's log, and it alone says
// whether the job ended (never the log's content).
type Source interface {
	Logs(ctx context.Context, handle string, req backend.LogRequest) (<-chan backend.LogChunk, error)
	// Ended is whether the job's workload ended or is gone.
	Ended(ctx context.Context, handle string) bool
}

// Config is what a Saver needs from the agent.
type Config struct {
	// Dir holds the queue: a directory per job.
	Dir    string
	Source Source
	// Open is the job's bucket with its credentials, resolved on the
	// machine.
	Open   func(ctx context.Context, job Job) (Store, error)
	Logger logr.Logger
}

// Saver saves the logs of a machine's jobs.
type Saver struct {
	cfg  Config
	ctx  context.Context
	stop context.CancelFunc
	sem  chan struct{}
	wg   sync.WaitGroup

	mu   sync.Mutex
	jobs map[string]*saving

	hurried atomic.Bool
	queued  atomic.Int64
	disk    int64

	// What the constants say, which a test shortens.
	pieceBytes, objectBytes int
	objectAge, hurriedAge   time.Duration
	jobQueue                int64
	reopen                  time.Duration
	backoff                 func(try int) time.Duration
	// atEnd, a test's, runs once a log's end is known, before its last
	// object is sealed.
	atEnd func()
}

// New is a Saver whose queue is under cfg.Dir.
func New(cfg Config) (*Saver, error) {
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(cfg.Dir, &fs); err != nil {
		return nil, fmt.Errorf("the size of %s: %w", cfg.Dir, err)
	}
	ctx, stop := context.WithCancel(context.Background())
	return &Saver{cfg: cfg, ctx: ctx, stop: stop, sem: make(chan struct{}, puts), jobs: map[string]*saving{},
		disk:       int64(fs.Blocks) * int64(fs.Bsize) / diskShare,
		pieceBytes: pieceBytes, objectBytes: objectBytes, objectAge: objectAge, hurriedAge: hurriedAge,
		jobQueue: jobQueue, reopen: reopen, backoff: backoff}, nil
}

// backoff is the wait before try (from 0): exponential from 1 s up to a
// minute, drawn at random below it.
func backoff(try int) time.Duration {
	bound := min(time.Second<<min(try, 6), maxBackoff)
	return bound/2 + rand.N(bound/2+1)
}

// state is a job's file in the queue, written after each object it records.
type state struct {
	Job Job `json:"job"`
	// Next is the next object's number: 0..Next-1 are sealed.
	Next int64 `json:"next"`
	// Cursor is where the read goes on: right after the last object.
	Cursor string `json:"cursor,omitempty"`
	// Opened says a read gave its first chunk: until then each read starts
	// at the log's first byte (a pod's log that is not there yet), and the
	// next ones at Cursor.
	Opened bool `json:"opened,omitempty"`
	// Saved is how much of the log the sealed objects hold.
	Saved int64 `json:"saved"`
	// Gap is how many bytes never came; backend.Uncounted when some
	// could not be counted.
	Gap       int64 `json:"gap,omitempty"`
	Truncated bool  `json:"truncated,omitempty"`
	// Ended: the read reached the log's end, and no object follows.
	Ended bool `json:"ended,omitempty"`
	// Refused is why nothing more is saved, for good.
	Refused string `json:"refused,omitempty"`
}

func (st *state) addGap(n int64) {
	switch {
	case n == 0:
	case n < 0 || st.Gap < 0:
		st.Gap = backend.Uncounted
	default:
		st.Gap += n
	}
}

// saving is one job's log on its way.
type saving struct {
	s   *Saver
	dir string

	mu sync.Mutex
	st state
	// confirmed is the first object not in the bucket yet; queued the
	// bytes of those that wait.
	confirmed int64
	queued    int64
	// full is set while the reader waits for room.
	full bool
	// done closes when nothing more is to be sent: END.json is in, or the
	// bucket refused the log. forget, under the Saver's mu as done closes,
	// drops the job from the Saver then.
	done   chan struct{}
	forget bool

	sealed   chan struct{}
	room     chan struct{}
	sealNow  chan struct{}
	stopRead chan struct{}
	stopOnce sync.Once
	refused  chan struct{}
	refuse1  sync.Once
	reading  chan struct{}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Start saves job's log, from its first byte. A job it saves already (a
// Submit sent again) goes on as it is.
func (s *Saver) Start(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[job.Handle]; ok {
		return nil
	}
	j := s.newSaving(filepath.Join(s.cfg.Dir, url.PathEscape(job.Handle)), state{Job: job})
	if err := os.MkdirAll(j.dir, 0o700); err != nil {
		return err
	}
	if err := j.writeState(); err != nil {
		return err
	}
	s.jobs[job.Handle] = j
	s.run(j)
	return nil
}

func (s *Saver) newSaving(dir string, st state) *saving {
	return &saving{s: s, dir: dir, st: st, done: make(chan struct{}), sealed: make(chan struct{}, 1), room: make(chan struct{}, 1),
		sealNow: make(chan struct{}, 1), stopRead: make(chan struct{}), refused: make(chan struct{}), reading: make(chan struct{})}
}

func (s *Saver) run(j *saving) {
	if j.st.Refused != "" {
		// Refused before the agent restarted: nothing is sent or read, and
		// its state is kept for the reason until Forget.
		close(j.reading)
		close(j.done)
		return
	}
	holding.Add(j.st.Job.Handle)
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		defer close(j.reading)
		defer holding.Done(j.st.Job.Handle)
		if !j.st.Ended && !j.st.Truncated && j.st.Refused == "" {
			j.read(s.ctx)
		}
	}()
	go func() {
		defer s.wg.Done()
		j.upload(s.ctx)
	}()
}

// Resume goes on with the logs a previous agent left in the queue: what was
// sealed is sent, and each read goes on from right after its last object.
func (s *Saver) Resume() {
	entries, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		s.cfg.Logger.Error(err, "could not read the queue of the logs saved to buckets")
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(s.cfg.Dir, entry.Name())
		raw, err := os.ReadFile(filepath.Join(dir, stateName))
		var st state
		if err == nil {
			err = json.Unmarshal(raw, &st)
		}
		if err != nil {
			// A job whose first state never reached the disk: nothing of its
			// log was sealed.
			s.cfg.Logger.Error(err, "a saved log's state does not read; its queue is removed", "dir", dir)
			_ = os.RemoveAll(dir)
			continue
		}
		j := s.newSaving(dir, st)
		if err := j.recover(); err != nil {
			s.cfg.Logger.Error(err, "could not read a saved log's queue", "dir", dir)
			continue
		}
		s.mu.Lock()
		if _, ok := s.jobs[st.Job.Handle]; !ok {
			s.jobs[st.Job.Handle] = j
			s.queued.Add(j.queued)
			s.run(j)
		}
		s.mu.Unlock()
	}
}

// recover finds the objects a previous agent sealed and did not send:
// those state records. An object written after the last state is dropped:
// its bytes are read again from the state's cursor.
func (j *saving) recover() error {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return err
	}
	j.confirmed = j.st.Next
	for _, entry := range entries {
		name := entry.Name()
		seq, ok := objectSeq(name)
		switch {
		case strings.HasSuffix(name, ".tmp"), ok && seq >= j.st.Next:
			_ = os.Remove(filepath.Join(j.dir, name))
		case ok:
			info, err := entry.Info()
			if err != nil {
				return err
			}
			j.queued += info.Size()
			j.confirmed = min(j.confirmed, seq)
		}
	}
	signal(j.sealed)
	return nil
}

func objectSeq(name string) (int64, bool) {
	digits, ok := strings.CutSuffix(name, objectSuffix)
	if !ok {
		return 0, false
	}
	seq, err := strconv.ParseInt(digits, 10, 64)
	return seq, err == nil && seq >= 0
}

// Problem is why job handle's log is not saved whole, or nil.
func (s *Saver) Problem(handle string) *v1.Reason {
	s.mu.Lock()
	j, ok := s.jobs[handle]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	dest := j.st.Job.Logs.Destination
	switch {
	case j.st.Refused != "":
		return &v1.Reason{Code: v1.ReasonLogsNotSaved, Detail: fmt.Sprintf("the log is not saved to %s: %s", dest, j.st.Refused)}
	case j.st.Gap < 0:
		return &v1.Reason{Code: v1.ReasonLogsIncomplete, Detail: fmt.Sprintf("a part of the log was lost before it was saved to %s", dest)}
	case j.st.Gap > 0:
		return &v1.Reason{Code: v1.ReasonLogsIncomplete, Detail: fmt.Sprintf("%d bytes of the log were lost before they were saved to %s", j.st.Gap, dest)}
	case j.st.Truncated:
		return &v1.Reason{Code: v1.ReasonLogsIncomplete, Detail: fmt.Sprintf("the log passed logs.maxBytes (%d bytes): the rest is not saved to %s", j.st.Job.Logs.Max(), dest)}
	case j.full:
		return &v1.Reason{Code: v1.ReasonLogsIncomplete, Detail: fmt.Sprintf("%s takes the log slower than the job writes it: %d MiB wait on the machine, and what its file frees meanwhile is lost", dest, j.queued>>20)}
	}
	return nil
}

// Forget drops job handle once nothing more is to be sent for it: its
// Cancel. A log still being saved goes on until its end.
func (s *Saver) Forget(handle string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[handle]
	if !ok {
		return
	}
	select {
	case <-j.done:
		s.drop(j)
	default:
		j.forget = true
	}
}

// drop removes a job whose saving is done, and its state: a refused log's,
// kept for its reason until then. s.mu is held.
func (s *Saver) drop(j *saving) {
	delete(s.jobs, j.st.Job.Handle)
	_ = os.RemoveAll(j.dir)
}

// Hurry is the machine's Spot notice: every open object is sealed now, and
// then every 2 s.
func (s *Saver) Hurry() {
	s.hurried.Store(true)
	for _, j := range s.snapshot() {
		signal(j.sealNow)
	}
}

// Drain is the agent stopping: each read seals what it holds and stops, and
// what waits is sent until ctx ends. The jobs run on; a restarted agent
// resumes them.
func (s *Saver) Drain(ctx context.Context) {
	jobs := s.snapshot()
	for _, j := range jobs {
		j.stopOnce.Do(func() { close(j.stopRead) })
	}
	for _, j := range jobs {
		select {
		case <-j.reading:
		case <-ctx.Done():
		}
	}
	for _, j := range jobs {
		for !j.sent() {
			select {
			case <-ctx.Done():
				s.stop()
				s.wg.Wait()
				return
			case <-j.done:
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	s.stop()
	s.wg.Wait()
}

func (s *Saver) snapshot() []*saving {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*saving, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	return out
}

// sent is whether every sealed object is in the bucket.
func (j *saving) sent() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.confirmed >= j.st.Next || j.st.Refused != ""
}

var encoder = sync.OnceValue(func() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithLowerEncoderMem(true))
	if err != nil {
		panic(err)
	}
	return enc
})

// open is the object the reader fills.
type open struct {
	piece  []byte
	frames []byte
	raw    int64
	gap    int64
	since  time.Time
	cursor string
}

func (o *open) empty() bool { return o.raw == 0 && o.gap == 0 }

func (j *saving) read(ctx context.Context) {
	s := j.s
	o := &open{cursor: j.st.Cursor}
	limit := j.st.Job.Logs.Max()
	for {
		if j.stopped() {
			j.seal(o, false)
			return
		}
		if j.overQueue(o) {
			j.seal(o, false)
			if !j.waitRoom() {
				j.seal(o, false)
				return
			}
			continue
		}
		req := backend.LogRequest{Follow: true}
		j.mu.Lock()
		opened := j.st.Opened
		j.mu.Unlock()
		switch {
		case !opened:
			req.TailLines, req.HistoryBytes = -1, backend.AllHistory
		case o.cursor != "":
			req.After = o.cursor
		default:
			// A log read without cursors goes on from now: what was
			// written since cannot be told.
			req.TailLines = 0
			j.gapAt(o, backend.Uncounted)
		}
		readCtx, cancel := context.WithCancel(ctx)
		chunks, err := s.cfg.Source.Logs(readCtx, j.st.Job.Handle, req)
		if err != nil {
			cancel()
			if s.cfg.Source.Ended(ctx, j.st.Job.Handle) {
				if !opened {
					j.gapAt(o, backend.Uncounted)
				}
				j.finish(o, true)
				return
			}
			if !j.pause(s.reopen) {
				j.seal(o, false)
				return
			}
			continue
		}
		ended, stop := j.consume(ctx, chunks, o, limit, !opened)
		cancel()
		if stop {
			return
		}
		if ended || s.cfg.Source.Ended(ctx, j.st.Job.Handle) {
			j.finish(o, false)
			return
		}
		if !j.pause(s.reopen) {
			j.seal(o, false)
			return
		}
	}
}

// consume packs what a read gives until it ends. ended says the log's end
// was read (the job's past maxBytes); stop that the read must not go on.
// The first chunk of the first read marks the log opened: a read that
// failed or gave nothing is opened again from the first byte.
func (j *saving) consume(ctx context.Context, chunks <-chan backend.LogChunk, o *open, limit int64, first bool) (ended, stop bool) {
	s := j.s
	age := time.NewTimer(time.Hour)
	defer age.Stop()
	arm := func() {
		window := s.objectAge
		if s.hurried.Load() {
			window = s.hurriedAge
		}
		age.Reset(max(time.Until(o.since.Add(window)), 0))
	}
	if !o.empty() {
		arm()
	}
	for {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				return false, false
			}
			if first {
				first = false
				j.mu.Lock()
				j.st.Opened = true
				j.mu.Unlock()
				j.gapAt(o, chunk.Omitted)
			}
			j.gapAt(o, chunk.Gap)
			data := chunk.Data
			j.mu.Lock()
			room := limit - j.st.Saved - o.raw
			j.mu.Unlock()
			truncated := int64(len(data)) > room
			if truncated {
				data = data[:max(room, 0)]
			}
			if len(data) > 0 && o.empty() {
				o.since = time.Now()
				arm()
			}
			o.add(data, s.pieceBytes)
			if chunk.Cursor != "" {
				o.cursor = chunk.Cursor
			}
			if truncated {
				// Past maxBytes nothing more is read: the log ends here.
				j.mu.Lock()
				j.st.Truncated = true
				j.mu.Unlock()
				j.seal(o, true)
				return true, true
			}
			if len(o.frames) >= s.objectBytes {
				j.seal(o, false)
			}
			if j.overQueue(o) {
				j.seal(o, false)
				return false, false
			}
		case <-age.C:
			j.seal(o, false)
		case <-j.sealNow:
			j.seal(o, false)
		case <-j.stopRead:
			j.seal(o, false)
			return false, true
		case <-j.refused:
			return false, true
		case <-ctx.Done():
			return false, true
		}
	}
}

// gapAt is n bytes of the log that never come, right here: they start an
// object, so its trailer says them where they are missing (Trailer.Gap).
func (j *saving) gapAt(o *open, n int64) {
	if n == 0 {
		return
	}
	if o.raw > 0 {
		j.seal(o, false)
	}
	switch {
	case n < 0 || o.gap < 0:
		o.gap = backend.Uncounted
	default:
		o.gap += n
	}
}

// add takes data into the object: a frame for each full piece.
func (o *open) add(data []byte, piece int) {
	o.raw += int64(len(data))
	for len(data) > 0 {
		n := min(piece-len(o.piece), len(data))
		o.piece = append(o.piece, data[:n]...)
		data = data[n:]
		if len(o.piece) == piece {
			o.pack()
		}
	}
}

func (o *open) pack() {
	if len(o.piece) > 0 {
		o.frames = encoder().EncodeAll(o.piece, o.frames)
		o.piece = o.piece[:0]
	}
}

// finish is the log's end, as the executor says it: what a read that broke
// just before it left is read to the end, and the last object sealed with
// the job recorded ended, for its END.json. gone says the log could not be
// read at all any more. A rest that cannot be read is a part missing: the
// read it follows may have broken before the end.
func (j *saving) finish(o *open, gone bool) {
	if !gone && o.cursor != "" {
		// The end the executor says may follow a read that broke: what
		// is left after the cursor is read once more, without following.
		ctx, cancel := context.WithTimeout(j.s.ctx, time.Minute)
		chunks, err := j.s.cfg.Source.Logs(ctx, j.st.Job.Handle, backend.LogRequest{After: o.cursor})
		if err != nil {
			j.s.cfg.Logger.Info("the end of a log saved to its bucket could not be read again; it may miss its last lines",
				"handle", j.st.Job.Handle, "error", err.Error())
			j.gapAt(o, backend.Uncounted)
		} else {
			limit := j.st.Job.Logs.Max()
			for chunk := range chunks {
				j.gapAt(o, chunk.Gap)
				j.mu.Lock()
				room := limit - j.st.Saved - o.raw
				j.mu.Unlock()
				data := chunk.Data
				if int64(len(data)) > room {
					data = data[:max(room, 0)]
					j.mu.Lock()
					j.st.Truncated = true
					j.mu.Unlock()
				}
				o.add(data, j.s.pieceBytes)
				if chunk.Cursor != "" {
					o.cursor = chunk.Cursor
				}
			}
		}
		cancel()
	}
	if j.s.atEnd != nil {
		j.s.atEnd()
	}
	j.seal(o, true)
}

// seal writes the open object to the queue, then the state that records it:
// a restarted agent never sends an object its state does not know. An
// object the disk does not take is a gap. ended records the log's end with
// its last object, at once: the uploader never sees an end whose last
// object is not sealed, and never writes END.json before it.
func (j *saving) seal(o *open, ended bool) {
	o.pack()
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.st.Refused != "" {
		*o = open{cursor: o.cursor}
		return
	}
	// The part missing right before the object, then the object.
	j.st.addGap(o.gap)
	if o.raw > 0 {
		o.frames = AppendTrailer(o.frames, Trailer{Cursor: o.cursor, Bytes: j.st.Saved + o.raw, Gap: j.st.Gap})
		name := filepath.Join(j.dir, ObjectName(j.st.Next))
		if err := writeFile(name, o.frames); err != nil {
			j.s.cfg.Logger.Error(err, "could not queue a part of a log for its bucket; it is lost", "handle", j.st.Job.Handle)
			j.st.addGap(o.raw)
		} else {
			j.st.Next++
			j.st.Saved += o.raw
			j.queued += int64(len(o.frames))
			j.s.queued.Add(int64(len(o.frames)))
		}
	}
	if ended {
		j.st.Ended = true
	}
	if o.cursor != "" {
		j.st.Cursor = o.cursor
	}
	if err := j.writeStateLocked(); err != nil {
		j.s.cfg.Logger.Error(err, "could not record a saved log's state", "handle", j.st.Job.Handle)
	}
	*o = open{cursor: o.cursor, piece: o.piece[:0], frames: o.frames[:0]}
	signal(j.sealed)
}

func (j *saving) writeState() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.writeStateLocked()
}

func (j *saving) writeStateLocked() error {
	raw, err := json.Marshal(j.st)
	if err != nil {
		return err
	}
	return writeFile(filepath.Join(j.dir, stateName), raw)
}

// writeFile writes data at name whole or not at all, and to the disk.
func writeFile(name string, data []byte) error {
	tmp := name + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp, name)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if d, err := os.Open(filepath.Dir(name)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// overQueue is whether the queue is full: the job's own, or the disk's.
func (j *saving) overQueue(o *open) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	over := j.queued+int64(len(o.frames)) >= j.s.jobQueue || j.s.queued.Load() >= j.s.disk
	j.full = over
	return over
}

// waitRoom waits until an object leaves the queue; false when the read
// must stop.
func (j *saving) waitRoom() bool {
	select {
	case <-j.room:
		return true
	case <-j.stopRead:
	case <-j.refused:
	case <-j.s.ctx.Done():
	}
	return false
}

// pause waits d before a read is opened again; false when it must stop.
func (j *saving) pause(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-j.stopRead:
	case <-j.refused:
	case <-j.s.ctx.Done():
	}
	return false
}

func (j *saving) stopped() bool {
	select {
	case <-j.stopRead:
		return true
	case <-j.refused:
		return true
	case <-j.s.ctx.Done():
		return true
	default:
		return false
	}
}

// upload sends each sealed object in order, then END.json once the log
// ended and all are in.
func (j *saving) upload(ctx context.Context) {
	var store Store
	for {
		select {
		case <-j.sealed:
		case <-ctx.Done():
			return
		}
		for {
			j.mu.Lock()
			seq, next, refused := j.confirmed, j.st.Next, j.st.Refused
			ended := j.st.Ended
			j.mu.Unlock()
			if refused != "" {
				return
			}
			if store == nil && (seq < next || ended) {
				opened, err := j.openStore(ctx)
				if err != nil {
					j.refuse(err)
					return
				}
				if opened == nil {
					return
				}
				store = opened
			}
			if seq < next {
				name := filepath.Join(j.dir, ObjectName(seq))
				body, err := os.ReadFile(name)
				if err != nil {
					j.refuse(fmt.Errorf("its queued object %d: %w", seq, err))
					return
				}
				if err := j.put(ctx, store, ObjectName(seq), "application/zstd", body); err != nil {
					if ctx.Err() == nil {
						j.refuse(err)
					}
					return
				}
				_ = os.Remove(name)
				j.mu.Lock()
				j.confirmed++
				j.queued -= int64(len(body))
				j.mu.Unlock()
				j.s.queued.Add(-int64(len(body)))
				signal(j.room)
				continue
			}
			if !ended {
				break
			}
			if err := j.putEnd(ctx, store); err != nil {
				if ctx.Err() == nil {
					j.refuse(err)
				}
				return
			}
			j.done1()
			return
		}
	}
}

// openStore is the job's bucket, asked again while what keeps it from
// opening may pass (a credential's Secret not readable yet); nil when ctx
// ended.
func (j *saving) openStore(ctx context.Context) (Store, error) {
	for try := 0; ; try++ {
		store, err := j.s.cfg.Open(ctx, j.st.Job)
		if err == nil {
			return store, nil
		}
		if !fault.Retryable(err) {
			return nil, fmt.Errorf("its credentials: %w", err)
		}
		select {
		case <-time.After(j.s.backoff(try)):
		case <-ctx.Done():
			return nil, nil
		}
	}
}

// put sends one object, again after a failure that may pass, at most puts
// at a time for the agent. Its error never may.
func (j *saving) put(ctx context.Context, store Store, name, contentType string, body []byte) error {
	key := store.Key(j.st.Job.Prefix(), name)
	for try := 0; ; try++ {
		select {
		case j.s.sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		err := store.Put(ctx, key, contentType, body)
		<-j.s.sem
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !fault.Retryable(err) {
			return err
		}
		j.s.cfg.Logger.Info("a log's object did not reach its bucket; sent again", "handle", j.st.Job.Handle, "object", name, "error", err.Error())
		select {
		case <-time.After(j.s.backoff(try)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (j *saving) putEnd(ctx context.Context, store Store) error {
	j.mu.Lock()
	end := End{Status: StatusComplete, Objects: j.st.Next, Bytes: j.st.Saved, Gap: j.st.Gap, Truncated: j.st.Truncated}
	j.mu.Unlock()
	switch {
	case end.Gap != 0:
		end.Status = StatusGap
	case end.Truncated:
		end.Status = StatusTruncated
	}
	raw, err := json.Marshal(end)
	if err != nil {
		return err
	}
	return j.put(ctx, store, endName, "application/json", raw)
}

// refuse stops saving the log for good: the bucket or its credentials
// refused it. The queue goes; the state stays, for the reason.
func (j *saving) refuse(err error) {
	j.mu.Lock()
	j.st.Refused = errText(err)
	j.s.queued.Add(-j.queued)
	j.queued = 0
	if writeErr := j.writeStateLocked(); writeErr != nil {
		j.s.cfg.Logger.Error(writeErr, "could not record a saved log's state", "handle", j.st.Job.Handle)
	}
	j.mu.Unlock()
	j.refuse1.Do(func() { close(j.refused) })
	entries, _ := os.ReadDir(j.dir)
	for _, entry := range entries {
		if _, ok := objectSeq(entry.Name()); ok {
			_ = os.Remove(filepath.Join(j.dir, entry.Name()))
		}
	}
	j.s.cfg.Logger.Info("a job's log is not saved to its bucket", "handle", j.st.Job.Handle, "reason", j.st.Refused)
	j.done1()
}

// done1 ends the job's saving: its queue goes once END.json is in (a
// refused log keeps its state for the reason).
func (j *saving) done1() {
	j.mu.Lock()
	refused := j.st.Refused != ""
	j.mu.Unlock()
	if !refused {
		_ = os.RemoveAll(j.dir)
	}
	// Under the Saver's mu, where Forget looks at done: a Forget either
	// sees it closed and drops the job, or marks it before it closes.
	j.s.mu.Lock()
	defer j.s.mu.Unlock()
	close(j.done)
	if j.forget {
		j.s.drop(j)
	}
}

func errText(err error) string {
	text := err.Error()
	if len(text) > 512 {
		text = text[:512] + "..."
	}
	return text
}

// holding is the jobs whose log file is still read for its bucket: an
// executor removes no such file (Saving).
var holding = &held{handles: map[string]int{}}

type held struct {
	mu      sync.Mutex
	handles map[string]int
}

func (h *held) Add(handle string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handles[handle]++
}

func (h *held) Done(handle string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.handles[handle]--; h.handles[handle] <= 0 {
		delete(h.handles, handle)
	}
}

// Saving is whether job handle's log is still read for its bucket: its file
// must stay until then.
func Saving(handle string) bool {
	holding.mu.Lock()
	defer holding.mu.Unlock()
	return holding.handles[handle] > 0
}
