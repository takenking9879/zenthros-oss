// SPDX-License-Identifier: Apache-2.0

package logfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
)

const (
	// readBytes is the most one read takes: one chunk, under
	// backend.MaxLogChunk.
	readBytes = 128 << 10

	// Pace is how long a following reader waits after a read that brought
	// something before it reads again. What the job writes meanwhile comes in
	// one read, and the kernel merges its inotify events into one: with a
	// job that flushes every line, reading at each event cost 21 mCPU at 100
	// lines/s and 125 at 1,000, and with the wait 1.9 at both (W0, loglab
	// wake). A log that was quiet wakes its reader at once.
	Pace = 200 * time.Millisecond
)

// ErrCursor is a cursor that is not one this package gave.
var ErrCursor = errors.New("not a cursor of this log")

// Read reads the file as req asks, on a channel that closes at the end:
// without Follow, at the end of what the file holds; with Follow, once
// ended is closed and the bytes written until then are sent, or when ctx
// ends. ended is closed when the executor knows the job ended (its
// process, or its unit), never from what the log says.
func (f *File) Read(ctx context.Context, req backend.LogRequest, ended <-chan struct{}) (<-chan backend.LogChunk, error) {
	file, err := os.Open(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, backend.Classified(backend.ErrNotFound, err)
		}
		return nil, err
	}
	// Watched before the first read: a write between that read and the
	// watch would wake nobody.
	var watcher *fsnotify.Watcher
	if req.Follow {
		if watcher, err = fsnotify.NewWatcher(); err == nil {
			err = watcher.Add(f.path)
		}
		if err != nil {
			file.Close()
			if watcher != nil {
				watcher.Close()
			}
			return nil, fmt.Errorf("watch %s: %w", f.path, err)
		}
	}
	r := &reader{f: f, file: file, buf: make([]byte, readBytes)}
	if err := r.start(req); err != nil {
		file.Close()
		if watcher != nil {
			watcher.Close()
		}
		return nil, err
	}
	out := make(chan backend.LogChunk, 4)
	go func() {
		defer close(out)
		defer file.Close()
		if watcher != nil {
			defer watcher.Close()
		}
		r.run(ctx, req.Follow, ended, watcher, out)
	}()
	return out, nil
}

// reader is one read of a file.
type reader struct {
	f    *File
	file *os.File
	buf  []byte
	// pos is the next byte to read; gap what was lost right before it, and
	// omitted, until the first chunk, what the history left out.
	pos, gap, omitted int64
	first             bool
}

// start places the read: after a cursor, or at the start of its history.
func (r *reader) start(req backend.LogRequest) error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	size, floor := info.Size(), r.f.Floor()
	r.first = true
	if req.After != "" {
		pos, err := strconv.ParseInt(req.After, 10, 64)
		if err != nil || pos < 0 {
			return backend.Classified(backend.ErrInvalid, fmt.Errorf("%w: %q", ErrCursor, req.After))
		}
		r.pos = min(pos, size)
		if r.pos < floor {
			r.lost(floor)
		}
		return nil
	}
	switch history := req.History(); {
	case req.TailLines >= 0:
		r.pos = tailOffset(r.file, size, floor, req.TailLines)
	case history == backend.AllHistory || history >= size:
		r.pos = 0
	default:
		r.pos = size - history
	}
	if r.pos < floor {
		r.pos = floor
	}
	if r.pos > 0 {
		r.pos = r.lineStart(r.pos)
	}
	r.omitted = r.pos
	return nil
}

// lost moves the read past a freed part, to the start of the next line,
// and counts what it skipped as the gap the next chunk carries.
func (r *reader) lost(floor int64) {
	from := r.pos
	r.pos = r.lineStart(floor)
	r.gap += r.pos - from
}

// lineStart is the first line start at or after pos: pos when the byte
// before it ends a line, else the byte after the next newline (or the end
// of what is written, when the line is not finished).
func (r *reader) lineStart(pos int64) int64 {
	if pos == 0 {
		return 0
	}
	var one [1]byte
	if n, _ := r.file.ReadAt(one[:], pos-1); n == 1 && one[0] == '\n' {
		return pos
	}
	for {
		n, err := r.file.ReadAt(r.buf, pos)
		if i := bytes.IndexByte(r.buf[:n], '\n'); i >= 0 {
			return pos + int64(i) + 1
		}
		pos += int64(n)
		if err != nil {
			return pos
		}
	}
}

func (r *reader) run(ctx context.Context, follow bool, ended <-chan struct{}, watcher *fsnotify.Watcher, out chan<- backend.LogChunk) {
	var wait *time.Timer
	defer func() {
		if wait != nil {
			wait.Stop()
		}
	}()
	// last is whether ended was seen before the read that is about to
	// happen: the read after it is the last.
	last := false
	for {
		got, err := r.next(ctx, out)
		if err != nil {
			return
		}
		if got == len(r.buf) {
			continue // more is there already
		}
		if !follow || last {
			return
		}
		select {
		case <-ended:
			last = true
			continue
		default:
		}
		if got > 0 {
			if wait == nil {
				wait = time.NewTimer(Pace)
			} else {
				wait.Reset(Pace)
			}
			select {
			case <-ctx.Done():
				return
			case <-ended:
				last = true
			case <-wait.C:
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ended:
			last = true
		case _, ok := <-watcher.Events:
			if !ok {
				return
			}
		case _, ok := <-watcher.Errors:
			// An overflow lost events, not bytes: read again.
			if !ok {
				return
			}
		}
	}
}

// next reads what is there from pos, up to a chunk, and sends it. It
// returns how much it read; an error ends the read.
func (r *reader) next(ctx context.Context, out chan<- backend.LogChunk) (int, error) {
	n, err := r.file.ReadAt(r.buf, r.pos)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	// After the read: the keeper stores the floor before it frees under it,
	// so a read that could have met the hole sees the new floor here. What
	// it read may be zeros: it is dropped, and the read goes on past the
	// freed part.
	if floor := r.f.Floor(); r.pos < floor {
		r.lost(floor)
		return len(r.buf), nil // read again at once, from past the hole
	}
	chunk := backend.LogChunk{
		Stream:    "stdout",
		Data:      bytes.Clone(r.buf[:n]),
		Timestamp: time.Now(),
		Cursor:    strconv.FormatInt(r.pos+int64(n), 10),
		Gap:       r.gap,
		GapReason: backend.GapFreed,
	}
	if r.first {
		chunk.Omitted, r.first = r.omitted, false
	}
	select {
	case out <- chunk:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	r.pos += int64(n)
	r.gap = 0
	return n, nil
}

// tailOffset is where the last n lines of the file begin, looking no lower
// than floor: below it are zeros.
func tailOffset(file *os.File, size, floor int64, n int) int64 {
	if n == 0 {
		return size
	}
	var (
		pos   = size
		lines = 0
		buf   = make([]byte, block)
		// A trailing newline ends the last line; it does not start another.
		skipLast = true
	)
	for pos > floor {
		readSize := min(int64(block), pos-floor)
		pos -= readSize
		if _, err := file.ReadAt(buf[:readSize], pos); err != nil && !errors.Is(err, io.EOF) {
			return floor
		}
		for i := readSize - 1; i >= 0; i-- {
			if buf[i] != '\n' {
				skipLast = false
				continue
			}
			if skipLast {
				skipLast = false
				continue
			}
			lines++
			if lines == n {
				return pos + i + 1
			}
		}
	}
	return floor
}
