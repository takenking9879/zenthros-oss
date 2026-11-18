// SPDX-License-Identifier: Apache-2.0

package logpersist

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/objectput"
)

// Reading a saved log back (zctl logs --to; the SDK's remote/savedlogs.py
// reads it the same way): the bucket's objects of the job's run, each
// checked by its trailer and unpacked bounded, since a bucket is not this
// side's to trust.

// Bucket is what reading a saved log needs of its bucket (objectput.Store).
type Bucket interface {
	Key(parts ...string) string
	List(ctx context.Context, prefix string) ([]objectput.Object, error)
	Get(ctx context.Context, key string) ([]byte, error)
}

// Saved is the bucket's log of one run of a job.
type Saved struct {
	// Folder is the run's prefix, ending in "/": Job.Prefix.
	Folder string
	// Handle is the run's attempt on its machine, as a log read names it.
	Handle string
	// Objects are its objects' keys, in order.
	Objects []string
	// Ended says END.json is in.
	Ended bool
}

var runFolder = regexp.MustCompile(`^g(\d+)-a(\d+)$`)

// Find is the bucket's log of the job's run on handle (its
// status.backendJobName), nil when it holds none of it yet: an earlier
// attempt's is not this run's, whose machine has it all. Without a handle
// (a job between runs, or that ended without one) it is the last run's: the
// highest generation, then attempt, and of a run sent twice to its machine
// the handle written last.
func Find(ctx context.Context, b Bucket, name, uid, handle string) (*Saved, error) {
	under := b.Key(name, uid) + "/"
	listed, err := b.List(ctx, under)
	if err != nil {
		return nil, err
	}
	type run struct{ generation, attempt int64 }
	type folder struct {
		names   []string
		written time.Time
	}
	runs := map[run]map[string]*folder{}
	for _, object := range listed {
		parts := strings.Split(strings.TrimPrefix(object.Key, under), "/")
		if len(parts) != 3 || handle != "" && parts[1] != url.PathEscape(handle) {
			continue
		}
		number := runFolder.FindStringSubmatch(parts[0])
		if number == nil {
			continue
		}
		generation, _ := strconv.ParseInt(number[1], 10, 64)
		attempt, _ := strconv.ParseInt(number[2], 10, 64)
		r := run{generation, attempt}
		if runs[r] == nil {
			runs[r] = map[string]*folder{}
		}
		f := runs[r][parts[1]]
		if f == nil {
			f = &folder{}
			runs[r][parts[1]] = f
		}
		f.names = append(f.names, parts[2])
		if object.Modified.After(f.written) {
			f.written = object.Modified
		}
	}
	if len(runs) == 0 {
		return nil, nil
	}
	last := run{-1, -1}
	for r := range runs {
		if r.generation > last.generation || r.generation == last.generation && r.attempt > last.attempt {
			last = r
		}
	}
	var escaped string
	var newest *folder
	for h, f := range runs[last] {
		if newest == nil || f.written.After(newest.written) {
			escaped, newest = h, f
		}
	}
	unescaped, err := url.PathUnescape(escaped)
	if err != nil {
		return nil, corrupt("%s: handle %q: %v", under, escaped, err)
	}
	saved := &Saved{Folder: fmt.Sprintf("%sg%d-a%d/%s/", under, last.generation, last.attempt, escaped), Handle: unescaped}
	slices.Sort(newest.names)
	for _, name := range newest.names {
		switch {
		case name == endName:
			saved.Ended = true
		case strings.HasSuffix(name, objectSuffix):
			if want := ObjectName(int64(len(saved.Objects))); name != want {
				return nil, corrupt("%s: object %d is %s", saved.Folder, len(saved.Objects), name)
			}
			saved.Objects = append(saved.Objects, saved.Folder+name)
		}
	}
	return saved, nil
}

// End is the run's END.json, nil before it is in.
func (s *Saved) End(ctx context.Context, b Bucket) (*End, error) {
	if !s.Ended {
		return nil, nil
	}
	raw, err := b.Get(ctx, s.Folder+endName)
	if err != nil {
		return nil, err
	}
	var end End
	if err := json.Unmarshal(raw, &end); err != nil {
		return nil, corrupt("%s%s: %v", s.Folder, endName, err)
	}
	return &end, nil
}

// Read writes the log the run's objects hold to w, in order, and answers the
// last object's trailer: where the log goes on, how much the objects hold
// and how much never came. missing is called where a part never came,
// before the object that follows it, with its bytes (backend.Uncounted when
// no one counted them). An object that is not what its trailer vouches for,
// or that unpacks to other than it says, is corrupt (fault.Invalid), and the
// read stops there, part of it written.
func (s *Saved) Read(ctx context.Context, b Bucket, w io.Writer, missing func(n int64) error) (Trailer, error) {
	var last Trailer
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxWindow(maxWindow), zstd.WithDecoderLowmem(true),
		zstd.WithDecoderConcurrency(1))
	if err != nil {
		return last, err
	}
	defer decoder.Close()
	out := &writer{w: w}
	for _, key := range s.Objects {
		object, err := b.Get(ctx, key)
		if err != nil {
			if fault.Of(err) == fault.Absent {
				return last, corrupt("%s went missing while it was read", key)
			}
			return last, err
		}
		frames, said, err := ReadTrailer(object)
		if err != nil {
			return last, corrupt("%s: %v", key, err)
		}
		n := said.Bytes - last.Bytes
		if n < 1 {
			return last, corrupt("%s says it holds %d bytes", key, n)
		}
		if gap := Missing(last.Gap, said.Gap); gap != 0 {
			if err := missing(gap); err != nil {
				return last, err
			}
		}
		if err := decoder.Reset(bytes.NewReader(frames)); err != nil {
			return last, corrupt("%s: %v", key, err)
		}
		// One byte more than it says, to see that there is no more.
		copied, err := io.CopyN(out, decoder, n+1)
		switch {
		case out.err != nil:
			return last, out.err
		case copied > n:
			return last, corrupt("%s unpacks to more than the %d bytes it says", key, n)
		case copied < n:
			return last, corrupt("%s unpacks to %d bytes, not the %d it says: %v", key, copied, n, err)
		case err != nil && !errors.Is(err, io.EOF):
			return last, corrupt("%s: %v", key, err)
		}
		last = said
	}
	return last, nil
}

// Missing is what never came between two counts of it (Trailer.Gap,
// End.Gap): backend.Uncounted once a part could not be counted.
func Missing(before, after int64) int64 {
	switch {
	case after == before:
		return 0
	case after < 0:
		return backend.Uncounted
	}
	return after - before
}

// writer keeps the error of the writer a read writes to, apart from the
// unpacking's.
type writer struct {
	w   io.Writer
	err error
}

func (o *writer) Write(p []byte) (int, error) {
	n, err := o.w.Write(p)
	if err != nil {
		o.err = err
	}
	return n, err
}

// maxWindow is the most window a frame of the bucket may ask for: the agent
// packs 128 KiB a frame (pkg/logstream bounds a read's the same).
const maxWindow = 1 << 20

func corrupt(format string, args ...any) error {
	return fault.Wrap(fault.Invalid, "saved log", fmt.Errorf(format, args...))
}
