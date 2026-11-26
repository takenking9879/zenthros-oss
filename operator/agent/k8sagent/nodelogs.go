// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/takenking9879/zenthros-oss/operator/agent/logfile"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
)

// A container's log on its node, as the kubelet and containerd keep it:
// <root>/<namespace>_<pod>_<uid>/<container>/<restart>.log, in the CRI
// format, a line for each piece the container wrote ("<time> <stream> <P|F>
// <text>": P a piece of a line longer than 16 KiB, F the piece that ends
// it), and what the kubelet rotated beside it: <restart>.log.<YYYYMMDD-HHMMSS>,
// all but the newest of them gzipped. One file per restart of the container.
//
// A cursor is a file and a byte of it: the restart, the time of the file's
// first line (its name changes when it is rotated, and again when it is
// gzipped) and how far the read got, always the end of a whole line.

// PodLogRoot is where the kubelet keeps the pods' log files on a node.
const PodLogRoot = "/var/log/pods"

// nodeCursorPrefix starts a cursor of a read of the node's files;
// apiCursorPrefix one of a read through the Kubernetes API.
const (
	nodeCursorPrefix = "n/"
	apiCursorPrefix  = "a/"
)

// criReadBytes is the most of a file one read takes: whole lines of it,
// each under 16 KiB and its prefix.
const criReadBytes = 128 << 10

var criFileName = regexp.MustCompile(`^(\d+)\.log(\.\d{8}-\d{6})?(\.gz)?$`)

// PodLogDir is the directory of a container's log files under root. Each
// part is checked: the reader serves only the container a request names.
func PodLogDir(root, namespace, pod, uid, container string) (string, error) {
	for _, part := range []struct{ what, value string }{{"namespace", namespace}, {"container", container}} {
		if errs := validation.IsDNS1123Label(part.value); len(errs) > 0 {
			return "", fmt.Errorf("%w: %s %q: %s", backend.ErrInvalid, part.what, part.value, strings.Join(errs, "; "))
		}
	}
	if errs := validation.IsDNS1123Subdomain(pod); len(errs) > 0 {
		return "", fmt.Errorf("%w: pod %q: %s", backend.ErrInvalid, pod, strings.Join(errs, "; "))
	}
	if !podUID.MatchString(uid) {
		return "", fmt.Errorf("%w: pod UID %q", backend.ErrInvalid, uid)
	}
	return filepath.Join(root, namespace+"_"+pod+"_"+uid, container), nil
}

var podUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// criFile is one of a container's log files.
type criFile struct {
	name    string
	restart int
	// stamp is the rotation's; "" for the file being written.
	stamp string
	gz    bool
}

// listCRI is the container's files in the order they were written: by
// restart, the rotated ones by their stamp, the one being written last.
// A rotated file being gzipped is there twice for a moment: the plain one
// is kept.
func listCRI(dir string) ([]criFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, backend.Classified(backend.ErrNotFound, err)
		}
		return nil, err
	}
	var files []criFile
	for _, entry := range entries {
		m := criFileName.FindStringSubmatch(entry.Name())
		if m == nil {
			continue
		}
		restart, _ := strconv.Atoi(m[1])
		f := criFile{name: entry.Name(), restart: restart, stamp: strings.TrimPrefix(m[2], "."), gz: m[3] != ""}
		if f.gz && slices.ContainsFunc(entries, func(e os.DirEntry) bool { return e.Name() == strings.TrimSuffix(f.name, ".gz") }) {
			continue
		}
		files = append(files, f)
	}
	slices.SortFunc(files, func(a, b criFile) int {
		if a.restart != b.restart {
			return a.restart - b.restart
		}
		switch {
		case a.stamp == b.stamp:
			return 0
		case a.stamp == "":
			return 1
		case b.stamp == "":
			return -1
		}
		return strings.Compare(a.stamp, b.stamp)
	})
	return files, nil
}

// open opens f to read it from its start. A rotated name may have been
// gzipped since it was listed.
func (f criFile) open(dir string) (io.Reader, io.Closer, error) {
	file, err := os.Open(filepath.Join(dir, f.name))
	if errors.Is(err, os.ErrNotExist) && f.stamp != "" && !f.gz {
		f.name, f.gz = f.name+".gz", true
		file, err = os.Open(filepath.Join(dir, f.name))
	}
	if err != nil {
		return nil, nil, err
	}
	if !f.gz {
		return file, file, nil
	}
	zr, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return zr, file, nil
}

// firstTime is the time of f's first line: what names it while it moves.
// "" for a file with no whole line yet.
func (f criFile) firstTime(dir string) (string, error) {
	r, c, err := f.open(dir)
	if err != nil {
		return "", err
	}
	defer c.Close()
	head := make([]byte, 64)
	n, _ := io.ReadFull(r, head)
	ts, _, ok := bytes.Cut(head[:n], []byte(" "))
	if !ok {
		return "", nil
	}
	return string(ts), nil
}

// criText is the job's bytes in whole CRI lines: each piece's text, and a
// newline after the piece that ends a line.
func criText(lines []byte) []byte {
	out := make([]byte, 0, len(lines))
	for len(lines) > 0 {
		line, rest, _ := bytes.Cut(lines, []byte("\n"))
		lines = rest
		text, full := criLine(line)
		out = append(out, text...)
		if full {
			out = append(out, '\n')
		}
	}
	return out
}

// criLine is one CRI line's text, and whether its piece ends a line.
func criLine(line []byte) ([]byte, bool) {
	_, rest, _ := bytes.Cut(line, []byte(" ")) // the time
	_, rest, _ = bytes.Cut(rest, []byte(" "))  // stdout or stderr
	tag, text, _ := bytes.Cut(rest, []byte(" "))
	return text, len(tag) == 0 || tag[0] != 'P'
}

// scanCRI calls fn with each whole line of r: where it starts and ends, the
// bytes of the job's it holds, and whether it ends a line of the job's. fn
// returning false stops it.
func scanCRI(r io.Reader, fn func(off, end, text int64, full bool) bool) error {
	br := bufio.NewReaderSize(r, 64<<10)
	var off int64
	var long []byte
	for {
		line, err := br.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			long = append(long, line...)
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil // a line not whole yet is not read
			}
			return err
		}
		if long != nil {
			line, long = append(long, line...), nil
		}
		text, full := criLine(line[:len(line)-1])
		n := int64(len(text))
		if full {
			n++
		}
		if !fn(off, off+int64(len(line)), n, full) {
			return nil
		}
		off += int64(len(line))
	}
}

// nodeCursor is where a read of a container's files goes on.
type nodeCursor struct {
	restart int
	first   string // the time of the file's first line
	off     int64
}

func (c nodeCursor) String() string {
	return nodeCursorPrefix + strconv.Itoa(c.restart) + "/" + c.first + "/" + strconv.FormatInt(c.off, 10)
}

func parseNodeCursor(s string) (nodeCursor, error) {
	parts := strings.Split(strings.TrimPrefix(s, nodeCursorPrefix), "/")
	if !strings.HasPrefix(s, nodeCursorPrefix) || len(parts) != 3 {
		return nodeCursor{}, backend.Classified(backend.ErrInvalid, fmt.Errorf("%w: %q", logfile.ErrCursor, s))
	}
	restart, err1 := strconv.Atoi(parts[0])
	off, err2 := strconv.ParseInt(parts[2], 10, 64)
	if _, err3 := time.Parse(time.RFC3339Nano, parts[1]); err1 != nil || err2 != nil || err3 != nil || restart < 0 || off < 0 {
		return nodeCursor{}, backend.Classified(backend.ErrInvalid, fmt.Errorf("%w: %q", logfile.ErrCursor, s))
	}
	return nodeCursor{restart: restart, first: parts[1], off: off}, nil
}

// laterThan is whether a file of restart whose first line is at first was
// written after one of c's.
func (c nodeCursor) laterThan(restart int, first string) bool {
	if restart != c.restart {
		return restart > c.restart
	}
	if first == "" {
		return true // the file being written, with nothing in it yet
	}
	at, err1 := time.Parse(time.RFC3339Nano, first)
	was, err2 := time.Parse(time.RFC3339Nano, c.first)
	return err1 == nil && err2 == nil && at.After(was)
}

// ReadPodLog reads a container's log files in dir as req asks: without
// Follow to the end of what they hold; with Follow until ctx ends, through
// the kubelet's rotations and the container's restarts.
func ReadPodLog(ctx context.Context, dir string, req backend.LogRequest) (<-chan backend.LogChunk, error) {
	r := &criReader{dir: dir, ids: map[string]string{}, buf: make([]byte, criReadBytes), first: true}
	// Watched before the first read: a write between that read and the
	// watch would wake nobody.
	var watcher *fsnotify.Watcher
	if req.Follow {
		var err error
		if watcher, err = fsnotify.NewWatcher(); err == nil {
			err = watcher.Add(dir)
		}
		if err != nil {
			if watcher != nil {
				watcher.Close()
			}
			if errors.Is(err, os.ErrNotExist) {
				return nil, backend.Classified(backend.ErrNotFound, err)
			}
			return nil, fmt.Errorf("watch %s: %w", dir, err)
		}
	}
	if err := r.start(req); err != nil {
		if watcher != nil {
			watcher.Close()
		}
		return nil, err
	}
	out := make(chan backend.LogChunk, 4)
	go func() {
		defer close(out)
		defer r.close()
		if watcher != nil {
			defer watcher.Close()
		}
		r.run(ctx, watcher, out)
	}()
	return out, nil
}

// criReader is one read of a container's files.
type criReader struct {
	dir string
	// ids is the time of each rotated file's first line, by name: the name
	// of the file being written is the next one's after a rotation.
	ids map[string]string
	// cur is the file being read, from src, of which off bytes were read.
	cur     criFile
	curID   string
	src     io.Reader
	closer  io.Closer
	off     int64
	pending []byte
	buf     []byte
	// gap is what never comes right before the next chunk, and omitted,
	// until the first chunk, what the history left out.
	gap, omitted int64
	first        bool
	// placed, a test's, runs once the read's place is found, before its
	// file is opened.
	placed func()
}

func (r *criReader) close() {
	if r.closer != nil {
		r.closer.Close()
	}
}

func (r *criReader) id(f criFile) (string, error) {
	if id, ok := r.ids[f.name]; ok {
		return id, nil
	}
	id, err := f.firstTime(r.dir)
	if err != nil || id == "" || f.stamp == "" {
		return id, err
	}
	r.ids[f.name] = id
	return id, nil
}

// openAt makes f the file read, from byte off.
func (r *criReader) openAt(f criFile, off int64) error {
	src, closer, err := f.open(r.dir)
	if err != nil {
		return err
	}
	if off > 0 {
		if file, ok := src.(*os.File); ok {
			_, err = file.Seek(off, io.SeekStart)
		} else {
			_, err = io.CopyN(io.Discard, src, off)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			closer.Close()
			return err
		}
	}
	r.close()
	r.cur, r.src, r.closer, r.off, r.pending = f, src, closer, off, nil
	if file, ok := src.(*os.File); ok {
		// From the file opened: its name may be the next file's already.
		head := make([]byte, 64)
		n, _ := file.ReadAt(head, 0)
		ts, _, whole := bytes.Cut(head[:n], []byte(" "))
		r.curID = ""
		if whole {
			r.curID = string(ts)
		}
		return nil
	}
	r.curID, err = r.id(f)
	return err
}

// errRotated is a file the kubelet rotated while a read was placed in it:
// the read's place was counted in the file before.
var errRotated = errors.New("the log's file rotated while its read was placed in it")

// start places the read: after a cursor, or at the start of its history.
// The kubelet may rotate the file being written between the place's count
// and its opening: the file opened is checked by its first line, and the
// read placed again in the files as they are then.
func (r *criReader) start(req backend.LogRequest) error {
	var err error
	for range 3 {
		if err = r.place(req); !errors.Is(err, errRotated) {
			return err
		}
	}
	return backend.Classified(backend.ErrBackendUnavailable, fmt.Errorf("%s: %w three times", r.dir, err))
}

func (r *criReader) place(req backend.LogRequest) error {
	files, err := listCRI(r.dir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return backend.Classified(backend.ErrNotFound, fmt.Errorf("no log files in %s yet", r.dir))
	}
	if req.After != "" {
		return r.resume(files, req.After)
	}
	// The first line of each file being written, before its count.
	was := map[string]string{}
	for _, f := range files {
		if f.stamp == "" {
			if id, err := f.firstTime(r.dir); err == nil {
				was[f.name] = id
			}
		}
	}
	at, off, omitted, err := r.history(files, req)
	if err != nil {
		return err
	}
	if r.placed != nil {
		r.placed()
	}
	if err := r.openAt(files[at], off); err != nil {
		return err
	}
	if id := was[files[at].name]; id != "" && r.curID != id {
		return errRotated
	}
	r.omitted = omitted
	return nil
}

// resume places the read right after cursor. A file the kubelet removed
// since is a gap of a size no one knows.
func (r *criReader) resume(files []criFile, cursor string) error {
	c, err := parseNodeCursor(cursor)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.restart != c.restart {
			continue
		}
		if id, err := r.id(f); err == nil && id == c.first {
			if err := r.openAt(f, c.off); err != nil {
				return err
			}
			if r.curID != c.first {
				return errRotated
			}
			return nil
		}
	}
	for _, f := range files {
		id, err := r.id(f)
		if err == nil && c.laterThan(f.restart, id) {
			r.gap = backend.Uncounted
			return r.openAt(f, 0)
		}
	}
	return backend.Classified(backend.ErrInvalid, fmt.Errorf("%w: %q", logfile.ErrCursor, cursor))
}

// criStats is what one scan of a file says.
type criStats struct {
	text  int64 // the job's bytes in it
	lines int   // the lines of the job's it ends
	// lastFull is whether its last line ends a line of the job's.
	lastFull bool
	// end is where its last whole line ends.
	end int64
}

func (r *criReader) stats(f criFile) (criStats, error) {
	src, closer, err := f.open(r.dir)
	if err != nil {
		return criStats{}, err
	}
	defer closer.Close()
	var s criStats
	err = scanCRI(src, func(_, end, text int64, full bool) bool {
		s.text += text
		s.lastFull = full
		if full {
			s.lines++
		}
		s.end = end
		return true
	})
	return s, err
}

// history is where the read's history starts: the file and byte, and the
// job's bytes before it in the files the node keeps.
func (r *criReader) history(files []criFile, req backend.LogRequest) (int, int64, int64, error) {
	if req.TailLines < 0 && req.History() == backend.AllHistory {
		return 0, 0, 0, nil
	}
	stats := make([]criStats, len(files))
	scanned := make([]bool, len(files))
	stat := func(i int) error {
		if scanned[i] {
			return nil
		}
		s, err := r.stats(files[i])
		stats[i], scanned[i] = s, err == nil
		return err
	}
	// The file the history starts in, newest first, and what of the job's
	// it must hold to its end: bytes, or lines past the one being ended.
	var (
		at                     = -1
		needBytes, needLines   int64
		lastFull               bool
		laterBytes, laterLines int64
		byLines                = req.TailLines >= 0
		history                = req.History()
	)
	for i := len(files) - 1; i >= 0; i-- {
		if err := stat(i); err != nil {
			return 0, 0, 0, err
		}
		s := stats[i]
		if i == len(files)-1 {
			lastFull = s.lastFull
		}
		if byLines {
			want := int64(req.TailLines)
			if lastFull {
				want++ // the newline that ends the last line starts none
			}
			if req.TailLines == 0 {
				omitted, err := r.before(files, stats, scanned, len(files)-1, s.text)
				return len(files) - 1, s.end, omitted, err
			}
			if laterLines+int64(s.lines) >= want {
				at, needLines = i, want-laterLines
				break
			}
		} else if laterBytes+s.text > history {
			at, needBytes = i, history-laterBytes
			break
		}
		laterBytes += s.text
		laterLines += int64(s.lines)
	}
	if at < 0 {
		return 0, 0, 0, nil
	}
	off, inFile, err := r.startIn(files[at], stats[at], byLines, needBytes, needLines)
	if err != nil {
		return 0, 0, 0, err
	}
	omitted, err := r.before(files, stats, scanned, at, inFile)
	return at, off, omitted, err
}

// startIn is where in f, of the scan s, the history starts: the first line
// start from which f holds at most needBytes of the job's to its end, or
// right after the line needLines from its end. It returns the byte, and the
// job's bytes in f before it.
func (r *criReader) startIn(f criFile, s criStats, byLines bool, needBytes, needLines int64) (int64, int64, error) {
	src, closer, err := f.open(r.dir)
	if err != nil {
		return 0, 0, err
	}
	defer closer.Close()
	var (
		before    int64
		start     = s.end
		startText = s.text
		lineStart = true // the file's first line is taken for one
		ended     int64
		// The lines of the job's f ends before the start: up to the one
		// needLines from its end.
		skipLines = int64(s.lines) - needLines + 1
	)
	err = scanCRI(src, func(off, _, text int64, full bool) bool {
		if lineStart {
			if byLines && ended == skipLines || !byLines && s.text-before <= needBytes {
				start, startText = off, before
				return false
			}
		}
		before += text
		lineStart = full
		if full {
			ended++
		}
		return true
	})
	return start, startText, err
}

// before is the job's bytes in the files before files[at], and inFile.
func (r *criReader) before(files []criFile, stats []criStats, scanned []bool, at int, inFile int64) (int64, error) {
	total := inFile
	for i := 0; i < at; i++ {
		if !scanned[i] {
			s, err := r.stats(files[i])
			if err != nil {
				return 0, err
			}
			stats[i], scanned[i] = s, true
		}
		total += stats[i].text
	}
	return total, nil
}

func (r *criReader) run(ctx context.Context, watcher *fsnotify.Watcher, out chan<- backend.LogChunk) {
	var wait *time.Timer
	defer func() {
		if wait != nil {
			wait.Stop()
		}
	}()
	pause := func() bool {
		if wait == nil {
			wait = time.NewTimer(logfile.Pace)
		} else {
			wait.Reset(logfile.Pace)
		}
		select {
		case <-ctx.Done():
			return false
		case <-wait.C:
			return true
		}
	}
	for {
		got, full, err := r.next(ctx, out)
		if err != nil {
			return
		}
		if full {
			continue // more is there already
		}
		if got > 0 {
			if watcher == nil {
				continue
			}
			if !pause() {
				return
			}
			continue
		}
		// At the end of the file: the next one, if the kubelet rotated it
		// or the container restarted.
		next, ok, err := r.nextFile()
		if err != nil {
			return
		}
		if ok {
			if !r.cur.gz && watcher != nil {
				// What the runtime wrote to it before it opened the new one.
				if !pause() {
					return
				}
				if err := r.drain(ctx, out); err != nil {
					return
				}
			}
			if err := r.openAt(next, 0); err != nil {
				return
			}
			continue
		}
		if watcher == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
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

// drain reads the file being read to its end.
func (r *criReader) drain(ctx context.Context, out chan<- backend.LogChunk) error {
	for {
		got, full, err := r.next(ctx, out)
		if err != nil || got == 0 && !full {
			return err
		}
	}
}

// nextFile is the file written after the one being read, if there is one
// yet. One the kubelet removed meanwhile is a gap no one can count.
func (r *criReader) nextFile() (criFile, bool, error) {
	files, err := listCRI(r.dir)
	if err != nil {
		return criFile{}, false, err
	}
	if r.curID == "" {
		// Nothing in it yet, so the kubelet has not rotated it: what comes
		// after it is the next restart's.
		for _, f := range files {
			if f.restart > r.cur.restart {
				return f, true, nil
			}
		}
		return criFile{}, false, nil
	}
	c := nodeCursor{restart: r.cur.restart, first: r.curID}
	found := false
	for _, f := range files {
		id, err := r.id(f)
		if err != nil {
			continue // removed meanwhile
		}
		if f.restart == c.restart && id == c.first {
			found = true
			continue
		}
		if c.laterThan(f.restart, id) {
			if !found && f.restart == c.restart {
				r.gap = backend.Uncounted
			}
			return f, true, nil
		}
	}
	return criFile{}, false, nil
}

// next reads what is there of the file being read, up to a chunk of whole
// lines, and sends it. It returns how much it read, and whether it filled
// its buffer (more is there already); an error ends the read.
func (r *criReader) next(ctx context.Context, out chan<- backend.LogChunk) (int, bool, error) {
	n := copy(r.buf, r.pending)
	got, err := r.src.Read(r.buf[n:])
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, false, err
	}
	n += got
	full := n == len(r.buf)
	end := bytes.LastIndexByte(r.buf[:n], '\n') + 1
	r.pending = append(r.pending[:0], r.buf[end:n]...)
	if end == 0 {
		if full {
			return 0, false, fmt.Errorf("a line of %s past %d bytes", r.cur.name, len(r.buf))
		}
		return got, false, nil
	}
	if r.curID == "" {
		ts, _, _ := bytes.Cut(r.buf[:end], []byte(" "))
		r.curID = string(ts)
	}
	r.off += int64(end)
	chunk := backend.LogChunk{
		Stream:    "stdout",
		Data:      criText(r.buf[:end]),
		Timestamp: time.Now(),
		Cursor:    nodeCursor{restart: r.cur.restart, first: r.curID, off: r.off}.String(),
		Gap:       r.gap,
		GapReason: backend.GapFreed,
	}
	if r.first {
		chunk.Omitted, r.first = r.omitted, false
	}
	select {
	case out <- chunk:
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
	r.gap = 0
	return got, full, nil
}
