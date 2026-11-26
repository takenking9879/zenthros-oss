// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
)

// podLog writes a container's log files as containerd and the kubelet do.
type podLog struct {
	t   *testing.T
	dir string
	at  time.Time
}

func newPodLog(t *testing.T) *podLog {
	dir := filepath.Join(t.TempDir(), "ns_pod_0b5f6c56-1d2e-4c8f-9a3b-2f4e6d8c0a1b", "main")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return &podLog{t: t, dir: dir, at: time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)}
}

// write appends lines of the job's to the container's file of restart, one
// CRI line each (tag F), or the pieces given with their tags.
func (p *podLog) write(restart int, lines ...string) {
	p.t.Helper()
	f, err := os.OpenFile(filepath.Join(p.dir, fmt.Sprintf("%d.log", restart)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		p.t.Fatal(err)
	}
	defer f.Close()
	for _, line := range lines {
		tag, text := "F", line
		if piece, ok := strings.CutPrefix(line, "P:"); ok {
			tag, text = "P", piece
		}
		p.at = p.at.Add(time.Millisecond + time.Nanosecond)
		if _, err := fmt.Fprintf(f, "%s stdout %s %s\n", p.at.Format(time.RFC3339Nano), tag, text); err != nil {
			p.t.Fatal(err)
		}
	}
}

// rotate does what the kubelet does past a file's size: the file being
// written gets its stamp, and the runtime opens a new one.
func (p *podLog) rotate(restart int, stamp string) string {
	p.t.Helper()
	rotated := filepath.Join(p.dir, fmt.Sprintf("%d.log.%s", restart, stamp))
	if err := os.Rename(filepath.Join(p.dir, fmt.Sprintf("%d.log", restart)), rotated); err != nil {
		p.t.Fatal(err)
	}
	return rotated
}

// gzip does what the kubelet does to the older rotated files.
func (p *podLog) gzip(path string) {
	p.t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		p.t.Fatal(err)
	}
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	zw.Write(raw)
	zw.Close()
	if err := os.WriteFile(path+".gz", b.Bytes(), 0o640); err != nil {
		p.t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		p.t.Fatal(err)
	}
}

func lines(from, to int) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, fmt.Sprintf("line %04d", i))
	}
	return out
}

func text(from, to int) string {
	return strings.Join(lines(from, to), "\n") + "\n"
}

// collect reads chunks until want bytes of the log came, or a timeout.
func collect(t *testing.T, chunks <-chan backend.LogChunk, want int) ([]backend.LogChunk, string) {
	t.Helper()
	var got []backend.LogChunk
	var b strings.Builder
	timeout := time.After(10 * time.Second)
	for b.Len() < want {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				return got, b.String()
			}
			got = append(got, chunk)
			b.Write(chunk.Data)
		case <-timeout:
			t.Fatalf("after 10 s: %d bytes of %d: %q", b.Len(), want, b.String())
		}
	}
	return got, b.String()
}

func readAll(t *testing.T, dir string, req backend.LogRequest) ([]backend.LogChunk, string) {
	t.Helper()
	chunks, err := ReadPodLog(context.Background(), dir, req)
	if err != nil {
		t.Fatal(err)
	}
	return collect(t, chunks, 1<<30)
}

// A reader that only opened 0.log again after a rotation skipped a whole
// file when the kubelet rotated twice between its reads (the lab's
// TestPodFileFollowsRotation). It reads, in order, every file written after
// the one it read, gzipped meanwhile or not, and the file of the
// container's next restart.
func TestPodFileFollowsRotation(t *testing.T) {
	p := newPodLog(t)
	p.write(0, lines(0, 10)...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chunks, err := ReadPodLog(ctx, p.dir, backend.LogRequest{Follow: true, HistoryBytes: backend.AllHistory, TailLines: -1})
	if err != nil {
		t.Fatal(err)
	}
	_, got := collect(t, chunks, len(text(0, 10)))

	p.write(0, lines(10, 20)...)
	first := p.rotate(0, "20261114-100001")
	p.write(0, lines(20, 30)...)
	p.rotate(0, "20261114-100002")
	p.gzip(first)
	p.write(0, lines(30, 40)...)
	p.write(1, lines(40, 50)...)
	_, more := collect(t, chunks, len(text(10, 50)))
	if got+more != text(0, 50) {
		t.Fatalf("read %q", got+more)
	}
}

// containerd splits a line past 16 KiB into pieces: P, and F the last.
func TestPodLogJoinsALinesPieces(t *testing.T) {
	p := newPodLog(t)
	p.write(0, "before", "P:one ", "P:two ", "three", "after")
	if _, got := readAll(t, p.dir, backend.LogRequest{TailLines: -1, HistoryBytes: backend.AllHistory}); got != "before\none two three\nafter\n" {
		t.Fatalf("read %q", got)
	}
}

// The history is the end of the log from the start of a line, through the
// files the node keeps; the first chunk says how much of them it left out.
func TestPodLogHistoryStartsAtALine(t *testing.T) {
	p := newPodLog(t)
	p.write(0, lines(0, 10)...)
	p.gzip(p.rotate(0, "20261114-100001"))
	p.write(0, lines(10, 20)...)
	p.rotate(0, "20261114-100002")
	p.write(0, lines(20, 30)...)
	line := int64(len("line 0000\n"))

	for _, tc := range []struct {
		name    string
		req     backend.LogRequest
		from    int
		omitted int64
	}{
		{"bytes within the newest file", backend.LogRequest{TailLines: -1, HistoryBytes: 5*line + 3}, 25, 25 * line},
		{"bytes into an older file", backend.LogRequest{TailLines: -1, HistoryBytes: 15 * line}, 15, 15 * line},
		{"bytes into the gzipped one", backend.LogRequest{TailLines: -1, HistoryBytes: 25*line + 1}, 5, 5 * line},
		{"more than there is", backend.LogRequest{TailLines: -1, HistoryBytes: 1 << 20}, 0, 0},
		{"lines", backend.LogRequest{TailLines: 12}, 18, 18 * line},
		{"all", backend.LogRequest{TailLines: -1, HistoryBytes: backend.AllHistory}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks, got := readAll(t, p.dir, tc.req)
			if got != text(tc.from, 30) {
				t.Fatalf("read %q", got)
			}
			if chunks[0].Omitted != tc.omitted {
				t.Fatalf("omitted %d, want %d", chunks[0].Omitted, tc.omitted)
			}
		})
	}
}

// tail 0 sees only what comes next, and says all that came before.
func TestPodLogTailZeroSeesOnlyWhatComesNext(t *testing.T) {
	p := newPodLog(t)
	p.write(0, lines(0, 10)...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chunks, err := ReadPodLog(ctx, p.dir, backend.LogRequest{Follow: true, TailLines: 0})
	if err != nil {
		t.Fatal(err)
	}
	p.write(0, lines(10, 12)...)
	got, read := collect(t, chunks, len(text(10, 12)))
	if read != text(10, 12) || got[0].Omitted != int64(len(text(0, 10))) {
		t.Fatalf("read %q, omitted %d", read, got[0].Omitted)
	}
}

// A read opened again after its cursor goes on with the next byte, in the
// file the cursor was of after the kubelet rotated and gzipped it.
func TestPodLogGoesOnAfterItsCursor(t *testing.T) {
	p := newPodLog(t)
	p.write(0, lines(0, 10)...)
	chunks, first := readAll(t, p.dir, backend.LogRequest{TailLines: -1, HistoryBytes: backend.AllHistory})
	cursor := chunks[len(chunks)-1].Cursor
	if !strings.HasPrefix(cursor, nodeCursorPrefix) {
		t.Fatalf("cursor %q", cursor)
	}
	p.write(0, lines(10, 20)...)
	rotated := p.rotate(0, "20261114-100001")
	p.write(0, lines(20, 30)...)
	p.rotate(0, "20261114-100002")
	p.gzip(rotated)
	p.write(0, lines(30, 35)...)

	resumed, rest := readAll(t, p.dir, backend.LogRequest{After: cursor, TailLines: -1})
	if first+rest != text(0, 35) {
		t.Fatalf("read %q then %q", first, rest)
	}
	if resumed[0].Gap != 0 || resumed[0].Omitted != 0 {
		t.Fatalf("first chunk after the cursor: %+v", resumed[0])
	}
}

// A file the kubelet removed after the cursor was given is a gap no one can
// count; the read goes on with the next file.
func TestPodLogSaysWhenTheCursorsFileIsGone(t *testing.T) {
	p := newPodLog(t)
	p.write(0, lines(0, 10)...)
	chunks, _ := readAll(t, p.dir, backend.LogRequest{TailLines: -1, HistoryBytes: backend.AllHistory})
	cursor := chunks[len(chunks)-1].Cursor
	p.write(0, lines(10, 20)...)
	if err := os.Remove(p.rotate(0, "20261114-100001")); err != nil {
		t.Fatal(err)
	}
	p.write(0, lines(20, 30)...)

	resumed, rest := readAll(t, p.dir, backend.LogRequest{After: cursor, TailLines: -1})
	if rest != text(20, 30) || resumed[0].Gap != backend.Uncounted {
		t.Fatalf("read %q, gap %d", rest, resumed[0].Gap)
	}
}

// The reader serves only the container a request names: each part of the
// path is checked.
func TestPodLogDirChecksEachPart(t *testing.T) {
	const uid = "0b5f6c56-1d2e-4c8f-9a3b-2f4e6d8c0a1b"
	if dir, err := PodLogDir("/var/log/pods", "ns", "pod-0", uid, "main"); err != nil || dir != "/var/log/pods/ns_pod-0_"+uid+"/main" {
		t.Fatalf("dir %q: %v", dir, err)
	}
	for _, parts := range [][4]string{
		{"..", "pod", uid, "main"},
		{"ns", "../../etc", uid, "main"},
		{"ns", "pod", "../x", "main"},
		{"ns", "pod", uid, "a/b"},
		{"ns", "pod", uid, ""},
		{"ns_x", "pod", uid, "main"},
	} {
		if _, err := PodLogDir("/var/log/pods", parts[0], parts[1], parts[2], parts[3]); !errors.Is(err, backend.ErrInvalid) {
			t.Errorf("%v: %v", parts, err)
		}
	}
}

// A container that restarted before it wrote anything: its next restart's
// file is read.
func TestPodLogGoesOnFromAnEmptyFileToTheNextRestart(t *testing.T) {
	p := newPodLog(t)
	if err := os.WriteFile(filepath.Join(p.dir, "0.log"), nil, 0o640); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chunks, err := ReadPodLog(ctx, p.dir, backend.LogRequest{Follow: true, TailLines: -1})
	if err != nil {
		t.Fatal(err)
	}
	p.write(1, lines(0, 3)...)
	if _, got := collect(t, chunks, len(text(0, 3))); got != text(0, 3) {
		t.Fatalf("read %q", got)
	}
}

// The kubelet may rotate the file being written between a history's count
// and its opening: the read is placed again in the files as they are then,
// never at a byte of the new file counted in the old one.
func TestPodLogHistoryIsPlacedAgainAfterARotation(t *testing.T) {
	p := newPodLog(t)
	p.write(0, lines(0, 30)...)
	line := int64(len("line 0000\n"))
	r := &criReader{dir: p.dir, ids: map[string]string{}, buf: make([]byte, criReadBytes), first: true}
	rotations := 0
	r.placed = func() {
		if rotations++; rotations == 1 {
			p.rotate(0, "20261114-100001")
			p.write(0, lines(30, 32)...)
		}
	}
	if err := r.start(backend.LogRequest{TailLines: -1, HistoryBytes: 5 * line}); err != nil {
		t.Fatal(err)
	}
	out := make(chan backend.LogChunk, 16)
	go func() {
		defer close(out)
		defer r.close()
		r.run(context.Background(), nil, out)
	}()
	chunks, got := collect(t, out, 1<<30)
	if got != text(27, 32) {
		t.Fatalf("read %q, want lines 27 to 31", got)
	}
	if chunks[0].Omitted != 27*line {
		t.Fatalf("%d omitted, want %d", chunks[0].Omitted, 27*line)
	}
}
