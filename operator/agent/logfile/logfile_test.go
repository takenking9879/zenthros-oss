// SPDX-License-Identifier: Apache-2.0

package logfile

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
)

// line is the n-th line a test job writes: its content says where it is.
func line(n int) string { return fmt.Sprintf("line %07d %s\n", n, strings.Repeat("x", 40)) }

func writeLines(t *testing.T, path string, from, to int) {
	t.Helper()
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	var b strings.Builder
	for i := from; i < to; i++ {
		b.WriteString(line(i))
	}
	if _, err := out.WriteString(b.String()); err != nil {
		t.Fatal(err)
	}
}

func testKeeper(t *testing.T, max int64) *Keeper {
	k := newKeeper(max, logr.Discard())
	t.Cleanup(k.Close)
	return k
}

// collect reads everything a read sends until it closes or the test's
// deadline, and every chunk as it came.
func collect(t *testing.T, ch <-chan backend.LogChunk, within time.Duration) []backend.LogChunk {
	t.Helper()
	var chunks []backend.LogChunk
	deadline := time.After(within)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return chunks
			}
			chunks = append(chunks, c)
		case <-deadline:
			t.Fatalf("the read did not end within %s (%d chunks)", within, len(chunks))
		}
	}
}

func joined(chunks []backend.LogChunk) string {
	var b strings.Builder
	for _, c := range chunks {
		b.Write(c.Data)
	}
	return b.String()
}

func closed() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// The history is the end of the log from the start of a line, and the first
// chunk says what it left out.
func TestTheHistoryStartsAtALine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	writeLines(t, path, 0, 1000)
	f, err := testKeeper(t, 0).File(path)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := f.Read(context.Background(), backend.LogRequest{TailLines: -1, HistoryBytes: 10_000}, closed())
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch, 5*time.Second)
	got := joined(chunks)
	if len(got) > 10_000 || !strings.HasPrefix(got, "line ") || !strings.HasSuffix(got, line(999)) {
		t.Fatalf("history of %d bytes, starting %q", len(got), got[:min(len(got), 40)])
	}
	size := int64(1000 * len(line(0)))
	if chunks[0].Omitted != size-int64(len(got)) {
		t.Fatalf("omitted %d, want %d", chunks[0].Omitted, size-int64(len(got)))
	}
	if chunks[len(chunks)-1].Cursor != strconv.FormatInt(size, 10) {
		t.Fatalf("the last cursor is %q, the file %d bytes", chunks[len(chunks)-1].Cursor, size)
	}

	ch, err = f.Read(context.Background(), backend.LogRequest{TailLines: 3}, closed())
	if err != nil {
		t.Fatal(err)
	}
	if got := joined(collect(t, ch, 5*time.Second)); got != line(997)+line(998)+line(999) {
		t.Fatalf("tail 3: %q", got)
	}
}

// A read that goes on from a cursor gets every byte once: what came before
// it never again, what came after it all.
func TestAReadAfterACursorGetsEachByteOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	writeLines(t, path, 0, 100)
	f, err := testKeeper(t, 0).File(path)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := f.Read(context.Background(), backend.LogRequest{TailLines: -1, HistoryBytes: backend.AllHistory}, closed())
	if err != nil {
		t.Fatal(err)
	}
	first := collect(t, ch, 5*time.Second)
	cursor := first[len(first)-1].Cursor
	// Half a line, then the rest: a cursor can be anywhere.
	out, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	_, _ = out.WriteString("half ")
	out.Close()
	ch, err = f.Read(context.Background(), backend.LogRequest{After: cursor}, closed())
	if err != nil {
		t.Fatal(err)
	}
	second := collect(t, ch, 5*time.Second)
	writeLines(t, path, 100, 200)
	ch, err = f.Read(context.Background(), backend.LogRequest{After: second[len(second)-1].Cursor}, closed())
	if err != nil {
		t.Fatal(err)
	}
	third := collect(t, ch, 5*time.Second)
	var want strings.Builder
	for i := 0; i < 100; i++ {
		want.WriteString(line(i))
	}
	want.WriteString("half ")
	for i := 100; i < 200; i++ {
		want.WriteString(line(i))
	}
	if got := joined(first) + joined(second) + joined(third); got != want.String() {
		t.Fatalf("%d bytes over three reads, want %d", len(got), want.Len())
	}
	if _, err := f.Read(context.Background(), backend.LogRequest{After: "nope"}, closed()); err == nil {
		t.Fatal("a cursor that is not one was taken")
	}
}

// A following read is woken by the write, not by asking: a line after a
// quiet moment comes well before the pace.
func TestAFollowerWakesOnTheWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	writeLines(t, path, 0, 1)
	f, err := testKeeper(t, 0).File(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ended := make(chan struct{})
	ch, err := f.Read(ctx, backend.LogRequest{Follow: true, TailLines: -1}, ended)
	if err != nil {
		t.Fatal(err)
	}
	<-ch                 // the history
	time.Sleep(2 * Pace) // quiet
	start := time.Now()
	writeLines(t, path, 1, 2)
	select {
	case c := <-ch:
		if string(c.Data) != line(1) {
			t.Fatalf("got %q", c.Data)
		}
		if late := time.Since(start); late >= Pace {
			t.Fatalf("the line came %s after its write", late)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the write woke nobody")
	}
	// The end comes from the executor, and what was written before it is
	// read.
	writeLines(t, path, 2, 3)
	close(ended)
	if got := joined(collect(t, ch, 5*time.Second)); got != line(2) {
		t.Fatalf("after the end: %q", got)
	}
}

// tail 0 is no history: a follower sees only what the job writes next.
func TestTailZeroSeesOnlyWhatComesNext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	writeLines(t, path, 0, 100)
	f, err := testKeeper(t, 0).File(path)
	if err != nil {
		t.Fatal(err)
	}
	ended := make(chan struct{})
	ch, err := f.Read(context.Background(), backend.LogRequest{Follow: true, TailLines: 0}, ended)
	if err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, 100, 102)
	time.Sleep(2 * Pace)
	close(ended)
	chunks := collect(t, ch, 5*time.Second)
	if got := joined(chunks); got != line(100)+line(101) {
		t.Fatalf("read %q", got)
	}
	if chunks[0].Omitted != int64(100*len(line(0))) {
		t.Fatalf("omitted %d", chunks[0].Omitted)
	}
}

// The keeper frees what is older than the cap in whole blocks, and keeps
// the file's size: every byte keeps its position.
func TestTheKeeperFreesTheOldPart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	writeLines(t, path, 0, 20_000) // ~1 MiB
	k := testKeeper(t, 256<<10)
	f, err := k.File(path)
	if err != nil {
		t.Fatal(err)
	}
	k.trimAll()
	info, _ := os.Stat(path)
	floor := f.Floor()
	if floor == 0 || floor%block != 0 || info.Size()-floor < 256<<10 || info.Size()-floor >= 256<<10+block {
		t.Fatalf("size %d, floor %d", info.Size(), floor)
	}
	if blocks := info.Sys().(*syscall.Stat_t).Blocks * 512; blocks > 300<<10 {
		t.Fatalf("%d bytes on disk after the trim", blocks)
	}
	// An agent that comes back finds where the kept part starts.
	again, err := testKeeper(t, 256<<10).File(path)
	if err != nil {
		t.Fatal(err)
	}
	if again.Floor() != floor {
		t.Fatalf("a new keeper starts at %d, the old one freed to %d", again.Floor(), floor)
	}
	// The failure message reads above it.
	if tail := again.Tail(2); tail != strings.TrimSpace(line(19998))+" | "+strings.TrimSpace(line(19999)) {
		t.Fatalf("tail %q", tail)
	}
}

// The floor is stored before the bytes under it are freed: a reader that
// loads it after a read that met the hole drops what it read.
func TestTheFloorMovesBeforeTheHole(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	writeLines(t, path, 0, 20_000)
	k := testKeeper(t, 256<<10)
	f, err := k.File(path)
	if err != nil {
		t.Fatal(err)
	}
	was := punch
	defer func() { punch = was }()
	var seen int64 = -1
	punch = func(fd int, off, n int64) error {
		seen = f.Floor()
		if seen != off+n {
			t.Errorf("the hole [%d, %d) is punched with the floor at %d", off, off+n, seen)
		}
		return was(fd, off, n)
	}
	k.trimAll()
	if seen < 0 {
		t.Fatal("nothing was freed")
	}
}

// A read behind the floor, a reconnect late or one the trim overtook, never
// gets the zeros: it gets a gap, then the log from the next line.
func TestAReadBehindTheFloorGetsAGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	writeLines(t, path, 0, 20_000)
	k := testKeeper(t, 256<<10)
	f, err := k.File(path)
	if err != nil {
		t.Fatal(err)
	}
	k.trimAll()
	ch, err := f.Read(context.Background(), backend.LogRequest{After: "100"}, closed())
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch, 5*time.Second)
	got := joined(chunks)
	if bytes.IndexByte([]byte(got), 0) >= 0 {
		t.Fatal("a reader got the freed zeros")
	}
	if chunks[0].Gap < f.Floor()-100 || chunks[0].GapReason != backend.GapFreed || !strings.HasPrefix(got, "line ") {
		t.Fatalf("gap %d (floor %d), starting %q", chunks[0].Gap, f.Floor(), got[:40])
	}
	if !strings.HasSuffix(got, line(19999)) {
		t.Fatal("the end is missing")
	}
}

// While the job writes and the keeper frees, a follower gets only bytes the
// job wrote, each where it wrote it, and says where it lost some.
func TestAFollowerUnderTheTrimGetsOnlyWhatWasWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	writeLines(t, path, 0, 1)
	k := testKeeper(t, 64<<10)
	f, err := k.File(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ended := make(chan struct{})
	ch, err := f.Read(ctx, backend.LogRequest{Follow: true, TailLines: -1, HistoryBytes: backend.AllHistory}, ended)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer close(ended)
		for i := 1; i < 40_000; i += 200 {
			writeLines(t, path, i, i+200)
		}
	}()
	stop := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				k.trimAll()
			}
		}
	}()
	var gaps int64
	for c := range ch {
		gaps += c.Gap
		end, _ := strconv.ParseInt(c.Cursor, 10, 64)
		start := end - int64(len(c.Data))
		// Every line is the same length: a byte's position says what it is.
		size := int64(len(line(0)))
		for i, b := range c.Data {
			pos := start + int64(i)
			if want := line(int(pos / size))[pos%size]; b != want {
				t.Fatalf("byte %d is %q, the job wrote %q there", pos, b, want)
			}
		}
	}
	close(stop)
	wg.Wait()
	if gaps == 0 {
		t.Log("the reader kept up with the trim")
	}
}

// Prune keeps the newest files and those still in use.
func TestPruneKeepsTheNewestAndTheLive(t *testing.T) {
	dir := t.TempDir()
	k := testKeeper(t, 0)
	now := time.Now()
	for i := 0; i < 5; i++ {
		path := filepath.Join(dir, fmt.Sprintf("job-%d.log", i))
		writeLines(t, path, 0, 1)
		_ = os.Chtimes(path, now.Add(time.Duration(i)*time.Minute), now.Add(time.Duration(i)*time.Minute))
	}
	_ = os.WriteFile(filepath.Join(dir, "other"), nil, 0o600)
	k.Prune(dir, ".log", 2, func(name string) bool { return name == "job-0.log" })
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if strings.Join(names, " ") != "job-0.log job-3.log job-4.log other" {
		t.Fatalf("left %v", names)
	}
}
