// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"

	"github.com/zenthros/operator/agent/logfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/owner"
)

// Submit held the executor's lock through the job's git clone: Status and
// Cancel of every other job waited for it. A slow clone holds up no one else.
func TestProcagentSubmitDoesNotBlockStatusDuringClone(t *testing.T) {
	bin := t.TempDir()
	fakeGit := "#!/bin/sh\nsleep 2\nfor last; do :; done\nmkdir -p \"$last\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(fakeGit), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	e := newTestExecutor(t, "")
	other, err := e.Submit(context.Background(), shellJob("other", "sleep 1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	slow := shellJob("cloning", "true")
	slow.Source = v1.Source{Git: "https://example.invalid/repo.git"}
	cloned := make(chan string, 1)
	go func() {
		handle, _ := e.Submit(context.Background(), slow, nil)
		cloned <- handle
	}()
	// Both jobs end before the test does: a job still writing its state
	// when the test's directory is removed failed the cleanup.
	defer func() {
		waitTerminal(t, e, other)
		if handle := <-cloned; handle != "" {
			waitTerminal(t, e, handle)
		}
	}()
	time.Sleep(200 * time.Millisecond)

	started := time.Now()
	if _, err := e.Status(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(started); waited > 500*time.Millisecond {
		t.Fatalf("Status of another job waited %s for a clone", waited)
	}
}

// A noisy job's log grew without bound and filled the 10 GB overlay of a
// RunPod or Vast.ai container, where the agent's certificate lives. Then it
// stopped at a cap and dropped what came after it: the end of a log, its
// error, was what was lost (P11). The file keeps its end, and the disk
// holds no more than the cap.
func TestAContainerJobsLogKeepsItsEnd(t *testing.T) {
	e := newTestExecutor(t, "")
	e.logs.Close()
	e.logs = logfile.NewKeeper(64<<10, logr.Discard())
	t.Cleanup(e.logs.Close)
	handle, err := e.Submit(context.Background(), shellJob("noisy", "yes 0123456789abcdef | head -c 1048576; echo; echo the-end"), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, e, handle)
	path := filepath.Join(e.jobDir(handle), logFile)
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if used := info.Sys().(*syscall.Stat_t).Blocks * 512; used <= (64<<10)+8192 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job log takes %d bytes of disk, over the %d cap", info.Sys().(*syscall.Stat_t).Blocks*512, 64<<10)
		}
		time.Sleep(100 * time.Millisecond)
	}
	ch, err := e.Logs(context.Background(), handle, backend.LogRequest{TailLines: -1, HistoryBytes: backend.AllHistory})
	if err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	var omitted int64
	for chunk := range ch {
		if got.Len() == 0 {
			omitted = chunk.Omitted
		}
		got.Write(chunk.Data)
	}
	if !strings.HasSuffix(got.String(), "the-end\n") || strings.ContainsRune(got.String(), 0) || omitted < 900<<10 {
		t.Fatalf("read %d bytes after %d omitted, ending %q", got.Len(), omitted, got.String()[max(0, got.Len()-30):])
	}
}

// When result.json could not be written (a full disk), Status said
// NotFound and the watcher kept the job counted as running: a MaxJobs=1
// container stayed BUSY for ever. The result is kept in memory too.
func TestGhostJobBecomesTerminalWhenResultUnwritable(t *testing.T) {
	e := newTestExecutor(t, "")
	handle, err := e.Submit(context.Background(), shellJob("ghost", "sleep 0.3; exit 4"), nil)
	if err != nil {
		t.Fatal(err)
	}
	// A directory where result.json goes: writing it fails.
	if err := os.MkdirAll(filepath.Join(e.jobDir(handle), resultFile, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	state := waitTerminal(t, e, handle)
	if state.Phase != v1.PhaseFailed {
		t.Fatalf("phase %s for a job that exited 4 with its result unwritable", state.Phase)
	}
}

// A Cancel that comes while the job is still being prepared wins: the job
// does not run once its start ends (start outside the lock).
func TestCancelDuringStartStopsTheJob(t *testing.T) {
	bin := t.TempDir()
	fakeGit := "#!/bin/sh\nsleep 1\nfor last; do :; done\nmkdir -p \"$last\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(fakeGit), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	e := newTestExecutor(t, "")
	slow := shellJob("cancelled-early", "sleep 30")
	slow.Source = v1.Source{Git: "https://example.invalid/repo.git"}
	submitted := make(chan error, 1)
	go func() { _, err := e.Submit(context.Background(), slow, nil); submitted <- err }()
	time.Sleep(200 * time.Millisecond)
	if state, err := e.Status(context.Background(), slow.ID); err != nil || state.Phase != v1.PhasePending {
		t.Fatalf("status while starting: %+v, %v; want Pending", state, err)
	}
	if err := e.Cancel(context.Background(), slow.ID, owner.Ref{}); err != nil {
		t.Fatal(err)
	}
	if err := <-submitted; err != nil {
		t.Fatal(err)
	}
	if n := e.Running(); n != 0 {
		t.Fatalf("%d jobs running after a cancel during their start", n)
	}
	if _, err := e.Status(context.Background(), slow.ID); err == nil {
		t.Fatal("the cancelled job is still known")
	}
}

// A job run again in the directory an earlier agent session left (no
// result in it) starts a log of its own: the keeper forgets the old file
// before it goes, or the new one is read from where the old one was freed.
func TestAJobRunAgainReadsItsNewLogFromItsFirstByte(t *testing.T) {
	e := newTestExecutor(t, "")
	dir := e.jobDir("again")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(dir, logFile)
	if err := os.WriteFile(old, bytes.Repeat([]byte("x"), 64<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	// The old file freed below 32 KiB, as the keeper frees one past its cap.
	f, err := os.OpenFile(old, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 0, 32<<10); err != nil {
		t.Skipf("no punch hole here: %v", err)
	}
	f.Close()
	if kept, err := e.logs.File(old); err != nil || kept.Floor() != 32<<10 {
		t.Fatalf("the old log kept from %v (%v)", kept, err)
	}
	handle, err := e.Submit(context.Background(), shellJob("again", "echo new-run"), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, e, handle)
	ch, err := e.Logs(context.Background(), handle, backend.LogRequest{TailLines: -1, HistoryBytes: backend.AllHistory})
	if err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for chunk := range ch {
		if chunk.Gap != 0 || chunk.Omitted != 0 {
			t.Fatalf("the new log says %d bytes missing, %d left out", chunk.Gap, chunk.Omitted)
		}
		got.Write(chunk.Data)
	}
	if got.String() != "new-run\n" {
		t.Fatalf("read %q", got.String())
	}
}
