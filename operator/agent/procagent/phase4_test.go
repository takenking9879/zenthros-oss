// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/zenthros/operator/api/v1"
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
// RunPod or Vast.ai container, where the agent's certificate lives.
// The log stops at a cap, with a marker that says so.
func TestProcagentJobLogIsCapped(t *testing.T) {
	e := newTestExecutor(t, "")
	e.cfg.MaxLogBytes = 64 << 10
	handle, err := e.Submit(context.Background(), shellJob("noisy", "yes 0123456789abcdef | head -c 1048576"), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, e, handle)
	info, err := os.Stat(filepath.Join(e.jobDir(handle), logFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > (64<<10)+4096 {
		t.Fatalf("the job log is %d bytes, over the %d cap", info.Size(), 64<<10)
	}
	raw, _ := os.ReadFile(filepath.Join(e.jobDir(handle), logFile))
	if !strings.Contains(string(raw), "truncated") {
		t.Fatal("a capped log does not say it was cut")
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
