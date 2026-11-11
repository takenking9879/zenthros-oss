// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/owner"
)

// A job that asked to keep its work directory keeps it through every
// Cancel: the GC cancels a finished job, and a second Cancel (a retry, or
// one after an agent restart) found no job in memory and deleted it (07#2).
func TestARetainedWorkDirSurvivesASecondCancel(t *testing.T) {
	e := newTestExecutor(t, "")
	spec := shellJob("kept", "echo data > out.txt")
	spec.RetainWorkdir = true
	h, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, e, h)
	for range 2 {
		if err := e.Cancel(context.Background(), h, owner.Ref{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(e.jobDir(h), workDirName, "out.txt")); err != nil {
		t.Fatalf("a second Cancel removed the retained work directory: %v", err)
	}
}

// The jobs of an agent that died die with it: a restarted agent on the same
// state directory kills every process an earlier session's jobs left, its
// leader and the children it started, before it takes a job. They held the
// directory a resubmit clears and the slot it counts (07#5, D9). Another
// agent's jobs on the host are not its to kill.
func TestARestartedAgentKillsTheJobsItsPredecessorLeft(t *testing.T) {
	e := newTestExecutor(t, "")
	other := newTestExecutor(t, "")
	left := leftByAnEarlierAgent(t, e, "left", "sleep 300 & sleep 300")
	neighbour, err := other.Submit(context.Background(), shellJob("neighbour", "sleep 300"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Cancel(context.Background(), neighbour, owner.Ref{})
	pids := waitProcesses(t, e.jobDir("left"), 2)

	if _, err := New(e.cfg); err != nil {
		t.Fatal(err)
	}
	_ = left.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for len(processesOf(e.jobDir("left"))) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if alive := processesOf(e.jobDir("left")); len(alive) > 0 {
		t.Fatalf("processes %v of %v outlived the agent that started them", alive, pids)
	}
	if len(processesOf(other.jobDir(neighbour))) == 0 {
		t.Fatal("the restarted agent killed another agent's job")
	}
}

// leftByAnEarlierAgent starts what a job of e's state directory runs, as an
// agent session that then died left it: its directory and its process
// group, which no Executor waits for.
func leftByAnEarlierAgent(t *testing.T, e *Executor, handle, script string) *exec.Cmd {
	t.Helper()
	dir := e.jobDir(handle)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(), envJobDir+"="+dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	return cmd
}

// processesOf lists the live processes whose environment names dir as their
// job's directory.
func processesOf(dir string) []int {
	var pids []int
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		environ, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "environ"))
		if err != nil {
			continue
		}
		stat, _ := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if bytes.Contains(stat, []byte(") Z ")) {
			continue // a zombie its parent has not reaped
		}
		if bytes.Contains(environ, []byte(envJobDir+"="+dir+"\x00")) {
			pids = append(pids, pid)
		}
	}
	return pids
}

func waitProcesses(t *testing.T, dir string, n int) []int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pids := processesOf(dir); len(pids) >= n {
			return pids
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the job did not start %d processes", n)
	return nil
}
