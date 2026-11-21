// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/zenthros/operator/agent/executortest"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/containerfacts"
	"github.com/zenthros/operator/pkg/owner"
)

// memoryEvents is a container's memory.events with oom_kill at n: a counter
// of the cgroup the agent and every job share.
func memoryEvents(n string) string {
	return "low 0\nhigh 0\nmax 12\noom 1\noom_kill " + n + "\noom_group_kill 0\n"
}

// killedBy ends a job by SIGKILL; for memory, the kernel's count of
// processes it killed for memory in the container grew while it ran.
func killedBy(memory bool) func(t *testing.T, e executortest.Executor, handle string) {
	return func(t *testing.T, e executortest.Executor, handle string) {
		p := e.(*Executor)
		if memory {
			write(t, filepath.Join(p.cfg.Facts.Root, "sys/fs/cgroup/memory.events"), memoryEvents("4"))
		}
		p.mu.Lock()
		j := p.jobs[handle]
		p.mu.Unlock()
		_ = syscall.Kill(-j.pgid, syscall.SIGKILL)
		waitTerminal(t, p, handle)
	}
}

func TestProcagentKeepsTheExecutorContract(t *testing.T) {
	executortest.Contract(t, executortest.Harness{
		New: func(t *testing.T) executortest.Executor {
			e := newTestExecutor(t, "")
			// A container of 4 GiB that killed 3 processes for memory
			// before: a count already high is no evidence.
			root := cgroupTree(t, "max 100000\n", "4294967296\n")
			write(t, filepath.Join(root, "sys/fs/cgroup/memory.events"), memoryEvents("3"))
			e.cfg.Facts = containerfacts.Sources{Root: root}
			t.Cleanup(func() { e.Close(context.Background()) })
			return e
		},
		Spec: shellJob("contract", "sleep 300"),
		End: func(t *testing.T, e executortest.Executor, handle string) {
			p := e.(*Executor)
			p.mu.Lock()
			j := p.jobs[handle]
			p.mu.Unlock()
			p.stop(context.Background(), j)
			waitTerminal(t, p, handle)
		},
		Missing:         "never-submitted",
		KilledForMemory: killedBy(true),
		Killed:          killedBy(false),
	})
}

// A container that restarted knows no job from before, and their directories,
// secrets included, are still on its disk. A detached one has no later chance.
func TestAJobOfAnEarlierSessionIsListedAsEnded(t *testing.T) {
	ctx := context.Background()
	e := newTestExecutor(t, "")
	secrets := filepath.Join(e.jobDir("earlier"), secretsDirName)
	if err := os.MkdirAll(secrets, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secrets, "token"), []byte("plaintext"), 0o600); err != nil {
		t.Fatal(err)
	}
	jobs, err := e.EndedJobs(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if want := []backend.InstalledJob{{JobID: "earlier", Handle: "earlier"}}; !slices.Equal(jobs, want) {
		t.Fatalf("ended = %v, want %v", jobs, want)
	}
	if err := e.Cancel(ctx, "earlier", owner.Ref{Kind: "ZenthrosJob", JobID: "earlier"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(secrets); !os.IsNotExist(err) {
		t.Fatalf("its secrets stayed (stat: %v)", err)
	}
}
