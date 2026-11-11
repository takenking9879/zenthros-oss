// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/zenthros/operator/agent/executortest"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/owner"
)

func TestProcagentKeepsTheExecutorContract(t *testing.T) {
	executortest.Contract(t, executortest.Harness{
		New: func(t *testing.T) executortest.Executor {
			e := newTestExecutor(t, "")
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
		Missing: "never-submitted",
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
