// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/jobspec"
)

// gatedExecutor holds each Submit until its job ID's gate opens, so two
// Submits are in flight at once.
type gatedExecutor struct {
	mismatchExecutor
	entered chan string
	gates   map[string]chan struct{}
}

func (g gatedExecutor) Submit(_ context.Context, spec jobspec.JobSpec, _ map[string]string) (string, error) {
	g.entered <- spec.ID
	if gate, ok := g.gates[spec.ID]; ok {
		<-gate
	}
	return "zenthros-" + spec.ID + ".service", nil
}

func submitRequest(t *testing.T, id string) *agentpb.SubmitRequest {
	t.Helper()
	job, err := envelope.Encode(jobspec.JobSpec{ID: id, Name: id, Namespace: "tenant-default", Kind: v1.WorkloadKindJob, Entrypoint: "sleep 60"})
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.SubmitRequest{RequestId: "r-" + id, Job: job}
}

// The controller admits on the last heartbeat's count, so two of its workers
// can both send a job for a machine's last slot. The agent takes one and
// answers BACKEND_BUSY to the other, while a Submit sent again for a job it
// already runs or is submitting takes no slot.
func TestSubmitTakesTheLastSlotOnce(t *testing.T) {
	executor := gatedExecutor{entered: make(chan string, 8), gates: map[string]chan struct{}{"uid-b": make(chan struct{})}}
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm", MaxJobs: 2}, executor, logr.Discard())
	ctx := context.Background()

	if resp := a.Submit(ctx, submitRequest(t, "uid-a")); resp.GetError() != "" {
		t.Fatalf("first job: %s", resp.GetError())
	}
	<-executor.entered

	inFlight := make(chan *agentpb.SubmitResponse)
	go func() { inFlight <- a.Submit(ctx, submitRequest(t, "uid-b")) }()
	<-executor.entered

	busy := a.Submit(ctx, submitRequest(t, "uid-c"))
	if busy.GetErrorClass() != agentpb.ErrorClass_BACKEND_BUSY {
		t.Fatalf("Submit for a third job on two slots = %+v, want BACKEND_BUSY", busy)
	}
	if len(executor.entered) != 0 {
		t.Fatal("the refused job reached the executor")
	}

	again := make(chan *agentpb.SubmitResponse)
	go func() { again <- a.Submit(ctx, submitRequest(t, "uid-b")) }()
	if id := <-executor.entered; id != "uid-b" {
		t.Fatalf("executor entered %s, want the resent uid-b", id)
	}
	close(executor.gates["uid-b"])
	for _, ch := range []chan *agentpb.SubmitResponse{inFlight, again} {
		if resp := <-ch; resp.GetError() != "" {
			t.Fatalf("uid-b: %s", resp.GetError())
		}
	}

	if resp := a.Submit(ctx, submitRequest(t, "uid-a")); resp.GetError() != "" {
		t.Fatalf("a Submit sent again for a running job was refused: %s", resp.GetError())
	}
	<-executor.entered
	if got := a.tracker.Count(); got != 2 {
		t.Fatalf("tracked jobs = %d, want 2", got)
	}
}

// A failed Submit gives its slot back.
func TestFailedSubmitFreesItsSlot(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm", MaxJobs: 1}, mismatchExecutor{}, logr.Discard())
	ctx := context.Background()
	for range 2 {
		if resp := a.Submit(ctx, submitRequest(t, "uid-x")); resp.GetErrorClass() != agentpb.ErrorClass_SPEC_MISMATCH {
			t.Fatalf("Submit = %+v, want the executor's SPEC_MISMATCH, not BACKEND_BUSY", resp)
		}
	}
	if _, ok := a.tracker.Claim("uid-y", 1); !ok {
		t.Fatal("a failed Submit kept its slot")
	}
}

// A submitted job takes one slot, from the moment it is tracked: it counted
// in both the tracked jobs and the claims until its Submit's deferred
// release, which waits for the Service's ports and the image digest, so a
// concurrent Submit found the machine full one slot early (05#5).
func TestATrackedJobHoldsOneSlotNotTwo(t *testing.T) {
	tracker := newJobTracker()
	release, ok := tracker.Claim("job-a", 2)
	if !ok {
		t.Fatal("the first claim of two slots was refused")
	}
	defer release()
	tracker.Add("job-a", "handle-a")
	if _, ok := tracker.Claim("job-b", 2); !ok {
		t.Fatal("one job tracked, its Submit still returning, and the second of two slots was refused")
	}
}
