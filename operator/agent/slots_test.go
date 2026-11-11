// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

// slotStream hands the agent one job, then stays open, and keeps what the
// agent sends in order.
type slotStream struct {
	job  *agentpb.JobEnvelope
	once sync.Once
	hold chan struct{}
	mu   sync.Mutex
	sent []*agentpb.RegisterRequest
}

func (s *slotStream) Send(request *agentpb.RegisterRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, request)
	return nil
}

func (s *slotStream) Recv() (*agentpb.RegisterResponse, error) {
	var submit *agentpb.RegisterResponse
	s.once.Do(func() {
		submit = &agentpb.RegisterResponse{Submit: &agentpb.SubmitRequest{RequestId: "r-1", Job: s.job}}
	})
	if submit != nil {
		return submit, nil
	}
	<-s.hold
	return nil, io.EOF
}

func (s *slotStream) messages() []*agentpb.RegisterRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentpb.RegisterRequest(nil), s.sent...)
}

// endingExecutor runs one job until ended is set.
type endingExecutor struct {
	blockingExecutor
	ended *atomic.Bool
}

func (endingExecutor) Submit(context.Context, jobspec.JobSpec, map[string]string) (string, error) {
	return "handle-1", nil
}

func (e endingExecutor) Status(context.Context, string) (backend.JobState, error) {
	if e.ended.Load() {
		return backend.JobState{Phase: v1.PhaseSucceeded}, nil
	}
	return backend.JobState{Phase: v1.PhaseRunning}, nil
}

// The hub learns how many jobs a machine runs from its heartbeats, and
// refuses a job to a machine it believes full. A machine says so as soon as
// it takes a job and as soon as the job's end is delivered, in that order:
// the end first, then the free slot. With the heartbeat alone the slot of a
// job that had ended stayed taken until the next one, up to 15 s, and the
// next job of the pool was refused all that time (scale lab, 2026-10-10).
func TestMachineSaysItsSlotIsFreeWhenItsJobEnds(t *testing.T) {
	job, err := envelope.Encode(jobspec.JobSpec{ID: "uid-slot", Kind: v1.WorkloadKindJob, Entrypoint: "true"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	ended := &atomic.Bool{}
	a := NewWithExecutor(Config{BackendID: "vm-slot", BackendType: "vm", MaxJobs: 1}, endingExecutor{ended: ended}, logr.Discard())
	stream := &slotStream{job: job, hold: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.ServeStream(ctx, stream) }()
	t.Cleanup(func() {
		cancel()
		close(stream.hold)
		<-done
	})

	// find is the index of the first message from index from on that
	// matches, waiting up to within for it.
	find := func(what string, from int, within time.Duration, match func(*agentpb.RegisterRequest) bool) int {
		t.Helper()
		deadline := time.Now().Add(within)
		for {
			sent := stream.messages()
			for i := from; i < len(sent); i++ {
				if match(sent[i]) {
					return i
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("no %s within %s; the heartbeat interval is %s", what, within, a.cfg.heartbeatInterval())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	beat := func(running int32) func(*agentpb.RegisterRequest) bool {
		return func(m *agentpb.RegisterRequest) bool {
			return m.GetHeartbeat() != nil && m.GetHeartbeat().GetRunningJobs() == running
		}
	}

	// Both waits are far inside the heartbeat interval: only a heartbeat
	// sent for the change can meet them.
	taken := find("heartbeat with the job's slot taken", 0, time.Second, beat(1))
	ended.Store(true)
	end := find("the job's end", taken, statusWatchInterval+2*time.Second, func(m *agentpb.RegisterRequest) bool {
		return m.GetStatusUpdate() != nil && m.GetStatusUpdate().GetStatus().GetPhase() == string(v1.PhaseSucceeded)
	})
	free := find("heartbeat with the slot free", taken+1, statusWatchInterval+2*time.Second, beat(0))
	if free < end {
		t.Fatalf("the slot was reported free (message %d) before the job's end was delivered (message %d)", free, end)
	}
}
