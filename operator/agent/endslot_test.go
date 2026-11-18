// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/jobspec"
)

// commandStream is a control stream whose commands the test sends, and
// which keeps what the agent sent, in order.
type commandStream struct {
	mu   sync.Mutex
	sent []*agentpb.RegisterRequest
	recv chan *agentpb.RegisterResponse
	wake chan struct{}
}

func newCommandStream() *commandStream {
	return &commandStream{recv: make(chan *agentpb.RegisterResponse), wake: make(chan struct{}, 1)}
}

func (s *commandStream) Send(msg *agentpb.RegisterRequest) error {
	s.mu.Lock()
	s.sent = append(s.sent, msg)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

func (s *commandStream) Recv() (*agentpb.RegisterResponse, error) {
	msg, ok := <-s.recv
	if !ok {
		return nil, context.Canceled
	}
	return msg, nil
}

// await is the first message sent that match finds, within d.
func (s *commandStream) await(t *testing.T, d time.Duration, what string, match func(*agentpb.RegisterRequest) bool) (int, *agentpb.RegisterRequest) {
	t.Helper()
	return s.awaitAfter(t, -1, d, what, match)
}

// awaitAfter is the first message sent after index after that match finds,
// within d.
func (s *commandStream) awaitAfter(t *testing.T, after int, d time.Duration, what string, match func(*agentpb.RegisterRequest) bool) (int, *agentpb.RegisterRequest) {
	t.Helper()
	deadline := time.After(d)
	for {
		s.mu.Lock()
		for i, msg := range s.sent {
			if i > after && match(msg) {
				s.mu.Unlock()
				return i, msg
			}
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-deadline:
			t.Fatalf("no %s within %v", what, d)
		}
	}
}

func submitCommand(t *testing.T, requestID, jobID string) *agentpb.RegisterResponse {
	t.Helper()
	job, err := envelope.Encode(jobspec.JobSpec{ID: jobID, Kind: v1.WorkloadKindJob, Entrypoint: "true"})
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.RegisterResponse{Submit: &agentpb.SubmitRequest{RequestId: requestID, Job: job}}
}

// A job's end the controller hears by asking (its reconcile reads Status, a
// submitted job's source of truth) frees the job's slot before the answer
// goes, as the watcher's push of that end does. The watcher saw the end up to
// its 2 s later: the next call, sent at once by a caller that had its result,
// was refused BACKEND_BUSY by this agent, and the hub sent it again 2 s
// later. The end is still on the stream before the heartbeat that frees its
// slot, and both before the answer.
func TestAnEndAnsweredFreesItsSlotBeforeTheAnswer(t *testing.T) {
	exec := newMockExecutor()
	a := NewWithExecutor(Config{BackendID: "ct-test", BackendType: "container", MaxJobs: 1}, exec, logr.Discard())
	stream := newCommandStream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.ServeStream(ctx, stream) }()

	stream.recv <- submitCommand(t, "r-1", "job-1")
	_, first := stream.await(t, 2*time.Second, "answer to the first submit", func(m *agentpb.RegisterRequest) bool {
		return m.GetSubmitResponse().GetRequestId() == "r-1"
	})
	handle := first.GetSubmitResponse().GetHandleName()
	if handle == "" || first.GetSubmitResponse().GetError() != "" {
		t.Fatalf("first submit: %+v", first.GetSubmitResponse())
	}
	exec.setPhase(handle, v1.PhaseSucceeded)

	stream.recv <- &agentpb.RegisterResponse{Status: &agentpb.StatusRequest{RequestId: "s-1", Id: "job-1", HandleName: handle}}
	answered, answer := stream.await(t, time.Second, "answer to the status", func(m *agentpb.RegisterRequest) bool {
		return m.GetStatusResponse().GetRequestId() == "s-1"
	})
	if phase := answer.GetStatusResponse().GetStatus().GetPhase(); phase != string(v1.PhaseSucceeded) {
		t.Fatalf("status answered %q", phase)
	}
	pushed, _ := stream.await(t, 0, "push of the end", func(m *agentpb.RegisterRequest) bool {
		u := m.GetStatusUpdate()
		return u.GetJobId() == "job-1" && u.GetStatus().GetPhase() == string(v1.PhaseSucceeded)
	})
	busy, _ := stream.await(t, 0, "heartbeat with the job running", func(m *agentpb.RegisterRequest) bool {
		return m.GetHeartbeat().GetRunningJobs() == 1
	})
	freed, _ := stream.awaitAfter(t, busy, 0, "heartbeat with the slot free", func(m *agentpb.RegisterRequest) bool {
		return m.GetHeartbeat() != nil && m.GetHeartbeat().GetRunningJobs() == 0
	})
	if pushed > freed || freed > answered {
		t.Fatalf("sent the end at %d, the free slot at %d, the answer at %d: want them in that order", pushed, freed, answered)
	}

	stream.recv <- submitCommand(t, "r-2", "job-2")
	_, second := stream.await(t, time.Second, "answer to the second submit", func(m *agentpb.RegisterRequest) bool {
		return m.GetSubmitResponse().GetRequestId() == "r-2"
	})
	if err := second.GetSubmitResponse().GetError(); err != "" {
		t.Fatalf("the next job was refused with the first one's end known: %s", err)
	}
}
