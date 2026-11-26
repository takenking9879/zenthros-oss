// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/takenking9879/zenthros-oss/operator/api/v1"
	"github.com/takenking9879/zenthros-oss/operator/internal/grpc/envelope"
	agentpb "github.com/takenking9879/zenthros-oss/operator/internal/grpc/gen"
	"github.com/takenking9879/zenthros-oss/operator/pkg/capability"
	"github.com/takenking9879/zenthros-oss/operator/pkg/jobspec"
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

// endKnowingExecutor knows when each job ends, as procagent does when its
// process exits and the VM executor when systemd says the unit ended.
type endKnowingExecutor struct {
	*mockExecutor
	endsMu sync.Mutex
	ends   map[string]chan struct{}
}

func (e *endKnowingExecutor) Ended(_ context.Context, handle string) <-chan struct{} {
	return e.endOf(handle)
}

func (e *endKnowingExecutor) endOf(handle string) chan struct{} {
	e.endsMu.Lock()
	defer e.endsMu.Unlock()
	if e.ends[handle] == nil {
		e.ends[handle] = make(chan struct{})
	}
	return e.ends[handle]
}

// end ends the job: its state first, then the word that it ended.
func (e *endKnowingExecutor) end(handle string) {
	e.setPhase(handle, v1.PhaseSucceeded)
	close(e.endOf(handle))
}

// A job's end is pushed, and its slot freed, as soon as its executor knows
// it, not at the status watcher's next tick: with a tick every 2 s each task
// of a map on one machine started ~1 s after the one before had ended, on
// average, and up to 2 s (This computer, 2026-11-18). A job just submitted
// is waited on at once, so a short one is too. The tick is 2 s away here:
// only the end can bring the push within 1 s.
func TestAJobsEndIsPushedWhenItsExecutorKnowsIt(t *testing.T) {
	exec := &endKnowingExecutor{mockExecutor: newMockExecutor(), ends: map[string]chan struct{}{}}
	a := NewWithExecutor(Config{BackendID: "ct-test", BackendType: "container", MaxJobs: 1}, exec, logr.Discard())
	stream := newCommandStream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.ServeStream(ctx, stream) }()

	for i, id := range []string{"job-1", "job-2"} {
		request := fmt.Sprintf("r-%d", i+1)
		stream.recv <- submitCommand(t, request, id)
		_, answer := stream.await(t, time.Second, "answer to "+request, func(m *agentpb.RegisterRequest) bool {
			return m.GetSubmitResponse().GetRequestId() == request
		})
		handle := answer.GetSubmitResponse().GetHandleName()
		if handle == "" || answer.GetSubmitResponse().GetError() != "" {
			t.Fatalf("submit %s: %+v", id, answer.GetSubmitResponse())
		}
		exec.setPhase(handle, v1.PhaseRunning)
		if i == 1 {
			// Running a while, as a map's task: it is waited on by then.
			time.Sleep(100 * time.Millisecond)
		}
		exec.end(handle)
		pushed, _ := stream.await(t, time.Second, "push of "+id+"'s end", func(m *agentpb.RegisterRequest) bool {
			u := m.GetStatusUpdate()
			return u.GetJobId() == id && u.GetStatus().GetPhase() == string(v1.PhaseSucceeded)
		})
		stream.awaitAfter(t, pushed, time.Second, "heartbeat with "+id+"'s slot free", func(m *agentpb.RegisterRequest) bool {
			return m.GetHeartbeat() != nil && m.GetHeartbeat().GetRunningJobs() == 0
		})
	}
}

// slowDeclaringExecutor declares what it runs as a VM's does, which probes
// containerd with a command on every call: slowly here.
type slowDeclaringExecutor struct {
	*endKnowingExecutor
	declareTakes time.Duration
}

func (e *slowDeclaringExecutor) Declare(context.Context) (capability.Set, capability.Host) {
	time.Sleep(e.declareTakes)
	return capability.VM(false, capability.RuntimeSystemd), capability.Host{Arch: "amd64"}
}

// The heartbeat that frees an ended job's slot follows its end on the stream
// at once: it does not read the machine's declaration first. The hub sends
// the next job as it hears the end, and a read of the machine between the
// two found it still full and kept that for 1 s: on a VM, whose declaration
// runs systemctl, each task of a map waited 1.75 s of BACKEND_BUSY backoff
// (lab, 2026-11-18). The periodic heartbeat carries the declaration.
func TestTheHeartbeatThatFreesASlotFollowsTheEndAtOnce(t *testing.T) {
	exec := &slowDeclaringExecutor{
		endKnowingExecutor: &endKnowingExecutor{mockExecutor: newMockExecutor(), ends: map[string]chan struct{}{}},
		declareTakes:       200 * time.Millisecond,
	}
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm", MaxJobs: 1}, exec, logr.Discard())
	stream := newCommandStream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.ServeStream(ctx, stream) }()

	stream.recv <- submitCommand(t, "r-1", "job-1")
	_, answer := stream.await(t, 2*time.Second, "answer to r-1", func(m *agentpb.RegisterRequest) bool {
		return m.GetSubmitResponse().GetRequestId() == "r-1"
	})
	handle := answer.GetSubmitResponse().GetHandleName()
	if handle == "" || answer.GetSubmitResponse().GetError() != "" {
		t.Fatalf("submit: %+v", answer.GetSubmitResponse())
	}
	exec.setPhase(handle, v1.PhaseRunning)
	// The stream's first heartbeat, which declares, is out by then.
	time.Sleep(4 * exec.declareTakes)
	exec.end(handle)
	pushed, _ := stream.await(t, time.Second, "push of job-1's end", func(m *agentpb.RegisterRequest) bool {
		return m.GetStatusUpdate().GetStatus().GetPhase() == string(v1.PhaseSucceeded)
	})
	endAt := time.Now()
	stream.awaitAfter(t, pushed, time.Second, "heartbeat with job-1's slot free", func(m *agentpb.RegisterRequest) bool {
		return m.GetHeartbeat() != nil && m.GetHeartbeat().GetRunningJobs() == 0
	})
	if took := time.Since(endAt); took > exec.declareTakes/4 {
		t.Fatalf("the heartbeat freeing the slot came %v after the end's push, want under %v: it waited on the declaration", took, exec.declareTakes/4)
	}
}
