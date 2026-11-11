// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// failingStream refuses every Send while down, as a stream that broke.
type failingStream struct {
	*recordingStream
	mu   sync.Mutex
	down bool
}

func (s *failingStream) Send(msg *agentpb.RegisterRequest) error {
	s.mu.Lock()
	down := s.down
	s.mu.Unlock()
	if down {
		return errors.New("stream broken")
	}
	return s.recordingStream.Send(msg)
}

func (s *failingStream) setDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

// The controller no longer asks a live machine how its jobs are doing: it
// learns from these pushes. A push that fails is sent again on the next
// poll, not marked as seen.
func TestStatusWatcherRetriesAFailedPush(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	stream := &failingStream{recordingStream: newRecordingStream(), down: true}
	exec.setPhase("handle-1", v1.PhaseRunning)
	tracker.Add("job-1", "handle-1")
	watcher := newStatusWatcher(exec, tracker, newStreamSender(stream), "vm-test", logr.Discard())

	watcher.pollAll(context.Background())
	if n := len(stream.statusUpdates()); n != 0 {
		t.Fatalf("a down stream recorded %d updates", n)
	}
	stream.setDown(false)
	watcher.pollAll(context.Background())
	if u := stream.statusUpdates(); len(u) != 1 || u[0].GetStatus().GetPhase() != string(v1.PhaseRunning) {
		t.Fatalf("after the stream came back: %+v, want one Running push", u)
	}
}

// A job's end is the push that matters most: it is not dropped from the
// tracker until it was handed to the stream.
func TestStatusWatcherKeepsATerminalJobUntilItsEndIsPushed(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	stream := &failingStream{recordingStream: newRecordingStream(), down: true}
	exec.setPhase("handle-1", v1.PhaseSucceeded)
	tracker.Add("job-1", "handle-1")
	watcher := newStatusWatcher(exec, tracker, newStreamSender(stream), "vm-test", logr.Discard())

	watcher.pollAll(context.Background())
	if tracker.Count() != 1 {
		t.Fatalf("a terminal job whose push failed was dropped: tracker count %d", tracker.Count())
	}
	stream.setDown(false)
	watcher.pollAll(context.Background())
	if tracker.Count() != 0 {
		t.Fatalf("tracker count = %d after the end was pushed, want 0", tracker.Count())
	}
	if u := stream.statusUpdates(); len(u) != 1 || u[0].GetStatus().GetPhase() != string(v1.PhaseSucceeded) {
		t.Fatalf("updates = %+v, want one Succeeded push", u)
	}
}

// A new stream gets a new watcher, which has seen nothing: every running
// job's state is pushed again, so a controller that missed pushes while
// the machine was away catches up on reconnect.
func TestANewStreamPushesEveryJobAgain(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	exec.setPhase("handle-1", v1.PhaseRunning)
	exec.setPhase("handle-2", v1.PhaseRunning)
	tracker.Add("job-1", "handle-1")
	tracker.Add("job-2", "handle-2")

	first := newRecordingStream()
	newStatusWatcher(exec, tracker, newStreamSender(first), "vm-test", logr.Discard()).pollAll(context.Background())
	second := newRecordingStream()
	newStatusWatcher(exec, tracker, newStreamSender(second), "vm-test", logr.Discard()).pollAll(context.Background())
	if n := len(second.statusUpdates()); n != 2 {
		t.Fatalf("a new stream pushed %d jobs, want both", n)
	}
}
