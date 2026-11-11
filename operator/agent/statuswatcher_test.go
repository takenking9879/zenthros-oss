// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
)

type mockExecutor struct {
	mu      sync.Mutex
	phases  map[string]v1.Phase
	reasons map[string][]v1.Reason
	images  map[string]string
}

func newMockExecutor() *mockExecutor {
	return &mockExecutor{phases: make(map[string]v1.Phase), reasons: make(map[string][]v1.Reason), images: make(map[string]string)}
}

func (m *mockExecutor) Submit(_ context.Context, spec jobspec.JobSpec, _ map[string]string) (string, error) {
	return "zenthros-" + spec.ID + ".service", nil
}

func (m *mockExecutor) Cancel(_ context.Context, handle string, _ owner.Ref) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.phases, handle)
	return nil
}

func (m *mockExecutor) Status(_ context.Context, handle string) (backend.JobState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	phase, ok := m.phases[handle]
	if !ok {
		phase = v1.PhasePending
	}
	return backend.JobState{Phase: phase, Reasons: m.reasons[handle], ResolvedImage: m.images[handle]}, nil
}

func (m *mockExecutor) Logs(_ context.Context, _ string, _ bool, _ int) (<-chan backend.LogChunk, error) {
	ch := make(chan backend.LogChunk)
	close(ch)
	return ch, nil
}

func (m *mockExecutor) setPhase(handle string, phase v1.Phase) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.phases[handle] = phase
}

func (m *mockExecutor) setReasons(handle string, reasons []v1.Reason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reasons[handle] = reasons
}

type recordingStream struct {
	mu   sync.Mutex
	sent []*agentpb.RegisterRequest
	recv chan *agentpb.RegisterResponse
}

func newRecordingStream() *recordingStream {
	return &recordingStream{recv: make(chan *agentpb.RegisterResponse, 10)}
}

func (s *recordingStream) Send(msg *agentpb.RegisterRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, msg)
	return nil
}

func (s *recordingStream) Recv() (*agentpb.RegisterResponse, error) {
	msg, ok := <-s.recv
	if !ok {
		return nil, context.Canceled
	}
	return msg, nil
}

func (s *recordingStream) statusUpdates() []*agentpb.StatusUpdate {
	s.mu.Lock()
	defer s.mu.Unlock()
	var updates []*agentpb.StatusUpdate
	for _, msg := range s.sent {
		if u := msg.GetStatusUpdate(); u != nil {
			updates = append(updates, u)
		}
	}
	return updates
}

func (s *recordingStream) heartbeats() []*agentpb.Heartbeat {
	s.mu.Lock()
	defer s.mu.Unlock()
	var hbs []*agentpb.Heartbeat
	for _, msg := range s.sent {
		if hb := msg.GetHeartbeat(); hb != nil {
			hbs = append(hbs, hb)
		}
	}
	return hbs
}

func TestStatusWatcherSendsUpdateOnPhaseChange(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	stream := newRecordingStream()
	sender := newStreamSender(stream)

	exec.setPhase("handle-1", v1.PhaseRunning)
	tracker.Add("job-1", "handle-1")

	watcher := newStatusWatcher(exec, tracker, sender, "vm-test", logr.Discard())
	watcher.interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go watcher.Run(ctx)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for status update")
		default:
		}
		if len(stream.statusUpdates()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	updates := stream.statusUpdates()
	if len(updates) == 0 {
		t.Fatal("expected at least one status update")
	}
	got := updates[0]
	if got.GetJobId() != "job-1" {
		t.Errorf("jobId = %q, want job-1", got.GetJobId())
	}
	if got.GetStatus().GetPhase() != string(v1.PhaseRunning) {
		t.Errorf("phase = %q, want Running", got.GetStatus().GetPhase())
	}
}

func TestStatusWatcherRemovesTerminalJobs(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	stream := newRecordingStream()
	sender := newStreamSender(stream)

	exec.setPhase("handle-1", v1.PhaseSucceeded)
	tracker.Add("job-1", "handle-1")

	watcher := newStatusWatcher(exec, tracker, sender, "vm-test", logr.Discard())
	watcher.interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go watcher.Run(ctx)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for terminal job removal")
		default:
		}
		if tracker.Count() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	if tracker.Count() != 0 {
		t.Errorf("tracker count = %d, want 0 after terminal phase", tracker.Count())
	}
}

func TestStatusWatcherNoUpdateWhenPhaseUnchanged(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	stream := newRecordingStream()
	sender := newStreamSender(stream)

	exec.setPhase("handle-1", v1.PhaseRunning)
	tracker.Add("job-1", "handle-1")

	watcher := newStatusWatcher(exec, tracker, sender, "vm-test", logr.Discard())
	watcher.interval = 50 * time.Millisecond
	watcher.lastPhase["handle-1"] = v1.PhaseRunning

	ctx, cancel := context.WithCancel(context.Background())
	go watcher.Run(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	if n := len(stream.statusUpdates()); n != 0 {
		t.Errorf("expected 0 status updates for unchanged phase, got %d", n)
	}
}

// TestStatusWatcherPushesReasons verifies the watcher carries structured failure
// reasons (e.g. IMAGE_PULL_FAILED) in the pushed StatusUpdate so the controller can
// surface them on the CRD.
func TestStatusWatcherPushesReasons(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	stream := newRecordingStream()
	sender := newStreamSender(stream)

	exec.setPhase("handle-1", v1.PhaseFailed)
	exec.setReasons("handle-1", []v1.Reason{{Code: v1.ReasonImagePullFailed, Detail: "repo not found"}})
	exec.mu.Lock()
	exec.images["handle-1"] = "docker.io/library/python@sha256:0123"
	exec.mu.Unlock()
	tracker.Add("job-1", "handle-1")

	watcher := newStatusWatcher(exec, tracker, sender, "vm-test", logr.Discard())
	watcher.interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go watcher.Run(ctx)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for status update")
		default:
		}
		if len(stream.statusUpdates()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	got := stream.statusUpdates()[0].GetStatus()
	if len(got.GetReasons()) != 1 || got.GetReasons()[0].GetCode() != string(v1.ReasonImagePullFailed) {
		t.Errorf("reasons = %+v, want IMAGE_PULL_FAILED", got.GetReasons())
	}
	// A push is the same status a Status answer is: the pushed one used to
	// drop the resolved image.
	if got.GetResolvedImage() != "docker.io/library/python@sha256:0123" {
		t.Errorf("pushed resolved image = %q", got.GetResolvedImage())
	}
}

// teardownExecutor stops a unit on Cancel, which ends its process the way a
// SIGTERM does (a Failed read until the unit is removed), and runs
// duringCancel in that window.
type teardownExecutor struct {
	*mockExecutor
	duringCancel func()
}

func (e *teardownExecutor) Cancel(ctx context.Context, handle string, _ owner.Ref) error {
	e.setPhase(handle, v1.PhaseFailed)
	e.duringCancel()
	return e.mockExecutor.Cancel(ctx, handle, owner.Ref{})
}

// A poll while a Cancel tears the unit down reads the stop's SIGTERM end. It
// must not reach the controller: a terminal push replaces the job's phase,
// and the controller cancelled this job (a deleted job must end CANCELLED).
func TestStatusWatcherDoesNotReportACancelledUnitsTeardown(t *testing.T) {
	exec := &teardownExecutor{mockExecutor: newMockExecutor()}
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, exec, logr.Discard())
	stream := newRecordingStream()
	watcher := newStatusWatcher(exec, a.tracker, newStreamSender(stream), "vm-test", logr.Discard())
	ctx := context.Background()

	exec.setPhase("handle-1", v1.PhaseRunning)
	a.tracker.Add("job-1", "handle-1")
	watcher.pollAll(ctx)
	exec.duringCancel = func() { watcher.pollAll(ctx) }
	if resp := a.Cancel(ctx, &agentpb.CancelRequest{Id: "job-1", HandleName: "handle-1"}); resp.GetError() != "" {
		t.Fatalf("Cancel: %s", resp.GetError())
	}

	for _, update := range stream.statusUpdates() {
		if update.GetStatus().GetPhase() != string(v1.PhaseRunning) {
			t.Fatalf("watcher pushed %s for a unit a Cancel was tearing down", update.GetStatus().GetPhase())
		}
	}
	if a.tracker.Watching("handle-1") {
		t.Fatal("a cancelled job must no longer be watched")
	}
}

// batchExecutor reads every job's state in one call, as the systemd executor
// reads every unit in one `systemctl show`.
type batchExecutor struct {
	*mockExecutor
	batches [][]string
	singles int
}

func (b *batchExecutor) StatusAll(ctx context.Context, handles []string) ([]backend.JobState, []error) {
	b.batches = append(b.batches, append([]string(nil), handles...))
	states, errs := make([]backend.JobState, len(handles)), make([]error, len(handles))
	for i, handle := range handles {
		states[i], errs[i] = b.mockExecutor.Status(ctx, handle)
	}
	return states, errs
}

func (b *batchExecutor) Status(ctx context.Context, handle string) (backend.JobState, error) {
	b.singles++
	return b.mockExecutor.Status(ctx, handle)
}

// A sweep over many jobs is one read when the executor can batch: a
// `systemctl show` per job every two seconds was most of what a VM agent
// spent with many jobs. Each answer is still
// pushed for its own job.
func TestStatusWatcherReadsEveryJobInOneSweep(t *testing.T) {
	exec := &batchExecutor{mockExecutor: newMockExecutor()}
	tracker := newJobTracker()
	stream := newRecordingStream()
	for i := 0; i < 30; i++ {
		handle := fmt.Sprintf("handle-%d", i)
		tracker.Add(fmt.Sprintf("job-%d", i), handle)
		if i%2 == 0 {
			exec.setPhase(handle, v1.PhaseRunning)
		} else {
			exec.setPhase(handle, v1.PhaseSucceeded)
		}
	}
	watcher := newStatusWatcher(exec, tracker, newStreamSender(stream), "vm-test", logr.Discard())
	watcher.pollAll(context.Background())

	if len(exec.batches) != 1 || len(exec.batches[0]) != 30 || exec.singles != 0 {
		t.Fatalf("one sweep over 30 jobs: %d batches (%v), %d single reads; want one batch of 30", len(exec.batches), exec.batches, exec.singles)
	}
	updates := stream.statusUpdates()
	if len(updates) != 30 {
		t.Fatalf("%d updates, want 30", len(updates))
	}
	for _, u := range updates {
		var i int
		fmt.Sscanf(u.GetJobId(), "job-%d", &i)
		want := v1.PhaseRunning
		if i%2 == 1 {
			want = v1.PhaseSucceeded
		}
		if u.GetHandleName() != fmt.Sprintf("handle-%d", i) || u.GetStatus().GetPhase() != string(want) {
			t.Errorf("update %s/%s = %s, want %s", u.GetJobId(), u.GetHandleName(), u.GetStatus().GetPhase(), want)
		}
	}
	// The ended jobs leave the next sweep.
	watcher.pollAll(context.Background())
	if len(exec.batches) != 2 || len(exec.batches[1]) != 15 {
		t.Errorf("second sweep read %v, want the 15 running jobs", exec.batches[len(exec.batches)-1])
	}
}

// The controller asks Status right after its Submit and writes the state it
// is answered (admission): the watcher does not push that state again, which
// cost the hub a read of the job per job admitted. What changes after is
// pushed, and a new stream pushes every job again.
func TestStatusAnsweredToTheControllerIsNotPushedAgain(t *testing.T) {
	ctx := context.Background()
	exec := newMockExecutor()
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, exec, logr.Discard())
	stream := newRecordingStream()
	watcher := newStatusWatcher(exec, a.tracker, newStreamSender(stream), "vm-test", logr.Discard())

	exec.setPhase("handle-1", v1.PhaseRunning)
	a.tracker.Add("job-1", "handle-1")
	if resp := a.Status(ctx, &agentpb.StatusRequest{Id: "job-1", HandleName: "handle-1"}); resp.GetStatus().GetPhase() != string(v1.PhaseRunning) {
		t.Fatalf("Status answered %+v", resp)
	}
	watcher.pollAll(ctx)
	if n := len(stream.statusUpdates()); n != 0 {
		t.Fatalf("%d pushes of the state the controller was answered, want 0", n)
	}

	exec.setPhase("handle-1", v1.PhaseSucceeded)
	watcher.pollAll(ctx)
	if updates := stream.statusUpdates(); len(updates) != 1 || updates[0].GetStatus().GetPhase() != string(v1.PhaseSucceeded) {
		t.Fatalf("pushes after the job ended: %+v, want its Succeeded", updates)
	}

	exec.setPhase("handle-2", v1.PhaseRunning)
	a.tracker.Add("job-2", "handle-2")
	a.Status(ctx, &agentpb.StatusRequest{Id: "job-2", HandleName: "handle-2"})
	watcher.pollAll(ctx)
	again := newRecordingStream()
	newStatusWatcher(exec, a.tracker, newStreamSender(again), "vm-test", logr.Discard()).pollAll(ctx)
	if updates := again.statusUpdates(); len(updates) != 1 || updates[0].GetJobId() != "job-2" {
		t.Fatalf("a new stream's pushes: %+v, want job-2's state again", updates)
	}

	// Only the first push is spared: a poll answered after the watcher saw
	// the job is not kept for a later stream, whose first push of every job
	// is what a new hub learns them from.
	exec.setPhase("handle-3", v1.PhaseRunning)
	a.tracker.Add("job-3", "handle-3")
	seen := newRecordingStream()
	watcher = newStatusWatcher(exec, a.tracker, newStreamSender(seen), "vm-test", logr.Discard())
	watcher.pollAll(ctx)
	a.Status(ctx, &agentpb.StatusRequest{Id: "job-3", HandleName: "handle-3"})
	watcher.pollAll(ctx)
	later := newRecordingStream()
	newStatusWatcher(exec, a.tracker, newStreamSender(later), "vm-test", logr.Discard()).pollAll(ctx)
	if updates := later.statusUpdates(); !slices.ContainsFunc(updates, func(u *agentpb.StatusUpdate) bool { return u.GetJobId() == "job-3" }) {
		t.Fatalf("a later stream's pushes: %+v, want job-3's state again", updates)
	}
}

// A job's end goes after its held checkpoint commits: the controller judges
// the end on the job's progress, and an end pushed first was judged on a
// progress without the commits the agent still held, the phase's COMPLETE
// refused when it came, for a job already over.
func TestAJobsEndWaitsForItsHeldCommits(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	stream := newRecordingStream()
	exec.setPhase("handle-1", v1.PhaseSucceeded)
	tracker.Add("job-1", "handle-1")
	watcher := newStatusWatcher(exec, tracker, newStreamSender(stream), "vm-test", logr.Discard())
	held := true
	watcher.heldCommits = func(_ context.Context, jobID string) bool { return held && jobID == "job-1" }

	ctx := context.Background()
	watcher.pollAll(ctx)
	if updates := stream.statusUpdates(); len(updates) != 0 || tracker.Count() != 1 {
		t.Fatalf("pushed %d updates with the job's commits held (tracked %d), want none and the job still watched", len(updates), tracker.Count())
	}
	held = false
	watcher.pollAll(ctx)
	if updates := stream.statusUpdates(); len(updates) != 1 || updates[0].GetStatus().GetPhase() != string(v1.PhaseSucceeded) {
		t.Fatalf("after the commits were decided pushed %v, want the Succeeded", updates)
	}
}
