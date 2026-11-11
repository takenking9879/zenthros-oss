// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/zenthros/operator/pkg/runtimegate"

	"github.com/zenthros/operator/pkg/capability"
)

func TestHeartbeatDefaultIntervalIsFifteenSeconds(t *testing.T) {
	hb := newHeartbeatSender(newJobTracker(), newStreamSender(newRecordingStream()), "vm-test", 1, nil, logr.Discard())
	if hb.interval != 15*time.Second {
		t.Fatalf("interval = %s, want 15s", hb.interval)
	}
}

func TestHeartbeatIntervalFromConfig(t *testing.T) {
	cfg := Config{HeartbeatInterval: 40 * time.Millisecond}
	if cfg.heartbeatInterval() != 40*time.Millisecond {
		t.Fatalf("configured interval = %s, want 40ms", cfg.heartbeatInterval())
	}
	if (Config{}).heartbeatInterval() != 15*time.Second {
		t.Fatalf("empty config interval = %s, want 15s", (Config{}).heartbeatInterval())
	}
}

func TestGuestRuntimeReadyUsesMarkerFiles(t *testing.T) {
	dir := t.TempDir()
	ready := dir + "/runtime-ready"
	pending := dir + "/runtime-pending"
	t.Setenv("ZENTHROS_RUNTIME_READY_FILE", ready)
	t.Setenv("ZENTHROS_RUNTIME_PENDING_FILE", pending)
	if !guestRuntimeReady() {
		t.Fatal("neither file: runtime is ready (no guest module)")
	}
	if err := os.WriteFile(pending, []byte("python"), 0o644); err != nil {
		t.Fatal(err)
	}
	if guestRuntimeReady() {
		t.Fatal("pending file: runtime must not be ready")
	}
	if err := os.WriteFile(ready, []byte("python"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !guestRuntimeReady() {
		t.Fatal("ready file wins over pending")
	}
}

func TestHeartbeatSendsImmediatelyOnRun(t *testing.T) {
	stream := newRecordingStream()
	hb := newHeartbeatSender(newJobTracker(), newStreamSender(stream), "vm-test", 0, nil, logr.Discard())
	hb.interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hb.Run(ctx)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("first heartbeat must not wait for the ticker")
		default:
		}
		if len(stream.heartbeats()) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestHeartbeatSendsWhenRuntimeBecomesReady(t *testing.T) {
	dir := t.TempDir()
	ready := dir + "/runtime-ready"
	pending := dir + "/runtime-pending"
	t.Setenv("ZENTHROS_RUNTIME_READY_FILE", ready)
	t.Setenv("ZENTHROS_RUNTIME_PENDING_FILE", pending)
	if err := os.WriteFile(pending, []byte("python"), 0o644); err != nil {
		t.Fatal(err)
	}
	stream := newRecordingStream()
	hb := newHeartbeatSender(newJobTracker(), newStreamSender(stream), "vm-test", 4, nil, logr.Discard())
	hb.interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hb.Run(ctx)
	first := 0
	for wait := time.Now().Add(2 * time.Second); first == 0; first = len(stream.heartbeats()) {
		if time.Now().After(wait) {
			t.Fatal("expected immediate heartbeat while pending")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stream.heartbeats()[0].GetMaxJobs() != -1 {
		t.Fatalf("pending heartbeat maxJobs = %d, want -1", stream.heartbeats()[0].GetMaxJobs())
	}
	if err := os.WriteFile(ready, []byte("python"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("expected a heartbeat once runtime-ready appeared")
		default:
		}
		hbs := stream.heartbeats()
		if len(hbs) > first {
			got := hbs[len(hbs)-1]
			if got.GetMaxJobs() != 4 {
				t.Fatalf("ready heartbeat maxJobs = %d, want 4", got.GetMaxJobs())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHeartbeatSendsPeriodically(t *testing.T) {
	tracker := newJobTracker()
	stream := newRecordingStream()
	sender := newStreamSender(stream)

	tracker.Add("job-1", "handle-1")
	tracker.Add("job-2", "handle-2")

	hb := newHeartbeatSender(tracker, sender, "vm-test", 0, nil, logr.Discard())
	hb.interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go hb.Run(ctx)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for heartbeat")
		default:
		}
		if len(stream.heartbeats()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	hbs := stream.heartbeats()
	if len(hbs) == 0 {
		t.Fatal("expected at least one heartbeat")
	}
	got := hbs[0]
	if got.GetBackendId() != "vm-test" {
		t.Errorf("backendId = %q, want vm-test", got.GetBackendId())
	}
	if got.GetRunningJobs() != 2 {
		t.Errorf("runningJobs = %d, want 2", got.GetRunningJobs())
	}
	if got.GetTimestampUnix() == 0 {
		t.Error("expected non-zero timestamp")
	}
}

func TestHeartbeatStopsOnContextCancel(t *testing.T) {
	tracker := newJobTracker()
	stream := newRecordingStream()
	sender := newStreamSender(stream)

	hb := newHeartbeatSender(tracker, sender, "vm-test", 0, nil, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		hb.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat did not stop after context cancel")
	}
}

// TestHeartbeatReportsCapacity covers Phase J: the heartbeat carries the agent's
// advertised concurrent-job capacity (max_jobs) alongside the running count, so the
// controller can refuse new submissions to a backend that is full (BACKEND_BUSY).
func TestHeartbeatReportsCapacity(t *testing.T) {
	tracker := newJobTracker()
	stream := newRecordingStream()
	sender := newStreamSender(stream)

	tracker.Add("job-1", "handle-1")

	hb := newHeartbeatSender(tracker, sender, "vm-test", 4, nil, logr.Discard())
	hb.send()

	hbs := stream.heartbeats()
	if len(hbs) != 1 {
		t.Fatalf("heartbeats = %d, want 1", len(hbs))
	}
	if got := hbs[0].GetRunningJobs(); got != 1 {
		t.Errorf("runningJobs = %d, want 1", got)
	}
	if got := hbs[0].GetMaxJobs(); got != 4 {
		t.Errorf("maxJobs = %d, want 4", got)
	}
	if hbs[0].GetPreempted() {
		t.Error("preempted must stay unset when notices are disabled")
	}
}

func TestHeartbeatReportsPreemptedNotice(t *testing.T) {
	tracker := newJobTracker()
	stream := newRecordingStream()
	sender := newStreamSender(stream)
	hb := newHeartbeatSender(tracker, sender, "vm-test", 1, nil, logr.Discard())
	deadline := time.Unix(1790000000, 0)
	hb.preempted = func() *preemptionNotice {
		return &preemptionNotice{Preempted: true, Source: noticeAWSIMDS, Action: "terminate", Deadline: deadline}
	}
	hb.send()
	hbs := stream.heartbeats()
	if len(hbs) != 1 {
		t.Fatalf("heartbeats = %d, want 1", len(hbs))
	}
	if !hbs[0].GetPreempted() {
		t.Fatal("heartbeat must carry preempted when a notice exists")
	}
	// The controller bounds the worker's drain window by the provider's deadline.
	if hbs[0].GetPreemptSource() != noticeAWSIMDS || hbs[0].GetPreemptAction() != "terminate" || hbs[0].GetPreemptDeadlineUnix() != deadline.Unix() {
		t.Fatalf("notice fields = %s %s %d", hbs[0].GetPreemptSource(), hbs[0].GetPreemptAction(), hbs[0].GetPreemptDeadlineUnix())
	}
}

func TestHeartbeatReportsRebalanceRecommendation(t *testing.T) {
	stream := newRecordingStream()
	hb := newHeartbeatSender(newJobTracker(), newStreamSender(stream), "vm-test", 1, nil, logr.Discard())
	hb.send()
	at := time.Unix(1790000000, 0)
	hb.rebalance = func() *time.Time { return &at }
	hb.send()
	hbs := stream.heartbeats()
	if len(hbs) != 2 || hbs[0].GetRebalanceRecommendedUnix() != 0 || hbs[1].GetRebalanceRecommendedUnix() != at.Unix() {
		t.Fatalf("heartbeats %v, want no recommendation, then %d", hbs, at.Unix())
	}
	if hbs[1].GetPreempted() {
		t.Fatal("a recommendation is not a notice")
	}
}

// The ack of a heartbeat is timed from its send, once: an ack with no
// heartbeat out (a duplicate, or one from before a reconnect) is not timed.
// This is the isolation gauge: if logs or a slow Gateway hold the control
// stream, the send-to-ack time grows.
func TestHeartbeatAckIsTimedOncePerSend(t *testing.T) {
	hb := newHeartbeatSender(newJobTracker(), newStreamSender(newRecordingStream()), "vm-test", 0, nil, logr.Discard())
	var observed []time.Duration
	hb.observeAck = func(d time.Duration) { observed = append(observed, d) }

	hb.acked()
	if len(observed) != 0 {
		t.Fatalf("ack before any heartbeat timed %v", observed)
	}
	hb.send()
	hb.acked()
	hb.acked()
	if len(observed) != 1 {
		t.Fatalf("one heartbeat, two acks: timed %d, want 1", len(observed))
	}
	if observed[0] < 0 || observed[0] > time.Second {
		t.Fatalf("send-to-ack = %v", observed[0])
	}
}

// The gateway keeps the last heartbeat's job count. A tick that read a job
// still running, then waited on the executor's declaration, was sent after
// the heartbeat of that job's end: the machine stayed full until the next
// tick, 15 s later, and the job sent to it waited that long (P4 at 30 jobs/s,
// 0,67 % of jobs ~8 s, 2026-10-25). Heartbeats go one at a time, each with
// the count when its turn comes.
func TestAHeartbeatOfAJobsEndIsNotOvertakenByAnOlderCount(t *testing.T) {
	tracker := newJobTracker()
	stream := newRecordingStream()
	tracker.Add("job-1", "handle-1")
	hb := newHeartbeatSender(tracker, newStreamSender(stream), "vm-test", 1, nil, logr.Discard())
	inDeclare, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	hb.advertised = &atomic.Value{}
	hb.declare = func(context.Context) (capability.Set, capability.Host, string) {
		if calls.Add(1) == 1 {
			close(inDeclare)
			<-release
		}
		return capability.Set{}, capability.Host{}, ""
	}
	tick := make(chan struct{})
	go func() { hb.send(); close(tick) }()
	<-inDeclare
	tracker.Remove("handle-1")
	end := make(chan struct{})
	go func() { hb.send(); close(end) }()
	time.Sleep(50 * time.Millisecond)
	close(release)
	<-tick
	<-end
	hbs := stream.heartbeats()
	if len(hbs) != 2 {
		t.Fatalf("heartbeats = %d, want 2", len(hbs))
	}
	if got := hbs[len(hbs)-1].GetRunningJobs(); got != 0 {
		t.Fatalf("the last heartbeat says %d running jobs after the only one ended: the machine looks full until the next tick", got)
	}
}

// A guest runtime that could not be prepared is said in the heartbeat, with
// the gate closed: the machine takes no work and the hub knows why.
func TestHeartbeatSaysWhyTheRuntimeFailed(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ZENTHROS_RUNTIME_READY_FILE", dir+"/ready")
	t.Setenv("ZENTHROS_RUNTIME_PENDING_FILE", dir+"/pending")
	stream := newRecordingStream()
	hb := newHeartbeatSender(newJobTracker(), newStreamSender(stream), "vm-test", 4, nil, logr.Discard())
	if err := runtimegate.MarkPending(); err != nil {
		t.Fatal(err)
	}
	hb.send()
	if err := runtimegate.MarkFailed("No solution found"); err != nil {
		t.Fatal(err)
	}
	hb.send()
	hbs := stream.heartbeats()
	if hbs[0].GetRuntimeFailure() != "" || hbs[1].GetRuntimeFailure() != "No solution found" || hbs[1].GetMaxJobs() != -1 {
		t.Fatalf("heartbeats %v, want no failure, then the failure with the gate closed", hbs)
	}
}
