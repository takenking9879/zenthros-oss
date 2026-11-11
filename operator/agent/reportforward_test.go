// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/reportspool"
)

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
	waits   []time.Duration
}

type fakeWaiter struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0).UTC()}
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waits = append(c.waits, d)
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, fakeWaiter{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	var keep []fakeWaiter
	for _, waiter := range c.waiters {
		if waiter.at.After(c.now) {
			keep = append(keep, waiter)
			continue
		}
		waiter.ch <- c.now
	}
	c.waiters = keep
}

func (c *fakeClock) Waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.waits))
	copy(out, c.waits)
	return out
}

type scriptedStream struct {
	mu   sync.Mutex
	sent []*agentpb.RegisterRequest
	fail error
	hang chan struct{}
}

func (s *scriptedStream) Send(msg *agentpb.RegisterRequest) error {
	if s.hang != nil {
		<-s.hang
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.sent = append(s.sent, msg)
	return nil
}

func (s *scriptedStream) Recv() (*agentpb.RegisterResponse, error) {
	return nil, context.Canceled
}

func (s *scriptedStream) setFail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = err
}

func (s *scriptedStream) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func testForwarder(t *testing.T, clock *fakeClock) (*reportForwarder, *reportspool.Spool) {
	t.Helper()
	spool, err := reportspool.Open(context.Background(), reportspool.Config{
		Path: filepath.Join(t.TempDir(), "reports.sqlite"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	forwarder := newReportForwarder(spool, logr.Discard())
	forwarder.afterFn = clock.After
	forwarder.jitter = func(d time.Duration) time.Duration { return d }
	forwarder.sendTimeout = 0
	return forwarder, spool
}

func insertReport(t *testing.T, spool *reportspool.Spool, jobID, reportID string) {
	t.Helper()
	_, err := spool.Insert(context.Background(), executionreport.Snapshot{
		APIVersion: executionreport.APIVersion,
		Kind:       executionreport.Kind,
		JobID:      jobID,
		ProducerID: "worker-0",
		ReportID:   reportID,
		ProducedAt: time.Unix(1, 0).UTC(),
		Payload:    json.RawMessage(`{"n":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func lastWaitIs(clock *fakeClock, want time.Duration) bool {
	waits := clock.Waits()
	return len(waits) > 0 && waits[len(waits)-1] == want
}

func hasWait(clock *fakeClock, want time.Duration) bool {
	for _, wait := range clock.Waits() {
		if wait == want {
			return true
		}
	}
	return false
}

func TestReportForwarderFlushSendsUnacked(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	insertReport(t, spool, "job-1", "rep-1")
	stream := &scriptedStream{}
	forwarder.setSender(newStreamSender(stream))
	if err := forwarder.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stream.sentCount() != 1 {
		t.Fatalf("sent = %d", stream.sentCount())
	}
}

func startForwarder(t *testing.T, forwarder *reportForwarder) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go forwarder.Run(ctx)
	return cancel
}

func TestReportCircuitWaitSequenceAndCap(t *testing.T) {
	var circuit reportCircuit
	if circuit.wait() != 0 {
		t.Fatalf("idle wait = %s", circuit.wait())
	}
	want := []time.Duration{
		2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second, 64 * time.Second, 128 * time.Second, 128 * time.Second,
	}
	for i, expected := range want {
		circuit.fail()
		if got := circuit.wait(); got != expected {
			t.Fatalf("after %d failures wait = %s, want %s", i+1, got, expected)
		}
	}
	circuit.reset()
	if circuit.wait() != 0 {
		t.Fatalf("reset wait = %s", circuit.wait())
	}
}

func TestReportForwarderBackoffSequenceCapsAndResetsOnSuccess(t *testing.T) {
	// The sender may be set before the forwarder runs: its reset is then
	// left pending, and the backoff it wakes registers the same wait again.
	// Under -race with every package running, the forwarder's goroutine
	// started late and the test waited for the next step while the
	// forwarder waited on a repeat of this one (2026-10-21).
	for name, senderFirst := range map[string]bool{"forwarder started first": false, "sender set first": true} {
		t.Run(name, func(t *testing.T) {
			clock := newFakeClock()
			forwarder, spool := testForwarder(t, clock)
			insertReport(t, spool, "job-1", "rep-1")
			stream := &scriptedStream{fail: errors.New("destination down")}
			if senderFirst {
				forwarder.setSender(newStreamSender(stream))
				startForwarder(t, forwarder)
			} else {
				startForwarder(t, forwarder)
				forwarder.setSender(newStreamSender(stream))
			}
			backoffSequence(t, forwarder, spool, clock, stream)
		})
	}
}

func backoffSequence(t *testing.T, forwarder *reportForwarder, spool *reportspool.Spool, clock *fakeClock, stream *scriptedStream) {
	t.Helper()
	sequence := []time.Duration{
		2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second, 64 * time.Second, 128 * time.Second, 128 * time.Second,
	}
	// Each step is a wait registered after the step before ended: the cap
	// is a new 128s, not the last one seen again. Only the first step may
	// come twice (the pending reset above).
	seen := 0
	for i, want := range sequence {
		var unexpected []time.Duration
		testutil.Eventually(t, "the forwarder waits the next backoff step", func() bool {
			waits := clock.Waits()
			for ; seen < len(waits); seen++ {
				switch got := waits[seen]; {
				case got == want:
					seen++
					return true
				case i == 1 && got == sequence[0]:
					clock.Advance(got)
				default:
					unexpected = waits[seen:]
					return true
				}
			}
			return false
		})
		if unexpected != nil {
			t.Fatalf("backoff step %d: waits %v, want %s next", i, unexpected, want)
		}
		if stream.sentCount() != 0 {
			t.Fatalf("successful send during backoff step %d waits=%v", i, clock.Waits())
		}
		clock.Advance(want)
	}

	stream.setFail(nil)
	forwarder.Wake()
	testutil.Eventually(t, "the report goes once the stream answers", func() bool {
		clock.Advance(128 * time.Second)
		return stream.sentCount() >= 1
	})

	insertReport(t, spool, "job-1", "rep-2")
	stream.setFail(errors.New("down again"))
	forwarder.Wake()
	testutil.Eventually(t, "hasWait(clock, 2*time.Second) && lastWaitIs(clock, 2*time.Second)", func() bool { return hasWait(clock, 2*time.Second) && lastWaitIs(clock, 2*time.Second) })
}

func TestReportForwarderSetSenderResetsBackoff(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	insertReport(t, spool, "job-1", "rep-1")
	down := &scriptedStream{fail: errors.New("down")}
	startForwarder(t, forwarder)
	forwarder.setSender(newStreamSender(down))
	testutil.Eventually(t, "hasWait(clock, 2*time.Second)", func() bool { return hasWait(clock, 2*time.Second) })

	up := &scriptedStream{}
	forwarder.setSender(newStreamSender(up))
	testutil.Eventually(t, "up.sentCount() >= 1", func() bool { return up.sentCount() >= 1 })
	if hasWait(clock, 4*time.Second) {
		t.Fatalf("reconnect must reset the circuit, waits = %v", clock.Waits())
	}
}

func TestReportForwarderOneCircuitForManyJobs(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	for i := 0; i < 8; i++ {
		insertReport(t, spool, fmt.Sprintf("job-%d", i), fmt.Sprintf("rep-%d", i))
	}
	stream := &scriptedStream{fail: errors.New("down")}
	startForwarder(t, forwarder)
	forwarder.setSender(newStreamSender(stream))
	testutil.Eventually(t, "hasWait(clock, 2*time.Second)", func() bool { return hasWait(clock, 2*time.Second) })
	if stream.sentCount() != 0 {
		t.Fatalf("failed send must abort the flush, sent = %d", stream.sentCount())
	}
}

func TestReportForwarderHappyPathIsImmediate(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	stream := &scriptedStream{}
	startForwarder(t, forwarder)
	forwarder.setSender(newStreamSender(stream))
	insertReport(t, spool, "job-1", "rep-1")
	forwarder.Wake()
	// The clock never moves: no backoff stood before the send.
	testutil.Eventually(t, "stream.sentCount() >= 1", func() bool { return stream.sentCount() >= 1 })
}

func TestReportForwarderSendTimeoutTripsCircuit(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	forwarder.sendTimeout = 5 * time.Second
	insertReport(t, spool, "job-1", "rep-1")
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	stream := &scriptedStream{hang: hang}
	startForwarder(t, forwarder)
	forwarder.setSender(newStreamSender(stream))
	testutil.Eventually(t, "hasWait(clock, 5*time.Second)", func() bool { return hasWait(clock, 5*time.Second) })
	clock.Advance(5 * time.Second)
	testutil.Eventually(t, "hasWait(clock, 2*time.Second)", func() bool { return hasWait(clock, 2*time.Second) })
}

// A wake sends only the reports not already on their way: a report sent on
// the current stream waits resendAfter for its ack, an acked one is never
// sent again, and a new stream gets everything still unacked.
func TestReportForwarderDoesNotResendWhatIsInFlight(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	forwarder.nowFn = func() time.Time {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		return clock.now
	}
	forwarder.resendAfter = 30 * time.Second
	stream := &scriptedStream{}
	forwarder.setSender(newStreamSender(stream))
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		insertReport(t, spool, "job-1", fmt.Sprintf("rep-%d", i))
		if err := forwarder.flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := stream.sentCount(); n != 3 {
		t.Fatalf("three reports, each flushed on its own wake, cost %d sends; want 3", n)
	}

	forwarder.HandleAck(ctx, &agentpb.ExecutionReportAck{JobId: "job-1", ProducerId: "worker-0", ReportId: "rep-1", Accepted: true})
	clock.Advance(31 * time.Second)
	if err := forwarder.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := stream.sentCount(); n != 5 {
		t.Fatalf("sends = %d after the ack window, want 5 (rep-2 and rep-3 again, not the acked rep-1)", n)
	}

	fresh := &scriptedStream{}
	forwarder.setSender(newStreamSender(fresh))
	if err := forwarder.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := fresh.sentCount(); n != 2 {
		t.Fatalf("a new stream got %d reports, want the 2 still unacked", n)
	}
}

// A report the Hub could not store (STORE_UNAVAILABLE) is sent again after
// the backoff with nothing else to wake the forwarder. A job's result is its
// last report: right after a controller restart the Hub refused it, and it
// waited in the spool for a wake that never came.
func TestRetryableAckSendsTheReportAgain(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	forwarder.nowFn = func() time.Time {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		return clock.now
	}
	stream := &scriptedStream{}
	startForwarder(t, forwarder)
	forwarder.setSender(newStreamSender(stream))
	insertReport(t, spool, "job-1", "rep-1")
	forwarder.Wake()
	// Idle: the report is out and its resend window is set.
	testutil.Eventually(t, "stream.sentCount() == 1 && hasWait(clock, reportResendAfter)", func() bool { return stream.sentCount() == 1 && hasWait(clock, reportResendAfter) })

	forwarder.HandleAck(context.Background(), &agentpb.ExecutionReportAck{JobId: "job-1", ProducerId: "worker-0", ReportId: "rep-1",
		ErrorCode: executionreport.CodeStoreUnavailable})
	testutil.Eventually(t, "hasWait(clock, reportBackoffInitial)", func() bool { return hasWait(clock, reportBackoffInitial) })
	if stream.sentCount() != 1 {
		t.Fatal("a refused report went again before the backoff")
	}
	clock.Advance(reportBackoffInitial)
	testutil.Eventually(t, "stream.sentCount() == 2", func() bool { return stream.sentCount() == 2 })

	// Refused again: the wait doubles.
	forwarder.HandleAck(context.Background(), &agentpb.ExecutionReportAck{JobId: "job-1", ProducerId: "worker-0", ReportId: "rep-1",
		ErrorCode: executionreport.CodeStoreUnavailable})
	testutil.Eventually(t, "hasWait(clock, 2*reportBackoffInitial)", func() bool { return hasWait(clock, 2*reportBackoffInitial) })
	clock.Advance(reportBackoffInitial)
	if stream.sentCount() != 2 {
		t.Fatal("a second refusal waited only the first backoff")
	}
	clock.Advance(reportBackoffInitial)
	testutil.Eventually(t, "stream.sentCount() == 3", func() bool { return stream.sentCount() == 3 })
}

// A report sent and never acked (the ack was lost with a Gateway) goes again
// once resendAfter passes, with no new report or stream to wake it.
func TestUnackedReportGoesAgainWithoutAWake(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	forwarder.nowFn = func() time.Time {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		return clock.now
	}
	forwarder.resendAfter = 30 * time.Second
	stream := &scriptedStream{}
	startForwarder(t, forwarder)
	forwarder.setSender(newStreamSender(stream))
	insertReport(t, spool, "job-1", "rep-1")
	forwarder.Wake()
	testutil.Eventually(t, "stream.sentCount() == 1 && hasWait(clock, 30*time.Second)", func() bool { return stream.sentCount() == 1 && hasWait(clock, 30*time.Second) })
	clock.Advance(29 * time.Second)
	time.Sleep(50 * time.Millisecond)
	if n := stream.sentCount(); n != 1 {
		t.Fatalf("sent %d times before its 30 s", n)
	}
	// The wait seen may be a pass the setSender reset and the wake left
	// behind; the forwarder's last pass asks the fake clock for its 30 s
	// from whenever it gets there, so the clock moves a second at a time
	// until it fires: by 59 s at the latest, never minutes later.
	advanced := 29 * time.Second
	testutil.Eventually(t, "stream.sentCount() >= 2", func() bool {
		if stream.sentCount() >= 2 {
			return true
		}
		if advanced > time.Minute {
			t.Fatalf("not sent again %s after its first send", advanced)
		}
		clock.Advance(time.Second)
		advanced += time.Second
		return false
	})
}

// A flush reads the payload of a report it sends, not of every one that
// waits for its ack: each wake (one per new report) read the whole unacked
// backlog with its payloads before it found none was due (05#3).
func TestAFlushReadsOnlyThePayloadsItSends(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	forwarder.resendAfter = time.Hour
	loads := 0
	forwarder.loadFn = func(ctx context.Context, key reportspool.Key) (executionreport.Snapshot, bool, error) {
		loads++
		return spool.Load(ctx, key)
	}
	for i := range 3 {
		insertReport(t, spool, "job-1", fmt.Sprintf("rep-%d", i))
	}
	stream := &scriptedStream{}
	forwarder.setSender(newStreamSender(stream))
	if err := forwarder.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stream.sentCount() != 3 || loads != 3 {
		t.Fatalf("first flush sent %d, read %d payloads; want 3 and 3", stream.sentCount(), loads)
	}
	insertReport(t, spool, "job-1", "rep-new")
	if err := forwarder.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stream.sentCount() != 4 || loads != 4 {
		t.Fatalf("a flush with one new report sent %d in all, read %d payloads; want 4 and 4", stream.sentCount(), loads)
	}
}

// ackingStream is a hub so fast its ack lands before Send returns.
type ackingStream struct {
	scriptedStream
	forwarder *reportForwarder
}

func (s *ackingStream) Send(msg *agentpb.RegisterRequest) error {
	if err := s.scriptedStream.Send(msg); err != nil {
		return err
	}
	report := msg.GetExecutionReport()
	s.forwarder.HandleAck(context.Background(), &agentpb.ExecutionReportAck{
		JobId: report.GetJobId(), ProducerId: report.GetProducerId(), ReportId: report.GetReportId(), Accepted: true})
	return nil
}

// An ack that lands before the send is recorded left an in-flight entry for
// a report the spool no longer holds: once due it was due forever, and Run
// flushed in a loop until the stream changed (05#6). In flight is only what
// the spool still has unacked.
func TestAnAckBeforeTheSendReturnsLeavesNothingInFlight(t *testing.T) {
	clock := newFakeClock()
	forwarder, spool := testForwarder(t, clock)
	insertReport(t, spool, "job-1", "rep-1")
	forwarder.setSender(newStreamSender(&ackingStream{forwarder: forwarder}))
	ctx := context.Background()
	for range 2 {
		if err := forwarder.flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	forwarder.mu.Lock()
	defer forwarder.mu.Unlock()
	if due, ok := forwarder.nextDue(); ok {
		t.Fatalf("an acked report is still in flight, due %s", due)
	}
}
