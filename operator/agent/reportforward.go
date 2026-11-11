// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"sync"
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/metrics"
	"github.com/zenthros/operator/pkg/reportspool"
)

var errReportSendTimeout = errors.New("execution report send timed out")

type reportForwarder struct {
	spool       *reportspool.Spool
	logger      Logger
	afterFn     func(time.Duration) <-chan time.Time
	jitter      func(time.Duration) time.Duration
	sendTimeout time.Duration
	// resendAfter is how long a report sent on the current stream waits for
	// its ack before it goes again; zero uses reportResendAfter.
	resendAfter time.Duration
	nowFn       func() time.Time
	// loadFn reads one report's payload to send; nil reads the spool.
	loadFn func(context.Context, reportspool.Key) (executionreport.Snapshot, bool, error)

	mu      sync.Mutex
	sender  *streamSender
	circuit reportCircuit
	// inflight is each report sent on the current stream and not acked yet.
	// Without it every wake (one per new report) sent the whole backlog
	// again: N reports faster than their acks cost about N²/2 sends, each a
	// Gateway forward and a Hub write.
	inflight map[string]inflightReport
	wake     chan struct{}
	reset    chan struct{}
}

// reportResendAfter is how long a sent report waits for its ack before it
// goes again on the same stream.
const reportResendAfter = 30 * time.Second

// inflightReport is when a sent report may go again: resendAfter past its
// send, or a backoff past the Hub's refusal to store it.
type inflightReport struct {
	due time.Time
	// refusals counts the retryable acks in a row; each doubles the wait.
	refusals int
}

func inflightKey(jobID, producerID, reportID string) string {
	return jobID + "/" + producerID + "/" + reportID
}

func (f *reportForwarder) now() time.Time {
	if f.nowFn != nil {
		return f.nowFn()
	}
	return time.Now()
}

func newReportForwarder(spool *reportspool.Spool, logger Logger) *reportForwarder {
	return &reportForwarder{
		spool:       spool,
		logger:      logger.WithName("report-forward"),
		sendTimeout: reportSendTimeout,
		wake:        make(chan struct{}, 1),
		reset:       make(chan struct{}, 1),
	}
}

func (f *reportForwarder) setSender(sender *streamSender) {
	f.mu.Lock()
	f.sender = sender
	f.circuit.reset()
	// What went out on the old stream may never have arrived.
	f.inflight = nil
	f.mu.Unlock()
	select {
	case f.reset <- struct{}{}:
	default:
	}
	f.Wake()
}

func (f *reportForwarder) Wake() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *reportForwarder) Run(ctx context.Context) {
	for {
		if err := f.waitBackoff(ctx); err != nil {
			return
		}
		err := f.flush(ctx)
		if ctx.Err() != nil {
			return
		}
		f.mu.Lock()
		if err != nil {
			f.circuit.fail()
			wait := f.circuit.wait()
			f.mu.Unlock()
			f.logger.Error(err, "failed to forward execution reports; backing off", "backoff", wait)
			continue
		}
		f.circuit.reset()
		// A report still waiting for its ack, or refused, goes again when
		// it is due, whether or not anything else wakes the forwarder.
		var resend <-chan time.Time
		if due, ok := f.nextDue(); ok {
			resend = f.after(max(due.Sub(f.now()), 0))
		}
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-f.wake:
		case <-f.reset:
		case <-resend:
		}
	}
}

// nextDue is the earliest time an in-flight report may go again. The
// caller holds mu.
func (f *reportForwarder) nextDue() (time.Time, bool) {
	var next time.Time
	for _, report := range f.inflight {
		if next.IsZero() || report.due.Before(next) {
			next = report.due
		}
	}
	return next, !next.IsZero()
}

func (f *reportForwarder) resendWindow() time.Duration {
	if f.resendAfter > 0 {
		return f.resendAfter
	}
	return reportResendAfter
}

func (f *reportForwarder) waitBackoff(ctx context.Context) error {
	for {
		f.mu.Lock()
		wait := f.circuit.wait()
		f.mu.Unlock()
		if wait <= 0 {
			return nil
		}
		wait = f.backoff(wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.after(wait):
			return nil
		case <-f.reset:
			// setSender resets the circuit; a leftover reset while the
			// circuit is still open must not skip the backoff.
		}
	}
}

// backoff is wait with its jitter.
func (f *reportForwarder) backoff(wait time.Duration) time.Duration {
	if f.jitter != nil {
		return f.jitter(wait)
	}
	return jitterWait(wait)
}

func (f *reportForwarder) after(d time.Duration) <-chan time.Time {
	if f.afterFn != nil {
		return f.afterFn(d)
	}
	return time.After(d)
}

func (f *reportForwarder) flush(ctx context.Context) error {
	f.mu.Lock()
	sender := f.sender
	f.mu.Unlock()
	if sender == nil {
		return nil
	}
	unacked, err := f.spool.UnackedKeys(ctx)
	if err != nil {
		f.logger.Error(err, "failed to list unacked reports")
		return err
	}
	resendAfter := f.resendWindow()
	listed := make(map[string]bool, len(unacked))
	for _, spooled := range unacked {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		key := inflightKey(spooled.JobID, spooled.ProducerID, spooled.ReportID)
		listed[key] = true
		now := f.now()
		f.mu.Lock()
		report, sent := f.inflight[key]
		current := f.sender == sender
		f.mu.Unlock()
		if !current {
			return nil // setSender replaced the stream; its own wake resends
		}
		if sent && now.Before(report.due) {
			continue
		}
		snapshot, ok, err := f.load(ctx, spooled)
		if err != nil {
			f.logger.Error(err, "failed to read an unacked report", "reportID", spooled.ReportID)
			return err
		}
		if !ok {
			continue // acked since it was listed
		}
		msg := &agentpb.RegisterRequest{ExecutionReport: executionreport.SnapshotToProto(snapshot)}
		if err := f.sendWithTimeout(ctx, sender, msg); err != nil {
			f.logger.Error(err, "failed to forward execution report", "reportID", snapshot.ReportID)
			return err
		}
		f.mu.Lock()
		if f.sender == sender {
			if f.inflight == nil {
				f.inflight = map[string]inflightReport{}
			}
			report.due = now.Add(resendAfter)
			f.inflight[key] = report
		}
		f.mu.Unlock()
	}
	// In flight is only what the spool still holds unacked: an ack that
	// landed before its send was recorded left an entry due forever, and Run
	// flushed in a loop (05#6).
	f.mu.Lock()
	if f.sender == sender {
		for key := range f.inflight {
			if !listed[key] {
				delete(f.inflight, key)
			}
		}
	}
	f.mu.Unlock()
	return nil
}

func (f *reportForwarder) load(ctx context.Context, key reportspool.Key) (executionreport.Snapshot, bool, error) {
	if f.loadFn != nil {
		return f.loadFn(ctx, key)
	}
	return f.spool.Load(ctx, key)
}

func (f *reportForwarder) sendWithTimeout(ctx context.Context, sender *streamSender, msg *agentpb.RegisterRequest) error {
	if f.sendTimeout <= 0 {
		return sender.Send(msg)
	}
	done := make(chan error, 1)
	go func() {
		done <- sender.Send(msg)
	}()
	select {
	case err := <-done:
		return err
	case <-f.after(f.sendTimeout):
		return errReportSendTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *reportForwarder) HandleAck(ctx context.Context, ack *agentpb.ExecutionReportAck) {
	snapshot := executionreport.Snapshot{
		JobID: ack.GetJobId(), ProducerID: ack.GetProducerId(), ReportID: ack.GetReportId(), Sequence: ack.GetSequence(),
	}
	if ack.GetAccepted() {
		if created, ok, err := f.spool.CreatedAt(ctx, snapshot); err == nil && ok {
			metrics.ObserveAgentReportDelivery(time.Since(created))
		}
	}
	controllerAck := executionreport.AckFromProto(ack)
	retry := executionreport.IsRetryableACK(controllerAck)
	key := inflightKey(snapshot.JobID, snapshot.ProducerID, snapshot.ReportID)
	f.mu.Lock()
	if report, ok := f.inflight[key]; ok && retry {
		// The Hub could not store it (a controller restarting): it stays
		// spooled and goes again after a backoff of its own. Nothing else
		// may wake the forwarder: a job's result is its last report.
		report.due = f.now().Add(f.backoff((&reportCircuit{failures: report.refusals + 1}).wait()))
		report.refusals++
		f.inflight[key] = report
	} else {
		delete(f.inflight, key)
	}
	f.mu.Unlock()
	if retry {
		f.Wake() // re-arms Run's timer for the new due time
	}
	if err := f.spool.Ack(ctx, snapshot, controllerAck); err != nil {
		f.logger.Error(err, "failed to apply controller report ack", "reportID", ack.GetReportId())
	}
	if stats, err := f.spool.Stats(ctx); err == nil {
		metrics.SetAgentSpoolPending(stats.Count, stats.Bytes, stats.OldestAge)
	}
}

func (a *Agent) injectReportEnv(ctx context.Context, spec *jobspec.JobSpec, env map[string]string) map[string]string {
	if a.spool == nil {
		return env
	}
	token, err := a.spool.IssueToken(ctx, spec.ID)
	if err != nil {
		a.logger.Error(err, "failed to issue report capability token", "job_id", spec.ID)
		return env
	}
	if err := a.spool.BeginAttempt(ctx, spec.ID, spec.Attempt); err != nil {
		a.logger.Error(err, "failed to record the job's attempt in the report spool", "job_id", spec.ID)
		return env
	}
	out := map[string]string{}
	for k, v := range env {
		out[k] = v
	}
	out[EnvJobID] = spec.ID
	out[EnvReportToken] = token
	if a.cfg.ReportEndpoint != "" {
		out[EnvReportEndpoint] = a.cfg.ReportEndpoint
	}
	if a.cfg.PreemptDir != "" {
		out[jobspec.EnvPreemptFile] = preemptFile(a.cfg.PreemptDir, spec.ID)
	}
	return out
}
