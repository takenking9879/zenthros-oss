// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"time"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/metrics"
)

const statusWatchInterval = 2 * time.Second

// StatusWatcher polls the executor for each active job and pushes a StatusUpdate
// over the control stream when the observed phase changes. Terminal jobs are
// removed from the tracker after the final update is sent. One watcher serves
// one stream: a new stream starts with nothing seen, so every job's state is
// pushed again after a reconnect.
type StatusWatcher struct {
	executor  Executor
	tracker   *jobTracker
	sender    *streamSender
	backendID string
	logger    Logger
	interval  time.Duration
	lastPhase map[string]v1.Phase
	// lastEndpoints is each job's endpoint key last pushed: a Service whose
	// port opens is pushed with its phase unchanged.
	lastEndpoints map[string]string
	// decorate adds what the agent knows beyond the executor (a Service's
	// endpoints); finished is told a job ended. Both optional.
	decorate func(ctx context.Context, handle string, state backend.JobState) backend.JobState
	finished func(handle string)
	// heldCommits reports a job with checkpoint commits the controller has
	// not decided yet; its end is pushed after them. Optional.
	heldCommits func(ctx context.Context, jobID string) bool
}

func newStatusWatcher(executor Executor, tracker *jobTracker, sender *streamSender, backendID string, logger Logger) *StatusWatcher {
	return &StatusWatcher{
		executor:  executor,
		tracker:   tracker,
		sender:    sender,
		backendID: backendID,
		logger:    logger.WithName("status-watcher"),
		interval:  statusWatchInterval,
		lastPhase: make(map[string]v1.Phase),

		lastEndpoints: make(map[string]string),
	}
}

// Run polls active jobs until ctx is cancelled. It is meant to run as a goroutine
// alongside serveStream.
func (w *StatusWatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.pollAll(ctx)
		}
	}
}

// batchStatusReader is an Executor that reads many jobs' states at once:
// the systemd one reads every unit in one `systemctl show` per sweep instead
// of a command per job.
type batchStatusReader interface {
	// StatusAll answers each handle at its index.
	StatusAll(ctx context.Context, handles []string) ([]backend.JobState, []error)
}

// readStates reads every job's state, at once when the executor can.
func (w *StatusWatcher) readStates(ctx context.Context, jobs []activeJob) ([]backend.JobState, []error) {
	if batch, ok := w.executor.(batchStatusReader); ok && len(jobs) > 0 {
		handles := make([]string, len(jobs))
		for i, job := range jobs {
			handles[i] = job.handleName
		}
		return batch.StatusAll(ctx, handles)
	}
	states, errs := make([]backend.JobState, len(jobs)), make([]error, len(jobs))
	for i, job := range jobs {
		states[i], errs[i] = w.executor.Status(ctx, job.handleName)
	}
	return states, errs
}

func (w *StatusWatcher) pollAll(ctx context.Context) {
	started := time.Now()
	pollErrors := 0
	jobs := w.tracker.Snapshot()
	states, errs := w.readStates(ctx, jobs)
	for i, job := range jobs {
		state, err := states[i], errs[i]
		if err != nil {
			pollErrors++
			w.logger.Error(err, "status poll failed", "handle", job.handleName)
			continue
		}
		// Checked after the read: a Cancel that began before it may have
		// stopped the unit, and the state read is then its teardown.
		if !w.tracker.Watching(job.handleName) {
			continue
		}
		if w.decorate != nil {
			state = w.decorate(ctx, job.handleName, state)
		}
		_, seen := w.lastPhase[job.handleName]
		key := endpointKey(state)
		// Taken at every poll: a state answered after the watcher saw the
		// job was kept, and spared a later stream's first push of it.
		answered, wasAnswered := w.tracker.TakeAnswered(job.handleName)
		if !seen && !state.Phase.Terminal() {
			// The state the controller was answered at admission is the one
			// it wrote then: not pushed again. An end is always pushed: it
			// is what lets the job go.
			if wasAnswered && answered == statusKey(state) {
				w.observeLifecycle(job, state)
				w.lastPhase[job.handleName], w.lastEndpoints[job.handleName] = state.Phase, key
				continue
			}
		}
		if seen && state.Phase == w.lastPhase[job.handleName] && key == w.lastEndpoints[job.handleName] {
			continue
		}
		// The controller judges an end on the job's progress: its held
		// commits go first (checkpointWaiter.Run), and the next poll pushes
		// the end.
		if state.Phase.Terminal() && w.heldCommits != nil && w.heldCommits(ctx, job.jobID) {
			continue
		}
		// A push that fails is not seen: the next poll sends it again, and a
		// terminal job stays tracked until its end is delivered. The
		// controller learns a job's end from this push, not by asking.
		if !w.push(ctx, job, state) {
			continue
		}
		w.observeLifecycle(job, state)
		w.lastPhase[job.handleName] = state.Phase
		w.lastEndpoints[job.handleName] = key
		if state.Phase.Terminal() {
			w.tracker.MarkFinished(job.handleName, time.Now())
			w.tracker.Remove(job.handleName)
			delete(w.lastPhase, job.handleName)
			delete(w.lastEndpoints, job.handleName)
			if w.finished != nil {
				w.finished(job.handleName)
			}
		}
	}
	metrics.ObserveStatusPoll(time.Since(started), pollErrors)
}

// statusKey is what a push of state says that the watcher compares: its
// phase and its endpoints.
func statusKey(state backend.JobState) string {
	return string(state.Phase) + "/" + endpointKey(state)
}

// observeLifecycle records the agent job metrics driven by phase transitions:
// setup time the first time a job is observed Running, and the terminal result
// counter. Teardown is measured at Cancel, not here (handlers.go).
func (w *StatusWatcher) observeLifecycle(job activeJob, state backend.JobState) {
	switch {
	case state.Phase == v1.PhaseRunning && w.lastPhase[job.handleName] != v1.PhaseRunning:
		if !job.submittedAt.IsZero() {
			metrics.ObserveAgentJobSetup(time.Since(job.submittedAt))
		}
	case state.Phase == v1.PhaseSucceeded:
		metrics.RecordAgentJobResult(metrics.JobSucceeded)
	case state.Phase == v1.PhaseFailed:
		metrics.RecordAgentJobResult(metrics.JobFailed)
	}
}

// push reports whether the update was handed to the stream.
func (w *StatusWatcher) push(ctx context.Context, job activeJob, state backend.JobState) bool {
	msg := &agentpb.RegisterRequest{
		StatusUpdate: &agentpb.StatusUpdate{
			BackendId:  w.backendID,
			JobId:      job.jobID,
			HandleName: job.handleName,
			Status:     toProtoStatus(state),
		},
	}
	if err := w.sender.Send(msg); err != nil {
		w.logger.Error(err, "failed to push status update", "handle", job.handleName, "phase", state.Phase)
		return false
	}
	w.logger.Info("pushed status update", "job_id", job.jobID, "handle", job.handleName, "phase", state.Phase)
	return true
}
