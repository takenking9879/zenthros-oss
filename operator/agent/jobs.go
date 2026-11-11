// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"sync"
	"time"

	"github.com/zenthros/operator/pkg/backend"
)

// activeJob is a submitted job the agent tracks for status watching.
type activeJob struct {
	jobID       string
	handleName  string
	submittedAt time.Time
}

// jobTracker holds the set of non-terminal jobs the agent has submitted. Bounded
// by active jobs, not total jobs: terminal jobs are removed by the StatusWatcher.
type jobTracker struct {
	mu   sync.Mutex
	jobs map[string]activeJob
	// finished remembers when a job reached a terminal phase, keyed by handle.
	// It outlives Remove so a later Cancel can measure teardown time; entries
	// are cleared by Forget. Cleanup of finished-but-never-cancelled jobs rides
	// on the GC, which cancels every finished job.
	finished map[string]time.Time
	// cancelling holds the handles a Cancel is tearing down.
	cancelling map[string]bool
	// cancels counts each Cancel's begin and end: a Status that read the
	// executor before either re-tracks nothing (Retrack). Counting begins
	// alone missed a Cancel already under way that ended during the Status
	// (05#4).
	cancels uint64
	// submitting holds the job IDs whose Submit has a slot but is not
	// tracked yet (Claim).
	submitting map[string]bool
	// answered holds, by handle, the state a Status answered the controller
	// (statusKey): the state its admission writes, which the watcher does
	// not push again (TakeAnswered).
	answered map[string]string
}

func newJobTracker() *jobTracker {
	return &jobTracker{jobs: make(map[string]activeJob), finished: make(map[string]time.Time),
		cancelling: make(map[string]bool), submitting: make(map[string]bool), answered: make(map[string]string)}
}

// Claim takes one of max slots for jobID before its Submit runs, so two
// Submits never both take the last one; max 0 is unlimited. A job tracked or
// already being submitted (a Submit sent again) takes none. Add ends the
// claim of a job it tracks; release frees the slot of one whose Submit
// failed, and is a no-op after Add.
func (t *jobTracker) Claim(jobID string, max int) (release func(), ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.submitting[jobID] || t.tracksJob(jobID) {
		return func() {}, true
	}
	if max > 0 && len(t.jobs)+len(t.submitting) >= max {
		return nil, false
	}
	t.submitting[jobID] = true
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		delete(t.submitting, jobID)
	}, true
}

// RunsJob reports a tracked attempt of jobID: another run of the job is on
// this machine.
func (t *jobTracker) RunsJob(jobID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tracksJob(jobID)
}

func (t *jobTracker) tracksJob(jobID string) bool {
	for _, job := range t.jobs {
		if job.jobID == jobID {
			return true
		}
	}
	return false
}

// BeginCancel marks a handle whose unit a Cancel is about to stop; EndCancel
// clears the mark. The job stays tracked (and counted) until the Cancel
// succeeds.
func (t *jobTracker) BeginCancel(handleName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cancelling[handleName] = true
	t.cancels++
}

// Cancels is the count Retrack compares: read it before asking the
// executor.
func (t *jobTracker) Cancels() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cancels
}

// Retrack tracks a running job the agent lost track of (it restarted), unless
// a Cancel began since cancels was read: Status runs beside Cancel, and the
// job a Cancel just removed read as running before it, so it was tracked
// again, counted and polled for ever. A job
// skipped here is tracked by the next Status, if it still runs.
func (t *jobTracker) Retrack(jobID, handleName string, cancels uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, tracked := t.jobs[handleName]; tracked || t.cancels != cancels || t.cancelling[handleName] {
		return false
	}
	t.jobs[handleName] = activeJob{jobID: jobID, handleName: handleName, submittedAt: time.Now()}
	return true
}

func (t *jobTracker) EndCancel(handleName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.cancelling, handleName)
	t.cancels++
}

// Watching reports whether the status watcher may still report the handle:
// it is tracked and no Cancel is tearing it down.
func (t *jobTracker) Watching(handleName string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, tracked := t.jobs[handleName]
	return tracked && !t.cancelling[handleName]
}

// Tracked reports whether the handle is tracked at all, cancelling or not.
func (t *jobTracker) Tracked(handleName string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.jobs[handleName]
	return ok
}

func (t *jobTracker) Add(jobID, handleName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.jobs[handleName] = activeJob{jobID: jobID, handleName: handleName, submittedAt: time.Now()}
	// Tracked, it holds its slot as a job: its claim ends here, not at its
	// Submit's return (05#5).
	delete(t.submitting, jobID)
}

// HandleOf is the handle of the tracked job jobID: a Cancel by job ID, for an
// attempt whose Submit reply never reached the controller.
func (t *jobTracker) HandleOf(jobID string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for handle, job := range t.jobs {
		if job.jobID == jobID {
			return handle, true
		}
	}
	return "", false
}

func (t *jobTracker) Remove(handleName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.jobs, handleName)
	delete(t.answered, handleName)
}

// Answered records the state a Status answered the controller for a tracked
// job.
func (t *jobTracker) Answered(handleName, state string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.jobs[handleName]; ok {
		t.answered[handleName] = state
	}
}

// TakeAnswered is the state last answered the controller for the job, and
// forgets it: it spares one push, the first, and only on the stream that
// takes it.
func (t *jobTracker) TakeAnswered(handleName string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.answered[handleName]
	delete(t.answered, handleName)
	return state, ok
}

// MarkFinished records when a job's workload reached a terminal phase.
func (t *jobTracker) MarkFinished(handleName string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finished[handleName] = at
}

// FinishedAt reports when a job reached a terminal phase, if observed.
func (t *jobTracker) FinishedAt(handleName string) (time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	at, ok := t.finished[handleName]
	return at, ok
}

// Forget drops a finished record after its teardown was measured.
func (t *jobTracker) Forget(handleName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.finished, handleName)
}

func (t *jobTracker) Snapshot() []activeJob {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]activeJob, 0, len(t.jobs))
	for _, job := range t.jobs {
		out = append(out, job)
	}
	return out
}

func (t *jobTracker) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.jobs)
}

// InstalledJob is a workload an executor finds already installed.
type InstalledJob = backend.InstalledJob

// installedLister is an executor that can list the workloads this backend
// installed, so a restarted agent rebuilds its tracker instead of starting
// empty.
type installedLister interface {
	InstalledJobs(ctx context.Context, backendID string) ([]InstalledJob, error)
}

// endedLister is an executor that can list the jobs of this backend that ended
// and left something here, secrets included, until their Cancel takes it, so a
// detached machine, which no Cancel reaches, keeps none of it.
type endedLister interface {
	EndedJobs(ctx context.Context, backendID string) ([]InstalledJob, error)
}

// rebuild tracks every installed workload not already tracked. The status
// watcher then reports each one, and running_jobs in the first heartbeat counts
// them. One that ended while no agent watched is not installed work: the
// controller asks about it when the session registers (sessionRegistered).
func (t *jobTracker) rebuild(jobs []InstalledJob) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	added := 0
	for _, job := range jobs {
		if job.JobID == "" || job.Handle == "" {
			continue
		}
		if _, ok := t.jobs[job.Handle]; ok {
			continue
		}
		t.jobs[job.Handle] = activeJob{jobID: job.JobID, handleName: job.Handle, submittedAt: time.Now()}
		added++
	}
	return added
}
