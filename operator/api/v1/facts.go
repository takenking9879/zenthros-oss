// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Facts about a job that several components decide on. Each has one
// definition here, and every reader calls it: a fact rebuilt from raw fields
// in each package drifted (the Scheduler forgot fleetRef; three pools of a
// job; two attempt numbers). test/lint keeps raw reads of these fields
// inside their owners.

// Automatic reports a job the automatic Scheduler places: it names no
// backend, no pool and no fleet. The Scheduler decides for no other job.
func (j *ZenthrosJob) Automatic() bool {
	return SeatWhere(j.Spec) == SeatWhereScheduler
}

// DecisionInForce is the Scheduler decision a job lives by, nil when it has
// none: the one of its current generation or, once the job is ending or
// holds a seat, the last one it got. The Scheduler places neither (see
// Ending, Seated), and what ends a job moves its generation past that
// decision (spec.cancel; the apiserver bumps the generation when it stamps
// deletionTimestamp on an object with finalizers), as an edit of a running
// job's spec does: asking for the current generation there forgot the pool
// of the slot the job held, and its cancel left the slot allocated.
func (j *ZenthrosJob) DecisionInForce() *SchedulingDecision {
	if d := j.CurrentDecision(); d != nil {
		return d
	}
	if s := j.Status.Scheduling; s != nil && (j.Ending() || j.Seated()) {
		return &s.Decision
	}
	return nil
}

// Ending reports a job on its way out: ended, cancelled (spec.cancel) or
// being deleted. Nothing places it again.
func (j *ZenthrosJob) Ending() bool {
	return j.Status.Phase.Terminal() || j.Spec.Cancel || j.DeletionTimestamp != nil
}

// TakenIn reports a job the controller has written once: its ID, its place
// in the queue and its finalizer go in that one write. The Scheduler decides
// a job only after it, so the two never write a new job at once.
func (j *ZenthrosJob) TakenIn() bool {
	for _, f := range j.Finalizers {
		if f == FinalizerBackendCleanup {
			return true
		}
	}
	return false
}

// WaitsForUpstream reports a job that cannot run yet: its dependsOn have not
// all ended well. No place is decided for it and it stands in no queue until
// they have: a decision made when it was sent goes stale in the meantime (a
// Create held in a pool that filled with idle machines), and a place in a
// queue could hold back the job it waits for.
func (j *ZenthrosJob) WaitsForUpstream() bool {
	return len(j.Spec.DependsOn) > 0 && j.Status.UpstreamsEndedAt == nil
}

// Seated reports a job that holds a slot or runs on a backend. The
// Scheduler does not place it again: a new decision for it released the
// slot its workload ran on. A fenced lease (LeaseFenced) is no seat: the
// job lost its worker, and one given back waits on the Scheduler.
func (j *ZenthrosJob) Seated() bool {
	return j.Status.CapacityRequest != nil || j.Status.BackendJobName != "" || j.Status.Lease != nil && !j.LeaseFenced()
}

// CurrentDecision is the Scheduler decision for the job as it is now (its
// current generation), nil when the Scheduler has not decided on it yet. A
// job that has not ended lives by this one.
func (j *ZenthrosJob) CurrentDecision() *SchedulingDecision {
	if s := j.Status.Scheduling; s != nil && s.ObservedGeneration == j.Generation {
		return &s.Decision
	}
	return nil
}

// OfferInForce is the offer of the Pool decision the job lives by
// (DecisionInForce), nil when there is none: the one a slot must match to
// seat the job, and its replacement's. A seated job whose spec was edited
// lives by the decision it got: asked for its current generation's, its
// replacement had no offer, and the pool made the first type it allows.
func (j *ZenthrosJob) OfferInForce() *SchedulingOffer {
	if d := j.DecisionInForce(); d != nil && d.Mode == SchedulingModePool {
		return d.Offering
	}
	return nil
}

// DecidedOffer is the offer of the job's last Pool decision, of whatever
// generation, nil when that decision is not a pool's or names no offer: what
// the pool supplies for the job. A generation the Scheduler has not decided
// on yet keeps the offer in demand until it replaces it: dropping it there
// deleted a slot being made for the job, and the replan made it again.
func (j *ZenthrosJob) DecidedOffer() *SchedulingOffer {
	if s := j.Status.Scheduling; s != nil && s.Decision.Mode == SchedulingModePool {
		return s.Decision.Offering
	}
	return nil
}

// OfferOfMachine is the offer backend was made from, nil when that machine took
// the job from no pool: yours, or a cluster's.
func (j *ZenthrosJob) OfferOfMachine(backend string) *MachineOffer {
	for i := range j.Status.MachineOffers {
		if j.Status.MachineOffers[i].Backend == backend {
			return &j.Status.MachineOffers[i]
		}
	}
	return nil
}

// DecidedStep is the seat step of the job's last Pool decision, of whatever
// generation, "" when that decision is not a pool's or was given back:
// Reuse when a slot of the pool seats it (the pool supplies one), Create
// when its own machine does (the controller asks for it). As DecidedOffer,
// a generation not decided yet keeps the step in demand until the
// Scheduler replaces it; a decision given back holds no place (GivenBack),
// so the pool asks for nothing on it, the offer that just failed least.
func (j *ZenthrosJob) DecidedStep() SeatStep {
	if s := j.Status.Scheduling; s != nil && s.Decision.Mode == SchedulingModePool && !s.GivenBack() {
		return s.Decision.Step
	}
	return ""
}

// DecidedPool is the pool of the job's last Pool decision, of whatever
// generation, "" when that decision is not a pool's: the pool DecidedOffer
// is supplied from.
func (j *ZenthrosJob) DecidedPool() string {
	if s := j.Status.Scheduling; s != nil && s.Decision.Mode == SchedulingModePool {
		return s.Decision.Pool
	}
	return ""
}

// GivenBack reports a decision the controller gave back: nothing is
// seated on it again, and it holds no place on its backend or pool.
func (s *SchedulingStatus) GivenBack() bool {
	return s != nil && s.Abandon != nil && s.Abandon.Of(s.Decision)
}

// ReplacementInForce is the decision of a job that lost its worker (its
// lease fenced) made for its current lease generation, nil when the
// Scheduler has not decided that generation's replacement yet or the
// decision was given back: the one decision a fenced job is seated on.
func (j *ZenthrosJob) ReplacementInForce() *SchedulingDecision {
	d := j.CurrentDecision()
	if !j.LeaseFenced() || d == nil || d.LeaseGeneration != j.LeaseGeneration() || j.Status.Scheduling.GivenBack() {
		return nil
	}
	return d
}

// Replaces reports a give-back of a lost or draining worker: what the
// Scheduler decides for it is the job's replacement, not a replan.
func (a *SchedulingAbandon) Replaces() bool {
	return a != nil && replacingKind(a.Kind)
}

// Replaced reports an attempt left because its worker was lost or drained:
// its place did not fail the job, and it is neither a failed fleet member
// nor an offer to leave out.
func (a SchedulingAttempt) Replaced() bool {
	return a.AbandonedAt != nil && replacingKind(a.AbandonKind)
}

func replacingKind(kind string) bool {
	return kind == AbandonWorkerLost || kind == AbandonDraining
}

// Of reports whether the give-back is of decision: its backend, or its pool
// and, when the decision names one, its offer.
func (a *SchedulingAbandon) Of(decision SchedulingDecision) bool {
	switch decision.Mode {
	case SchedulingModeBackend:
		return a.Backend == decision.Backend
	case SchedulingModePool:
		return a.Pool == decision.Pool &&
			(decision.Offering == nil || (a.Offer != nil && a.Offer.ID == decision.Offering.ID && a.Offer.Zone == decision.Offering.Zone))
	}
	return false
}

// LastStarted is when the job's latest run started (its own, or the latest
// re-run's), nil when none did: a walk of places that found no capacity
// starts over after it, the machine it got having ended that walk.
func (j *ZenthrosJob) LastStarted() *metav1.Time {
	last := j.Status.StartTime
	for i := range j.Status.Attempts {
		if t := j.Status.Attempts[i].StartTime; t != nil && (last == nil || t.After(last.Time)) {
			last = t
		}
	}
	return last
}

// FailedMembers are the fleet members whose machine failed the job: the
// pools of the decisions it gave back (abandoned attempts, and the decision
// given back now). Each once, in order. A member whose worker was lost or
// drained (Replaced, Replaces) did not fail it.
func (j *ZenthrosJob) FailedMembers() []string {
	var failed []string
	add := func(pool string) {
		if pool != "" && !slices.Contains(failed, pool) {
			failed = append(failed, pool)
		}
	}
	s := j.Status.Scheduling
	if s == nil {
		return failed
	}
	for _, attempt := range s.Attempts {
		if attempt.AbandonedAt != nil && !attempt.Replaced() && attempt.Decision.Mode == SchedulingModePool {
			add(attempt.Decision.Pool)
		}
	}
	if s.GivenBack() && !s.Abandon.Replaces() {
		add(s.Abandon.Pool)
	}
	return failed
}

// NamesPool reports a spec that names its pool: a VMPool, or a VMFleet whose
// members are pools.
func (s ZenthrosJobSpec) NamesPool() bool {
	return s.PoolRef != "" || s.FleetRef != ""
}

// PoolOf is the VMPool a job's slot comes from: spec.poolRef; else, for a
// fleet job, the fleet member its seat is in; else the pool of the
// Scheduler decision in force (a fleet job's member until it is seated).
// "" when none.
func (j *ZenthrosJob) PoolOf() string {
	if j.Spec.PoolRef != "" {
		return j.Spec.PoolRef
	}
	if j.Spec.FleetRef != "" && j.Status.FleetPool != "" {
		return j.Status.FleetPool
	}
	if d := j.DecisionInForce(); d != nil && d.Mode == SchedulingModePool {
		return d.Pool
	}
	return ""
}

// LeaseGeneration is the generation of the job's lease: 1 before the first
// lease is granted, so the first run and its checkpoints are generation 1.
func (j *ZenthrosJob) LeaseGeneration() int64 {
	if l := j.Status.Lease; l != nil && l.Generation > 0 {
		return l.Generation
	}
	return 1
}

// Run is the one attempt number: every run counts, a re-run after the
// process failed and a run lost with its machine alike. The first run is 0.
// Status, reports, the executor's identity and ZENTHROS_ATTEMPT all use it.
// A backend's own in-place restarts (Restarts where the controller does not
// re-run) are inside one attempt.
func (s *ZenthrosJobStatus) Run() int32 {
	if s.RerunByController() {
		return s.Restarts + s.WorkerLostRetries
	}
	return s.WorkerLostRetries
}

// RerunByController reports a runtime whose failed process the controller
// re-runs itself, each run a new attempt. Elsewhere the backend restarts
// the workload in place, and Restarts mirrors its count.
func (s *ZenthrosJobStatus) RerunByController() bool {
	switch s.Runtime {
	case RuntimeSystemd, RuntimeContainerd, RuntimeWorkspaceContainers, RuntimeProcess:
		return true
	}
	return false
}

// User is the user a job runs for (the zenthros.ai/user label, else the
// annotation of that name), "" for none: a slot a user's job reserved is
// held for that user.
func (j *ZenthrosJob) User() string {
	if user := strings.TrimSpace(j.Labels[LabelJobUser]); user != "" {
		return user
	}
	return strings.TrimSpace(j.Annotations[LabelJobUser])
}

// Wall is the job's time limit: a Service's stopAfterSeconds (service true),
// else spec.activeDeadlineSeconds; 0 when it has none. The API refuses
// activeDeadlineSeconds on a Service, so they never meet.
func (j *ZenthrosJob) Wall() (seconds int64, service bool) {
	if s := j.Spec.Workload.Template.Service; s != nil {
		if s.StopAfterSeconds != nil && *s.StopAfterSeconds > 0 {
			return *s.StopAfterSeconds, true
		}
		return 0, true
	}
	if limit := j.Spec.ActiveDeadlineSeconds; limit != nil && *limit > 0 {
		return *limit, false
	}
	return 0, false
}

// WithoutBookkeeping is status without what moves while nothing a reader
// acts on changes: the lease's expiry, renewed about every 45 s while the job
// runs; each member's, which mirrors its task's lease; and the progress past
// its phase (each checkpoint's sequence, object and digest). zenthros-api's
// wait answers when anything else changes (pkg/api jobWait), and the
// controller's predicate is woken by nothing less (pkg/controller
// onlyBookkeeping). The lease, members and progress are copies: status's own
// stay as they are.
func WithoutBookkeeping(status ZenthrosJobStatus) ZenthrosJobStatus {
	if status.Lease != nil {
		lease := *status.Lease
		lease.ExpiresAt = nil
		status.Lease = &lease
	}
	if len(status.Members) > 0 {
		members := slices.Clone(status.Members)
		for i := range members {
			members[i].ExpiresAt = nil
		}
		status.Members = members
	}
	if status.Progress != nil {
		status.Progress = &JobProgress{Phase: status.Progress.Phase}
	}
	return status
}

// SizesOf is how big a job asked to be, as it wrote it: compute over
// workers, field by field. Empty and zero ask for nothing. sizing.HardwareOf
// reads the place a job needs from it, and a job's pod or container is built
// from it, so the machine chosen and what runs on it cannot disagree.
func SizesOf(workers WorkerGroup, compute *ComputeConstraints) (cpu, memory string, gpus int32) {
	cpu, memory, gpus = workers.CPU, workers.Memory, workers.GPUsPerNode
	if compute == nil {
		return cpu, memory, gpus
	}
	if compute.CPU != "" {
		cpu = compute.CPU
	}
	if compute.Memory != "" {
		memory = compute.Memory
	}
	if compute.GPUCount > 0 {
		gpus = compute.GPUCount
	}
	return cpu, memory, gpus
}
