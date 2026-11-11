// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"reflect"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAutomaticIsSeatWhereScheduler(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec ZenthrosJobSpec
		want bool
	}{
		{"no target", ZenthrosJobSpec{}, true},
		{"backends", ZenthrosJobSpec{Target: Target{Backends: []string{"a"}}}, false},
		{"pool", ZenthrosJobSpec{PoolRef: "p"}, false},
		// The Scheduler's own gate forgot fleetRef and decided for fleet jobs.
		{"fleet", ZenthrosJobSpec{FleetRef: "f"}, false},
	} {
		if got := (&ZenthrosJob{Spec: tc.spec}).Automatic(); got != tc.want {
			t.Errorf("%s: Automatic = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestPoolOf(t *testing.T) {
	decided := func(gen, observed int64, phase Phase) *ZenthrosJob {
		j := &ZenthrosJob{}
		j.Generation = gen
		j.Status.Phase = phase
		j.Status.Scheduling = &SchedulingStatus{ObservedGeneration: observed, Decision: SchedulingDecision{Mode: SchedulingModePool, Pool: "auto"}}
		return j
	}
	if got := decided(2, 2, PhaseRunning).PoolOf(); got != "auto" {
		t.Errorf("current decision: %q", got)
	}
	// A fleet job's member is its decision until it is seated, then the
	// member its seat is in: the FIFO queued it nowhere before (B4).
	member := decided(2, 2, PhasePending)
	member.Spec.FleetRef = "mix"
	if got := member.PoolOf(); got != "auto" {
		t.Errorf("an unseated fleet job: %q, want its decided member", got)
	}
	member.Status.FleetPool = "seated-member"
	if got := member.PoolOf(); got != "seated-member" {
		t.Errorf("a seated fleet job: %q, want the member its seat is in", got)
	}
	if got := decided(3, 2, PhaseRunning).PoolOf(); got != "" {
		t.Errorf("a decision for an older spec is not in force: %q", got)
	}
	if got := decided(3, 2, PhaseCancelled).PoolOf(); got != "auto" {
		t.Errorf("an ended job keeps its last decision: %q", got)
	}
	// spec.cancel moves the generation before the job is Cancelled, and an
	// edit of a seated job's spec moves it too: both keep the pool of the
	// slot they hold, or the cancel left the slot allocated (R1 ronda 3, G3).
	cancelling := decided(3, 2, PhaseRunning)
	cancelling.Spec.Cancel = true
	if got := cancelling.PoolOf(); got != "auto" {
		t.Errorf("a cancelled job keeps its last decision: %q", got)
	}
	edited := decided(3, 2, PhaseRunning)
	edited.Status.CapacityRequest = &CapacityRequestReference{Name: "slot"}
	if got := edited.PoolOf(); got != "auto" {
		t.Errorf("a seated job keeps the decision it is seated by: %q", got)
	}
	// A fenced lease is no seat: a job that lost its worker and was edited
	// waits on the Scheduler's next decision (R1 ronda 4, G1).
	fenced := decided(3, 2, PhasePending)
	fenced.Status.Lease = &JobLease{Generation: 2}
	if fenced.Seated() || fenced.PoolOf() != "" {
		t.Errorf("a fenced job is seated (%v) or keeps an old decision: %q", fenced.Seated(), fenced.PoolOf())
	}
	fenced.Status.Lease.BackendID = "b1"
	if !fenced.Seated() {
		t.Error("a held lease is a seat")
	}
	// The pool a decision of an older spec was made for stays the pool of
	// its offer: a slot of it that found no capacity calls for a replan.
	if got := decided(3, 2, PhasePending).DecidedPool(); got != "auto" {
		t.Errorf("the decided pool is of the last decision: %q", got)
	}
	backend := decided(1, 1, PhasePending)
	backend.Status.Scheduling.Decision.Mode = SchedulingModeBackend
	if got := backend.DecidedPool(); got != "" {
		t.Errorf("a Backend decision decides no pool: %q", got)
	}
	fleet := decided(1, 1, PhaseRunning)
	fleet.Spec.FleetRef, fleet.Status.FleetPool = "f", "member"
	if got := fleet.PoolOf(); got != "member" {
		t.Errorf("fleet: %q", got)
	}
	fleet.Spec.PoolRef = "named"
	if got := fleet.PoolOf(); got != "named" {
		t.Errorf("poolRef first: %q", got)
	}
}

func TestLeaseGenerationAndRun(t *testing.T) {
	j := &ZenthrosJob{}
	if j.LeaseGeneration() != 1 {
		t.Fatal("no lease yet is generation 1")
	}
	j.Status.Lease = &JobLease{Generation: 4}
	if j.LeaseGeneration() != 4 {
		t.Fatal("the lease's generation")
	}
	j.Status.Restarts, j.Status.WorkerLostRetries = 2, 1
	j.Status.Runtime = RuntimeSystemd
	if j.Status.Run() != 3 {
		t.Fatal("every run counts, lost ones too")
	}
	j.Status.Runtime = "" // a Kubernetes backend: it restarts in place
	if j.Status.Run() != 1 {
		t.Fatal("a backend's own restarts are inside one attempt")
	}
}

// A fleet member whose worker was lost or drained did not fail the job: its
// machine went, not the member. Counted, a single-member fleet's job ended
// fleetExhausted at its first lost worker instead of being replaced.
func TestALostWorkersMemberIsNoFailedMember(t *testing.T) {
	at := metav1.Now()
	pool := func(name string) SchedulingDecision { return SchedulingDecision{Mode: SchedulingModePool, Pool: name} }
	job := &ZenthrosJob{}
	job.Status.Scheduling = &SchedulingStatus{
		Decision: pool("c"),
		Attempts: []SchedulingAttempt{
			{Index: 1, Decision: pool("a"), AbandonedAt: &at, AbandonKind: AbandonWorkerLost},
			{Index: 2, Decision: pool("b"), AbandonedAt: &at, AbandonKind: AbandonRefused},
			{Index: 3, Decision: pool("d"), AbandonedAt: &at, AbandonKind: AbandonDraining},
			{Index: 4, Decision: pool("c")},
		},
		Abandon: &SchedulingAbandon{Kind: AbandonWorkerLost, Pool: "c"},
	}
	if got := job.FailedMembers(); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("failed members %v, want only b, whose machine failed the job", got)
	}
	job.Status.Scheduling.Abandon.Kind = AbandonRefused
	if got := job.FailedMembers(); !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("failed members %v, want b and c, refused now", got)
	}
}

// A job's next seat replaces a worker only when it lost one: a first seat
// fenced by its machine finding no capacity is still its first, on its
// acquire steps.
func TestAFencedFirstSeatStillAcquires(t *testing.T) {
	job := &ZenthrosJob{Spec: ZenthrosJobSpec{PoolRef: "gpu",
		Capacity:   &CapacitySpec{Acquire: []SeatStep{SeatStepCreate}},
		Resilience: &ResilienceSpec{OnWorkerLost: OnWorkerLostReschedule, Replace: []SeatStep{SeatStepReuse}}}}
	job.Status.Lease = &JobLease{Generation: 2}
	steps := func() []SeatStep { return WhereOf(job.Spec).Steps(job.ReplacingWorker()) }
	if job.ReplacingWorker() || !slices.Equal(steps(), []SeatStep{SeatStepCreate}) {
		t.Fatalf("fenced first seat: replacing %v steps %v, want its acquire steps", job.ReplacingWorker(), steps())
	}
	job.Status.WorkerLostRetries = 1
	if !job.ReplacingWorker() || !slices.Equal(steps(), []SeatStep{SeatStepReuse}) {
		t.Fatalf("after a lost worker: replacing %v steps %v, want its replace steps", job.ReplacingWorker(), steps())
	}
}

// Bookkeeping is the lease's expiry, each member's, and the progress past its
// phase; the rest of the status is what a reader acts on, and the status
// given is not touched.
func TestWithoutBookkeepingDropsOnlyWhatMovesWithNothingChanged(t *testing.T) {
	at := metav1.Now()
	status := ZenthrosJobStatus{
		Phase:    PhaseRunning,
		Lease:    &JobLease{BackendID: "vm-a", Generation: 2, ExpiresAt: &at, PreemptedAt: &at},
		Members:  []MemberStatus{{Rank: 0, Job: "j-0", Phase: PhaseRunning, ExpiresAt: &at, PreemptedAt: &at}},
		Progress: &JobProgress{URI: "s3://b/c/3", Phase: "train", Seq: 3, SHA256: "ab"},
	}
	got := WithoutBookkeeping(status)
	if got.Lease.ExpiresAt != nil || got.Members[0].ExpiresAt != nil {
		t.Errorf("an expiry is left: lease %v, member %v", got.Lease.ExpiresAt, got.Members[0].ExpiresAt)
	}
	if got.Lease.BackendID != "vm-a" || got.Lease.Generation != 2 || got.Lease.PreemptedAt == nil ||
		got.Members[0].Phase != PhaseRunning || got.Members[0].PreemptedAt == nil || got.Phase != PhaseRunning {
		t.Errorf("more than bookkeeping went: %+v", got)
	}
	if !reflect.DeepEqual(*got.Progress, JobProgress{Phase: "train"}) {
		t.Errorf("progress %+v, want its phase alone", *got.Progress)
	}
	if status.Lease.ExpiresAt == nil || status.Members[0].ExpiresAt == nil || status.Progress.Seq != 3 {
		t.Errorf("the status given changed: %+v", status)
	}
}
