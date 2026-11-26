// SPDX-License-Identifier: Apache-2.0

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// CleanupPolicy values for GCPolicy.
const (
	// CleanupPolicyDelete garbage-collects a finished job's resources once the
	// retention windows elapse (default).
	CleanupPolicyDelete = "Delete"
	// CleanupPolicyRetain keeps the CRD and its backend workload indefinitely,
	// for debugging. GC never touches it.
	CleanupPolicyRetain = "Retain"
)

// LabelJobUser is the session owner for CapacityRequest.HeldByUser.
const LabelJobUser = "zenthros.ai/user"

// OnWorkerLost values for ResilienceSpec.
const (
	// OnWorkerLostFail fails the job when the worker-lost lease expires.
	OnWorkerLostFail = "Fail"
	// OnWorkerLostReschedule substitutes a replacement worker (default).
	OnWorkerLostReschedule = "Reschedule"
	// DefaultWorkerLostRetryLimit is how many replacements a Reschedule job
	// may consume before Fail. 0 means the first death is terminal.
	DefaultWorkerLostRetryLimit int32 = 3
)

// GCPolicy controls garbage collection of a finished job's resources (Phase L).
// Unset fields fall back to the controller's configured defaults.
type GCPolicy struct {
	// CleanupPolicy selects whether finished resources are deleted or retained.
	// +kubebuilder:validation:Enum=Delete;Retain
	CleanupPolicy string `json:"cleanupPolicy,omitempty"`

	// TTLSecondsAfterFinished is how long the CRD is kept after the job reaches a
	// terminal phase before GC deletes it. Deletion cascades workload cleanup via
	// the finalizer, so it also bounds the workload's lifetime. 0 deletes as soon
	// as the job finishes.
	// +kubebuilder:validation:Minimum=0
	TTLSecondsAfterFinished *int64 `json:"ttlSecondsAfterFinished,omitempty"`

	// RetainLogsForSeconds is how long the backend workload (and therefore its
	// logs) is kept after completion before GC cancels it, independent of the CRD
	// TTL. On k8s pod logs die with the workload; on a VM journald retention
	// bounds how long journal entries survive unit removal.
	// +kubebuilder:validation:Minimum=0
	RetainLogsForSeconds *int64 `json:"retainLogsForSeconds,omitempty"`

	// RetainWorkdir keeps the VM work directory (/var/lib/zenthros/jobs/<id>)
	// when the job is cleaned up. Ignored by the k8s backend.
	RetainWorkdir bool `json:"retainWorkdir,omitempty"`
}

// BackendFallbackAny is the optional last backends token: Scheduler among
// enrolled backends only. It never provisions a VM.
const BackendFallbackAny = "any"

// SeatStep is one walk step for first seat (acquire) or worker-lost (replace).
// +kubebuilder:validation:Enum=Backends;Reuse;Create
type SeatStep string

const (
	SeatStepBackends SeatStep = "Backends"
	SeatStepReuse    SeatStep = "Reuse"
	SeatStepCreate   SeatStep = "Create"
)

// Target selects enrolled backends. Changing Backends must not require changing
// the Workload — that is what makes backend switching cheap. poolRef and
// backends combine; when both are absent the Scheduler places the job.
//
// +kubebuilder:validation:XValidation:rule="!has(self.backends) || self.backends.all(x, self.backends.exists_one(y, x == y))",message="spec.target.backends must be unique"
// +kubebuilder:validation:XValidation:rule="!has(self.backends) || size(self.backends.filter(x, x == 'any')) == 0 || (size(self.backends.filter(x, x == 'any')) == 1 && self.backends[self.backends.size()-1] == 'any')",message="any may appear only last in spec.target.backends"
type Target struct {
	// Backends is the ordered enrolled backend id list. Token any is last-only
	// and means Scheduler among enrolled backends outside the named prefix.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=253
	Backends []string `json:"backends,omitempty"`

	// RayCluster runs this RayJob on an existing RayCluster instead of a
	// dedicated one. The cluster must be in the job's namespace on the one
	// backend named in Backends. Zenthros owns only the per-job RayJob and
	// never changes the cluster.
	// +optional
	RayCluster *RayClusterReference `json:"rayCluster,omitempty"`
}

// HasBackends reports whether an ordered backends list is set.
func (t Target) HasBackends() bool {
	return len(t.Backends) > 0
}

// NamedBackends is Backends without empty names and without the any token.
func (t Target) NamedBackends() []string {
	list := make([]string, 0, len(t.Backends))
	for _, name := range t.Backends {
		if name != "" && name != BackendFallbackAny {
			list = append(list, name)
		}
	}
	return list
}

// ContainsBackend reports whether id is a named (non-any) backends entry.
func (t Target) ContainsBackend(id string) bool {
	if id == "" || id == BackendFallbackAny {
		return false
	}
	for _, name := range t.Backends {
		if name == id {
			return true
		}
	}
	return false
}

// FirstBackend is the first named backends entry, or empty.
func (t Target) FirstBackend() string {
	for _, name := range t.NamedBackends() {
		return name
	}
	return ""
}

// CapacitySpec is the first-seat walk. Omitted acquire uses the mode default.
type CapacitySpec struct {
	// Acquire is the first-seat walk. Empty/omitted uses the mode default;
	// do not send [].
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	// +kubebuilder:validation:XValidation:rule="self.all(x, self.exists_one(y, x == y))",message="spec.capacity.acquire must not contain duplicates"
	Acquire []SeatStep `json:"acquire,omitempty"`
}

// ZenthrosJobSpec describes intent, not infrastructure.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.cancel) || !oldSelf.cancel || (has(self.cancel) && self.cancel)",message="spec.cancel cannot be unset once true"
// +kubebuilder:validation:XValidation:rule="!(has(self.target) && has(self.target.rayCluster)) || self.workload.kind == 'RayJob'",message="spec.target.rayCluster is valid only for RayJob workloads"
// +kubebuilder:validation:XValidation:rule="!(has(self.target) && has(self.target.rayCluster)) || (has(self.target.backends) && size(self.target.backends) == 1 && self.target.backends[0] != 'any')",message="spec.target.rayCluster requires exactly one named backend in spec.target.backends"
// +kubebuilder:validation:XValidation:rule="!(has(self.target) && has(self.target.rayCluster)) || !has(self.poolRef) || size(self.poolRef) == 0",message="spec.target.rayCluster cannot be combined with spec.poolRef"
// +kubebuilder:validation:XValidation:rule="!(has(self.capacity) && has(self.capacity.acquire) && ('Backends' in self.capacity.acquire) && !(has(self.target) && has(self.target.backends) && size(self.target.backends) > 0))",message="spec.capacity.acquire Backends requires spec.target.backends"
// +kubebuilder:validation:XValidation:rule="!(has(self.capacity) && has(self.capacity.acquire) && (('Reuse' in self.capacity.acquire) || ('Create' in self.capacity.acquire)) && !(has(self.poolRef) && size(self.poolRef) > 0) && !has(self.fleetRef) && has(self.target) && has(self.target.backends) && size(self.target.backends) > 0)",message="spec.capacity.acquire Reuse/Create require spec.poolRef, spec.fleetRef or scheduler mode"
// +kubebuilder:validation:XValidation:rule="!(has(self.resilience) && has(self.resilience.replace) && ('Backends' in self.resilience.replace) && !(has(self.target) && has(self.target.backends) && size(self.target.backends) > 0))",message="spec.resilience.replace Backends requires spec.target.backends"
// +kubebuilder:validation:XValidation:rule="!(has(self.resilience) && has(self.resilience.replace) && (('Reuse' in self.resilience.replace) || ('Create' in self.resilience.replace)) && !(has(self.poolRef) && size(self.poolRef) > 0) && !has(self.fleetRef) && has(self.target) && has(self.target.backends) && size(self.target.backends) > 0)",message="spec.resilience.replace Reuse/Create require spec.poolRef, spec.fleetRef or scheduler mode"
// +kubebuilder:validation:XValidation:rule="!has(self.fleetRef) || !has(self.poolRef) || size(self.poolRef) == 0",message="spec.fleetRef and spec.poolRef are exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.workload.template.distribution) || self.workload.template.distribution.count <= 1 || (has(self.poolRef) && size(self.poolRef) > 0) || has(self.fleetRef) || (has(self.target) && has(self.target.backends) && size(self.target.backends) > 0)",message="a distributed job needs spec.target.backends, spec.poolRef or spec.fleetRef: the Scheduler keeps its members apart among the places it names"
// +kubebuilder:validation:XValidation:rule="!has(self.fleetRef) || !(has(self.target) && has(self.target.rayCluster))",message="spec.target.rayCluster cannot be combined with spec.fleetRef"
// +kubebuilder:validation:XValidation:rule="!has(self.placement) || !(has(self.target) && has(self.target.rayCluster))",message="spec.placement does not apply to a RayJob on spec.target.rayCluster: its pods are the standing cluster's"
// +kubebuilder:validation:XValidation:rule="!has(self.placement) || self.workload.kind != 'Manifest'",message="spec.placement does not apply to a Manifest: its object says where its pods run"
// +kubebuilder:validation:XValidation:rule="!has(self.workload.template.service) || !has(self.activeDeadlineSeconds)",message="a Service runs until it is cancelled; spec.activeDeadlineSeconds does not apply"
// +kubebuilder:validation:XValidation:rule="!has(self.workload.template.service) || (has(self.poolRef) && size(self.poolRef) > 0) || has(self.fleetRef) || (has(self.target) && has(self.target.backends) && size(self.target.backends) > 0)",message="a Service needs spec.poolRef, spec.fleetRef or spec.target.backends: its ports are published by the pool or machine it runs on"
type ZenthrosJobSpec struct {
	// ResumeFrom starts this job from the last acked checkpoint of an ended job
	// of its namespace. It names the job, never a place: the controller reads
	// the cursor, and the runner checks each blob against its digest.
	// +optional
	ResumeFrom *ResumeFrom `json:"resumeFrom,omitempty"`

	// Workspace is an optional assertion checked against the identity derived
	// from metadata.namespace. It never overrides namespace authorization.
	// +optional
	Workspace string `json:"workspace,omitempty"`

	// Target selects enrolled backends. Combines with PoolRef.
	// +optional
	Target Target `json:"target,omitempty"`

	// PoolRef names the VMPool that must provide managed capacity for this
	// job. Combines with Target.Backends.
	// +optional
	// +kubebuilder:validation:MinLength=1
	PoolRef string `json:"poolRef,omitempty"`

	// FleetRef names the VMFleet that provides managed capacity: the seat
	// walk tries its member pools, heaviest weight first, within the
	// fleet's constraints. Excludes PoolRef.
	// +optional
	// +kubebuilder:validation:MinLength=1
	FleetRef string `json:"fleetRef,omitempty"`

	// Priority ranks waiters of the same exclusive unit. Higher admits first.
	// Equal priority stays FIFO (creationTimestamp, then name). Occupancy 1/1
	// for exclusive VM/raycluster is unchanged. It ranks one Workspace's own
	// jobs only: while jobs of several Workspaces wait for a unit, the queue
	// is FIFO.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Priority int32 `json:"priority,omitempty"`

	// Compute constrains placement. instanceType and purchaseOption apply to
	// Create; CPU/RAM/GPU apply to Reuse/Backends fit. backendKind/region/zone
	// remain scheduler-only when poolRef is set.
	// +optional
	Compute *ComputeConstraints `json:"compute,omitempty"`

	// Capacity is the first-seat walk. Omitted acquire uses the mode default.
	// +optional
	Capacity *CapacitySpec `json:"capacity,omitempty"`

	// Workload is the logical work to execute.
	Workload Workload `json:"workload"`

	// Cancel stops the job and keeps it: the workload is cancelled, any
	// capacity it held is released, and the ZenthrosJob stays as Cancelled
	// with its history until its TTL. Deleting the ZenthrosJob also cancels,
	// but loses the record. Once true it cannot be set back to false.
	// +optional
	Cancel bool `json:"cancel,omitempty"`

	// DependsOn chains ZenthrosJobs by name for multi-stage pipelines.
	DependsOn []string `json:"dependsOn,omitempty"`

	// GCPolicy controls garbage collection of the job's resources after it
	// finishes (Phase L). Nil applies the controller's configured defaults.
	// +optional
	GCPolicy *GCPolicy `json:"gcPolicy,omitempty"`

	// Resilience controls worker-lost recovery. It is not RetryPolicy (submit
	// backoff) and not gcPolicy (post-success cleanup).
	// +optional
	Resilience *ResilienceSpec `json:"resilience,omitempty"`

	// ActiveDeadlineSeconds is the wall from status.startTime, or from
	// status.submissionTime when start is unset. Empty or zero means no wall.
	// Heartbeats renew the worker-lost lease; they do not move this deadline.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// Placement constrains where the job's seats go: with or apart from
	// other jobs, spread over zones or regions, only in some regions.
	// +optional
	Placement *PlacementPolicy `json:"placement,omitempty"`
}

// ResilienceSpec is the job knob for a worker that went silent after Submit.
type ResilienceSpec struct {
	// OnWorkerLost is Fail or Reschedule. Empty defaults to Reschedule.
	// Fail ignores Replace.
	// +kubebuilder:default=Reschedule
	// +kubebuilder:validation:Enum=Fail;Reschedule
	OnWorkerLost string `json:"onWorkerLost,omitempty"`

	// Replace is the worker-lost walk. Empty/omitted uses the mode default;
	// do not send []. Ignored when OnWorkerLost is Fail.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	// +kubebuilder:validation:XValidation:rule="self.all(x, self.exists_one(y, x == y))",message="spec.resilience.replace must not contain duplicates"
	Replace []SeatStep `json:"replace,omitempty"`

	// WorkerLostRetryLimit bounds Reschedule replacements. Empty defaults to 3.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	WorkerLostRetryLimit *int32 `json:"workerLostRetryLimit,omitempty"`

	// JobGrace is the Allocated/running silence window before WORKER_LOST.
	// Empty uses the cluster default (45s). Must be at least twice the
	// heartbeat interval; shorter values are raised to that floor.
	// +optional
	JobGrace *metav1.Duration `json:"jobGrace,omitempty"`

	// PreemptNotice says how the process learns its machine is being taken
	// back. The agent always writes the notice to $ZENTHROS_PREEMPT_FILE; a
	// signal is sent only when asked for here.
	// +optional
	PreemptNotice *PreemptNotice `json:"preemptNotice,omitempty"`
}

// PreemptNotice is how a job hears of its machine's interruption notice.
type PreemptNotice struct {
	// Signal is sent to the process after the notice file is written.
	// Empty sends none: the process polls the file (the SDK does).
	// +optional
	// +kubebuilder:validation:Enum=SIGTERM;SIGINT;SIGUSR1;SIGUSR2
	Signal string `json:"signal,omitempty"`
}

// ResolvedOnWorkerLost is Fail or Reschedule, defaulting empty to Reschedule.
func (s ZenthrosJobSpec) ResolvedOnWorkerLost() string {
	if s.Resilience == nil || s.Resilience.OnWorkerLost == "" {
		return OnWorkerLostReschedule
	}
	return s.Resilience.OnWorkerLost
}

// ResolvedWorkerLostRetryLimit is the replacement bound, defaulting nil to 3.
func (s ZenthrosJobSpec) ResolvedWorkerLostRetryLimit() int32 {
	if s.Resilience == nil || s.Resilience.WorkerLostRetryLimit == nil {
		return DefaultWorkerLostRetryLimit
	}
	return *s.Resilience.WorkerLostRetryLimit
}

// CapacityRequestReference identifies the managed capacity attempt serving a
// poolRef job. It is a read-only correlation pointer: CapacityRequest.status
// remains the source of truth for every infrastructure timestamp.
type CapacityRequestReference struct {
	// Namespace is the pool capacity management namespace.
	Namespace string `json:"namespace"`

	// Name of the pool-owned CapacityRequest.
	Name string `json:"name"`

	// UID is the CapacityRequest's: slot names are reused, and the name
	// alone named the next incarnation too.
	// +optional
	UID types.UID `json:"uid,omitempty"`

	// RequestID is the request's stable provider idempotency key.
	RequestID string `json:"requestID"`

	// ReservationID is the stable compare-and-swap token for this job lease.
	ReservationID string `json:"reservationID"`
}

// Reason is a structured explanation attached to a phase.
type Reason struct {
	// Code is a machine-readable reason, e.g. INSUFFICIENT_CPU.
	Code string `json:"code"`

	// Detail is a human-readable message with context.
	Detail string `json:"detail,omitempty"`
}

// AbandonedWorkload is one attempt of a job left on a backend it was lost
// from.
type AbandonedWorkload struct {
	// BackendID is the backend the attempt ran on.
	BackendID string `json:"backendID"`
	// Handle is the backend job name of the attempt. Empty for an attempt
	// whose Submit reply was lost: it is cancelled by the job's ID.
	Handle string `json:"handle"`
	// Generation is the lease generation the attempt was left under. A job
	// whose lease is back on BackendID under a later generation, with the
	// same handle, came back to the attempt: it is the job's own again.
	// +optional
	Generation int64 `json:"generation,omitempty"`
	// Submit numbers the Submits in doubt (no handle) on BackendID under
	// Generation: one the GC cancelled is never the job's again, so the next
	// Submit there in doubt is the next number, its own attempt.
	// +optional
	Submit int32 `json:"submit,omitempty"`
}

// Attempt is one run of the job's process that failed and was re-run.
type Attempt struct {
	// Number is 0 for the first run, n for the n-th re-run.
	Number int32 `json:"number"`
	// Handle is the backend's name for this attempt's workload.
	Handle string `json:"handle,omitempty"`
	// Backend is the backend it ran on.
	Backend string `json:"backend,omitempty"`
	// StartTime and EndTime bound the attempt.
	StartTime *metav1.Time `json:"startTime,omitempty"`
	EndTime   *metav1.Time `json:"endTime,omitempty"`
	// Message is how the attempt ended, as its backend said it.
	Message string `json:"message,omitempty"`
	// Cleaned is set once the attempt's workload was cancelled; the next
	// attempt is submitted only after that.
	Cleaned bool `json:"cleaned,omitempty"`
}

// MachineOffer is the offer a machine that took the job was made from.
type MachineOffer struct {
	// Backend is the machine.
	Backend string `json:"backend"`
	// ID is the catalog's identifier of the offer.
	ID string `json:"id,omitempty"`
	// InstanceType is the provider's name of the machine's type.
	InstanceType string `json:"instanceType,omitempty"`
	// Region and Zone are where it ran.
	Region string `json:"region,omitempty"`
	Zone   string `json:"zone,omitempty"`
	// PurchaseOption is OnDemand or Spot.
	PurchaseOption string `json:"purchaseOption,omitempty"`
}

// ZenthrosJobStatus reflects the observed state, derived from the backend resource.
type ZenthrosJobStatus struct {
	// Phase is the current lifecycle phase.
	Phase Phase `json:"phase,omitempty"`

	// Message is a one-line human-readable summary of the current state.
	Message string `json:"message,omitempty"`

	// Reasons explain the current phase.
	Reasons []Reason `json:"reasons,omitempty"`

	// ObservedGeneration is the spec generation last reconciled.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// BackendJobName is the name of the native resource created in the backend.
	BackendJobName string `json:"backendJobName,omitempty"`

	// SubmissionTime when the workload was accepted by the backend. Paired with
	// StartTime/CompletionTime it bounds the workload execution window; the
	// capacity wait is derived by joining CapacityRequest.status timestamps.
	SubmissionTime *metav1.Time `json:"submissionTime,omitempty"`

	// StartTime when the backend resource started running.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime when the job reached a terminal phase.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// ExitCode is what the workload's process returned, once it ended and
	// returned one: a test that failed reads 1 here, a machine that was lost
	// reads none. It says how the process ended, not what happens next: the
	// phase and the reasons do.
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`

	// StateObservedAt is when the backend state this status shows (phase,
	// message, times, restarts) last changed by an observation: the
	// controller's poll or the agent's push. Of two observations of one
	// attempt, the later one is kept.
	// +optional
	StateObservedAt *metav1.Time `json:"stateObservedAt,omitempty"`

	// DrainingLease is the interrupted worker's lease after its notice. Its
	// lease ended at once so a replacement starts now, but until Until (the
	// provider's warning, capped) or progress.frozenAt the worker may still
	// commit checkpoints, so the replacement resumes from the latest one.
	// +optional
	DrainingLease *DrainingLease `json:"drainingLease,omitempty"`

	// Standby is the replacement prepared while the interrupted worker
	// drains: a slot reserved for this job, asked for so it is Ready when
	// the window ends, and used then. Its pool's interruption.standby says
	// where it comes from.
	// +optional
	Standby *StandbySlot `json:"standby,omitempty"`

	// PendingTerminal holds a Succeeded the backend reported while a
	// checkpointed phase has no acked COMPLETE. A COMPLETE that arrives within
	// two heartbeats lets it through; otherwise the job fails
	// CHECKPOINT_INCOMPLETE instead of staying Running forever.
	// +optional
	PendingTerminal *PendingTerminal `json:"pendingTerminal,omitempty"`

	// FirstStartTime is when the job's first attempt started running. It never
	// moves on a re-run, so spec.activeDeadlineSeconds spans every attempt.
	FirstStartTime *metav1.Time `json:"firstStartTime,omitempty"`

	// SubmitRetries counts consecutive submissions the backend refused
	// transiently (busy, unavailable); it drives the controller's backoff and
	// resets when a submission is accepted.
	SubmitRetries int32 `json:"submitRetries,omitempty"`

	// Restarts counts re-runs after the workload's process failed, within
	// workload.template.restartPolicy.backoffLimit: the backend's own
	// (Kubernetes Job pods, KubeRay, Spark) or, on a machine, the
	// controller's. A lost worker is not a restart.
	Restarts int32 `json:"restarts,omitempty"`

	// Attempts records the attempts the controller re-ran on a machine,
	// oldest first; at most the last 10 are kept.
	// +optional
	// +kubebuilder:validation:MaxItems=10
	Attempts []Attempt `json:"attempts,omitempty"`

	// MachineOffers are the offers of the machines that took the job, sealed
	// when each does: its capacity request goes with the machine, minutes after
	// the job ends, and what the job cost does not change with that.
	// +optional
	// +kubebuilder:validation:MaxItems=10
	MachineOffers []MachineOffer `json:"machineOffers,omitempty"`

	// CapacityRequest references the managed capacity attempt created for a
	// poolRef job. Absent for explicit-target jobs.
	CapacityRequest *CapacityRequestReference `json:"capacityRequest,omitempty"`

	// FleetPool is the member of spec.fleetRef the job's slot comes from. It
	// is written in the same status update as capacityRequest.
	// +optional
	FleetPool string `json:"fleetPool,omitempty"`

	// Scheduling is the Scheduler-owned durable placement decision, for
	// every job: the controller seats a job only where it says.
	Scheduling *SchedulingStatus `json:"scheduling,omitempty"`

	// Results are the SHA-256s the job's runners reported for the results
	// they wrote (the zenthros.result report), each accepted from a live
	// generation; a distributed job's tasks report into it too. A reader
	// unpickles a result only when its bytes match one: the store is not the
	// job's to vouch for. Only the control plane writes status, and it
	// outlives the report store.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=1024
	Results []ResultDigest `json:"results,omitempty"`

	// UpstreamResults pins, for each spec.dependsOn job that reported
	// results, the digest of its results (resultdigest.Pin), taken when the
	// dependencies were met. The machine reads it as
	// ZENTHROS_UPSTREAM_RESULTS and unpickles an upstream's result only when
	// what it read hashes to it.
	// +optional
	UpstreamResults map[string]string `json:"upstreamResults,omitempty"`

	// UpstreamsEndedAt is when the controller saw every spec.dependsOn job
	// end well. The Scheduler places a job with dependsOn only after it.
	// +optional
	UpstreamsEndedAt *metav1.Time `json:"upstreamsEndedAt,omitempty"`

	// Lease is the submitted-job liveness token. Grant on successful Submit;
	// renew on heartbeat from that backend. Expiry is WORKER_LOST.
	// +optional
	Lease *JobLease `json:"lease,omitempty"`

	// WorkerLostRetries counts worker-lost replacements already consumed.
	WorkerLostRetries int32 `json:"workerLostRetries,omitempty"`

	// AbandonedWorkloads are workloads left on backends the job was lost
	// from. A lost machine can come back with the work still running; the
	// garbage collector cancels each one once its backend is connected
	// again, so the job neither keeps a dead attempt alive nor runs twice.
	// +optional
	// +listType=atomic
	AbandonedWorkloads []AbandonedWorkload `json:"abandonedWorkloads,omitempty"`

	// Progress is the controller-acked checkpoint cursor. Replacement loads
	// this URI; object-storage listings are not the source of truth. Frozen
	// on lease expiry.
	// +optional
	Progress *JobProgress `json:"progress,omitempty"`

	// Runtime is the runtime the job was submitted under: the one it asked
	// for in spec.compute.runtime, or its backend's declared default.
	// +optional
	Runtime string `json:"runtime,omitempty"`

	// ResolvedImage is the digest the job's image resolved to on the first
	// run that pulled it. Later attempts (a replacement backend, a Spot
	// standby, a process retry) run that digest, so a tag that moves cannot
	// change the code in the middle of a job or under a resumed checkpoint.
	// +optional
	ResolvedImage *ResolvedImage `json:"resolvedImage,omitempty"`

	// Lifecycle is how the job ends: RunToCompletion ends when the work
	// does; Service (a RayCluster, a Job with workload.template.service, a
	// Manifest that says so) runs until it is cancelled, deleted, or fails,
	// and never Succeeds.
	// +optional
	Lifecycle string `json:"lifecycle,omitempty"`

	// Endpoints are where a Service answers, as the run that serves it
	// published them: one per port of workload.template.service (once the
	// port accepts connections) or per manifest.endpointFrom. Empty while no
	// run serves: before it is ready, between attempts, on another machine
	// until that one is ready, and once the job ends. A replacement publishes
	// new URLs; read them again rather than keeping one.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=8
	Endpoints []Endpoint `json:"endpoints,omitempty"`

	// Members are a distributed job's members, in rank order, as each
	// member job reports itself. Empty on a member and on a job of one.
	// +optional
	// +listType=map
	// +listMapKey=rank
	Members []MemberStatus `json:"members,omitempty"`

	// Distribution is a distributed job's own progress.
	// +optional
	Distribution *DistributionStatus `json:"distribution,omitempty"`

	// Children are the groups of child jobs this job spawned while it ran
	// (fan-out), each recorded before its first child was created.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	Children []ChildGroup `json:"children,omitempty"`

	// Conditions carry what went wrong without ending the job, in the
	// backend's own words: a Status query the backend refused
	// (ConditionStatusQuery) or a cancel it could not perform
	// (ConditionCleanup).
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition types on ZenthrosJobStatus.Conditions.
const (
	// ConditionStatusQuery is True while the backend answers Status with an
	// error other than "not found"; Message is that error.
	ConditionStatusQuery = "StatusQuery"
	// ConditionCleanup is True while cancelling the workload fails; Message
	// is the backend's error.
	ConditionCleanup = "Cleanup"
	// ConditionServiceReady is True while a Service publishes an endpoint
	// for every port it declares; False (WaitingForPort, EndpointUnresolved)
	// while it runs without them.
	ConditionServiceReady = "ServiceReady"
	// ConditionUnavailable is True while the job waits for a destination
	// that is not connected (NotConnected), that no Gateway replica could
	// tell about (Unknown), or that failed its Submit with a transient error
	// (the reason code); its LastTransitionTime is when the wait began.
	// A destination not connected for about a minute fails the job
	// BACKEND_UNAVAILABLE; one nobody could tell about never does; transient
	// Submit errors fail it SUBMIT_RETRIES_EXHAUSTED after the submit budget.
	ConditionUnavailable = "Unavailable"
	// ConditionRateLimited is True while the job's Submit is throttled; it
	// shares the submit budget with ConditionUnavailable.
	ConditionRateLimited = "RateLimited"
	// ConditionResubmitting is True from a run lost with its agent (the
	// backend answered NotFound for a job seen Running) until the Submit of
	// the next run is accepted: the loss counts once against
	// workerLostRetryLimit however many passes that Submit takes.
	ConditionResubmitting = "Resubmitting"
)

// Endpoint scopes: from where a URL is reachable.
const (
	// EndpointScopePublic is reachable from the internet.
	EndpointScopePublic = "Public"
	// EndpointScopePrivate is reachable from the host's own network.
	EndpointScopePrivate = "Private"
	// EndpointScopeCluster is reachable inside the Kubernetes cluster.
	EndpointScopeCluster = "Cluster"
)

// Endpoint is one URL a Service answers on.
type Endpoint struct {
	// Name is the port's (or the endpointFrom entry's) name.
	Name string `json:"name"`

	// Port is the port the process listens on; 0 for a Manifest's.
	// +optional
	Port int32 `json:"port,omitempty"`

	// URL is where clients reach it.
	// +kubebuilder:validation:MaxLength=2048
	URL string `json:"url"`

	// Scope is Public, Private or Cluster.
	// +optional
	Scope string `json:"scope,omitempty"`

	// Backend and Attempt are the run that published it: a replacement on
	// another machine, or a re-run, publishes its own.
	// +optional
	Backend string `json:"backend,omitempty"`
	// +optional
	Attempt int32 `json:"attempt,omitempty"`

	// ReadyAt is when the run first published it.
	// +optional
	ReadyAt *metav1.Time `json:"readyAt,omitempty"`
}

// ResumeFrom is the ended job a new job takes its checkpoint from.
type ResumeFrom struct {
	// Job is the name of a job of this namespace.
	// +kubebuilder:validation:MinLength=1
	Job string `json:"job"`

	// UID is that job's: another job of the same name is another job.
	// +kubebuilder:validation:MinLength=1
	UID string `json:"uid"`
}

// JobProgress is the last controller-acked checkpoint {uri, phase, seq, sha256}.
type JobProgress struct {
	// URI is the object-storage location of the acked blob.
	URI string `json:"uri,omitempty"`

	// Phase is the @zenthros.phase name that produced this cursor.
	Phase string `json:"phase,omitempty"`

	// Seq is the monotonic checkpoint sequence within Phase.
	Seq int64 `json:"seq,omitempty"`

	// SHA256 is the hex digest of the committed blob.
	SHA256 string `json:"sha256,omitempty"`

	// Complete is true when this cursor is a phase COMPLETE.
	Complete bool `json:"complete,omitempty"`

	// FrozenAt is when a replacement was submitted after an interruption
	// notice. It was persisted before the replacement's resume env was built
	// from this cursor, so a commit from the interrupted worker after it is
	// refused: it would not reach the replacement.
	// +optional
	FrozenAt *metav1.Time `json:"frozenAt,omitempty"`

	// Phases is the acked cursor of every phase the job checkpointed. A
	// resumed job reads all of them (ZENTHROS_CHECKPOINT_CURSORS), so it skips
	// every phase it completed, not only the latest.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	Phases []PhaseCursor `json:"phases,omitempty"`
}

// PhaseCursor is one phase's latest acked checkpoint.
type PhaseCursor struct {
	// Name is the @zenthros.phase name.
	Name string `json:"name"`
	// URI is the acked blob.
	URI string `json:"uri"`
	// Seq is its sequence within the phase.
	Seq int64 `json:"seq"`
	// SHA256 is the blob's digest.
	SHA256 string `json:"sha256,omitempty"`
	// Complete is true once the phase's COMPLETE was acked.
	Complete bool `json:"complete,omitempty"`
}

// DrainingLease is an interrupted worker's window to commit checkpoints.
type DrainingLease struct {
	// Generation is the interrupted lease generation.
	Generation int64 `json:"generation"`
	// BackendID is the interrupted worker.
	BackendID string `json:"backendID"`
	// Until is the end of the window.
	Until metav1.Time `json:"until"`
	// Source is who gave the notice (aws-imds, gcp-metadata).
	// +optional
	Source string `json:"source,omitempty"`
}

// Where a standby slot came from.
const (
	// StandbyFromIdle is a Ready idle slot of the pool, claimed at the notice.
	StandbyFromIdle = "Idle"
	// StandbyFromCreate is a machine created for the replacement.
	StandbyFromCreate = "Created"
)

// StandbySlot is a replacement slot prepared during a drain window.
type StandbySlot struct {
	// Generation is the lease generation the replacement runs at.
	Generation int64 `json:"generation"`
	// StartAt is when the slot was, or will be, asked for.
	StartAt metav1.Time `json:"startAt"`
	// Plan says how StartAt was chosen, in words.
	// +optional
	Plan string `json:"plan,omitempty"`
	// Source is Idle or Created, once the slot is asked for.
	// +optional
	Source string `json:"source,omitempty"`
	// Pool is the slot's VMPool.
	// +optional
	Pool string `json:"pool,omitempty"`
	// CapacityRequest is the slot, reserved by this job; empty until asked for.
	// +optional
	CapacityRequest *CapacityRequestReference `json:"capacityRequest,omitempty"`
}

// PendingTerminal is an end the backend reported that the controller has
// not accepted yet.
type PendingTerminal struct {
	// Phase is what the backend reported.
	Phase Phase `json:"phase"`
	// Since is when it was first reported.
	Since metav1.Time `json:"since"`
	// Message is why it is held.
	Message string `json:"message,omitempty"`
}

// JobLease is the controller-acked liveness window for one worker generation.
type JobLease struct {
	// BackendID is the worker that currently holds the lease. Empty while a
	// replacement is being substituted (stale StatusUpdate is dropped).
	BackendID string `json:"backendID,omitempty"`

	// Generation is the worker-lost generation. StatusUpdate / Succeeded from
	// an older generation is ignored.
	Generation int64 `json:"generation,omitempty"`

	// ExpiresAt is now+jobGrace at grant/renew. Cluster default 45s; a job
	// may override spec.resilience.jobGrace (≥ 2 × heartbeat interval).
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`

	// PreemptedAt is when BackendID announced its preemption. The lease then
	// expires at once and nothing extends it again; a new seat grants a new
	// lease without it.
	// +optional
	PreemptedAt *metav1.Time `json:"preemptedAt,omitempty"`

	// GrantedAt is when this generation's lease was first granted to
	// BackendID. Evidence about the machine older than it (a provider-gone
	// stamp) is about an earlier seat.
	// +optional
	GrantedAt *metav1.Time `json:"grantedAt,omitempty"`
}

// ResultDigest is the SHA-256 a runner reported for the result it wrote at URI.
type ResultDigest struct {
	// +kubebuilder:validation:MaxLength=2048
	URI string `json:"uri"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	SHA256 string `json:"sha256"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=zj
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.spec.target.backends[0]`
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.workload.kind`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// Selectable fields let an uncached client (the Gateway's heartbeat path, the
// Controller's FIFO re-reads) filter on the apiserver instead of listing every
// job. They mirror the scalar pkg/jobindex keys; derived keys stay cache-only.
// +kubebuilder:selectablefield:JSONPath=`.status.lease.backendID`
// +kubebuilder:selectablefield:JSONPath=`.spec.poolRef`
// +kubebuilder:selectablefield:JSONPath=`.status.scheduling.decision.backend`
// +kubebuilder:selectablefield:JSONPath=`.status.scheduling.decision.pool`
// +kubebuilder:selectablefield:JSONPath=`.status.fleetPool`
// .status.phase lets a process that reads only jobs that have not ended
// (the Scheduler, the provisioner) watch those alone: a day of finished jobs
// was most of what each kept in memory.
// +kubebuilder:selectablefield:JSONPath=`.status.phase`

// ZenthrosJob is the domain entity describing a distributed workload.
type ZenthrosJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ZenthrosJobSpec   `json:"spec,omitempty"`
	Status ZenthrosJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ZenthrosJobList is a list of ZenthrosJob.
type ZenthrosJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZenthrosJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ZenthrosJob{}, &ZenthrosJobList{})
}

// ResolvedImage pins a job's image by the digest its first run pulled.
type ResolvedImage struct {
	// Requested is the image as the job's template names it.
	Requested string `json:"requested"`
	// Ref is Requested pinned by digest: repository@sha256:....
	Ref string `json:"ref"`
	// Backend and Attempt identify the run that resolved it. That run keeps
	// the reference it was submitted with; every other run gets Ref.
	// +optional
	Backend string `json:"backend,omitempty"`
	// +optional
	Attempt int32 `json:"attempt,omitempty"`
}
