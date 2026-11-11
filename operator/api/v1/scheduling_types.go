// SPDX-License-Identifier: Apache-2.0

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Backend kind values for compute constraints and scheduling candidates.
const (
	BackendKindK8s = "k8s"
	BackendKindVM  = "vm"
	// BackendKindContainer is an agent inside one unprivileged container.
	BackendKindContainer = "container"
)

// AnnotationExclusiveHold is a job's claim, written before Submit, on a unit
// that runs one job at a time: "backend/<id>" or
// "raycluster/<namespace>/<name>". The controller writes it; the policy
// keeps it from anyone else.
const AnnotationExclusiveHold = "zenthros.ai/exclusive-hold"

// Machine execution modes. Empty on a VM Job means systemd. Ignored on k8s.
const (
	RuntimeSystemd             = "systemd"
	RuntimeContainerd          = "containerd"
	RuntimeWorkspaceContainers = "workspace-containers"
	// RuntimeProcess is a child process of the container agent, inside the
	// container's own image.
	RuntimeProcess = "process"
)

// Workload lifespan after the process exits. Empty derives from poolRef vs
// target.backends and gcPolicy. Retain keeps the finished Job/Pod for debug;
// the next task still creates a new one.
const (
	LifespanEphemeral = "Ephemeral"
	LifespanRetain    = "Retain"
)

// Purchase option values for compute constraints.
const (
	PurchaseOptionOnDemand = "OnDemand"
	PurchaseOptionSpot     = "Spot"
)

// Scheduling decision modes.
const (
	SchedulingModeBackend = "Backend"
	SchedulingModePool    = "Pool"
	SchedulingModeNone    = "None"
)

// Scheduling candidate kinds.
const (
	CandidateKindBackend = "Backend"
	CandidateKindPool    = "Pool"
)

// Fallback steps are the bounded replan order. A later step is only considered
// after earlier ones are exhausted or forbidden by a user constraint.
// AlternateAZ is a FailureDomain walk (GCP zone or AWS AZ) in the same region.
const (
	FallbackStepAlternateAZ     = "alternate-az"
	FallbackStepEquivalentType  = "equivalent-type"
	FallbackStepAlternatePool   = "alternate-pool"
	FallbackStepAlternateRegion = "alternate-region"
	FallbackStepPurchaseOption  = "purchase-option"
)

// FallbackStepDepth is the 1-based position of a fallback step in the
// bounded order. An unknown step is deeper than any permitted depth, so a
// depth cap skips it.
func FallbackStepDepth(step string) int {
	switch step {
	case FallbackStepAlternateAZ:
		return 1
	case FallbackStepEquivalentType:
		return 2
	case FallbackStepAlternatePool:
		return 3
	case FallbackStepAlternateRegion:
		return 4
	case FallbackStepPurchaseOption:
		return 5
	default:
		return 99
	}
}

// ExhaustionBound names the exact policy bound that stopped further fallback.
const (
	BoundMaxReplans             = "maxReplans"
	BoundMaxFallbackDepth       = "maxFallbackDepth"
	BoundMaxCostIncrease        = "maxCostIncrease"
	BoundUserConstraint         = "userConstraint"
	BoundNoFeasibleCandidate    = "noFeasibleCandidate"
	BoundMaxAlternateAZ         = "maxAlternateAZ"
	BoundReplacementAttempts    = "maxReplacementAttempts"
	BoundPurchaseOptionChanges  = "maxPurchaseOptionChanges"
	BoundCumulativeFallbackCost = "maxCumulativeFallbackCost"
	BoundQuotaExceeded          = "quotaExceeded"
	BoundBudgetExceeded         = "budgetExceeded"
	BoundProvisioningSuspended  = "provisioningSuspended"
	// BoundFleetExhausted: every member of the job's VMFleet failed to
	// provision its machine.
	BoundFleetExhausted = "fleetExhausted"
	// BoundPoolExhausted: every placement of the VMPool the job names (its
	// zones and allowed types) found no capacity for the job's machine.
	BoundPoolExhausted = "poolExhausted"
	// BoundFleetRejected: the VMFleet the job names does not exist.
	BoundFleetRejected = "fleetRejected"
	// BoundPoolRejected: the VMPool the job names does not exist, or
	// supplies nodes to a Kubernetes cluster.
	BoundPoolRejected = "poolRejected"
	// BoundNoReplacement: the job lost its worker and no place takes its
	// replacement; the Controller fails it WORKER_LOST.
	BoundNoReplacement = "noReplacement"
	// BoundForbidden: the job may not run where it names (a VMFleet its
	// Workspace may not use), or runs as no Workspace (its namespace is
	// none's).
	BoundForbidden = "forbidden"
	// BoundPlacementUnsatisfied: every seat the job may take breaks its
	// spec.placement; it waits until a seat satisfies the rules.
	BoundPlacementUnsatisfied = "placementUnsatisfied"

	// DecisionReasonAdopted is the reason of a decision made from the seat
	// a job holds: the backend it was sent to while its queue moved.
	DecisionReasonAdopted = "adopted from the seat the job held"

	AdmissionHold = "Hold"
	AdmissionFail = "Fail"
)

// ReplanRequiredReason is the CapacityRequest Degraded reason that asks the
// Scheduler to abandon the current selection. It must match the Provisioner's
// condition reason; the Scheduler never imports the Provisioner package.
const ReplanRequiredReason = "ReplanRequired"

// ComputeConstraints is the scheduler-facing demand a targetless job must satisfy.
// Fields left empty are unconstrained; CPU and memory default to the workload's
// workers.cpu/workers.memory when this struct does not set them.
type ComputeConstraints struct {
	// BackendKind narrows placement to "k8s", "vm" or "container".
	// +kubebuilder:validation:Enum=k8s;vm;container
	// +optional
	BackendKind string `json:"backendKind,omitempty"`

	// CPU is the per-machine vCPU requirement, e.g. "8".
	// +optional
	CPU string `json:"cpu,omitempty"`

	// Memory is the per-machine memory requirement, e.g. "32Gi".
	// +optional
	Memory string `json:"memory,omitempty"`

	// GPUModel pins the accelerator model, e.g. "H100" or "A10G".
	// +optional
	GPUModel string `json:"gpuModel,omitempty"`

	// GPUCount is the minimum accelerator count per machine.
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPUCount int32 `json:"gpuCount,omitempty"`

	// Architecture pins the Catalog architecture, e.g. "x86_64" or "arm64".
	// +optional
	Architecture string `json:"architecture,omitempty"`

	// PurchaseOption restricts the allowed purchase mode. OnDemand permits only
	// On-Demand capacity; Spot permits Spot. Empty permits either.
	// +kubebuilder:validation:Enum=OnDemand;Spot
	// +optional
	PurchaseOption string `json:"purchaseOption,omitempty"`

	// InstanceType pins Create to a VMPool.spec.instancePolicy.allowedTypes
	// entry. Ignored by Reuse and Backends.
	// +optional
	InstanceType string `json:"instanceType,omitempty"`

	// Region constrains placement to a provider region.
	// +optional
	Region string `json:"region,omitempty"`

	// Zone constrains placement to one availability zone within Region.
	// +optional
	Zone string `json:"zone,omitempty"`

	// MaxHourlyPriceCents is the maximum estimated hourly price, in US cents,
	// accepted for a selected offer. Zero means unconstrained. Cents (not a
	// float) keep the value exact across every API surface.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxHourlyPriceCents int64 `json:"maxHourlyPriceCents,omitempty"`

	// Runtime selects how a machine runs the job: on a VM systemd (default),
	// containerd, or workspace-containers; in a container, process. Ignored on
	// k8s backends. Do not infer this from image.
	// +kubebuilder:validation:Enum=systemd;containerd;workspace-containers;process
	// +optional
	Runtime string `json:"runtime,omitempty"`

	// Lifespan is Ephemeral (delete native Job/Pod after GC) or Retain (keep the
	// finished object for debug). Empty derives from gcPolicy and pool vs target.
	// +kubebuilder:validation:Enum=Ephemeral;Retain
	// +optional
	Lifespan string `json:"lifespan,omitempty"`
}

// SchedulingOffer is the selected provider-catalog placement for a Pool decision.
type SchedulingOffer struct {
	// ID is the catalog offering identifier.
	ID string `json:"id,omitempty"`

	// InstanceType is the provider instance type name.
	InstanceType string `json:"instanceType,omitempty"`

	// Region and Zone are the provider placement location.
	Region string `json:"region,omitempty"`
	Zone   string `json:"zone,omitempty"`

	// PurchaseOption is OnDemand or Spot.
	PurchaseOption string `json:"purchaseOption,omitempty"`

	// HourlyPriceCents is the estimated hourly price known at decision time.
	// Persisted so a later Catalog refresh cannot rewrite historical cost.
	// +optional
	HourlyPriceCents int64 `json:"hourlyPriceCents,omitempty"`
}

// SchedulingDecision is the Scheduler's selected placement for one targetless job.
type SchedulingDecision struct {
	// Mode is Backend, Pool, or None (no compatible candidate).
	Mode string `json:"mode"`

	// Backend is the selected existing backend id when Mode is Backend.
	Backend string `json:"backend,omitempty"`

	// Pool is the selected VMPool name when Mode is Pool.
	Pool string `json:"pool,omitempty"`

	// Offering is the selected catalog offer when Mode is Pool.
	Offering *SchedulingOffer `json:"offering,omitempty"`

	// Score is the deterministic score of the selected candidate.
	Score int64 `json:"score,omitempty"`

	// Reason summarizes why this decision was made (or why none was).
	Reason string `json:"reason,omitempty"`

	// Step is how the job takes its seat: Backends (a named or enrolled
	// backend), Reuse (a Ready slot of the pool) or Create (a new machine
	// of the pool, on Offering).
	// +optional
	Step SeatStep `json:"step,omitempty"`

	// Domain is the failure domain (zone) the seat must be in, when the
	// job's placement chose one; empty when any.
	// +optional
	Domain string `json:"domain,omitempty"`

	// Alternatives is how many other candidates could take the job: a
	// decision with none is the job's only place, where it queues and is
	// never given back as Busy.
	// +optional
	Alternatives int32 `json:"alternatives,omitempty"`

	// Queued is a decision made while none of the job's destinations could
	// take it (a backend at its maxJobs, a pool with no free slot at its
	// bound): the job queues on this one, and until it is sent or seated
	// there the Scheduler moves it to the first that can.
	// +optional
	Queued bool `json:"queued,omitempty"`

	// LeaseGeneration is the lease generation the decision was made for
	// when the job had lost its worker (its lease fenced): the decision is
	// that generation's replacement, and the Controller seats a fenced job
	// only on its own generation's. Zero for a job that had not.
	// +optional
	LeaseGeneration int64 `json:"leaseGeneration,omitempty"`
}

// SchedulingCandidate records one evaluated candidate with its outcome. This is a
// historical explanation, never a live recomputation.
type SchedulingCandidate struct {
	// Kind is Backend or Pool.
	Kind string `json:"kind"`

	// ID is the backend id (Backend) or VMPool name (Pool).
	ID string `json:"id"`

	// Offering records the exact Catalog choice evaluated for a Pool candidate.
	Offering *SchedulingOffer `json:"offering,omitempty"`

	// Selected is true only for the chosen candidate.
	Selected bool `json:"selected,omitempty"`

	// Score is the deterministic score; zero when the candidate was rejected.
	Score int64 `json:"score,omitempty"`

	// RejectionReason explains a filtered candidate; empty when accepted.
	RejectionReason string `json:"rejectionReason,omitempty"`

	// ScoreComponents is the per-component breakdown for accepted candidates.
	ScoreComponents map[string]int64 `json:"scoreComponents,omitempty"`

	// FallbackStep is the bounded replan step this candidate represents relative
	// to the abandoned selection. Empty on the original decision.
	// +optional
	FallbackStep string `json:"fallbackStep,omitempty"`
}

// SchedulingAttempt is one persisted placement try, including abandoned
// selections. The list is append-only historical evidence; it is never
// recomputed after a restart.
type SchedulingAttempt struct {
	// Index is 1-based attempt order. The original selection is index 1.
	Index int32 `json:"index"`

	// Decision is the placement chosen for this attempt.
	Decision SchedulingDecision `json:"decision"`

	// AbandonedAt is when the Scheduler abandoned this selection. Nil while
	// the attempt is still the current decision.
	// +optional
	AbandonedAt *metav1.Time `json:"abandonedAt,omitempty"`

	// ProviderReason is the durable provider/class reason that forced the
	// replan, e.g. "replan-required".
	// +optional
	ProviderReason string `json:"providerReason,omitempty"`

	// FallbackStep is the policy step used to reach this attempt from the
	// previous one. Empty on the original selection.
	// +optional
	FallbackStep string `json:"fallbackStep,omitempty"`

	// CostDeltaCents is this attempt's estimated hourly price minus the
	// original selection's price. Zero when either price is unknown.
	// +optional
	CostDeltaCents int64 `json:"costDeltaCents,omitempty"`

	// NextSelection identifies the candidate chosen after this one was
	// abandoned (pool/offering or backend id).
	// +optional
	NextSelection string `json:"nextSelection,omitempty"`

	// AbandonKind is the kind of the Controller's give-back that left this
	// attempt (SchedulingAbandon.Kind), empty when the Scheduler left it on
	// its own. A lost or draining worker's (Replaced) says nothing against
	// the place: the machine was lost, not the place.
	// +optional
	AbandonKind string `json:"abandonKind,omitempty"`
}

// SchedulingStatus is the Scheduler's persisted decision for a targetless job. It
// survives Scheduler and Controller restarts and is historical, not recomputed.
type SchedulingStatus struct {
	// ObservedGeneration is the spec generation the decision was computed for.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Decision is the selected placement.
	Decision SchedulingDecision `json:"decision,omitempty"`

	// Candidates records every evaluated candidate and its outcome.
	Candidates []SchedulingCandidate `json:"candidates,omitempty"`

	// CatalogVersion and CatalogTimestamp identify the catalog snapshot used.
	CatalogVersion   int64        `json:"catalogVersion,omitempty"`
	CatalogTimestamp *metav1.Time `json:"catalogTimestamp,omitempty"`

	// PolicyVersion identifies the scheduler policy that produced the decision.
	PolicyVersion string `json:"policyVersion,omitempty"`

	// DecidedAt is when the current decision was persisted.
	DecidedAt *metav1.Time `json:"decidedAt,omitempty"`

	// OriginalOffer is the first Pool selection for this generation. Cost
	// bounds are measured against it so fallback cannot ratchet silently.
	// +optional
	OriginalOffer *SchedulingOffer `json:"originalOffer,omitempty"`

	// Attempts is the append-only history of selections for this generation,
	// including the current one. Restart-safe explanation is reconstructed
	// from this list, never from a live recomputation.
	// +optional
	Attempts []SchedulingAttempt `json:"attempts,omitempty"`

	// ReplanCount is how many times the Scheduler has abandoned a selection
	// for this generation.
	// +optional
	ReplanCount int32 `json:"replanCount,omitempty"`

	// ExhaustionBound is the exact policy bound that stopped further
	// fallback. Empty while a current selection is still in play.
	// +optional
	ExhaustionBound string `json:"exhaustionBound,omitempty"`

	// AdmissionAction is Hold or Fail when ExhaustionBound is a quota,
	// budget, or suspension gate. Empty for T13 placement bounds.
	// +optional
	AdmissionAction string `json:"admissionAction,omitempty"`

	// ReplacementAttempts counts abandoned Pool selections for this
	// generation. Distinct from ReplanCount so backend-only replans do
	// not consume the replacement cap.
	// +optional
	ReplacementAttempts int32 `json:"replacementAttempts,omitempty"`

	// PurchaseOptionChanges counts fallback steps that changed OnDemand/Spot.
	// +optional
	PurchaseOptionChanges int32 `json:"purchaseOptionChanges,omitempty"`

	// CumulativeFallbackCostCents is the running sum of positive cost
	// deltas across abandoned attempts, persisted so a restart cannot
	// reset the fallback spend bound.
	// +optional
	CumulativeFallbackCostCents int64 `json:"cumulativeFallbackCostCents,omitempty"`

	// Abandon is the Controller giving the current decision back: its
	// backend refused the job before it ran (unavailable, or busy past the
	// retry bound), its pool found no capacity where the job's machine was
	// asked for, or its worker was lost or is draining. The Scheduler places
	// the job again without that place: a refusal as one replan, a lost
	// worker as its replacement. The Controller does not seat the job on a
	// decision it gave back.
	// +optional
	Abandon *SchedulingAbandon `json:"abandon,omitempty"`
}

// The kinds of give-back: why the Controller handed a decision back.
const (
	AbandonRefused    = "Refused"
	AbandonBusy       = "Busy"
	AbandonNoCapacity = "NoCapacity"
	AbandonWorkerLost = "WorkerLost"
	// AbandonDraining: the worker got an interruption notice; the job keeps
	// its seat until the drain window ends, and its replacement is
	// prepared meanwhile.
	AbandonDraining = "Draining"
)

// SchedulingAbandon names the decision the Controller gave back and what
// it saw: the backend, or the pool and the offer its machine was asked on.
type SchedulingAbandon struct {
	// Kind is why: Refused, Busy, NoCapacity, WorkerLost or Draining.
	// +optional
	Kind string `json:"kind,omitempty"`

	// Backend is the decided backend given back.
	// +optional
	Backend string `json:"backend,omitempty"`

	// Pool is the decided pool given back.
	// +optional
	Pool string `json:"pool,omitempty"`

	// Offer is where the pool's machine was asked for: the decision's
	// offer, or the pool's own when the decision named none.
	// +optional
	Offer *SchedulingOffer `json:"offer,omitempty"`

	// Slot is the CapacityRequest that failed.
	// +optional
	Slot string `json:"slot,omitempty"`

	// LeaseGeneration is the generation of the job's lease once it gave
	// the decision back: the one its next machine is asked under.
	// +optional
	LeaseGeneration int64 `json:"leaseGeneration,omitempty"`

	// Reason is the refusal: its reason code, then the backend's or the
	// provider's words.
	Reason string `json:"reason"`
}
