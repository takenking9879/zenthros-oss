// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	QuotaScopeGlobal    = "Global"
	QuotaScopeNamespace = "Namespace"
	QuotaScopeVMPool    = "VMPool"
	QuotaScopeWorkload  = "Workload"

	QuotaEnforcementHard     = "Hard"
	QuotaEnforcementAdvisory = "Advisory"
	QuotaViolationHold       = "Hold"
	QuotaViolationFail       = "Fail"
	QuotaUnknownPriceHold    = "Hold"
	QuotaUnknownPriceFail    = "Fail"
	QuotaBudgetTripSuspend   = "Suspend"
	QuotaBudgetTripHold      = "Hold"
	QuotaBudgetTripFail      = "Fail"

	QuotaPhaseRequested    = "requested"
	QuotaPhaseProvisioning = "provisioning"
	QuotaPhaseReady        = "ready"
	QuotaPhaseReserved     = "reserved"
	QuotaPhaseAllocated    = "allocated"

	LabelQuotaNamespace = "infrastructure.zenthros.io/quota-namespace"
	LabelQuotaWorkload  = "infrastructure.zenthros.io/quota-workload"
)

// QuotaScope selects which requests a policy matches. Unset selector fields are
// ignored. Global matches everything; more specific kinds still combine with
// Global through intersection of Hard limits.
type QuotaScope struct {
	// Kind is Global, Namespace, VMPool, or Workload.
	// +kubebuilder:validation:Enum=Global;Namespace;VMPool;Workload
	Kind string `json:"kind"`

	// Namespace is the tenant namespace for Namespace scope.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Pool is the VMPool name for VMPool scope.
	// +optional
	Pool string `json:"pool,omitempty"`

	// WorkloadKind is the ZenthrosJob workload kind for Workload scope,
	// for example RayJob or SparkApplication.
	// +optional
	WorkloadKind string `json:"workloadKind,omitempty"`
}

// QuotaLimits are optional caps. A nil (omitted) field is unconstrained by
// this policy. Zero is a real cap and denies any positive demand.
type QuotaLimits struct {
	// +kubebuilder:validation:Minimum=0
	// +optional
	Instances *int32 `json:"instances,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +optional
	GPUs *int32 `json:"gpus,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +optional
	VCPUs *int32 `json:"vcpus,omitempty"`

	// ConcurrentProvisioning caps in-flight provider creates, not ready machines.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ConcurrentProvisioning *int32 `json:"concurrentProvisioning,omitempty"`

	// EstimatedHourlyCostCents caps the sum of known hourly prices of live claims.
	// +kubebuilder:validation:Minimum=0
	// +optional
	EstimatedHourlyCostCents *int64 `json:"estimatedHourlyCostCents,omitempty"`

	// AccumulatedDailyCostCents caps accrued daily spend plus the next hour of
	// live and requested hourly cost. Cents stay exact across API surfaces.
	// +kubebuilder:validation:Minimum=0
	// +optional
	AccumulatedDailyCostCents *int64 `json:"accumulatedDailyCostCents,omitempty"`
}

// LoopGuards bound Scheduler/Provisioner retry loops. Nil fields fall through
// to the Scheduler's versioned Policy defaults. Set values are Hard caps when
// the parent policy is Hard.
type LoopGuards struct {
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxReplans *int32 `json:"maxReplans,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxFallbackDepth *int32 `json:"maxFallbackDepth,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxCostIncreaseCents *int64 `json:"maxCostIncreaseCents,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxReplacementAttempts *int32 `json:"maxReplacementAttempts,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxPurchaseOptionChanges *int32 `json:"maxPurchaseOptionChanges,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxCumulativeFallbackCostCents *int64 `json:"maxCumulativeFallbackCostCents,omitempty"`
}

// QuotaPolicySpec is operator configuration. Accounting lives on QuotaLedger
// so concurrent components serialize through one object.
type QuotaPolicySpec struct {
	Scope QuotaScope `json:"scope"`

	// Enforcement is Hard (blocks) or Advisory (Events only).
	// +kubebuilder:validation:Enum=Hard;Advisory
	// +kubebuilder:default=Hard
	Enforcement string `json:"enforcement,omitempty"`

	Limits QuotaLimits `json:"limits"`

	// UnknownPrice is Hold or Fail when a budget limit is set and the Catalog
	// has no price for the selected offer. It never silently continues.
	// +kubebuilder:validation:Enum=Hold;Fail
	// +kubebuilder:default=Hold
	UnknownPrice string `json:"unknownPrice,omitempty"`

	// StalePrice is Hold or Fail when a budget limit is set and the Catalog
	// snapshot or price observation is stale.
	// +kubebuilder:validation:Enum=Hold;Fail
	// +kubebuilder:default=Hold
	StalePrice string `json:"stalePrice,omitempty"`

	// OnViolation is Hold or Fail when a Hard resource quota is exceeded.
	// +kubebuilder:validation:Enum=Hold;Fail
	// +kubebuilder:default=Hold
	OnViolation string `json:"onViolation,omitempty"`

	// OnBudgetTrip is Suspend (automatic Provisioner suspension that requires
	// explicit resume), Hold, or Fail when a Hard cost cap is exceeded.
	// +kubebuilder:validation:Enum=Suspend;Hold;Fail
	// +kubebuilder:default=Suspend
	OnBudgetTrip string `json:"onBudgetTrip,omitempty"`

	// +optional
	LoopGuards LoopGuards `json:"loopGuards,omitempty"`
}

// QuotaPolicyStatus is informational. The QuotaLedger is the accounting source
// of truth so two writers cannot fork usage.
type QuotaPolicyStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	MatchingClaims     int32 `json:"matchingClaims,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=qpol
// +kubebuilder:printcolumn:name="Scope",type=string,JSONPath=`.spec.scope.kind`
// +kubebuilder:printcolumn:name="Enforcement",type=string,JSONPath=`.spec.enforcement`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// QuotaPolicy declares global, namespace, VMPool, or workload caps. Hard
// matching policies intersect (the minimum set cap wins). Advisory policies
// never raise a Hard cap.
type QuotaPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QuotaPolicySpec   `json:"spec,omitempty"`
	Status QuotaPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type QuotaPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QuotaPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&QuotaPolicy{}, &QuotaPolicyList{})
}
