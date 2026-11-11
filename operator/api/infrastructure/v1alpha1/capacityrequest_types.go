// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const CapacityRequestFinalizer = "capacityrequest.infrastructure.zenthros.io/cleanup"

// CapacityRequestPhase is the durable lifecycle of one capacity attempt.
// +kubebuilder:validation:Enum=Pending;Provisioning;Booting;AwaitingEnrollment;Ready;Reserved;Allocated;Cleaning;Draining;Unhealthy;Terminating;Failed;Unknown;Orphaned
type CapacityRequestPhase string

const (
	CapacityPending            CapacityRequestPhase = "Pending"
	CapacityProvisioning       CapacityRequestPhase = "Provisioning"
	CapacityBooting            CapacityRequestPhase = "Booting"
	CapacityAwaitingEnrollment CapacityRequestPhase = "AwaitingEnrollment"
	CapacityReady              CapacityRequestPhase = "Ready"
	CapacityReserved           CapacityRequestPhase = "Reserved"
	CapacityAllocated          CapacityRequestPhase = "Allocated"
	CapacityCleaning           CapacityRequestPhase = "Cleaning"
	CapacityDraining           CapacityRequestPhase = "Draining"
	CapacityUnhealthy          CapacityRequestPhase = "Unhealthy"
	CapacityTerminating        CapacityRequestPhase = "Terminating"
	CapacityFailed             CapacityRequestPhase = "Failed"
	CapacityUnknown            CapacityRequestPhase = "Unknown"
	CapacityOrphaned           CapacityRequestPhase = "Orphaned"
)

func (p CapacityRequestPhase) Terminal() bool {
	return p == CapacityFailed
}

// CapacityDemand is the provider-neutral capacity required by one request.
type CapacityDemand struct {
	// Count is the number of machines requested. T4 persists the value; bounded
	// multi-machine reconciliation is introduced by the provisioning engine.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	Count int32 `json:"count,omitempty"`

	// ResourceClass is a stable scheduler-facing class such as gpu-a10.
	ResourceClass string `json:"resourceClass,omitempty"`

	// Zone is the availability zone this demand requires. Empty matches any slot.
	Zone string `json:"zone,omitempty"`

	// PurchaseOption is OnDemand or Spot. Empty matches any slot.
	PurchaseOption string `json:"purchaseOption,omitempty"`

	// Resources expresses provider-neutral compute demand.
	Resources corev1.ResourceList `json:"resources,omitempty"`
}

// CapacityCleanupPolicy controls provider-resource cleanup after termination.
// +kubebuilder:validation:Enum=Delete;Retain
type CapacityCleanupPolicy string

const (
	CapacityCleanupDelete CapacityCleanupPolicy = "Delete"
	CapacityCleanupRetain CapacityCleanupPolicy = "Retain"
)

// CapacityRequestSpec is immutable request intent. Reconciliation progress and
// provider identities belong in status so restarts cannot create a second machine.
// +kubebuilder:validation:XValidation:rule="self.requestID == oldSelf.requestID",message="requestID is immutable"
// +kubebuilder:validation:XValidation:rule="self.demand == oldSelf.demand",message="demand is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.vmpoolRef) == has(oldSelf.vmpoolRef) && (!has(self.vmpoolRef) || self.vmpoolRef == oldSelf.vmpoolRef)",message="vmpoolRef is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.poolUID) == has(oldSelf.poolUID) && (!has(self.poolUID) || self.poolUID == oldSelf.poolUID)",message="poolUID is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.slotID) == has(oldSelf.slotID) && (!has(self.slotID) || self.slotID == oldSelf.slotID)",message="slotID is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.requestedOffer) == has(oldSelf.requestedOffer) && (!has(self.requestedOffer) || self.requestedOffer == oldSelf.requestedOffer)",message="requestedOffer is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.provider) == has(oldSelf.provider) && (!has(self.provider) || self.provider == oldSelf.provider)",message="provider is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.account) == has(oldSelf.account) && (!has(self.account) || self.account == oldSelf.account)",message="account is immutable"
type CapacityRequestSpec struct {
	// RequestID is the stable provider idempotency key.
	// +kubebuilder:validation:MinLength=1
	RequestID string `json:"requestID"`

	Demand CapacityDemand `json:"demand"`

	// VMPoolRef optionally pins the request to an explicit pool.
	VMPoolRef string `json:"vmpoolRef,omitempty"`

	// PoolUID prevents a deleted and recreated pool from adopting old slots.
	PoolUID string `json:"poolUID,omitempty"`

	// SlotID is stable for the lifetime of one pool-owned capacity position.
	SlotID string `json:"slotID,omitempty"`

	// Provider is the registry name of the provider whose machine serves the
	// request (the pool's providerRef when it was created). The repair sweeper
	// classifies a request only in this provider's inventory, also once its
	// pool is gone.
	Provider string `json:"provider,omitempty"`

	// Account is the CloudAccount the request's machine is in (the pool's
	// providerRef.account when it was created); empty is the installation's
	// own. With Provider it finds the machine once its pool is gone.
	Account string `json:"account,omitempty"`

	// RequestedOffer is the Scheduler-selected Catalog offer this stable slot
	// must provision. Nil: the pool's own types.
	RequestedOffer *OfferReference `json:"requestedOffer,omitempty"`

	// CleanupPolicy is the requested finalizer behavior.
	// +kubebuilder:default=Delete
	CleanupPolicy CapacityCleanupPolicy `json:"cleanupPolicy,omitempty"`
}

// OfferReference is the immutable provider-catalog choice used by this attempt.
type OfferReference struct {
	ID             string `json:"id"`
	InstanceType   string `json:"instanceType,omitempty"`
	Region         string `json:"region,omitempty"`
	Zone           string `json:"zone,omitempty"`
	PurchaseOption string `json:"purchaseOption,omitempty"`
}

// JobReference binds the reservation to the workload holding it.
type JobReference struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// Matches reports whether the reference points at the given job identity.
func (r *JobReference) Matches(namespace, name, uid string) bool {
	return r != nil && r.Namespace == namespace && r.Name == name && r.UID == uid
}

// ObservedMachineIdentity is the provider-authenticated identity presented at
// managed enrollment. It contains no bearer token or private key material.
type ObservedMachineIdentity struct {
	Provider   string      `json:"provider"`
	AccountID  string      `json:"accountID,omitempty"`
	Region     string      `json:"region,omitempty"`
	ResourceID string      `json:"resourceID"`
	VerifiedAt metav1.Time `json:"verifiedAt"`
}

// CapacityRequestStatus is the persistent source of truth for one machine
// attempt, including identities required to reconcile an ambiguous create.
type CapacityRequestStatus struct {
	Phase CapacityRequestPhase `json:"phase,omitempty"`

	SelectedVMPool string          `json:"selectedVMPool,omitempty"`
	SelectedOffer  *OfferReference `json:"selectedOffer,omitempty"`

	ProviderResourceID string `json:"providerResourceID,omitempty"`
	// ProviderHost is the provider's host the machine runs on, where its
	// provider names one (provider.Machine.Host). A boot that fails there
	// marks the host for the next creates (catalog.FailedHosts).
	ProviderHost string `json:"providerHost,omitempty"`
	BackendID    string `json:"backendID,omitempty"`
	// LastHeartbeat is the last observed agent heartbeat for this enrolled
	// slot. Idle refill waits idleGrace from this stamp so a disconnect can
	// resume; missing directory entries do not skip the grace window.
	LastHeartbeat *metav1.Time `json:"lastHeartbeat,omitempty"`
	// PreemptedAt is when the agent reported heartbeat.preempted on this
	// enrolled slot. Idle refill skips idleGrace once this is set. Observe
	// never writes it (Gone-only).
	PreemptedAt *metav1.Time `json:"preemptedAt,omitempty"`
	// PreemptionDeadline is the end of the notice's window, stamped with
	// PreemptedAt: the provider's deadline, capped per source with room left
	// before it acts (liveness.DrainUntil). A joined pool's node retires by
	// then.
	// +optional
	PreemptionDeadline *metav1.Time `json:"preemptionDeadline,omitempty"`
	// RebalanceRecommendedAt is when the agent first reported that its
	// provider recommends rebalancing the machine away: it is at elevated
	// risk of interruption (an EC2 rebalance recommendation). Advisory: the
	// machine keeps running; a pool with interruption.capacityRebalance
	// replaces it.
	// +optional
	RebalanceRecommendedAt *metav1.Time `json:"rebalanceRecommendedAt,omitempty"`
	// NodeJoin is a joined pool's machine asking its cluster for a node
	// credential, and what came of it.
	// +optional
	NodeJoin *NodeJoinStatus `json:"nodeJoin,omitempty"`
	// ProviderGoneAt is when the provisioner, which holds the provider
	// credentials, first saw the machine gone: Describe NotFound, which is
	// how terminated, deleted and reclaimed machines read. The controller
	// reads it to skip the job grace without credentials of its own. A
	// machine seen again clears it.
	// +optional
	ProviderGoneAt *metav1.Time `json:"providerGoneAt,omitempty"`
	// LifetimeEndedAt is when the provisioner found this machine, holding a
	// job, past its pool's spec.scaling.maxLifetime. The controller reads it
	// as a notice for the job on it; the machine ends once the job left it.
	// +optional
	LifetimeEndedAt *metav1.Time `json:"lifetimeEndedAt,omitempty"`
	// ProviderState is the machine state the provisioner's last Describe
	// returned, or NotFound.
	// +optional
	ProviderState    string                   `json:"providerState,omitempty"`
	Address          string                   `json:"address,omitempty"`
	Ownership        BackendOwnership         `json:"ownership,omitempty"`
	ObservedIdentity *ObservedMachineIdentity `json:"observedIdentity,omitempty"`

	ProvisionAttempts int32 `json:"provisionAttempts,omitempty"`
	// CreateEpoch counts the keys the request's creates went through: an
	// operator sent it back to Pending after its machine was gone (zctl repair
	// retry), or a key will answer the same on every repeat. A provider's
	// idempotency key is the request ID with it: the same key made AWS hand
	// back the instance that was gone, and GCE repeat a refusal with the quota
	// free. The request ID, which tags the machine, stays.
	// +optional
	CreateEpoch         int32 `json:"createEpoch,omitempty"`
	EnrollmentAttempts  int32 `json:"enrollmentAttempts,omitempty"`
	TerminationAttempts int32 `json:"terminationAttempts,omitempty"`
	// ResolveAttempts counts find-by-request-ID misses while reconciling an
	// ambiguous (Unknown) outcome. Bounded so a lost machine fails explicitly
	// instead of resolving forever (Phase T5).
	ResolveAttempts int32 `json:"resolveAttempts,omitempty"`
	// RefusedCreates counts the creates the provider refused with nothing
	// made (an atomic call turned down). A deleted request with no recorded
	// machine is searched for by request ID only when some create was not
	// refused: one that may have launched a machine.
	// +optional
	RefusedCreates int32 `json:"refusedCreates,omitempty"`

	// OrphanKind classifies a detected anomaly. Empty until the Provisioner
	// records an observation. It is not a second timing model.
	OrphanKind string `json:"orphanKind,omitempty"`

	// FirstObservedAt is when the current anomaly was first seen. It does
	// not move across sweeper passes.
	FirstObservedAt *metav1.Time `json:"firstObservedAt,omitempty"`
	// LastObservedAt is refreshed on every observation of the same anomaly.
	LastObservedAt *metav1.Time `json:"lastObservedAt,omitempty"`
	// GraceExpiresAt is FirstObservedAt plus the configured orphan grace.
	GraceExpiresAt *metav1.Time `json:"graceExpiresAt,omitempty"`

	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
	Message    string             `json:"message,omitempty"`

	// ReservedBy is the job currently holding the machine. Nil while the
	// machine is unreserved; a reservation is exclusive for the request's
	// lifetime.
	ReservedBy *JobReference `json:"reservedBy,omitempty"`

	// HeldByUser is the session owner of this seat. Reserve stamps it from
	// the job's user label. Release and CompleteCleanup leave it in place so
	// the same user keeps the machine after each job in a playground run.
	// When that run is terminal the playground worker clears it. zctl free
	// is leftover-hold only. Worker-lost Create/Reuse copies this onto the
	// replacement while the job is still running. Empty means anyone can
	// take the Ready slot.
	HeldByUser string `json:"heldByUser,omitempty"`

	// ReservationID is the compare-and-swap token for the current lease cycle.
	ReservationID        string       `json:"reservationID,omitempty"`
	ReservationExpiresAt *metav1.Time `json:"reservationExpiresAt,omitempty"`
	ReservationRenewedAt *metav1.Time `json:"reservationRenewedAt,omitempty"`

	// Lifecycle timestamps are the canonical timing evidence for the attempt.
	// Durations are always derived from pairs, never persisted separately:
	// provisioning = provisioningAt->bootingAt, time-to-ready =
	// requestedAt->readyAt, reservation hold = reservedAt->releasedAt,
	// cleanup = terminatingAt->terminatedAt, end-to-end = requestedAt->
	// terminatedAt.
	RequestedAt        *metav1.Time `json:"requestedAt,omitempty"`
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
	ProvisioningAt     *metav1.Time `json:"provisioningAt,omitempty"`
	// ProviderCreatedAt is when the provider reported the instance created
	// (the create acceptance or the earliest observed launch time).
	ProviderCreatedAt *metav1.Time `json:"providerCreatedAt,omitempty"`
	BootingAt         *metav1.Time `json:"bootingAt,omitempty"`
	EnrollmentAt      *metav1.Time `json:"enrollmentAt,omitempty"`
	ReadyAt           *metav1.Time `json:"readyAt,omitempty"`
	// AvailableAt is reset whenever verified cleanup returns the slot to Ready.
	AvailableAt *metav1.Time `json:"availableAt,omitempty"`
	// ReservedAt is when a workload claimed the machine.
	ReservedAt *metav1.Time `json:"reservedAt,omitempty"`
	// ReleasedAt is when the holding workload released its claim. Single-use
	// vertical: release is followed by termination after the grace period.
	ReleasedAt *metav1.Time `json:"releasedAt,omitempty"`
	// AllocatedAt is when the reserved machine was handed to a submitted
	// workload (Reserved -> Allocated). It is not a reservation timestamp.
	AllocatedAt   *metav1.Time `json:"allocatedAt,omitempty"`
	CleaningAt    *metav1.Time `json:"cleaningAt,omitempty"`
	CleanedAt     *metav1.Time `json:"cleanedAt,omitempty"`
	QuarantinedAt *metav1.Time `json:"quarantinedAt,omitempty"`
	TerminatingAt *metav1.Time `json:"terminatingAt,omitempty"`

	// ShutdownRequestedAt is when the provisioner, refused by the machine's
	// cloud account, asked for the machine to power itself off instead of
	// being terminated: its termination is asked, not confirmed. The hub
	// passes the request on to the machine's agent. The request stays until
	// the account answers again and the machine is seen gone.
	// +optional
	ShutdownRequestedAt *metav1.Time `json:"shutdownRequestedAt,omitempty"`
	// TerminatedAt is when the provider reported the machine gone during
	// termination. It is the completion of the infrastructure attempt.
	TerminatedAt *metav1.Time `json:"terminatedAt,omitempty"`
	// CompletedAt is when the attempt reached its terminal lifecycle outcome
	// (Failed, or Terminating for a deleted request). It is not a provider
	// termination timestamp; TerminatedAt carries that meaning.
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=crq
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.status.selectedVMPool`
// +kubebuilder:printcolumn:name="Provider ID",type=string,JSONPath=`.status.providerResourceID`
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.status.backendID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// .status.backendID lets an uncached reader find a machine's slot on the
// apiserver (jobindex.CapacityRequestsForBackend): a node joining a cluster
// asks every 2 s, and each ask listed every slot (19#2).
// +kubebuilder:selectablefield:JSONPath=`.status.backendID`

// CapacityRequest persists one provider-backed capacity attempt. No provider call
// is possible until the provisioning engine is added in Phase T5.
type CapacityRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CapacityRequestSpec   `json:"spec,omitempty"`
	Status CapacityRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type CapacityRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CapacityRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CapacityRequest{}, &CapacityRequestList{})
}
