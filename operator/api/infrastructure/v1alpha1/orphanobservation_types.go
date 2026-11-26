// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const OrphanObservationFinalizer = "orphanobservation.infrastructure.zenthros.io/repair"

// OrphanObservationSpec is immutable identity of one detected anomaly.
// Progress belongs in status so restarts cannot create a second mutation.
// +kubebuilder:validation:XValidation:rule="self.provider == oldSelf.provider",message="provider is immutable"
// resourceID is optional (omitempty), so the rule may not read it unguarded:
// a finding without one could never be resolved (CEL "no such key").
// +kubebuilder:validation:XValidation:rule="has(self.resourceID) == has(oldSelf.resourceID) && (!has(self.resourceID) || self.resourceID == oldSelf.resourceID)",message="resourceID is immutable"
// +kubebuilder:validation:XValidation:rule="self.kind == oldSelf.kind",message="kind is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.requestUID) == has(oldSelf.requestUID) && (!has(self.requestUID) || self.requestUID == oldSelf.requestUID)",message="requestUID is immutable"
type OrphanObservationSpec struct {
	// Provider is the registry name (fake, aws).
	// +kubebuilder:validation:MinLength=1
	Provider string `json:"provider"`

	// ResourceID is the provider instance identity. Empty for findings that
	// have no live machine (missing resource, backend without resource).
	ResourceID string `json:"resourceID,omitempty"`

	RequestID string `json:"requestID,omitempty"`
	// RequestUID is the CapacityRequest incarnation the machine was created
	// for (its capacityrequest-uid tag), or the one whose create was given
	// up (orphan.IsGivenUp).
	RequestUID     string `json:"requestUID,omitempty"`
	InstallationID string `json:"installationID,omitempty"`

	// Kind is the classification of this finding.
	// +kubebuilder:validation:Enum=UntrackedInstance;MissingResource;DuplicateInstance;BackendWithoutResource;EnrollmentTimeout;ForeignInstallation;ExternalInfrastructure
	Kind string `json:"kind"`

	PoolUID   string `json:"poolUID,omitempty"`
	BackendID string `json:"backendID,omitempty"`

	// CapacityRequest identifies the control-plane object when one exists.
	CapacityRequest *NamespacedName `json:"capacityRequest,omitempty"`

	// Protected is true for foreign-installation and External findings. The
	// Provisioner must never adopt, mutate, or destroy those resources.
	Protected bool `json:"protected,omitempty"`
}

// NamespacedName is a durable pointer at a namespaced object.
type NamespacedName struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// OrphanObservationStatus is the persistent repair record. First/last
// observation and grace survive Provisioner restarts.
type OrphanObservationStatus struct {
	Phase string `json:"phase,omitempty"`

	FirstObservedAt *metav1.Time `json:"firstObservedAt,omitempty"`
	LastObservedAt  *metav1.Time `json:"lastObservedAt,omitempty"`
	GraceExpiresAt  *metav1.Time `json:"graceExpiresAt,omitempty"`
	QuarantinedAt   *metav1.Time `json:"quarantinedAt,omitempty"`

	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
	Message    string             `json:"message,omitempty"`
	Actor      string             `json:"actor,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=orphan
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.provider`
// +kubebuilder:printcolumn:name="Resource",type=string,JSONPath=`.spec.resourceID`
// +kubebuilder:printcolumn:name="Protected",type=boolean,JSONPath=`.spec.protected`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// OrphanObservation is the Provisioner's durable record of one tag-based
// finding. Destructive cleanup is automatic only for a machine whose create
// the Provisioner gave up (orphan.IsGivenUp); any other an operator must
// adopt, terminate, or release.
type OrphanObservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OrphanObservationSpec   `json:"spec,omitempty"`
	Status OrphanObservationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type OrphanObservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OrphanObservation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OrphanObservation{}, &OrphanObservationList{})
}
