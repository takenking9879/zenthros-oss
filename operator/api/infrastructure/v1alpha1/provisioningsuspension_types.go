// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	GlobalSuspensionName = "global"

	SuspensionReasonManual     = "Manual"
	SuspensionReasonBudgetTrip = "BudgetTrip"

	AnnotationSuspensionActor  = "infrastructure.zenthros.io/last-actor"
	AnnotationSuspensionAction = "infrastructure.zenthros.io/last-action"
)

// ProvisioningSuspensionSpec is the desired gate. Resume after a budget trip
// requires an explicit spec.suspended=false write; recovering budget does not
// clear it.
type ProvisioningSuspensionSpec struct {
	Suspended bool `json:"suspended"`

	// ResumeRequired is set on automatic budget trips so a later admit cannot
	// proceed until an operator runs zctl resume.
	ResumeRequired bool `json:"resumeRequired,omitempty"`

	// Pool pins the gate to one VMPool. Empty means cluster-wide.
	// +optional
	Pool string `json:"pool,omitempty"`

	// Reason is Manual or BudgetTrip.
	// +kubebuilder:validation:Enum=Manual;BudgetTrip
	// +optional
	Reason string `json:"reason,omitempty"`

	// +optional
	Message string `json:"message,omitempty"`
}

// ProvisioningSuspensionStatus is the observed gate used by Controller,
// Scheduler, and Provisioner. All three read this CR; none pass it in memory.
type ProvisioningSuspensionStatus struct {
	ObservedGeneration int64        `json:"observedGeneration,omitempty"`
	Suspended          bool         `json:"suspended,omitempty"`
	Reason             string       `json:"reason,omitempty"`
	Since              *metav1.Time `json:"since,omitempty"`
	LastResumeAt       *metav1.Time `json:"lastResumeAt,omitempty"`
	Actor              string       `json:"actor,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=psus
// +kubebuilder:printcolumn:name="Suspended",type=boolean,JSONPath=`.spec.suspended`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.spec.reason`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="ResumeRequired",type=boolean,JSONPath=`.spec.resumeRequired`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ProvisioningSuspension is the durable Provisioner gate. Its scope is
// spec.pool alone (every pool when empty); its name follows: "global" for
// every pool, else the pool's name.
// +kubebuilder:validation:XValidation:rule="has(self.spec) && has(self.spec.pool) && size(self.spec.pool) > 0 ? self.metadata.name == self.spec.pool : self.metadata.name == 'global'",message="a suspension is named after its pool, or global for every pool"
type ProvisioningSuspension struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProvisioningSuspensionSpec   `json:"spec,omitempty"`
	Status ProvisioningSuspensionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type ProvisioningSuspensionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ProvisioningSuspension `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ProvisioningSuspension{}, &ProvisioningSuspensionList{})
}
