// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AnnotationFleetDesired on a VMPool is the scaling a VMFleet wants from
// it: {"fleet":"f","minReady":1,"maxReady":4}. The fleet controller writes
// it; the capacity manager and the seat walk read it through
// EffectiveScaling. It narrows the pool's own spec.scaling, never widens it.
const AnnotationFleetDesired = "fleet.zenthros.io/desired"

// LabelAccelerator on a VMPool names the accelerator its machines carry
// (A10G, L4, ...), for fleet constraints by accelerator.
const LabelAccelerator = "infrastructure.zenthros.io/accelerator"

// FleetDesired is the value of AnnotationFleetDesired.
type FleetDesired struct {
	Fleet    string `json:"fleet"`
	MinReady int32  `json:"minReady"`
	MaxReady int32  `json:"maxReady"`
}

// EffectiveScaling is the pool's scaling after its fleet's share: the
// smaller maxReady, and the fleet's minReady within it. A pool in no fleet,
// or with an annotation that does not parse, keeps its spec.
func (p *VMPool) EffectiveScaling() ScalingPolicy {
	scaling := p.Spec.Scaling
	raw, ok := p.Annotations[AnnotationFleetDesired]
	if !ok {
		return scaling
	}
	var desired FleetDesired
	if err := json.Unmarshal([]byte(raw), &desired); err != nil {
		return scaling
	}
	if desired.MaxReady < scaling.MaxReady {
		scaling.MaxReady = max(desired.MaxReady, 0)
	}
	scaling.MinReady = min(max(desired.MinReady, 0), scaling.MaxReady)
	return scaling
}

// FleetCountBy is what a fleet constraint counts slots by.
// +kubebuilder:validation:Enum=purchasingMode;zone;region;provider;instanceType;accelerator
type FleetCountBy string

const (
	CountByPurchasingMode FleetCountBy = "purchasingMode"
	CountByZone           FleetCountBy = "zone"
	CountByRegion         FleetCountBy = "region"
	CountByProvider       FleetCountBy = "provider"
	CountByInstanceType   FleetCountBy = "instanceType"
	CountByAccelerator    FleetCountBy = "accelerator"
)

// FleetMember is one pool of the fleet and its share.
// +kubebuilder:validation:XValidation:rule="!has(self.min) || self.min <= self.max",message="min must be less than or equal to max"
type FleetMember struct {
	// PoolRef names a Managed VMPool. A pool belongs to at most one fleet.
	// +kubebuilder:validation:MinLength=1
	PoolRef string `json:"poolRef"`
	// Min is the fewest warm slots the fleet keeps in this pool.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Min int32 `json:"min,omitempty"`
	// Max is the most slots this pool holds for the fleet, warm or running.
	// +kubebuilder:validation:Minimum=1
	Max int32 `json:"max"`
	// Weight orders members: jobs try the heaviest first, and warm slots
	// are shared in proportion.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	Weight int32 `json:"weight,omitempty"`
}

// FleetTotal bounds the fleet as a whole.
type FleetTotal struct {
	// MinReady is the warm slots kept across members, shared by weight.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinReady int32 `json:"minReady,omitempty"`
	// Max is the most slots across members. Empty is the sum of members' max.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Max int32 `json:"max,omitempty"`
}

// FleetConstraint limits how many slots share a value of CountBy.
// +kubebuilder:validation:XValidation:rule="(has(self.max) ? 1 : 0) + (has(self.maxFraction) ? 1 : 0) + (has(self.maxPerDomain) ? 1 : 0) == 1",message="set exactly one of max, maxFraction and maxPerDomain"
// +kubebuilder:validation:XValidation:rule="has(self.maxPerDomain) != has(self.value)",message="max and maxFraction need a value; maxPerDomain applies to every value and takes none"
type FleetConstraint struct {
	CountBy FleetCountBy `json:"countBy"`
	// Value is the one value max or maxFraction limits (Spot, A10G, ...).
	// +optional
	Value string `json:"value,omitempty"`
	// Max is the most slots with Value.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Max *int32 `json:"max,omitempty"`
	// MaxFraction is the most slots with Value as a fraction of the fleet's
	// total max, rounded down: 0.75 of 4 is 3.
	// +kubebuilder:validation:Pattern=`^(0(\.[0-9]+)?|1(\.0+)?)$`
	// +optional
	MaxFraction string `json:"maxFraction,omitempty"`
	// MaxPerDomain is the most slots with any one value (per zone, ...).
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxPerDomain *int32 `json:"maxPerDomain,omitempty"`
}

// VMFleetSpec composes homogeneous VMPools into one heterogeneous source of
// capacity: providers, regions, zones, sizes, purchase modes, accelerators.
// The fleet never provisions; it shares warm slots among its pools and
// decides, slot by slot, which pool a job's new machine comes from.
type VMFleetSpec struct {
	// Access is the Workspace allowlist for fleetRef jobs.
	Access AccessPolicy `json:"access,omitempty"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=poolRef
	Members []FleetMember `json:"members"`
	// +optional
	Total FleetTotal `json:"total,omitempty"`
	// +kubebuilder:validation:MaxItems=8
	// +optional
	Constraints []FleetConstraint `json:"constraints,omitempty"`
}

// FleetMemberStatus is one member as the fleet controller last saw it.
type FleetMemberStatus struct {
	PoolRef string `json:"poolRef"`
	// Live are the member's slots that count against its max.
	Live int32 `json:"live"`
	// Ready are warm slots no job holds; Reserved those a job holds.
	Ready    int32 `json:"ready"`
	Reserved int32 `json:"reserved"`
	// DesiredMinReady and DesiredMaxReady are what the fleet wrote on the pool.
	DesiredMinReady int32 `json:"desiredMinReady"`
	DesiredMaxReady int32 `json:"desiredMaxReady"`
	// Message says why the member takes no new slot, when it does not.
	// +optional
	Message string `json:"message,omitempty"`
}

// FleetCount is how many live slots share a value.
type FleetCount struct {
	CountBy FleetCountBy `json:"countBy"`
	Value   string       `json:"value"`
	Count   int32        `json:"count"`
}

// VMFleetStatus is observed state.
type VMFleetStatus struct {
	ObservedGeneration int64               `json:"observedGeneration,omitempty"`
	Members            []FleetMemberStatus `json:"members,omitempty"`
	Counts             []FleetCount        `json:"counts,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=vmf
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VMFleet is a heterogeneous group of VMPools jobs run on through fleetRef.
type VMFleet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VMFleetSpec   `json:"spec,omitempty"`
	Status VMFleetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VMFleetList contains a list of VMFleet.
type VMFleetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VMFleet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VMFleet{}, &VMFleetList{})
}
