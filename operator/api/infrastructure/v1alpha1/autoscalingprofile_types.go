// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	LabelAutoscalingProfile    = "zenthros.ai/autoscaling-profile"
	LabelNodeOwner             = "zenthros.ai/node-owner"
	AnnotationReconcilesClaims = "zenthros.ai/reconciles-nodeclaims"
	NodeOwnerKarpenter         = "karpenter"
	ReconcilesNodeClaimsNever  = "false"
	KubernetesAutoscalingKind  = "KubernetesAutoscalingProfile"
)

// Purchase options that a profile may request. Spot is portable intent; AWS
// translation rejects it until Phase U enables purchasing.
const (
	PurchaseOnDemand = "on-demand"
	PurchaseSpot     = "spot"
)

const (
	ArchAMD64 = "amd64"
	ArchARM64 = "arm64"
)

// Portable instance families. AWS maps these to Karpenter instance categories.
const (
	InstanceFamilyGeneral = "general"
	InstanceFamilyCompute = "compute"
	InstanceFamilyMemory  = "memory"
	InstanceFamilyGPU     = "gpu"
)

const (
	GPUTypeTeslaT4 = "nvidia-tesla-t4"
	GPUTypeL4      = "nvidia-l4"
	GPUTypeA10G    = "nvidia-a10g"
	GPUTypeA100    = "nvidia-a100"
)

const (
	SizeSmall  = "small"
	SizeMedium = "medium"
	SizeLarge  = "large"
	SizeXLarge = "xlarge"
)

const (
	ConsolidationWhenEmpty                = "WhenEmpty"
	ConsolidationWhenEmptyOrUnderutilized = "WhenEmptyOrUnderutilized"
)

const (
	AutoscalingProviderAWS = "aws"
)

const (
	AutoscalingPending  AutoscalingProfilePhase = "Pending"
	AutoscalingApplied  AutoscalingProfilePhase = "Applied"
	AutoscalingRejected AutoscalingProfilePhase = "Rejected"
)

// AutoscalingProfilePhase is install progress of profile-derived Karpenter config.
// +kubebuilder:validation:Enum=Pending;Applied;Rejected
type AutoscalingProfilePhase string

// KubernetesAutoscalingProfileSpec is portable node-autoscaling intent.
// Translation to a provider autoscaler happens outside the Controller.
type KubernetesAutoscalingProfileSpec struct {
	// ManagedClusterRef is the hub ManagedCluster this profile configures.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	ManagedClusterRef string `json:"managedClusterRef"`

	// Provider selects the translator. Empty defaults to aws for this blueprint.
	// +kubebuilder:validation:Enum=aws
	// +kubebuilder:default=aws
	Provider string `json:"provider,omitempty"`

	CPU CPUAutoscalingBounds `json:"cpu"`

	// GPU is optional. Omitted means CPU-only nodes.
	GPU *GPUAutoscalingBounds `json:"gpu,omitempty"`

	// Architectures are kubernetes.io/arch values. Empty defaults to amd64.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	Architectures []string `json:"architectures,omitempty"`

	// InstanceFamilies are portable classes, not provider SKUs.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=4
	InstanceFamilies []string `json:"instanceFamilies,omitempty"`

	// AllowedSizes are portable size classes. Empty means any size inside CPU bounds.
	AllowedSizes []string `json:"allowedSizes,omitempty"`

	// Zones restrict placement. Empty means every blueprint subnet/AZ.
	Zones []string `json:"zones,omitempty"`

	// PurchaseOptions must be represented by the provider. Unsupported values
	// are rejected rather than dropped.
	// +kubebuilder:validation:MinItems=1
	PurchaseOptions []string `json:"purchaseOptions"`

	NodeLabels    map[string]string      `json:"nodeLabels,omitempty"`
	NodeTaints    []ProfileTaint         `json:"nodeTaints,omitempty"`
	Consolidation ConsolidationSpec      `json:"consolidation,omitempty"`
	Cost          CostConstraint         `json:"cost,omitempty"`
	Availability  AvailabilityConstraint `json:"availability,omitempty"`
}

// CPUAutoscalingBounds are cluster-wide and optional per-node CPU limits.
type CPUAutoscalingBounds struct {
	// Limit is the NodePool-wide CPU cap (cores as a Kubernetes quantity).
	// +kubebuilder:validation:MinLength=1
	Limit string `json:"limit"`

	// MinPerNode is optional minimum vCPU of a launched node.
	MinPerNode string `json:"minPerNode,omitempty"`

	// MaxPerNode is optional maximum vCPU of a launched node.
	MaxPerNode string `json:"maxPerNode,omitempty"`
}

// GPUAutoscalingBounds request accelerator nodes. Types are portable names.
type GPUAutoscalingBounds struct {
	// Limit is the NodePool-wide GPU cap.
	// +kubebuilder:validation:Minimum=1
	Limit int32 `json:"limit"`

	// Types are portable GPU names such as nvidia-tesla-t4.
	// +kubebuilder:validation:MinItems=1
	Types []string `json:"types"`
}

// ProfileTaint is a Kubernetes taint applied to autoscaled nodes.
type ProfileTaint struct {
	// +kubebuilder:validation:MinLength=1
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	// +kubebuilder:validation:Enum=NoSchedule;PreferNoSchedule;NoExecute
	Effect string `json:"effect"`
}

// ConsolidationSpec maps to the native autoscaler's empty-node policy.
type ConsolidationSpec struct {
	// Enabled defaults true so idle nodes are not left running.
	Enabled *bool `json:"enabled,omitempty"`

	// Policy is WhenEmpty or WhenEmptyOrUnderutilized.
	// +kubebuilder:validation:Enum=WhenEmpty;WhenEmptyOrUnderutilized
	Policy string `json:"policy,omitempty"`

	// After is the idle duration before consolidation, e.g. "1m".
	After string `json:"after,omitempty"`
}

// CostConstraint is a portable budget hint. Providers reject intent they
// cannot enforce rather than ignoring it.
type CostConstraint struct {
	// ClusterCPULimit duplicates CPU.Limit when set; both must agree.
	ClusterCPULimit string `json:"clusterCPULimit,omitempty"`

	// MaxHourlyUSD is an advisory cap used to refuse expensive GPU types.
	MaxHourlyUSD string `json:"maxHourlyUSD,omitempty"`
}

// AvailabilityConstraint covers node expiry. Disruption budgets beyond a
// single global consolidation policy are not portable yet.
type AvailabilityConstraint struct {
	// ExpireAfter is node lifetime, e.g. "720h". Empty uses the provider default.
	ExpireAfter string `json:"expireAfter,omitempty"`
}

// KubernetesAutoscalingProfileStatus records the last successful translation.
// NodeClaim identity never lives here; Karpenter owns those objects.
type KubernetesAutoscalingProfileStatus struct {
	// +kubebuilder:validation:Enum=Pending;Applied;Rejected
	Phase AutoscalingProfilePhase `json:"phase,omitempty"`

	RenderedNodePool   string             `json:"renderedNodePool,omitempty"`
	RenderedNodeClass  string             `json:"renderedNodeClass,omitempty"`
	CapabilityHash     string             `json:"capabilityHash,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Message            string             `json:"message,omitempty"`
	LastTransitionTime *metav1.Time       `json:"lastTransitionTime,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=kas
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.managedClusterRef`
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.provider`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type KubernetesAutoscalingProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KubernetesAutoscalingProfileSpec   `json:"spec,omitempty"`
	Status KubernetesAutoscalingProfileStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KubernetesAutoscalingProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KubernetesAutoscalingProfile `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KubernetesAutoscalingProfile{}, &KubernetesAutoscalingProfileList{})
}
