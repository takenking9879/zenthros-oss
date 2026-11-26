// SPDX-License-Identifier: Apache-2.0

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	WorkspaceFinalizer       = "workspace.zenthros.ai/namespace-cleanup"
	WorkspaceNamespacePrefix = "tenant-"
	WorkspaceServiceAccount  = "zenthros-workload"
	WorkspaceRole            = "zenthros-workspace"
	WorkspaceQuota           = "zenthros-workspace-quota"
	// QuotaJobCount is the quota's name for a Workspace's ZenthrosJobs: the
	// controller writes it in WorkspaceQuota, zenthros-api reads it in a
	// refusal.
	QuotaJobCount     corev1.ResourceName = "count/zenthrosjobs." + GroupName
	LabelWorkspace                        = "zenthros.ai/workspace"
	LabelWorkspaceUID                     = "zenthros.ai/workspace-uid"
	// LabelMachineBackend marks a Job whose pod is a machine of the
	// Workspace (zenthros.create("k8s:<cluster>")): the backend its pod
	// joins as. Its size is its pod's limits, not only its requests.
	LabelMachineBackend = "zenthros.ai/machine-backend"
	// LabelUser marks the ServiceAccount of a Workspace user (zctl user
	// add): an identity of a person, never one a workload runs as.
	LabelUser = "zenthros.ai/user"
	// LabelSignup marks a Workspace a person made by signing in with Google
	// (zenthros-api, usability R1). Its controller gives it SignupOwner, an
	// editor who brings machines of their own, and lets zenthros-api issue
	// keys for that one account. Set at creation only.
	LabelSignup = "zenthros.ai/signup"
	// LabelSignupOwner is, on a signup Workspace, the first 40 hex of the
	// SHA-256 of its owner's Google account (the Firebase subject): how the
	// next sign-in finds it. AnnotationSignupEmail is that account's email.
	LabelSignupOwner      = "zenthros.ai/signup-owner"
	AnnotationSignupEmail = "zenthros.ai/signup-email"
	// AnnotationTermsVersion and AnnotationTermsAcceptedAt are, on a signup
	// Workspace, the version of the hub's terms its owner accepted to sign
	// in, and when (RFC 3339, UTC). zenthros-api writes them when it makes
	// the Workspace and when its owner accepts a newer version; the chart's
	// admission policy lets it change nothing else of a Workspace.
	AnnotationTermsVersion    = "zenthros.ai/terms-version"
	AnnotationTermsAcceptedAt = "zenthros.ai/terms-accepted-at"
	// SignupOwner is the person's ServiceAccount in a signup Workspace.
	SignupOwner = "owner"
)

// WorkspaceSpec declares the Kubernetes isolation boundary for one Workspace.
type WorkspaceSpec struct {
	// Namespace overrides the compatibility mapping tenant-<workspace-name>.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="namespace is immutable"
	Namespace string `json:"namespace,omitempty"`

	// ServiceAccountName is bound to the Workspace Role in Namespace.
	// +kubebuilder:default=zenthros-workload
	// +kubebuilder:validation:MaxLength=63
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// Quota is applied to the Workspace namespace.
	Quota corev1.ResourceQuotaSpec `json:"quota,omitempty"`

	// ReportRetention optionally overrides global ExecutionReport retention.
	// FullHistory is an explicit opt-in and is never the default.
	// +optional
	ReportRetention *ReportRetentionSpec `json:"reportRetention,omitempty"`

	// Suspended takes the Workspace from its people while it stays (zctl
	// user suspend): their keys stop working, its jobs are cancelled, its
	// machines detached and its join tokens revoked, and a sign-in to it is
	// refused. Back to false, a signup Workspace's owner is made again and
	// gets a new key at the next sign-in.
	// +optional
	Suspended bool `json:"suspended,omitempty"`
}

// ReportRetentionSpec bounds current, intermediate, and final report history
// for one Workspace. Zero/omitted fields use controller defaults.
type ReportRetentionSpec struct {
	// CurrentSeconds is how long the current snapshot is kept after last receive.
	// 0 keeps it until Workspace deletion or explicit compaction of current.
	// +optional
	// +kubebuilder:validation:Minimum=0
	CurrentSeconds *int64 `json:"currentSeconds,omitempty"`

	// IntermediateSeconds is how long non-current, non-final history is kept.
	// +optional
	// +kubebuilder:validation:Minimum=0
	IntermediateSeconds *int64 `json:"intermediateSeconds,omitempty"`

	// FinalSeconds is how long final snapshots in history (not current) are kept.
	// +optional
	// +kubebuilder:validation:Minimum=0
	FinalSeconds *int64 `json:"finalSeconds,omitempty"`

	// FullHistory disables compaction. It is never the default.
	// +optional
	FullHistory bool `json:"fullHistory,omitempty"`
}

type WorkspaceStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	Namespace          string `json:"namespace,omitempty"`
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ws
// +kubebuilder:printcolumn:name="Namespace",type=string,JSONPath=`.status.namespace`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Workspace struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkspaceSpec   `json:"spec,omitempty"`
	Status WorkspaceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type WorkspaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Workspace `json:"items"`
}

func WorkspaceNamespace(workspace *Workspace) string {
	if workspace.Spec.Namespace != "" {
		return workspace.Spec.Namespace
	}
	return WorkspaceNamespacePrefix + workspace.Name
}

func WorkspaceServiceAccountName(workspace *Workspace) string {
	if workspace.Spec.ServiceAccountName != "" {
		return workspace.Spec.ServiceAccountName
	}
	return WorkspaceServiceAccount
}

func init() {
	SchemeBuilder.Register(&Workspace{}, &WorkspaceList{})
}
