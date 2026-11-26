// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"fmt"
	"regexp"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type BackendAccessMode string

const (
	AccessPrivate BackendAccessMode = "Private"
	AccessPublic  BackendAccessMode = "Public"
)

// BackendSelection says which jobs a backend takes.
type BackendSelection string

const (
	// SelectionAny: the jobs that name it, and those the Scheduler places.
	SelectionAny BackendSelection = "Any"
	// SelectionNamed: only the jobs that name it (target.backends). A
	// backend joined through zenthros-api is Named: whoever joins a machine
	// that way reaches only the jobs that ask for it.
	SelectionNamed BackendSelection = "Named"
)

// AccessPolicy controls Workspace visibility independently from infrastructure ownership.
// An omitted Workspace list is accepted only for the VMPool default
// (NormalizePoolAccess).
// BackendAccess adds a stricter required-list rule at its spec boundary.
// +kubebuilder:validation:XValidation:rule="has(self.workspaces) ? self.mode != 'Private' || size(self.workspaces) == 1 : self.mode == 'Private'",message="private access requires exactly one Workspace and public access requires explicit grants"
type AccessPolicy struct {
	// Mode is Private for one Workspace or Public for multiple explicit grants.
	// +kubebuilder:validation:Enum=Private;Public
	// +kubebuilder:default=Private
	Mode BackendAccessMode `json:"mode,omitempty"`

	// Workspaces is the explicit allowlist. Public does not mean anonymous.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=63
	Workspaces []string `json:"workspaces,omitempty"`
}

func (p AccessPolicy) Allows(workspace string) bool {
	for _, allowed := range p.Workspaces {
		if allowed == workspace {
			return true
		}
	}
	return false
}

// backendNameRE is spec.name's pattern, for code that checks a name before
// it writes it.
var backendNameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ValidBackendName is spec.name's rule: a DNS label, at most 63.
func ValidBackendName(name string) bool {
	return len(name) <= 63 && backendNameRE.MatchString(name)
}

// ValidateNewBackendID is the rule for a backend ID given out from now on:
// a DNS label of at most 63, since it is a label value (zenthros.ai/backend-id)
// and the machine's tailnet name, and not a name the Gateway's devices take
// (zenthros-gateway*, zenthros-controller), which tailnet cleanup would read
// as the Gateway's (N16). Only a new ID is checked: records enrolled before
// keep theirs.
func ValidateNewBackendID(id string) error {
	if !ValidBackendName(id) {
		return fmt.Errorf("backend ID %q must be a DNS label (lowercase letters, digits and '-', at most 63)", id)
	}
	if strings.HasPrefix(id, "zenthros-gateway") || id == "zenthros-controller" {
		return fmt.Errorf("backend ID %q is reserved for the Gateway's tailnet devices", id)
	}
	return nil
}

// BackendAccessSpec is the durable authorization record for one enrolled backend.
// +kubebuilder:validation:XValidation:rule="self.backendID == oldSelf.backendID",message="backendID is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.access.workspaces)",message="backend access requires at least one Workspace"
type BackendAccessSpec struct {
	// BackendID is the exact registered backend identifier.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	BackendID string `json:"backendID"`

	// Name is what the backend's Workspace calls it, from the token it
	// joined with (zenthros.create("local", name="mi-pc")); one per
	// Workspace. Empty: the backend has only its ID.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +optional
	Name string `json:"name,omitempty"`

	Access AccessPolicy `json:"access"`

	// BackendType is the physical backend kind (k8s, vm or container). Execution
	// routing is resolved by the Controller and is not persisted as a capability fact.
	// +kubebuilder:validation:Enum=k8s;vm;container
	BackendType string `json:"backendType,omitempty"`

	// Ownership prevents pool-owned Managed machines from bypassing their
	// reservation lease by appearing as direct existing-backend candidates.
	Ownership BackendOwnership `json:"ownership,omitempty"`

	// Selection is which jobs the backend takes: Any (empty) or Named, only
	// the jobs that name it. It comes from the token the backend enrolled
	// with and never loosens.
	// +kubebuilder:validation:Enum=Any;Named
	// +optional
	Selection BackendSelection `json:"selection,omitempty"`

	// Compute is operator-supplied, authoritative scheduler-visible capability
	// metadata. The registry publisher never mutates it.
	Compute BackendComputeProfile `json:"compute,omitempty"`

	// NodeSupply, on a Kubernetes backend, is which providers may supply
	// nodes to its cluster through joined VMPools. Absent: none.
	// +optional
	NodeSupply *NodeSupply `json:"nodeSupply,omitempty"`
}

// BackendComputeProfile describes schedulable capacity without agent-local state.
type BackendComputeProfile struct {
	Kind                    string `json:"kind,omitempty"`
	CPU                     string `json:"cpu,omitempty"`
	Memory                  string `json:"memory,omitempty"`
	GPUModel                string `json:"gpuModel,omitempty"`
	GPUCount                int32  `json:"gpuCount,omitempty"`
	Architecture            string `json:"architecture,omitempty"`
	Region                  string `json:"region,omitempty"`
	Zone                    string `json:"zone,omitempty"`
	PurchaseOption          string `json:"purchaseOption,omitempty"`
	EstimatedHourlyPrice    string `json:"estimatedHourlyPrice,omitempty"`
	EstimatedStartupSeconds int64  `json:"estimatedStartupSeconds,omitempty"`
}

// BackendAccessStatus is what the backend reports about itself.
type BackendAccessStatus struct {
	// NodeSupply is how the backend's cluster takes nodes, as its agent
	// last checked it.
	// +optional
	NodeSupply *ClusterNodeSupply `json:"nodeSupply,omitempty"`
}

// ClusterNodeSupply is a Kubernetes backend's cluster as new nodes see it.
// Its agent checks it every half minute: whether the cluster runs k3s or
// takes a bare kubelet, and, for a kubelet, that bootstrap tokens
// authenticate, a new node's certificate is approved, the CNI and
// kube-proxy DaemonSets tolerate the pool taint and the cluster DNS is
// known.
type ClusterNodeSupply struct {
	// ObservedAt is when the agent last checked. A check older than a few
	// minutes is no check: new machines wait for the next.
	ObservedAt metav1.Time `json:"observedAt"`
	// JoinMethod is how a node joins the cluster: K3s or Kubelet.
	// +kubebuilder:validation:Enum=Kubelet;K3s
	JoinMethod string `json:"joinMethod"`
	// ServerVersion is the cluster's Kubernetes version.
	// +kubebuilder:validation:MaxLength=128
	ServerVersion string `json:"serverVersion"`
	// +optional
	// +kubebuilder:validation:MaxItems=4
	// +kubebuilder:validation:items:MaxLength=64
	ClusterDNS []string `json:"clusterDNS,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// Conditions: NodeSupplyReady.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ba
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.spec.backendID`
// +kubebuilder:printcolumn:name="Access",type=string,JSONPath=`.spec.access.mode`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type BackendAccess struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackendAccessSpec   `json:"spec,omitempty"`
	Status BackendAccessStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type BackendAccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackendAccess `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BackendAccess{}, &BackendAccessList{})
}
