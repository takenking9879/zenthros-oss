// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Node supply: a VMPool whose machines join a Kubernetes cluster as nodes
// (docs/design/K8S_NODE_SUPPLY.md). The cluster's own autoscaler (Cluster
// Autoscaler, through the externalgrpc cloud provider) decides how many nodes
// and which one to remove; Zenthros only creates and destroys the machines.
const (
	// AnnotationDeleteRequested on a CapacityRequest of a joined pool marks a
	// node the cluster's autoscaler asked to delete (it has drained it). The
	// capacity manager removes such slots first, without waiting for idleTTL.
	AnnotationDeleteRequested = "infrastructure.zenthros.io/delete-requested"

	// AnnotationDeleteCounted on a joined VMPool lists the UIDs of the
	// delete-requested CapacityRequests whose removal spec.scaling.replicas
	// already accounts for. It is written in the same update as replicas, so
	// a retried DeleteNodes never subtracts a node twice and a lost reply
	// never leaves one unsubtracted. The capacity manager removes only
	// counted slots.
	AnnotationDeleteCounted = "infrastructure.zenthros.io/delete-counted"

	// ConditionJoinAccepted on a joined VMPool says whether Zenthros may
	// provision capacity of this pool for its cluster: the cluster's backend
	// is a Kubernetes backend whose nodeSupply allows the pool's provider.
	// It is not what a caller sees: NodeGroups also filters by the caller.
	ConditionJoinAccepted = "JoinAccepted"

	// ConditionNodeSupplyReady says whether the cluster can take new nodes
	// now, as its agent last checked it: on a BackendAccess, the cluster;
	// on a joined VMPool, the cluster for this pool (its join method too).
	// False holds back new machines only: nodes that joined stay.
	ConditionNodeSupplyReady = "NodeSupplyReady"

	// ProviderIDScheme is the scheme of a joined node's spec.providerID:
	// zenthros://<pool>/<capacityrequest>/<capacityrequest UID>.
	ProviderIDScheme = "zenthros://"

	// AnnotationRetireBy on a CapacityRequest of a joined pool marks a node
	// Zenthros retires without the cluster's autoscaler asking (its pool is
	// deleted, its provider revoked): RFC 3339, the time its machine is
	// terminated. Until then the cluster's agent cordons and drains the node
	// (evictions respect PodDisruptionBudgets). Set once; only a reclaim
	// moves it, and only sooner.
	AnnotationRetireBy = "infrastructure.zenthros.io/retire-by"
	// AnnotationRetireReason says why (a RetireReason*).
	AnnotationRetireReason = "infrastructure.zenthros.io/retire-reason"
	// AnnotationDrained is set (RFC 3339) when the cluster's agent reported
	// the retiring node drained: the machine may go before retire-by.
	AnnotationDrained = "infrastructure.zenthros.io/drained"
	// AnnotationRebalanceFor on a CapacityRequest of a joined pool names the
	// UID of the slot it replaces on a rebalance recommendation. Until that
	// slot retires, the replacement is one above the pool's target.
	AnnotationRebalanceFor = "infrastructure.zenthros.io/rebalance-for"
)

// Why a joined pool's node is retired.
const (
	RetireReasonPoolDeleted = "PoolDeleted"
	// RetireReasonSpotInterruption: the provider is reclaiming the machine
	// (a Spot notice). Its replacement is asked for at once; the Node drains
	// once the replacement is up, or in time before the provider acts.
	RetireReasonSpotInterruption = "SpotInterruption"
	RetireReasonJoinNotAccepted  = "JoinNotAccepted"
	// RetireReasonRebalanceRecommendation: the provider says the machine is
	// at elevated risk of interruption and the pool rebalances; its
	// replacement, in a capacity pool at no more risk, is already up.
	RetireReasonRebalanceRecommendation = "RebalanceRecommendation"
)

// Events on a joined VMPool (kubectl describe vmpool): what Cluster
// Autoscaler asked of it and what became of its machines.
const (
	// EventReasonScaledUpByCA: IncreaseSize raised the pool's target.
	EventReasonScaledUpByCA = "ScaledUpByCA"
	// EventReasonDeleteRequested: DeleteNodes took nodes CA drained.
	EventReasonDeleteRequested = "DeleteRequested"
	// EventReasonRetiring: Zenthros drains and retires a node of the pool.
	EventReasonRetiring = "Retiring"
	// EventReasonJoinFailed: a machine of the pool did not become a node.
	EventReasonJoinFailed = "JoinFailed"
)

// SlotPool is a reference to the slot's pool, enough to record an event
// on it.
func SlotPool(request *CapacityRequest) *VMPool {
	return &VMPool{ObjectMeta: metav1.ObjectMeta{Name: request.Labels[LabelVMPoolName], UID: types.UID(request.Labels[LabelVMPoolUID])}}
}

// RetireBy is when the slot's machine goes, if Zenthros is retiring it.
func RetireBy(request *CapacityRequest) (time.Time, bool) {
	raw, ok := request.Annotations[AnnotationRetireBy]
	if !ok {
		return time.Time{}, false
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		// Unreadable is retiring now: the annotation is ours, and a slot
		// marked for retirement never returns to service.
		return time.Time{}, true
	}
	return at, true
}

// Drained reports whether the cluster's agent drained the retiring slot's node.
func Drained(request *CapacityRequest) bool {
	_, ok := request.Annotations[AnnotationDrained]
	return ok
}

// Reasons of ConditionJoinAccepted.
const (
	JoinReasonAccepted           = "Accepted"
	JoinReasonBackendNotFound    = "BackendNotFound"
	JoinReasonBackendNotK8s      = "BackendNotKubernetes"
	JoinReasonProviderNotAllowed = "ProviderNotAllowed"
	JoinReasonPoolNotJoinable    = "PoolNotJoinable"
)

// Reasons of ConditionNodeSupplyReady besides those the cluster's agent
// reports (NodeSupplyReason*).
const (
	NodeSupplyReasonReady = "Ready"
	// NodeSupplyReasonNotReported: the cluster's agent has not checked its
	// cluster yet, or its last check is too old.
	NodeSupplyReasonNotReported = "NotReported"
	// NodeSupplyReasonJoinMethodMismatch: the pool asks for a join method
	// the cluster does not run.
	NodeSupplyReasonJoinMethodMismatch = "JoinMethodMismatch"
)

// Reasons the cluster's agent reports a cluster that cannot take new nodes.
const (
	NodeSupplyReasonBootstrapTokensDisabled  = "BootstrapTokensDisabled"
	NodeSupplyReasonBootstrapBindingsMissing = "BootstrapBindingsMissing"
	NodeSupplyReasonNoNetworkDaemonSet       = "NoNetworkDaemonSet"
	NodeSupplyReasonNetworkUntolerated       = "NetworkDaemonSetUntolerated"
	NodeSupplyReasonNoClusterDNS             = "NoClusterDNS"
	NodeSupplyReasonCheckFailed              = "CheckFailed"
)

// Join methods (KubernetesJoin.JoinMethod; ClusterNodeSupply.JoinMethod is
// Kubelet or K3s).
const (
	JoinMethodAuto    = "Auto"
	JoinMethodKubelet = "Kubelet"
	JoinMethodK3s     = "K3s"
)

// A joined pool's machine holds no cluster credential until it has enrolled:
// then, with its machine certificate, it asks the gateway (NodeJoin.Credential)
// with a key of its own. The cluster's agent sees the request among its
// slots, issues a bootstrap token for that one node (15 minutes) and seals it
// to the machine's key; the hub only carries the sealed credential and never
// holds a token it could use. The cluster's agent then reports the Node
// registered.
type NodeJoinStatus struct {
	// PublicKey is the machine's X25519 key the credential is sealed to.
	// +kubebuilder:validation:MaxLength=64
	PublicKey []byte `json:"publicKey,omitempty"`
	// RequestedAt is when the machine asked with PublicKey.
	RequestedAt metav1.Time `json:"requestedAt"`
	// Sealed is the credential the cluster's agent sealed to PublicKey.
	// +optional
	// +kubebuilder:validation:MaxLength=8192
	Sealed []byte `json:"sealed,omitempty"`
	// IssuedAt is when the cluster's agent issued Sealed.
	// +optional
	IssuedAt *metav1.Time `json:"issuedAt,omitempty"`
	// ServerVersion is the cluster's Kubernetes version as its agent read it
	// when issuing: the node's kubelet must not be newer.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	ServerVersion string `json:"serverVersion,omitempty"`
	// JoinMethod is how the cluster takes nodes, as its agent issued for it:
	// Kubelet or K3s.
	// +optional
	// +kubebuilder:validation:Enum=Kubelet;K3s
	JoinMethod string `json:"joinMethod,omitempty"`
	// ClusterDNS and ClusterDomain are the cluster's DNS, for a kubelet.
	// +optional
	// +kubebuilder:validation:MaxItems=4
	// +kubebuilder:validation:items:MaxLength=64
	ClusterDNS []string `json:"clusterDNS,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// RegisteredAt is when the cluster's agent first saw the machine's Node.
	// +optional
	RegisteredAt *metav1.Time `json:"registeredAt,omitempty"`
	// FailureReason says why the machine cannot join (a NodeJoinReason*);
	// the slot fails with it.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	FailureReason string `json:"failureReason,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	FailureMessage string `json:"failureMessage,omitempty"`
	// FailedAt is when FailureReason was recorded.
	// +optional
	FailedAt *metav1.Time `json:"failedAt,omitempty"`
}

// Why a joined pool's machine did not become a node. The slot fails with the
// reason; the cluster's autoscaler sees the error and backs the pool off.
const (
	// NodeJoinReasonUnreachable: the machine cannot reach the cluster's API
	// (the cluster and its nodes must share a network).
	NodeJoinReasonUnreachable = "JoinUnreachable"
	// NodeJoinReasonRejected: the cluster refused the credential.
	NodeJoinReasonRejected = "JoinRejected"
	// NodeJoinReasonVersionUnsupported: no pinned node version for the
	// cluster's minor.
	NodeJoinReasonVersionUnsupported = "VersionUnsupported"
	// NodeJoinReasonTimeout: no Node registered within the join timeout.
	NodeJoinReasonTimeout = "JoinTimeout"
	// NodeJoinReasonMethodUnsupported: the pool asks for a join method its
	// cluster does not run, or none is pinned for it.
	NodeJoinReasonMethodUnsupported = "JoinMethodUnsupported"
)

// JoinFailed reports a slot whose machine cannot become a node.
func JoinFailed(request *CapacityRequest) bool {
	return request.Status.NodeJoin != nil && request.Status.NodeJoin.FailureReason != ""
}

// NodeRegistered reports whether the cluster's agent saw the slot's Node.
func NodeRegistered(request *CapacityRequest) bool {
	return request.Status.NodeJoin != nil && request.Status.NodeJoin.RegisteredAt != nil
}

// JoinSpec hands a pool's machines to a cluster as nodes. A joined pool serves
// no ZenthrosJob directly: its machines are capacity for the cluster.
type JoinSpec struct {
	Kubernetes KubernetesJoin `json:"kubernetes"`
}

// KubernetesJoin is the cluster a joined pool's machines become nodes of.
type KubernetesJoin struct {
	// BackendID is the enrolled Kubernetes backend (its agent's identity)
	// whose cluster the machines join. Its BackendAccess must allow the
	// pool's provider in spec.nodeSupply.providers.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	BackendID string `json:"backendID"`

	// JoinMethod is how the machines join. Auto (the default) takes the
	// cluster's: k3s' agent on a k3s cluster, Kubernetes' kubelet with TLS
	// bootstrapping on any other. Kubelet or K3s requires that one: on a
	// cluster that runs the other, the pool gets no machines.
	// +optional
	// +kubebuilder:default=Auto
	// +kubebuilder:validation:Enum=Auto;Kubelet;K3s
	JoinMethod string `json:"joinMethod,omitempty"`

	// NodeLabels are set on every node, next to the pool label.
	// +optional
	// +kubebuilder:validation:MaxProperties=16
	NodeLabels map[string]string `json:"nodeLabels,omitempty"`

	// NodeTaints are set on every node, next to the pool taint
	// (infrastructure.zenthros.io/vmpool=<pool>:NoSchedule).
	// +optional
	// +kubebuilder:validation:MaxItems=8
	NodeTaints []NodeTaint `json:"nodeTaints,omitempty"`
}

// NodeTaint is a taint a joined node registers with.
type NodeTaint struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9./_-]+$`
	Key string `json:"key"`
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._-]*$`
	Value string `json:"value,omitempty"`
	// +kubebuilder:validation:Enum=NoSchedule;PreferNoSchedule;NoExecute
	Effect string `json:"effect"`
}

// NodeSupply is which providers may supply nodes to a Kubernetes backend's
// cluster. Absent: none (a joined pool naming the backend is not accepted).
type NodeSupply struct {
	// Providers are provider names (VMPool spec.providerRef.name).
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MinLength=1
	// +listType=set
	Providers []string `json:"providers"`
	// MaxNodes caps the cluster's joined nodes: the sum of the targets of
	// all its joined pools. Cluster Autoscaler's IncreaseSize past it is
	// refused (ResourceExhausted) and nothing is written. Unset, the pools'
	// maxReady are the only cap.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxNodes *int32 `json:"maxNodes,omitempty"`
}

// Allows reports whether provider may supply nodes.
func (s *NodeSupply) Allows(provider string) bool {
	if s == nil || provider == "" {
		return false
	}
	for _, p := range s.Providers {
		if p == provider {
			return true
		}
	}
	return false
}

// DeleteCounted is the set of AnnotationDeleteCounted UIDs on the pool.
func (p *VMPool) DeleteCounted() map[string]bool {
	counted := map[string]bool{}
	for _, uid := range strings.Split(p.Annotations[AnnotationDeleteCounted], ",") {
		if uid = strings.TrimSpace(uid); uid != "" {
			counted[uid] = true
		}
	}
	return counted
}

// SetDeleteCounted writes the set as AnnotationDeleteCounted, sorted; an empty
// set removes the annotation.
func (p *VMPool) SetDeleteCounted(counted map[string]bool) {
	uids := make([]string, 0, len(counted))
	for uid, ok := range counted {
		if ok {
			uids = append(uids, uid)
		}
	}
	if len(uids) == 0 {
		delete(p.Annotations, AnnotationDeleteCounted)
		return
	}
	sort.Strings(uids)
	if p.Annotations == nil {
		p.Annotations = map[string]string{}
	}
	p.Annotations[AnnotationDeleteCounted] = strings.Join(uids, ",")
}

// Joined reports whether the pool's machines join a Kubernetes cluster.
func (p *VMPool) Joined() bool {
	return p != nil && p.Spec.Join != nil
}

// JoinedNodeLabels are the labels a joined pool's node registers with, and so
// the labels Cluster Autoscaler's template node for the pool carries: the
// pool's nodeLabels, then the pool label, its one instance type and its
// placement (labels the kubelet may set on its own Node). The kubelet adds
// hostname, arch and os itself. One function for both keeps the template and
// the real node from disagreeing: Cluster Autoscaler scales up for a pod only
// if it fits the template, and the pod then has to fit the node.
func (p *VMPool) JoinedNodeLabels() map[string]string {
	labels := map[string]string{}
	if !p.Joined() {
		return labels
	}
	for k, v := range p.Spec.Join.Kubernetes.NodeLabels {
		labels[k] = v
	}
	labels[LabelVMPoolName] = p.Name
	if types := p.Spec.InstancePolicy.AllowedTypes; len(types) == 1 {
		labels[corev1.LabelInstanceTypeStable] = types[0]
	}
	if region := p.Spec.PlacementRef.Region; region != "" {
		labels[corev1.LabelTopologyRegion] = region
	}
	if zone := p.Spec.PlacementRef.Zone; zone != "" {
		labels[corev1.LabelTopologyZone] = zone
	}
	return labels
}

// NodeProviderID is the spec.providerID of the node a joined pool's machine
// registers: its pool, its CapacityRequest and that request's UID. A slot's
// name comes back when its machine is replaced (slot 0 is always slot 0); the
// UID does not, so a dead machine's Node never passes for its replacement.
func NodeProviderID(pool, request string, uid types.UID) string {
	return ProviderIDScheme + pool + "/" + request + "/" + string(uid)
}

// SlotProviderID is the providerID of request's machine.
func SlotProviderID(request *CapacityRequest) string {
	return NodeProviderID(request.Spec.VMPoolRef, request.Name, request.UID)
}

// ParseNodeProviderID splits a joined node's providerID into its pool,
// CapacityRequest name and UID; ok is false for any other providerID. The
// caller matches the UID too: the name alone may be a later machine's.
func ParseNodeProviderID(providerID string) (pool, request string, uid types.UID, ok bool) {
	rest, found := strings.CutPrefix(providerID, ProviderIDScheme)
	if !found {
		return "", "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], types.UID(parts[2]), true
}

// JoinedNodeName is the Node name request's machine registers with: the
// request's name and the start of its UID, unique per machine like the
// providerID. A replacement under a reused slot name registers a new Node
// instead of taking over the dead machine's (k3s refuses that: the node
// password differs).
func JoinedNodeName(request *CapacityRequest) string {
	uid := strings.ReplaceAll(string(request.UID), "-", "")
	return request.Name + "-" + uid[:min(len(uid), 8)]
}

// PoolTaint is the taint every node of a joined pool carries, so only pods
// that tolerate the pool land there.
func PoolTaint(pool string) NodeTaint {
	return NodeTaint{Key: LabelVMPoolName, Value: pool, Effect: "NoSchedule"}
}

// String renders a taint as kubelet's --register-with-taints wants it.
func (t NodeTaint) String() string {
	return fmt.Sprintf("%s=%s:%s", t.Key, t.Value, t.Effect)
}
