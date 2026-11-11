// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// The cluster's agent owns what Zenthros does to the Nodes it supplies
// (docs/design/K8S_NODE_SUPPLY.md): the hub decides about machines and has no
// credentials for this cluster. Every sweep it asks the hub for the slots
// behind the cluster's zenthros:// Nodes (NodeSupply.NodeSlots) and
//
//   - drains a retiring slot's Node: cordon, the retiring taint, evictions
//     (PodDisruptionBudgets hold them), then ReportDrained once only
//     DaemonSet and mirror pods are left. The hub terminates the machine;
//   - deletes a Node whose machine is gone (Cluster Autoscaler scaled it
//     down, the pool was deleted, the machine was lost). The hub removes a
//     slot only once the provider confirmed its machine gone (the
//     CapacityRequest finalizer), so a slot this agent saw and no longer
//     sees takes its Node with it at once, Ready or not: waiting for the
//     control plane to notice the silent kubelet (node-monitor-grace-period)
//     added nothing. A Node whose slot it never saw (it restarted since) goes
//     once NotReady;
//   - taints a Ready Node that names no slot (a providerID no machine of
//     Zenthros claims) so nothing new lands on it.
//
// When the hub cannot be asked, nothing is touched.

// RetiringTaint is on a Node Zenthros is retiring.
const RetiringTaint = "infrastructure.zenthros.io/retiring"

// ClusterAutoscalerDeletingTaint is Cluster Autoscaler's own mark of a node
// on its way out; its value is the Unix time it was set. CA counts a node
// carrying it as deleted, not toward its group's size (externalgrpc has no
// HasInstance, so this taint is all CA goes by). A retiring Node carries it,
// so CA counts the replacement the hub asked for as upcoming instead of
// growing the group again for the pods the Node hands over. CA removes it
// from every node when it starts; the next sweep puts it back.
const ClusterAutoscalerDeletingTaint = "ToBeDeletedByClusterAutoscaler"

// UnknownSlotTaint is on a Ready Node whose zenthros:// providerID names no
// slot of this cluster.
const UnknownSlotTaint = "infrastructure.zenthros.io/unknown-slot"

// NodeReconciler reconciles the cluster's zenthros:// Nodes with the hub.
type NodeReconciler struct {
	Client client.Client
	Hub    agentpb.NodeSupplyClient
	Logger logr.Logger
	Now    func() time.Time

	// Join issues the node credentials the cluster's new machines ask for;
	// nil issues none.
	Join *JoinIssuer

	// seen are the providerIDs of the slots of the last sweep that
	// completed. Lost on restart, which only makes the agent wait for
	// NotReady.
	seen map[string]bool
	// Cluster checks the cluster as new nodes find it; Run reports it to
	// the hub, issueJoin takes how nodes join from it.
	Cluster *ClusterChecker
	// joinTokens are the bootstrap tokens issued per providerID, deleted
	// once its Node registered. Lost on restart: the tokens then expire.
	joinTokens map[string][]*corev1.Secret
}

// NodeSweep is what one sweep did.
type NodeSweep struct {
	Deleted, Cordoned, Quarantined, Drained []string
	Evicted                                 int
	// Waiting are retiring Nodes whose pods wait for their pool to be up.
	Waiting []string
}

// Run sweeps every interval, and reports the cluster every
// ClusterCheckInterval, until ctx ends.
func (r *NodeReconciler) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var reported time.Time
	for {
		if r.Cluster != nil && time.Since(reported) >= ClusterCheckInterval {
			if err := r.ReportCluster(ctx); err != nil && ctx.Err() == nil {
				r.Logger.Error(err, "cluster report failed")
			} else {
				reported = time.Now()
			}
		}
		if _, err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
			r.Logger.Error(err, "node supply sweep failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ReportCluster checks the cluster and tells the hub.
func (r *NodeReconciler) ReportCluster(ctx context.Context) error {
	check := r.Cluster.Check(ctx)
	if !check.Ready {
		r.Logger.Info("the cluster cannot take new nodes", "reason", check.Reason, "message", check.Message)
	}
	_, err := r.Hub.ReportCluster(ctx, check.Report())
	return err
}

// Sweep reconciles every zenthros:// Node once.
func (r *NodeReconciler) Sweep(ctx context.Context) (NodeSweep, error) {
	var sweep NodeSweep
	resp, err := r.Hub.NodeSlots(ctx, &agentpb.NodeSlotsRequest{})
	if err != nil {
		return sweep, err
	}
	slots := make(map[string]*agentpb.NodeSlot, len(resp.GetSlots()))
	for _, slot := range resp.GetSlots() {
		slots[slot.GetProviderId()] = slot
	}
	nodes := &corev1.NodeList{}
	if err := r.Client.List(ctx, nodes); err != nil {
		return sweep, err
	}
	byProviderID := make(map[string]*corev1.Node, len(nodes.Items))
	for i := range nodes.Items {
		byProviderID[nodes.Items[i].Spec.ProviderID] = &nodes.Items[i]
	}
	for _, slot := range resp.GetSlots() {
		if len(slot.GetJoinPublicKey()) == 0 || byProviderID[slot.GetProviderId()] != nil {
			continue
		}
		// One machine's credential failing does not hold up the others,
		// nor the drains below.
		if err := r.issueJoin(ctx, slot); err != nil {
			r.Logger.Error(err, "node credential not issued", "providerID", slot.GetProviderId())
		}
	}
	pools := poolsOf(resp.GetSlots(), byProviderID)
	draining := map[string]bool{}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		// A Zenthros machine's Node by its providerID's owner: a Node whose
		// providerID only starts like one is no Zenthros machine's, and the
		// sweep deletes the slotless ones.
		if _, _, _, ours := infrav1alpha1.ParseNodeProviderID(node.Spec.ProviderID); !ours || !node.DeletionTimestamp.IsZero() {
			continue
		}
		slot := slots[node.Spec.ProviderID]
		if slot != nil && !slot.GetRegistered() {
			if err := r.registered(ctx, node.Spec.ProviderID); err != nil {
				r.Logger.Error(err, "node registration not reported", "node", node.Name)
			}
		}
		switch {
		case slot == nil:
			if err := r.slotless(ctx, node, r.seen[node.Spec.ProviderID], &sweep); err != nil {
				return sweep, err
			}
		case slot.GetRetireByUnix() > 0:
			if !slot.GetDrained() {
				draining[node.Name] = true
			}
			if err := r.retire(ctx, node, slot, pools[slot.GetPool()], &sweep); err != nil {
				return sweep, err
			}
		default:
			if err := r.setTaint(ctx, node, UnknownSlotTaint, false); err != nil {
				return sweep, err
			}
		}
	}
	if err := r.deletePrepulls(ctx, draining); err != nil {
		return sweep, err
	}
	// A slot gone before its Node registered (its join failed, or its
	// machine went first) leaves no one to join with its tokens.
	for providerID, tokens := range r.joinTokens {
		if slots[providerID] != nil {
			continue
		}
		for _, token := range tokens {
			r.deleteJoinToken(ctx, token)
		}
		delete(r.joinTokens, providerID)
	}
	// Only a completed sweep moves the memory: a Node whose deletion failed
	// keeps its slot in it, and goes on the next sweep.
	r.seen = make(map[string]bool, len(slots))
	for providerID := range slots {
		r.seen[providerID] = true
	}
	return sweep, nil
}

// slotless handles a Node no machine of Zenthros stands behind any more:
// its slot went away under this agent's eyes (terminated), or the Node is
// NotReady. A Ready Node whose slot this agent never saw is only tainted: a
// providerID no machine of Zenthros claims.
func (r *NodeReconciler) slotless(ctx context.Context, node *corev1.Node, terminated bool, sweep *NodeSweep) error {
	if terminated || !ready(node) {
		if err := r.Client.Delete(ctx, node, client.Preconditions{UID: &node.UID}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		r.Logger.Info("deleted Node whose Zenthros machine is gone", "node", node.Name, "providerID", node.Spec.ProviderID)
		sweep.Deleted = append(sweep.Deleted, node.Name)
		return nil
	}
	if hasTaint(node, UnknownSlotTaint) {
		return nil
	}
	if err := r.setTaint(ctx, node, UnknownSlotTaint, true); err != nil {
		return err
	}
	r.Logger.Info("Ready Node names no Zenthros slot; tainted", "node", node.Name, "providerID", node.Spec.ProviderID)
	sweep.Quarantined = append(sweep.Quarantined, node.Name)
	return nil
}

// poolState is what a retiring Node's handoff needs of its pool.
type poolState struct {
	// up is whether every live (not retiring) slot has a Ready Node.
	up bool
	// ready are those Nodes: where the retiring Nodes' pods go.
	ready []*corev1.Node
}

func poolsOf(slots []*agentpb.NodeSlot, byProviderID map[string]*corev1.Node) map[string]*poolState {
	pools := map[string]*poolState{}
	for _, slot := range slots {
		state := pools[slot.GetPool()]
		if state == nil {
			state = &poolState{up: true}
			pools[slot.GetPool()] = state
		}
		if slot.GetRetireByUnix() > 0 {
			continue
		}
		node := byProviderID[slot.GetProviderId()]
		if node == nil || !ready(node) {
			state.up = false
			continue
		}
		if !node.Spec.Unschedulable { // cordoned by someone: up, but no pod goes there
			state.ready = append(state.ready, node)
		}
	}
	return pools
}

func (r *NodeReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// retire cordons a retiring slot's Node, hands its pods over (see handoff.go)
// and reports it drained once they are gone.
func (r *NodeReconciler) retire(ctx context.Context, node *corev1.Node, slot *agentpb.NodeSlot, pool *poolState, sweep *NodeSweep) error {
	if !node.Spec.Unschedulable || !hasTaint(node, RetiringTaint) || !hasTaint(node, ClusterAutoscalerDeletingTaint) {
		patch := client.MergeFrom(node.DeepCopy())
		node.Spec.Unschedulable = true
		if !hasTaint(node, RetiringTaint) {
			node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: RetiringTaint, Value: slot.GetRetireReason(), Effect: corev1.TaintEffectNoSchedule})
		}
		if !hasTaint(node, ClusterAutoscalerDeletingTaint) {
			node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: ClusterAutoscalerDeletingTaint,
				Value: strconv.FormatInt(r.now().Unix(), 10), Effect: corev1.TaintEffectNoSchedule})
		}
		if err := r.Client.Patch(ctx, node, patch); err != nil {
			return client.IgnoreNotFound(err)
		}
		r.Logger.Info("cordoned retiring Node", "node", node.Name, "reason", slot.GetRetireReason(),
			"retireBy", time.Unix(slot.GetRetireByUnix(), 0).UTC())
		sweep.Cordoned = append(sweep.Cordoned, node.Name)
	}
	if slot.GetDrained() {
		return nil
	}
	pods := &corev1.PodList{}
	if err := r.Client.List(ctx, pods, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return err
	}
	var moving []*corev1.Pod
	terminating := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		switch {
		case !evictable(pod):
		case pod.DeletionTimestamp.IsZero():
			moving = append(moving, pod)
		default:
			// Asked to go but still running its grace period: the Node is
			// not drained yet (K10).
			terminating++
		}
	}
	if len(moving) > 0 {
		if now, err := r.handOver(ctx, node, slot, pool, moving); err != nil || !now {
			if err == nil {
				sweep.Waiting = append(sweep.Waiting, node.Name)
			}
			return err
		}
	}
	for _, pod := range moving {
		eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}
		if err := r.Client.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
			// 429: a PodDisruptionBudget holds it; the next sweep asks again.
			if apierrors.IsNotFound(err) || apierrors.IsTooManyRequests(err) {
				continue
			}
			return err
		}
		sweep.Evicted++
	}
	if len(moving) > 0 {
		return nil
	}
	if terminating > 0 {
		sweep.Waiting = append(sweep.Waiting, node.Name)
		return nil
	}
	if _, err := r.Hub.ReportDrained(ctx, &agentpb.ReportDrainedRequest{ProviderId: node.Spec.ProviderID}); err != nil {
		return err
	}
	r.Logger.Info("retiring Node drained", "node", node.Name)
	sweep.Drained = append(sweep.Drained, node.Name)
	return nil
}

// handOver is whether a retiring Node's pods are evicted now: its pool is up
// and their images are on the pool's Ready Nodes, or the deadline leaves no
// more room to wait. The images are pulled meanwhile.
func (r *NodeReconciler) handOver(ctx context.Context, node *corev1.Node, slot *agentpb.NodeSlot, pool *poolState, pods []*corev1.Pod) (bool, error) {
	if pool == nil {
		pool = &poolState{up: true}
	}
	warm := false
	if pool.up {
		var err error
		if warm, err = r.prepull(ctx, node, pods, pool.ready); err != nil {
			return false, err
		}
	}
	if pool.up && warm {
		return true, nil
	}
	return !r.now().Before(evictBy(time.Unix(slot.GetRetireByUnix(), 0), pods)), nil
}

// evictable is a pod a drain must move: not a DaemonSet's (it goes with the
// node), not a static pod's mirror, not a prepull, not finished. One already
// going still counts until it is gone.
func evictable(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	if _, prepull := pod.Labels[LabelPrepullFor]; prepull {
		return false
	}
	if _, mirror := pod.Annotations[corev1.MirrorPodAnnotationKey]; mirror {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" {
			return false
		}
	}
	return true
}

// ready is whether the Node's kubelet reports Ready.
func ready(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func hasTaint(node *corev1.Node, key string) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == key {
			return true
		}
	}
	return false
}

// setTaint adds (NoSchedule) or removes the taint key.
func (r *NodeReconciler) setTaint(ctx context.Context, node *corev1.Node, key string, on bool) error {
	if hasTaint(node, key) == on {
		return nil
	}
	patch := client.MergeFrom(node.DeepCopy())
	if on {
		node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: key, Effect: corev1.TaintEffectNoSchedule})
	} else {
		kept := node.Spec.Taints[:0]
		for _, taint := range node.Spec.Taints {
			if taint.Key != key {
				kept = append(kept, taint)
			}
		}
		node.Spec.Taints = kept
	}
	return client.IgnoreNotFound(r.Client.Patch(ctx, node, patch))
}
