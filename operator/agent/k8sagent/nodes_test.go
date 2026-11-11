// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/internal/testutil"
)

// fakeHub is the gateway's NodeSupply service; calls records what was asked,
// in order.
type fakeHub struct {
	slots []*agentpb.NodeSlot
	err   error
	calls *[]string
	// issued and registered record IssueJoin and ReportRegistered when set.
	issued     *[]*agentpb.IssueJoinRequest
	issueErr   error
	registered *[]string
	// clusters records ReportCluster when set.
	clusters *[]*agentpb.ClusterReport
}

func (h fakeHub) ReportCluster(_ context.Context, req *agentpb.ClusterReport, _ ...grpc.CallOption) (*agentpb.ClusterReportAck, error) {
	if h.clusters != nil {
		*h.clusters = append(*h.clusters, req)
	}
	return &agentpb.ClusterReportAck{}, nil
}

func (h fakeHub) IssueJoin(_ context.Context, req *agentpb.IssueJoinRequest, _ ...grpc.CallOption) (*agentpb.IssueJoinResponse, error) {
	if h.issued != nil {
		*h.issued = append(*h.issued, req)
	}
	if h.issueErr != nil {
		return nil, h.issueErr
	}
	return &agentpb.IssueJoinResponse{}, nil
}

func (h fakeHub) ReportRegistered(_ context.Context, req *agentpb.ReportRegisteredRequest, _ ...grpc.CallOption) (*agentpb.ReportRegisteredResponse, error) {
	if h.registered != nil {
		*h.registered = append(*h.registered, req.GetProviderId())
	}
	return &agentpb.ReportRegisteredResponse{}, nil
}

func (h fakeHub) NodeSlots(context.Context, *agentpb.NodeSlotsRequest, ...grpc.CallOption) (*agentpb.NodeSlotsResponse, error) {
	if h.err != nil {
		return nil, h.err
	}
	return &agentpb.NodeSlotsResponse{Slots: h.slots}, nil
}

func (h fakeHub) ReportDrained(_ context.Context, req *agentpb.ReportDrainedRequest, _ ...grpc.CallOption) (*agentpb.ReportDrainedResponse, error) {
	*h.calls = append(*h.calls, "drained "+req.GetProviderId())
	return &agentpb.ReportDrainedResponse{}, nil
}

var sweepNow = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func zNode(name string, ready corev1.ConditionStatus, since time.Duration) *corev1.Node {
	return supplyNode(name, infrav1alpha1.NodeProviderID("z", name, "uid-"+k8stypes.UID(name)), ready, since)
}

func supplyNode(name, providerID string, ready corev1.ConditionStatus, since time.Duration) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: k8stypes.UID("uid-" + name), CreationTimestamp: metav1.NewTime(sweepNow.Add(-time.Hour))},
		Spec:       corev1.NodeSpec{ProviderID: providerID},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: ready, LastTransitionTime: metav1.NewTime(sweepNow.Add(-since)),
		}}},
	}
}

func podOn(name, node, ownerKind string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if ownerKind != "" {
		pod.OwnerReferences = []metav1.OwnerReference{{Kind: ownerKind, Name: "owner", APIVersion: "apps/v1", UID: "uid-owner"}}
	}
	return pod
}

func nodeClient(t *testing.T, funcs *interceptor.Funcs, objects ...client.Object) client.Client {
	t.Helper()
	scheme := testutil.Scheme(t, clientgoscheme.AddToScheme)
	if funcs == nil {
		funcs = &interceptor.Funcs{}
	}
	if funcs.Create == nil {
		funcs.Create = testutil.CreateWithUID
	}
	if funcs.Delete == nil {
		funcs.Delete = testutil.DeleteWithUIDPrecondition
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(testutil.WithUIDs(objects...)...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
		WithInterceptorFuncs(*funcs).Build()
}

func getNode(t *testing.T, c client.Client, name string) *corev1.Node {
	t.Helper()
	node := &corev1.Node{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		t.Fatal(err)
	}
	return node
}

// Only a Node with no machine behind it is deleted: by an agent that never
// saw its slot, as soon as it is NotReady; a Ready one naming no slot is only
// tainted. Without the hub nothing is touched.
func TestNodeSweepDeletesOnlySlotlessNotReady(t *testing.T) {
	quarantined := zNode("unknown-ready", corev1.ConditionTrue, time.Hour)
	recovered := zNode("live", corev1.ConditionTrue, time.Hour)
	recovered.Spec.Taints = []corev1.Taint{{Key: UnknownSlotTaint, Effect: corev1.TaintEffectNoSchedule}}
	c := nodeClient(t, nil,
		zNode("gone", corev1.ConditionUnknown, 2*time.Minute),
		zNode("just-lost", corev1.ConditionUnknown, 30*time.Second),
		quarantined, recovered,
		zNode("live-notready", corev1.ConditionFalse, time.Hour),
		// The dead machine of slot live, which a replacement took over: same
		// slot name, another UID.
		supplyNode("live-earlier", infrav1alpha1.NodeProviderID("z", "live", "uid-earlier"), corev1.ConditionUnknown, 2*time.Minute),
		supplyNode("other-provider", "aws:///us-east-1a/i-0abc", corev1.ConditionUnknown, time.Hour),
		supplyNode("no-provider", "", corev1.ConditionUnknown, time.Hour),
		// Only starts like a Zenthros machine's: no machine of its own to
		// be gone (final review of Ola J, the providerID by its owner).
		supplyNode("look-alike", infrav1alpha1.ProviderIDScheme+"someone-else", corev1.ConditionUnknown, time.Hour),
	)
	var calls []string
	hub := fakeHub{calls: &calls, slots: []*agentpb.NodeSlot{
		{ProviderId: infrav1alpha1.NodeProviderID("z", "live", "uid-live"), Pool: "z"},
		{ProviderId: infrav1alpha1.NodeProviderID("z", "live-notready", "uid-live-notready"), Pool: "z"},
	}}

	down := &NodeReconciler{Client: c, Hub: fakeHub{err: errors.New("unavailable"), calls: &calls}}
	if _, err := down.Sweep(context.Background()); err == nil {
		t.Fatal("a sweep without the hub succeeded")
	}
	if getNode(t, c, "gone") == nil {
		t.Fatal("a Node was deleted while the hub could not be asked")
	}

	r := &NodeReconciler{Client: c, Hub: hub}
	sweep, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(sweep.Deleted)
	if !slices.Equal(sweep.Deleted, []string{"gone", "just-lost", "live-earlier"}) {
		t.Fatalf("deleted %v, want [gone just-lost live-earlier]", sweep.Deleted)
	}
	for _, kept := range []string{"unknown-ready", "live", "live-notready", "other-provider", "no-provider", "look-alike"} {
		if getNode(t, c, kept) == nil {
			t.Errorf("%s was deleted", kept)
		}
	}
	if !hasTaint(getNode(t, c, "unknown-ready"), UnknownSlotTaint) {
		t.Error("a Ready Node naming no slot was not tainted")
	}
	if hasTaint(getNode(t, c, "live"), UnknownSlotTaint) {
		t.Error("a Node whose slot is back kept the unknown-slot taint")
	}
	if len(calls) != 0 {
		t.Fatalf("hub calls %v, want none", calls)
	}
}

// A retiring Node is cordoned and tainted, its pods evicted (DaemonSet and
// mirror pods stay), and only then reported drained; an eviction a
// PodDisruptionBudget refuses keeps it from being reported.
func TestRetireCordonsEvictsThenReportsDrained(t *testing.T) {
	providerID := infrav1alpha1.NodeProviderID("z", "n1", "uid-n1")
	mirror := podOn("static", "n1", "")
	mirror.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "x"}
	pdbHeld := true
	funcs := &interceptor.Funcs{SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
		if sub == "eviction" && pdbHeld {
			return apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10)
		}
		if sub == "eviction" {
			return c.Delete(ctx, obj)
		}
		return c.SubResource(sub).Create(ctx, obj, subObj, opts...)
	}}
	c := nodeClient(t, funcs, zNode("n1", corev1.ConditionTrue, time.Hour),
		podOn("app", "n1", "ReplicaSet"), podOn("cni", "n1", "DaemonSet"), mirror, podOn("elsewhere", "n2", "ReplicaSet"))
	var calls []string
	hub := fakeHub{calls: &calls, slots: []*agentpb.NodeSlot{{
		ProviderId: providerID, Pool: "z", RetireByUnix: sweepNow.Add(10 * time.Minute).Unix(), RetireReason: infrav1alpha1.RetireReasonPoolDeleted,
	}}}
	r := &NodeReconciler{Client: c, Hub: hub}

	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	node := getNode(t, c, "n1")
	if !node.Spec.Unschedulable || !hasTaint(node, RetiringTaint) {
		t.Fatalf("retiring node not cordoned and tainted: unschedulable=%v taints=%v", node.Spec.Unschedulable, node.Spec.Taints)
	}
	if len(calls) != 0 {
		t.Fatalf("reported drained while a PodDisruptionBudget holds a pod: %v", calls)
	}

	pdbHeld = false
	sweep, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sweep.Evicted != 1 || len(calls) != 0 {
		t.Fatalf("second sweep evicted %d, calls %v: want the app pod evicted, no report yet", sweep.Evicted, calls)
	}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(calls, []string{"drained " + providerID}) {
		t.Fatalf("calls %v, want one drained report after the eviction", calls)
	}
	for _, stays := range []string{"cni", "static", "elsewhere"} {
		if err := c.Get(context.Background(), k8stypes.NamespacedName{Namespace: "default", Name: stays}, &corev1.Pod{}); err != nil {
			t.Errorf("%s was evicted: %v", stays, err)
		}
	}
	// Reported once: a drained slot is not reported again.
	hub.slots[0].Drained = true
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 {
		t.Fatalf("drained reported %d times", len(calls))
	}
}

// A retiring Node is one Cluster Autoscaler must not count: externalgrpc has
// no HasInstance, so CA counts a node of a group as deleted only by its own
// ToBeDeleted taint. Without it, a Spot node whose replacement is slower than
// the notice counts toward the group while its pods wait, and CA grows the
// group past the replacement the hub already asked for. CA strips the taint
// from every node when it restarts; the next sweep puts it back.
func TestRetiringNodeCountsAsDeletedForClusterAutoscaler(t *testing.T) {
	providerID := infrav1alpha1.NodeProviderID("z", "n1", "uid-n1")
	c := nodeClient(t, nil, zNode("n1", corev1.ConditionTrue, time.Hour))
	var calls []string
	hub := fakeHub{calls: &calls, slots: []*agentpb.NodeSlot{{
		ProviderId: providerID, Pool: "z", RetireByUnix: sweepNow.Add(2 * time.Minute).Unix(), RetireReason: infrav1alpha1.RetireReasonSpotInterruption,
	}}}
	r := &NodeReconciler{Client: c, Hub: hub, Now: func() time.Time { return sweepNow }}
	marked := func() string {
		t.Helper()
		if _, err := r.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		node := getNode(t, c, "n1")
		for _, taint := range node.Spec.Taints {
			if taint.Key == ClusterAutoscalerDeletingTaint {
				if taint.Effect != corev1.TaintEffectNoSchedule || taint.Value != strconv.FormatInt(sweepNow.Unix(), 10) {
					t.Fatalf("CA taint %+v, want NoSchedule with the Unix time it was set", taint)
				}
				return taint.Value
			}
		}
		t.Fatalf("retiring node lacks %s: taints %v", ClusterAutoscalerDeletingTaint, node.Spec.Taints)
		return ""
	}
	marked()

	// CA starting again removes it.
	node := getNode(t, c, "n1")
	patch := client.MergeFrom(node.DeepCopy())
	node.Spec.Taints = slices.DeleteFunc(node.Spec.Taints, func(t corev1.Taint) bool { return t.Key == ClusterAutoscalerDeletingTaint })
	if err := c.Patch(context.Background(), node, patch); err != nil {
		t.Fatal(err)
	}
	marked()
	if taints := getNode(t, c, "n1").Spec.Taints; len(taints) != 2 {
		t.Fatalf("taints %v, want the retiring and CA's, once each", taints)
	}
}

// The hub removes a slot only once its machine is gone, so a slot the agent
// saw and then no longer sees takes its Node at once, still Ready (the
// control plane has not noticed the silent kubelet yet). A failed sweep, or a
// failed deletion, keeps that memory; a restarted agent has none and only
// taints a Ready Node it cannot place.
func TestNodeSweepDeletesTerminatedSlotsNodeAtOnce(t *testing.T) {
	failDelete := true
	c := nodeClient(t, &interceptor.Funcs{Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if failDelete {
			failDelete = false
			return apierrors.NewServiceUnavailable("apiserver busy")
		}
		return cl.Delete(ctx, obj, opts...)
	}},
		zNode("a", corev1.ConditionTrue, time.Hour), zNode("b", corev1.ConditionTrue, time.Hour))
	var calls []string
	slotOf := func(name string) *agentpb.NodeSlot {
		return &agentpb.NodeSlot{ProviderId: infrav1alpha1.NodeProviderID("z", name, "uid-"+k8stypes.UID(name)), Pool: "z"}
	}
	r := &NodeReconciler{Client: c, Hub: fakeHub{calls: &calls, slots: []*agentpb.NodeSlot{slotOf("a"), slotOf("b")}}}
	if sweep, err := r.Sweep(context.Background()); err != nil || len(sweep.Deleted) != 0 {
		t.Fatalf("both slots live: deleted %v, err %v", sweep.Deleted, err)
	}

	// Slot a is terminated. The hub is down for a sweep: nothing happens,
	// nothing is forgotten.
	r.Hub = fakeHub{calls: &calls, err: errors.New("unavailable")}
	if _, err := r.Sweep(context.Background()); err == nil {
		t.Fatal("a sweep without the hub succeeded")
	}
	r.Hub = fakeHub{calls: &calls, slots: []*agentpb.NodeSlot{slotOf("b")}}
	if _, err := r.Sweep(context.Background()); err == nil {
		t.Fatal("a sweep whose deletion failed succeeded")
	}
	if getNode(t, c, "a") == nil {
		t.Fatal("the failed deletion deleted the Node")
	}
	sweep, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sweep.Deleted, []string{"a"}) || getNode(t, c, "a") != nil || getNode(t, c, "b") == nil {
		t.Fatalf("deleted %v; want the Ready Node of the terminated slot a, and b kept", sweep.Deleted)
	}

	// A restarted agent never saw slot c: its Ready Node is only tainted.
	c2 := nodeClient(t, nil, zNode("c", corev1.ConditionTrue, time.Hour))
	restarted := &NodeReconciler{Client: c2, Hub: fakeHub{calls: &calls}}
	if sweep, err := restarted.Sweep(context.Background()); err != nil || len(sweep.Deleted) != 0 || !hasTaint(getNode(t, c2, "c"), UnknownSlotTaint) {
		t.Fatalf("restarted agent: deleted %v, err %v, tainted %v; want only the taint", sweep.Deleted, err, hasTaint(getNode(t, c2, "c"), UnknownSlotTaint))
	}
}

// spotFixture is a pool z with a noticed Node "old" (one pod, image
// nginx:1.25) whose replacement slot "new" may have a Node yet.
func spotFixture(t *testing.T, funcs *interceptor.Funcs, objects ...client.Object) (client.Client, *[]string, fakeHub, string) {
	t.Helper()
	oldID := infrav1alpha1.NodeProviderID("z", "old", "uid-old")
	app := podOn("app", "old", "ReplicaSet")
	app.UID = "uid-app"
	app.Spec.Containers = []corev1.Container{{Name: "app", Image: "nginx:1.25"}}
	app.Spec.TerminationGracePeriodSeconds = ptr.To[int64](30)
	c := nodeClient(t, funcs, append([]client.Object{zNode("old", corev1.ConditionTrue, time.Hour), app}, objects...)...)
	var calls []string
	hub := fakeHub{calls: &calls, slots: []*agentpb.NodeSlot{
		{ProviderId: oldID, Pool: "z", RetireByUnix: sweepNow.Add(110 * time.Second).Unix(), RetireReason: infrav1alpha1.RetireReasonSpotInterruption},
		{ProviderId: infrav1alpha1.NodeProviderID("z", "new", "uid-new"), Pool: "z"},
	}}
	return c, &calls, hub, oldID
}

func prepullsOf(t *testing.T, c client.Client) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.HasLabels{LabelPrepullFor}); err != nil {
		t.Fatal(err)
	}
	return pods.Items
}

// A Spot-noticed Node is cordoned at once, but its pods wait for their pool:
// first for the replacement's Node to be Ready, then for their image to be on
// it (a prepull pod pulls it, running nothing). Only then are they evicted,
// and once gone the Node is reported drained and the prepull removed.
func TestSpotHandoffWaitsForReplacementAndImageThenEvicts(t *testing.T) {
	evicted := 0
	funcs := &interceptor.Funcs{SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
		evicted++
		return c.Delete(ctx, obj)
	}}
	c, calls, hub, oldID := spotFixture(t, funcs)
	r := &NodeReconciler{Client: c, Hub: hub, Now: func() time.Time { return sweepNow }}

	sweep, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	old := getNode(t, c, "old")
	if !old.Spec.Unschedulable || !hasTaint(old, RetiringTaint) || evicted != 0 || !slices.Equal(sweep.Waiting, []string{"old"}) {
		t.Fatalf("no replacement Node yet: cordoned %v, evicted %d, waiting %v; want cordoned, nothing evicted",
			old.Spec.Unschedulable, evicted, sweep.Waiting)
	}

	// The replacement's Node is Ready, without the image: it is prepulled.
	if err := c.Create(context.Background(), zNode("new", corev1.ConditionTrue, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	prepulls := prepullsOf(t, c)
	if len(prepulls) != 1 || evicted != 0 {
		t.Fatalf("%d prepull pods, %d evicted; want one prepull and no eviction before the image is there", len(prepulls), evicted)
	}
	prepull := prepulls[0]
	if prepull.Namespace != "default" || prepull.Spec.NodeName != "new" || len(prepull.Spec.Containers) != 1 ||
		prepull.Spec.Containers[0].Image != "nginx:1.25" || prepull.Spec.Containers[0].Command[0] != prepullCommand {
		t.Fatalf("prepull pod = %+v", prepull.Spec)
	}

	// Still pulling: nothing moves.
	prepull.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "image-0",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}
	if err := c.Status().Update(context.Background(), &prepull); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sweep(context.Background()); err != nil || evicted != 0 {
		t.Fatalf("image still pulling: evicted %d, err %v; want nothing evicted", evicted, err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(&prepull), &prepull); err != nil {
		t.Fatal(err)
	}

	// Pulled (the noop command then fails to start, as meant).
	prepull.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "image-0", ImageID: "docker.io/library/nginx@sha256:abc",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "StartError", ExitCode: 128}}}}
	if err := c.Status().Update(context.Background(), &prepull); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if evicted != 1 || len(*calls) != 0 {
		t.Fatalf("image on the replacement: evicted %d, calls %v; want the pod evicted, not yet reported", evicted, *calls)
	}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(*calls, []string{"drained " + oldID}) {
		t.Fatalf("calls %v, want one drained report", *calls)
	}
	hub.slots[0].Drained = true
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if left := prepullsOf(t, c); len(left) != 0 {
		t.Fatalf("%d prepull pods left after the drain", len(left))
	}
}

// Without its replacement, a noticed Node's pods still go in time to stop
// cleanly: at the deadline less their grace (30 s) and HandoffMargin.
func TestSpotHandoffEvictsAtDeadlineWithoutReplacement(t *testing.T) {
	evicted := 0
	funcs := &interceptor.Funcs{SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
		evicted++
		return c.Delete(ctx, obj)
	}}
	c, _, hub, _ := spotFixture(t, funcs)
	now := sweepNow.Add(110*time.Second - 30*time.Second - HandoffMargin - time.Second)
	r := &NodeReconciler{Client: c, Hub: hub, Now: func() time.Time { return now }}
	if _, err := r.Sweep(context.Background()); err != nil || evicted != 0 {
		t.Fatalf("a second before the eviction deadline: evicted %d, err %v", evicted, err)
	}
	now = now.Add(time.Second)
	if _, err := r.Sweep(context.Background()); err != nil || evicted != 1 {
		t.Fatalf("at the eviction deadline: evicted %d, err %v; want the pod evicted without a replacement", evicted, err)
	}
}

// A replacement that already has the image needs no prepull, and a prepull
// the namespace refuses never holds the handoff.
func TestPrepullSkipsPresentImageAndRefusalDoesNotHold(t *testing.T) {
	withImage := zNode("new", corev1.ConditionTrue, 0)
	withImage.Status.Images = []corev1.ContainerImage{{Names: []string{"docker.io/library/nginx:1.25"}}}
	evicted := 0
	evict := func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
		evicted++
		return c.Delete(ctx, obj)
	}
	c, _, hub, _ := spotFixture(t, &interceptor.Funcs{SubResourceCreate: evict}, withImage)
	r := &NodeReconciler{Client: c, Hub: hub, Now: func() time.Time { return sweepNow }}
	if _, err := r.Sweep(context.Background()); err != nil || evicted != 1 || len(prepullsOf(t, c)) != 0 {
		t.Fatalf("image present: evicted %d, prepulls %d, err %v; want evicted at once, no prepull", evicted, len(prepullsOf(t, c)), err)
	}

	evicted = 0
	refuse := &interceptor.Funcs{SubResourceCreate: evict, Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if pod, ok := obj.(*corev1.Pod); ok && pod.Labels[LabelPrepullFor] != "" {
			return apierrors.NewForbidden(corev1.Resource("pods"), pod.Name, errors.New("exceeded quota"))
		}
		return cl.Create(ctx, obj, opts...)
	}}
	c, _, hub, _ = spotFixture(t, refuse, zNode("new", corev1.ConditionTrue, 0))
	r = &NodeReconciler{Client: c, Hub: hub, Now: func() time.Time { return sweepNow }}
	if _, err := r.Sweep(context.Background()); err != nil || evicted != 1 {
		t.Fatalf("prepull refused: evicted %d, err %v; want evicted at once", evicted, err)
	}
}

func TestEvictByMakesRoomForGrace(t *testing.T) {
	by := sweepNow
	pod := func(grace *int64) *corev1.Pod {
		return &corev1.Pod{Spec: corev1.PodSpec{TerminationGracePeriodSeconds: grace}}
	}
	for name, tc := range map[string]struct {
		pods []*corev1.Pod
		want time.Duration
	}{
		"default grace":   {[]*corev1.Pod{pod(nil)}, -40 * time.Second},
		"longest wins":    {[]*corev1.Pod{pod(ptr.To[int64](5)), pod(ptr.To[int64](45))}, -55 * time.Second},
		"capped":          {[]*corev1.Pod{pod(ptr.To[int64](600))}, -70 * time.Second},
		"no grace at all": {[]*corev1.Pod{pod(ptr.To[int64](0))}, -10 * time.Second},
	} {
		if got := evictBy(by, tc.pods).Sub(by); got != tc.want {
			t.Errorf("%s: evict %s before the deadline, want %s", name, -got, -tc.want)
		}
	}
}

func TestNormalizeImage(t *testing.T) {
	for in, want := range map[string]string{
		"nginx":                          "docker.io/library/nginx:latest",
		"nginx:1.25":                     "docker.io/library/nginx:1.25",
		"bitnami/redis:7":                "docker.io/bitnami/redis:7",
		"ghcr.io/org/app:v1":             "ghcr.io/org/app:v1",
		"localhost:5000/zenthros/agent":  "localhost:5000/zenthros/agent:latest",
		"registry.k8s.io/pause:3.10":     "registry.k8s.io/pause:3.10",
		"nginx:1.25@sha256:abc":          "docker.io/library/nginx@sha256:abc",
		"123.dkr.ecr.aws/app@sha256:def": "123.dkr.ecr.aws/app@sha256:def",
	} {
		if got := normalizeImage(in); got != want {
			t.Errorf("normalizeImage(%q) = %q, want %q", in, got, want)
		}
	}
}

// A pod that was evicted but is still terminating still runs on the Node: the
// Node is drained when it is gone, not when it was asked to go. Reporting
// drained then let the hub terminate the machine under a pod still saving
// its checkpoint in its grace period (K10/06#2).
func TestATerminatingPodHoldsTheDrain(t *testing.T) {
	providerID := infrav1alpha1.NodeProviderID("z", "n1", "uid-n1")
	app := podOn("app", "n1", "ReplicaSet")
	// The finalizer stands for the grace period: the eviction marks the pod
	// deleted and it stays until the kubelet says it stopped.
	app.Finalizers = []string{"test/grace"}
	funcs := &interceptor.Funcs{SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
		if sub == "eviction" {
			return c.Delete(ctx, obj)
		}
		return c.SubResource(sub).Create(ctx, obj, subObj, opts...)
	}}
	c := nodeClient(t, funcs, zNode("n1", corev1.ConditionTrue, time.Hour), app)
	var calls []string
	hub := fakeHub{calls: &calls, slots: []*agentpb.NodeSlot{{
		ProviderId: providerID, Pool: "z", RetireByUnix: sweepNow.Add(10 * time.Minute).Unix(), RetireReason: infrav1alpha1.RetireReasonPoolDeleted,
	}}}
	r := &NodeReconciler{Client: c, Hub: hub}
	for range 2 {
		if _, err := r.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(calls) != 0 {
		t.Fatalf("reported %v while the evicted pod is still terminating", calls)
	}

	var pod corev1.Pod
	if err := c.Get(context.Background(), k8stypes.NamespacedName{Namespace: "default", Name: "app"}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Finalizers = nil
	if err := c.Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(calls, []string{"drained " + providerID}) {
		t.Fatalf("calls %v, want one drained report once the pod is gone", calls)
	}
}

// A prepull is over once each image is pulled or will not be: a pull the
// kubelet gave up on (whatever its reason) or a container that cannot
// start, which a prepull's is made to be.
func TestAPrepullIsOverOnEveryPullFailure(t *testing.T) {
	waiting := func(reason string) *corev1.Pod {
		return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "image-0"}}},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "image-0",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}}}}}
	}
	for reason, over := range map[string]bool{
		"ErrImagePull": true, "ImagePullBackOff": true, "RegistryUnavailable": true,
		"CreateContainerError": true, "ContainerCreating": false,
	} {
		if got := prepullOver(waiting(reason)); got != over {
			t.Errorf("waiting %s: over %v, want %v", reason, got, over)
		}
	}
}
