// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/zenthros/operator/pkg/capability"
)

func capacityNode(name, cpu, memory string, mutate ...func(*corev1.Node)) corev1.Node {
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory)},
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	for _, m := range mutate {
		m(&node)
	}
	return node
}

func requestingPod(node string, phase corev1.PodPhase, containers []corev1.Container, init ...corev1.Container) corev1.Pod {
	return corev1.Pod{
		Spec:   corev1.PodSpec{NodeName: node, Containers: containers, InitContainers: init},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func asking(cpu, memory string) corev1.Container {
	requests := corev1.ResourceList{}
	if cpu != "" {
		requests[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory != "" {
		requests[corev1.ResourceMemory] = resource.MustParse(memory)
	}
	return corev1.Container{Resources: corev1.ResourceRequirements{Requests: requests}}
}

// targets() shows a cluster by node, never its sum: a job runs on one node.
// The agent reports the nodes a job can land on (Ready, not cordoned, no
// NoSchedule or NoExecute taint) with what each has (allocatable) and what
// is left of it: allocatable minus the requests of the pods on it that have
// not ended, as the scheduler counts them (the larger of the containers' sum
// and the largest init container, plus the pod's overhead).
func TestTheNodesAJobCanLandOnAndWhatIsLeftOfThem(t *testing.T) {
	nodes := []corev1.Node{
		capacityNode("big", "8", "32Gi", func(n *corev1.Node) {
			n.Status.Allocatable["nvidia.com/gpu"] = resource.MustParse("2")
		}),
		capacityNode("small", "4", "16Gi"),
		capacityNode("cordoned", "16", "64Gi", func(n *corev1.Node) { n.Spec.Unschedulable = true }),
		capacityNode("down", "16", "64Gi", func(n *corev1.Node) {
			n.Status.Conditions[0].Status = corev1.ConditionFalse
		}),
		capacityNode("tainted", "16", "64Gi", func(n *corev1.Node) {
			n.Spec.Taints = []corev1.Taint{{Key: "zenthros.ai/retiring", Effect: corev1.TaintEffectNoSchedule}}
		}),
		capacityNode("evicting", "16", "64Gi", func(n *corev1.Node) {
			n.Spec.Taints = []corev1.Taint{{Key: "node.kubernetes.io/unreachable", Effect: corev1.TaintEffectNoExecute}}
		}),
		capacityNode("preferred", "2", "8Gi", func(n *corev1.Node) {
			n.Spec.Taints = []corev1.Taint{{Key: "spot", Effect: corev1.TaintEffectPreferNoSchedule}}
		}),
	}
	overhead := requestingPod("big", corev1.PodRunning, []corev1.Container{asking("500m", "1Gi")})
	overhead.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")}
	pods := []corev1.Pod{
		requestingPod("big", corev1.PodRunning, []corev1.Container{asking("1", "2Gi"), asking("500m", "")}),
		// An init container larger than the containers sets the request.
		requestingPod("big", corev1.PodPending, []corev1.Container{asking("250m", "1Gi")}, asking("2", "1Gi")),
		overhead,
		requestingPod("big", corev1.PodSucceeded, []corev1.Container{asking("4", "16Gi")}),
		requestingPod("big", corev1.PodFailed, []corev1.Container{asking("4", "16Gi")}),
		requestingPod("small", corev1.PodRunning, []corev1.Container{asking("", "")}),
		requestingPod("", corev1.PodPending, []corev1.Container{asking("4", "16Gi")}),
		requestingPod("small", corev1.PodRunning, []corev1.Container{asking("6", "20Gi")}),
	}
	gib := int64(1 << 30)
	want := []capability.Node{
		{Name: "big", MilliCPU: 8000, MemoryBytes: 32 * gib, GPUCount: 2,
			// 3,750m left, declared in steps of 500m.
			FreeMilliCPU: 3500, FreeMemoryBytes: 32*gib - 2*gib - gib - gib - gib/2},
		{Name: "preferred", MilliCPU: 2000, MemoryBytes: 8 * gib, FreeMilliCPU: 2000, FreeMemoryBytes: 8 * gib},
		// More asked than it has: nothing left, never less.
		{Name: "small", MilliCPU: 4000, MemoryBytes: 16 * gib},
	}
	if got := schedulableNodes(nodes, pods); !reflect.DeepEqual(got, want) {
		t.Fatalf("nodes =\n%+v\nwant\n%+v", got, want)
	}
}

// The agent reads the nodes and the pods of every namespace: what is left
// of a node counts every pod on it, not only the agent's.
func TestListNodesCountsThePodsOfEveryNamespace(t *testing.T) {
	big := capacityNode("big", "8", "32Gi")
	theirs := requestingPod("big", corev1.PodRunning, []corev1.Container{asking("1", "2Gi")})
	theirs.ObjectMeta = metav1.ObjectMeta{Name: "theirs", Namespace: "team-a"}
	ours := requestingPod("big", corev1.PodRunning, []corev1.Container{asking("2", "4Gi")})
	ours.ObjectMeta = metav1.ObjectMeta{Name: "ours", Namespace: "zenthros-jobs"}
	got, err := listNodes(fake.NewSimpleClientset(&big, &theirs, &ours))(context.Background())
	gib := int64(1 << 30)
	want := []capability.Node{{Name: "big", MilliCPU: 8000, MemoryBytes: 32 * gib, FreeMilliCPU: 5000, FreeMemoryBytes: 26 * gib}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("listNodes = %+v, %v; want %+v", got, err, want)
	}
}

// What is left of a node is declared in steps: every pod that started or
// ended changed the declaration's hash, so each heartbeat carried the whole
// host and the hub rewrote the directory. A
// change within a step changes nothing; the steps divide targets()' room.
func TestSmallChurnLeavesTheDeclarationAlone(t *testing.T) {
	node := capacityNode("big", "8", "32Gi")
	// 6,800m and 30,968 MiB left: within the 6,500m and 30,720 MiB steps.
	base := []corev1.Pod{requestingPod("big", corev1.PodRunning, []corev1.Container{asking("1200m", "1800Mi")})}
	more := append(append([]corev1.Pod{}, base...), requestingPod("big", corev1.PodRunning, []corev1.Container{asking("100m", "64Mi")}))
	if a, b := schedulableNodes([]corev1.Node{node}, base), schedulableNodes([]corev1.Node{node}, more); !reflect.DeepEqual(a, b) {
		t.Fatalf("a 100m pod changed the declaration: %+v -> %+v", a, b)
	}
	// pkg/api's nodeRoomMilliCPU and nodeRoomMemory.
	if 2000%freeMilliCPUStep != 0 || (4<<30)%freeMemoryStep != 0 {
		t.Fatal("the steps do not divide what targets() calls room")
	}
}

// Both LISTs are read a page at a time and every page is counted: one
// answer held a shared cluster's every pod.
func TestListNodesReadsPages(t *testing.T) {
	big := capacityNode("big", "8", "32Gi")
	clientset := fake.NewSimpleClientset(&big)
	var limits []int64
	page := 0
	clientset.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		options := action.(k8stesting.ListActionImpl).ListOptions
		limits = append(limits, options.Limit)
		page++
		pod := requestingPod("big", corev1.PodRunning, []corev1.Container{asking("1", "1Gi")})
		list := &corev1.PodList{Items: []corev1.Pod{pod}}
		if page == 1 {
			list.Continue = "next"
		} else if options.Continue != "next" {
			t.Errorf("page %d asked with continue %q", page, options.Continue)
		}
		return true, list, nil
	})
	got, err := listNodes(clientset)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(limits) != 2 || limits[0] != listPage || limits[1] != listPage {
		t.Fatalf("pod LISTs with limits %v, want two pages of %d", limits, listPage)
	}
	if len(got) != 1 || got[0].FreeMilliCPU != 6000 {
		t.Fatalf("both pages were not counted: %+v", got)
	}
}

// schedulableNodes is what a refresh declares from nodes and pods (listNodes):
// the nodes a job can land on are the nodes a job can land on (Ready, not cordoned, no
// NoSchedule or NoExecute taint), by name, with their allocatable CPU,
// memory and GPUs and what is left of CPU and memory after the requests of
// the pods on them that have not ended, in steps (freeMilliCPUStep,
// freeMemoryStep).
func schedulableNodes(nodes []corev1.Node, pods []corev1.Pod) []capability.Node {
	requested := nodeRequests{}
	requested.add(pods)
	return requested.left(nodes)
}
