// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/zenthros/operator/pkg/capability"
)

// gpuResource is the GPUs a node offers through the NVIDIA device plugin.
const gpuResource corev1.ResourceName = "nvidia.com/gpu"

const (
	// listPage bounds one LIST: a shared cluster's pods were one answer,
	// held whole in the agent every minute.
	listPage = 500
	// What is left of a node is declared in these steps, rounded down: a pod
	// that started or ended anywhere changed the declaration, so every
	// heartbeat carried the whole host and the hub rewrote the directory.
	// Both divide what targets() calls room (2 CPUs, 4 GiB).
	freeMilliCPUStep = 500
	freeMemoryStep   = 512 << 20
)

// listNodes is the cluster's nodes a job can land on, for the declaration's
// host (targets(): a cluster by node, never its sum). Both LISTs are read a
// page at a time, and of the pods only each node's sum is kept.
func listNodes(clientset kubernetes.Interface) func(ctx context.Context) ([]capability.Node, error) {
	return func(ctx context.Context) ([]capability.Node, error) {
		var nodes []corev1.Node
		for options := (metav1.ListOptions{Limit: listPage}); ; {
			page, err := clientset.CoreV1().Nodes().List(ctx, options)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, page.Items...)
			if options.Continue = page.Continue; options.Continue == "" {
				break
			}
		}
		requested := nodeRequests{}
		for options := (metav1.ListOptions{Limit: listPage, FieldSelector: "status.phase!=Succeeded,status.phase!=Failed"}); ; {
			page, err := clientset.CoreV1().Pods("").List(ctx, options)
			if err != nil {
				return nil, err
			}
			requested.add(page.Items)
			if options.Continue = page.Continue; options.Continue == "" {
				break
			}
		}
		return requested.left(nodes), nil
	}
}

type asked struct{ cpu, memory int64 }

// nodeRequests is the CPU and memory the pods that have not ended ask of
// each node.
type nodeRequests map[string]asked

func (r nodeRequests) add(pods []corev1.Pod) {
	for i := range pods {
		pod := &pods[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		cpu, memory := podRequests(pod)
		sum := r[pod.Spec.NodeName]
		r[pod.Spec.NodeName] = asked{sum.cpu + cpu, sum.memory + memory}
	}
}

func (r nodeRequests) left(nodes []corev1.Node) []capability.Node {
	requested := r
	var out []capability.Node
	for i := range nodes {
		node := &nodes[i]
		if !takesJobs(node) {
			continue
		}
		allocatable := node.Status.Allocatable
		cpu := allocatable.Cpu().MilliValue()
		memory := allocatable.Memory().Value()
		gpus := allocatable[gpuResource]
		used := requested[node.Name]
		out = append(out, capability.Node{
			Name: node.Name, MilliCPU: cpu, MemoryBytes: memory, GPUCount: int32(gpus.Value()),
			FreeMilliCPU:    roundDown(max(cpu-used.cpu, 0), freeMilliCPUStep),
			FreeMemoryBytes: roundDown(max(memory-used.memory, 0), freeMemoryStep),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// takesJobs reports a node the scheduler would place a new pod on: Ready,
// not cordoned, and no taint that keeps a pod without tolerations away.
func takesJobs(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// podRequests is a pod's CPU (thousandths) and memory requests as the
// scheduler counts them: the larger of its containers' sum and its largest
// init container, plus its overhead.
func podRequests(pod *corev1.Pod) (cpu, memory int64) {
	for _, c := range pod.Spec.Containers {
		cpu += quantity(c.Resources.Requests, corev1.ResourceCPU, true)
		memory += quantity(c.Resources.Requests, corev1.ResourceMemory, false)
	}
	for _, c := range pod.Spec.InitContainers {
		cpu = max(cpu, quantity(c.Resources.Requests, corev1.ResourceCPU, true))
		memory = max(memory, quantity(c.Resources.Requests, corev1.ResourceMemory, false))
	}
	cpu += quantity(pod.Spec.Overhead, corev1.ResourceCPU, true)
	memory += quantity(pod.Spec.Overhead, corev1.ResourceMemory, false)
	return cpu, memory
}

func quantity(list corev1.ResourceList, name corev1.ResourceName, milli bool) int64 {
	q, ok := list[name]
	if !ok {
		return 0
	}
	if milli {
		return q.MilliValue()
	}
	return q.Value()
}

func roundDown(value, step int64) int64 { return value - value%step }
