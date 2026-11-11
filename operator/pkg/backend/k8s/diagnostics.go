// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/pkg/backend/k8spod"
)

// RayJobPods returns the RayJob's submitter pod plus its RayCluster head/worker pods,
// deduplicated by name, each carrying the RayJob's job ID: a submitter Job of the
// same name left by an earlier run labels its pods job-name=<name> too (N12).
// Existing-cluster jobs (ClusterSelector, no RayClusterSpec) do not include the
// shared cluster's pods so a neighbor's image-pull cannot fail this job.
func RayJobPods(ctx context.Context, cl client.Client, rayJob *rayv1.RayJob) []corev1.Pod {
	byName := make(map[string]corev1.Pod)
	list := func(opts ...client.ListOption) {
		var pods corev1.PodList
		if err := cl.List(ctx, &pods, opts...); err != nil {
			return
		}
		for _, pod := range pods.Items {
			byName[pod.Name] = pod
		}
	}

	ns := client.InNamespace(rayJob.Namespace)
	id := rayJob.Labels[k8spod.LabelJobID]
	list(ns, client.MatchingLabels{"job-name": rayJob.Name, k8spod.LabelJobID: id})
	if cluster := rayJob.Status.RayClusterName; cluster != "" && rayJob.Spec.RayClusterSpec != nil {
		list(ns, client.MatchingLabels{"ray.io/cluster": cluster, k8spod.LabelJobID: id})
	}

	pods := make([]corev1.Pod, 0, len(byName))
	for _, pod := range byName {
		pods = append(pods, pod)
	}
	return pods
}

// RayJobObjects are the objects whose events speak for the RayJob: itself and
// the cluster it made.
func RayJobObjects(rayJob *rayv1.RayJob) []string {
	objects := []string{rayJob.Name}
	if cluster := rayJob.Status.RayClusterName; cluster != "" && rayJob.Spec.RayClusterSpec != nil {
		objects = append(objects, cluster)
	}
	return objects
}

// RayClusterPods are a standing cluster's head and worker pods.
func RayClusterPods(ctx context.Context, cl client.Client, cluster *rayv1.RayCluster) []corev1.Pod {
	var pods corev1.PodList
	if err := cl.List(ctx, &pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"ray.io/cluster": cluster.Name, k8spod.LabelJobID: cluster.Labels[k8spod.LabelJobID]}); err != nil {
		return nil
	}
	return pods.Items
}
