// SPDX-License-Identifier: Apache-2.0

package k8spod

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
)

// LabelTenant is the tenant of a workload object and its pods.
const LabelTenant = "zenthros.ai/tenant"

// RuntimeUIDs is who a kind's runtime runs as, for the files it is given:
// the owner of the git-sync checkout and the reader of the secret files.
type RuntimeUIDs struct{ Source, Secrets int64 }

// DefaultRuntime is the runtime of an image whose user Zenthros does not know.
var DefaultRuntime = RuntimeUIDs{Source: GitSyncUID, Secrets: SecretRuntimeUID}

// WorkloadLabels are the labels on every workload object the agent makes:
// the job's ID, tenant and Workspace, and the backend that runs it, which
// finds its own among another agent's in the same cluster.
func WorkloadLabels(spec jobspec.JobSpec) map[string]string {
	labels := map[string]string{LabelJobID: spec.ID, LabelTenant: spec.Tenant, owner.LabelBackendID: spec.TargetBackend}
	if spec.Workspace != "" {
		labels[v1.LabelWorkspace] = spec.Workspace
	}
	return labels
}

// WorkloadPodLabels are a workload pod's labels: the job's own, then
// Zenthros's, which win (PodLabels and WorkloadLabels).
func WorkloadPodLabels(spec jobspec.JobSpec) map[string]string {
	labels := PodLabels(spec.ID, spec.Name, spec.Labels)
	for k, v := range WorkloadLabels(spec) {
		labels[k] = v
	}
	return labels
}

// WorkloadPodTemplate is a workload pod as every kind makes it: the job's
// source and secret sources mounted for its runtime, its working directory,
// its placement rendered, and the labels other jobs' placement rules select
// (C-10, c2-F1#7). A kind brings only its containers, the workload container
// first, and what is its own after this. Spark's pods missed placement and
// the job's labels, and Ray's the tenant and Workspace.
func WorkloadPodTemplate(spec jobspec.JobSpec, pod corev1.PodSpec, runtime RuntimeUIDs) corev1.PodTemplateSpec {
	ApplySourceAs(&pod, spec, runtime.Source)
	// An explicit WorkingDir wins over the source-derived one.
	if spec.WorkingDir != "" {
		pod.Containers[0].WorkingDir = spec.WorkingDir
	}
	ApplySecretSourcesAsUser(&pod, spec.SecretSources, runtime.Secrets)
	ApplyPlacement(&pod, spec.ID, spec.Labels, spec.Placement)
	return corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: WorkloadPodLabels(spec)}, Spec: pod}
}
