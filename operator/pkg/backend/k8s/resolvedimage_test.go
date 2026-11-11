// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
)

// The Job's status carries the digest its running pod pulled.
func TestABatchJobCarriesTheDigestItsPodPulled(t *testing.T) {
	job := ownJob()
	job.Status.Active = 1
	running := &corev1.Pod{}
	running.Name, running.Namespace = "train-a", "tenant-a"
	running.Labels = map[string]string{"job-name": "train", "controller-uid": "u1"}
	running.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: WorkloadContainerName, ImageID: "docker.io/team/train@sha256:ab",
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	cl := fake.NewClientBuilder().WithObjects(running).Build()
	if got := enrichBatchJob(cl, job).ResolvedImage; got != "docker.io/team/train@sha256:ab" {
		t.Fatalf("ResolvedImage = %q", got)
	}
}

// ownJob is a Job as the apiserver stores it: its selector is its own
// controller UID.
func ownJob() *batchv1.Job {
	job := &batchv1.Job{}
	job.Name, job.Namespace = "train", "tenant-a"
	job.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"controller-uid": "u1"}}
	return job
}

// enrichBatchJob is the status rule over a Job's pods.
func enrichBatchJob(cl client.Client, job *batchv1.Job) backend.JobState {
	return k8spod.Enrich(context.Background(), cl, k8spod.Observed{Namespace: job.Namespace, Objects: []string{job.Name},
		State: MapBatchJob(job), Pods: BatchJobPods(context.Background(), cl, job), Container: WorkloadContainerName})
}

// A Job deleted in the background leaves pods labelled job-name=<name>; the
// next Job of that name reads only the pods its own selector picks (N12).
func TestAJobReadsOnlyThePodsItsSelectorPicks(t *testing.T) {
	left := &corev1.Pod{}
	left.Name, left.Namespace = "train-old", "tenant-a"
	left.Labels = map[string]string{"job-name": "train", "controller-uid": "u0"}
	cl := fake.NewClientBuilder().WithObjects(left).Build()
	if pods := BatchJobPods(context.Background(), cl, ownJob()); len(pods) != 0 {
		t.Fatalf("pods = %v, want none of the earlier Job's", pods)
	}
}
