// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
)

// enrichRayJob is the status rule over a RayJob's pods and objects.
func enrichRayJob(ctx context.Context, cl client.Client, rayJob *rayv1.RayJob, state backend.JobState) backend.JobState {
	return k8spod.Enrich(ctx, cl, k8spod.Observed{Namespace: rayJob.Namespace, Objects: RayJobObjects(rayJob), State: state,
		Pods: RayJobPods(ctx, cl, rayJob), Container: RayContainerName})
}

func diagnosticsClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).Build()
}

func diagRayJob() *rayv1.RayJob {
	return &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-default", Name: "job1", Labels: map[string]string{k8spod.LabelJobID: "uid-1"}},
		Spec:       rayv1.RayJobSpec{RayClusterSpec: &rayv1.RayClusterSpec{}},
		Status:     rayv1.RayJobStatus{RayClusterName: "job1-abc"},
	}
}

func TestEnrichExistingClusterIgnoresSharedClusterFailures(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-default", Name: "shared-head", Labels: map[string]string{"ray.io/cluster": "shared"}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image \"nope:1\": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image \"docker.io/library/nope:1\": failed to resolve reference \"docker.io/library/nope:1\": docker.io/library/nope:1: not found"}},
		}}},
	}
	rayJob := diagRayJob()
	rayJob.Spec.RayClusterSpec = nil
	rayJob.Spec.ClusterSelector = map[string]string{rayClusterSelectorKey: "shared"}
	rayJob.Status.RayClusterName = "shared"

	state := enrichRayJob(context.Background(), diagnosticsClient(t, pod), rayJob, backend.JobState{Phase: v1.PhaseSubmitted})
	if state.Phase != v1.PhaseSubmitted || len(state.Reasons) != 0 {
		t.Fatalf("shared cluster failure contaminated per-job status: %+v", state)
	}
}

func TestEnrichDetectsImagePullBackOff(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenant-default",
			Name:      "job1-abc-head-xyz",
			Labels:    map[string]string{"ray.io/cluster": "job1-abc", k8spod.LabelJobID: "uid-1"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "ray-head",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image \"ray:nope\": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image \"docker.io/library/ray:nope\": failed to resolve reference \"docker.io/library/ray:nope\": docker.io/library/ray:nope: not found"},
				},
			}},
		},
	}
	cl := diagnosticsClient(t, pod)

	state := enrichRayJob(context.Background(), cl, diagRayJob(), backend.JobState{Phase: v1.PhaseSubmitted})

	if state.Phase != v1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", state.Phase)
	}
	if len(state.Reasons) == 0 || state.Reasons[0].Code != v1.ReasonImagePullFailed {
		t.Errorf("reasons = %+v, want IMAGE_PULL_FAILED", state.Reasons)
	}
}

func TestEnrichDetectsQuotaExceeded(t *testing.T) {
	event := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: "tenant-default", Name: "job1-abc.evt"},
		InvolvedObject: corev1.ObjectReference{Name: "job1-abc"},
		Type:           corev1.EventTypeWarning,
		Reason:         "FailedToCreateWorkerPod",
		Message:        `pods "w" is forbidden: exceeded quota: tenant-default-quota, requested: requests.cpu=4`,
	}
	cl := diagnosticsClient(t, event)

	state := enrichRayJob(context.Background(), cl, diagRayJob(), backend.JobState{Phase: v1.PhaseSubmitted})

	if state.Phase != v1.PhaseFailed {
		t.Errorf("phase = %q, want Failed", state.Phase)
	}
	if len(state.Reasons) == 0 || state.Reasons[0].Code != v1.ReasonQuotaExceeded {
		t.Errorf("reasons = %+v, want QUOTA_EXCEEDED", state.Reasons)
	}
}

func TestEnrichMarksSchedulingUnschedulable(t *testing.T) {
	event := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: "tenant-default", Name: "job1-abc.sched"},
		InvolvedObject: corev1.ObjectReference{Name: "job1-abc"},
		Type:           corev1.EventTypeWarning,
		Reason:         "FailedScheduling",
		Message:        "0/1 nodes are available: 1 Insufficient cpu",
	}
	cl := diagnosticsClient(t, event)

	state := enrichRayJob(context.Background(), cl, diagRayJob(), backend.JobState{Phase: v1.PhaseSubmitted})

	if state.Phase != v1.PhaseUnschedulable {
		t.Errorf("phase = %q, want Unschedulable", state.Phase)
	}
	if len(state.Reasons) == 0 || state.Reasons[0].Code != v1.ReasonInsufficientCPU {
		t.Errorf("reasons = %+v, want INSUFFICIENT_CPU", state.Reasons)
	}
}

func TestEnrichSkipsTerminalOrReasonedState(t *testing.T) {
	cl := diagnosticsClient(t)

	terminal := enrichRayJob(context.Background(), cl, diagRayJob(), backend.JobState{Phase: v1.PhaseSucceeded})
	if terminal.Phase != v1.PhaseSucceeded {
		t.Errorf("terminal phase changed: %q", terminal.Phase)
	}

	reasoned := enrichRayJob(context.Background(), cl, diagRayJob(), backend.JobState{
		Phase:   v1.PhaseFailed,
		Reasons: []v1.Reason{{Code: v1.ReasonProcessFailed}},
	})
	if len(reasoned.Reasons) != 1 || reasoned.Reasons[0].Code != v1.ReasonProcessFailed {
		t.Errorf("existing reasons overridden: %+v", reasoned.Reasons)
	}
}

// A submitter Job of the same name left by an earlier run labels its pods
// job-name=<name> too: only the pods carrying this RayJob's job ID are its
// own (N12).
func TestARayJobReadsOnlyItsOwnPods(t *testing.T) {
	old := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-default", Name: "job1-old", Labels: map[string]string{"job-name": "job1", k8spod.LabelJobID: "uid-0"}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image \"nope:1\": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image \"docker.io/library/nope:1\": failed to resolve reference \"docker.io/library/nope:1\": docker.io/library/nope:1: not found"}},
		}}},
	}
	if pods := RayJobPods(context.Background(), diagnosticsClient(t, old), diagRayJob()); len(pods) != 0 {
		t.Fatalf("pods = %v, want none of the earlier run's", pods)
	}
}
