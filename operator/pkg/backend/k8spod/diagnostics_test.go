// SPDX-License-Identifier: Apache-2.0

package k8spod

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

func TestEnrichDoesNotOverrideSucceeded(t *testing.T) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "leftover-executor", Namespace: "tenant-default"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image \"nope:1\": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image \"docker.io/library/nope:1\": failed to resolve reference \"docker.io/library/nope:1\": docker.io/library/nope:1: not found"}},
		}}},
	}
	state := Enrich(context.Background(), nil, Observed{Namespace: "tenant-default", Objects: []string{"spark-pi"}, Pods: []corev1.Pod{pod}, State: backend.JobState{Phase: v1.PhaseSucceeded}})
	if state.Phase != v1.PhaseSucceeded || len(state.Reasons) != 0 {
		t.Fatalf("terminal success was rewritten: %+v", state)
	}
}

func TestDiagnosePodsMapsTerminatedExit(t *testing.T) {
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "driver"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
		}}},
	}}
	code, detail := DiagnosePods(pods)
	if code != v1.ReasonProcessFailed || !strings.Contains(detail, "driver") {
		t.Fatalf("DiagnosePods = %q %q", code, detail)
	}
}

func TestDiagnosePodsMapsEvictionBeforeExit(t *testing.T) {
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "worker"},
		Status: corev1.PodStatus{
			Reason:  "Evicted",
			Message: "The node was low on resource: memory",
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "Error"}},
			}},
		},
	}}
	code, detail := DiagnosePods(pods)
	if code != v1.ReasonPodEvicted || !strings.Contains(detail, "worker") {
		t.Fatalf("DiagnosePods = %q %q, want POD_EVICTED", code, detail)
	}
}

func TestEnrichOverlaysSubmissionFailedWithProcessExit(t *testing.T) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "submitter"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
		}}},
	}
	state := Enrich(context.Background(), nil, Observed{Namespace: "tenant-default", Objects: []string{"submitter"}, Pods: []corev1.Pod{pod}, State: backend.JobState{
		Phase:   v1.PhaseFailed,
		Reasons: []v1.Reason{{Code: v1.ReasonSubmissionFailed, Detail: "JobFailed"}},
	}})
	if state.Phase != v1.PhaseFailed || len(state.Reasons) == 0 || state.Reasons[0].Code != v1.ReasonProcessFailed {
		t.Fatalf("state = %+v, want PROCESS_FAILED overlay", state)
	}
}
