// SPDX-License-Identifier: Apache-2.0

package k8spod

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

func podWith(phase corev1.PodPhase, state corev1.ContainerState) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "w-0", Namespace: "ns"},
		Status: corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "workload", State: state, ImageID: "docker-pullable://docker.io/team/train@sha256:ab",
		}}},
	}
}

var (
	exited  = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}
	pulling = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "pull access denied"}}
	// The network cut the registry's answer (the SDK's quickstart on k3d,
	// 2026-11-06): the kubelet pulls again.
	cut = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull",
		Message: `failed to pull and unpack image "docker.io/library/python:3.13-bookworm": failed to read expected number of bytes: unexpected EOF`}}
)

// One rule for what pods add, for every kind (C-03): while the workload has
// not ended, a container that exited or a pod that was evicted is the native
// controller's to retry, and only "it cannot start" changes the phase. A
// Failed workload stays Failed and gains the pods' detail. A kind whose
// operator decides its fate (Manifest) only gains detail.
func TestEnrichIsOneRuleForEveryKind(t *testing.T) {
	evicted := podWith(corev1.PodFailed, exited)
	evicted.Status.Reason, evicted.Status.Message = "Evicted", "low on memory"
	for _, tc := range []struct {
		name       string
		state      backend.JobState
		pod        corev1.Pod
		detailOnly bool
		phase      v1.Phase
		code       string
		detail     bool
	}{
		{"a dead worker of a running workload is retried", backend.JobState{Phase: v1.PhaseRunning}, podWith(corev1.PodFailed, exited), false, v1.PhaseRunning, "", false},
		{"an evicted pod of a running workload is retried", backend.JobState{Phase: v1.PhaseRunning}, evicted, false, v1.PhaseRunning, "", false},
		{"an image that will not pull fails a pending workload", backend.JobState{Phase: v1.PhasePending}, podWith(corev1.PodPending, pulling), false, v1.PhaseFailed, v1.ReasonImagePullFailed, true},
		{"a pull the network cut leaves a pending workload waiting", backend.JobState{Phase: v1.PhasePending}, podWith(corev1.PodPending, cut), false, v1.PhasePending, "", false},
		{"a failure the kind explained keeps its reason and gains the pods' detail",
			backend.JobState{Phase: v1.PhaseFailed, Message: "Job failed", Reasons: []v1.Reason{{Code: v1.ReasonProcessFailed, Detail: "Job failed"}}},
			podWith(corev1.PodFailed, exited), false, v1.PhaseFailed, v1.ReasonProcessFailed, true},
		{"a failure without a cause takes the pods' cause",
			backend.JobState{Phase: v1.PhaseFailed, Message: "Failed", Reasons: []v1.Reason{{Code: v1.ReasonSubmissionFailed}}},
			podWith(corev1.PodFailed, exited), false, v1.PhaseFailed, v1.ReasonProcessFailed, true},
		{"an operator's workload that cannot pull only explains", backend.JobState{Phase: v1.PhaseSubmitted, Message: "applied"}, podWith(corev1.PodPending, pulling), true, v1.PhaseSubmitted, "", true},
		{"an operator's running workload is left alone", backend.JobState{Phase: v1.PhaseRunning}, podWith(corev1.PodPending, pulling), true, v1.PhaseRunning, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Enrich(context.Background(), nil, Observed{Namespace: "ns", Objects: []string{"w"}, State: tc.state,
				Pods: []corev1.Pod{tc.pod}, Container: "workload", DetailOnly: tc.detailOnly})
			code := ""
			if len(got.Reasons) > 0 {
				code = got.Reasons[0].Code
			}
			if got.Phase != tc.phase || code != tc.code || strings.Contains(got.Message, "pod w-0") != tc.detail {
				t.Fatalf("got %s %q %q, want %s %q detail=%v", got.Phase, code, got.Message, tc.phase, tc.code, tc.detail)
			}
			if got.ResolvedImage != "docker.io/team/train@sha256:ab" {
				t.Fatalf("resolved image %q", got.ResolvedImage)
			}
		})
	}
}

type eventLists struct {
	client.Client
	lists int
}

func (c *eventLists) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.EventList); ok {
		c.lists++
	}
	return c.Client.List(ctx, list, opts...)
}

// Events say why a workload cannot start; one with a running pod has
// started, and its events were listed on every poll (c2-F1#3).
func TestEventsAreReadOnlyWhileNoPodRuns(t *testing.T) {
	cl := &eventLists{Client: fake.NewClientBuilder().Build()}
	running := Observed{Namespace: "ns", Objects: []string{"w"}, State: backend.JobState{Phase: v1.PhaseRunning},
		Pods: []corev1.Pod{podWith(corev1.PodRunning, corev1.ContainerState{Running: &corev1.ContainerStateRunning{}})}}
	Enrich(context.Background(), cl, running)
	if cl.lists != 0 {
		t.Fatalf("%d event lists for a workload with a running pod", cl.lists)
	}
	waiting := running
	waiting.Pods = nil
	Enrich(context.Background(), cl, waiting)
	if cl.lists == 0 {
		t.Fatal("a workload with no pod running read no events")
	}
}

// The digest comes from the pod that pulled the image (F11): the workload
// container's imageID, whatever prefix the runtime gives it. A local image ID
// is no reference to pull and pins nothing.
// A failed workload's exit code is its workload container's, not a helper's.
func TestFailedWorkloadTakesItsContainersExitCode(t *testing.T) {
	pod := podWith(corev1.PodFailed, corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 7}})
	pod.Status.ContainerStatuses = append([]corev1.ContainerStatus{{
		Name: "sidecar", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137}},
	}}, pod.Status.ContainerStatuses...)
	cl := fake.NewClientBuilder().Build()
	got := Enrich(context.Background(), cl, Observed{
		Namespace: "ns", Container: "workload", Pods: []corev1.Pod{pod},
		State: backend.JobState{Phase: v1.PhaseFailed},
	})
	if got.ExitCode == nil || *got.ExitCode != 7 {
		t.Fatalf("exit code %v, want 7", got.ExitCode)
	}
	running := Enrich(context.Background(), cl, Observed{
		Namespace: "ns", Container: "workload", Pods: []corev1.Pod{pod},
		State: backend.JobState{Phase: v1.PhaseRunning},
	})
	if running.ExitCode != nil {
		t.Fatalf("a workload that has not ended has no exit code; got %d", *running.ExitCode)
	}
}

func TestResolvedImageFromWorkloadImageID(t *testing.T) {
	pod := func(container, imageID string) corev1.Pod {
		return corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: container, ImageID: imageID}}}}
	}
	for _, tc := range []struct {
		name string
		pods []corev1.Pod
		want string
	}{
		{"containerd", []corev1.Pod{pod("workload", "docker.io/library/python@sha256:ab")}, "docker.io/library/python@sha256:ab"},
		{"docker", []corev1.Pod{pod("workload", "docker-pullable://python@sha256:ab")}, "python@sha256:ab"},
		{"not pulled yet", []corev1.Pod{pod("workload", "")}, ""},
		{"local image ID", []corev1.Pod{pod("workload", "sha256:ab")}, ""},
		{"a sidecar", []corev1.Pod{pod("sidecar", "docker.io/library/envoy@sha256:ab")}, ""},
		{"second pod pulled", []corev1.Pod{pod("workload", ""), pod("workload", "repo/x@sha256:cd")}, "repo/x@sha256:cd"},
	} {
		if got := ResolvedImage(tc.pods, "workload"); got != tc.want {
			t.Errorf("%s: ResolvedImage = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// A term that names no job (stored before admission took exactly one of
// jobName and jobSelector) is no constraint: rendering it copied a nil
// selector and the agent panicked (20#4).
func TestATermThatNamesNoJobIsNoConstraint(t *testing.T) {
	var pod corev1.PodSpec
	ApplyPlacement(&pod, "uid-1", nil, &v1.PlacementPolicy{AntiAffinity: []v1.JobAffinityTerm{{TopologyKey: v1.TopologyZone}}})
	if pod.Affinity != nil {
		t.Fatalf("affinity = %+v, want none", pod.Affinity)
	}
}
