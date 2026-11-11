// SPDX-License-Identifier: Apache-2.0

package spark

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
)

func TestMapJobState(t *testing.T) {
	tests := []struct {
		name   string
		native string
		want   v1.Phase
	}{
		{name: "empty", native: "", want: v1.PhasePending},
		{name: "submitted", native: "DriverRequested", want: v1.PhaseSubmitted},
		{name: "running", native: "RunningHealthy", want: v1.PhaseRunning},
		{name: "succeeded", native: "Succeeded", want: v1.PhaseSucceeded},
		{name: "failed", native: "DriverStartTimedOut", want: v1.PhaseFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			application := &unstructured.Unstructured{Object: map[string]any{
				"status": map[string]any{"currentState": map[string]any{"currentStateSummary": tt.native}},
			}}
			if got := MapJobState(application); got.Phase != tt.want {
				t.Errorf("phase = %q, want %q", got.Phase, tt.want)
			}
		})
	}
}

func TestMapJobStateIncludesOperatorDiagnosticsTimestampsAndRetryCount(t *testing.T) {
	application := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"currentState": map[string]any{
				"currentStateSummary": "SchedulingFailure",
				"message":             "0/1 nodes are available: 1 Insufficient cpu",
				"lastTransitionTime":  "2026-08-07T10:02:00Z",
			},
			"currentAttemptSummary": map[string]any{
				"attemptInfo": map[string]any{"restartCounter": int64(2)},
				"stateTransitionHistory": map[string]any{
					"running": map[string]any{"currentStateSummary": "RunningHealthy", "lastTransitionTime": "2026-08-07T10:01:00Z"},
				},
			},
		},
	}}
	state := MapJobState(application)
	if state.Phase != v1.PhaseUnschedulable || state.Restarts != 2 {
		t.Fatalf("state = %#v", state)
	}
	if state.StartTime == nil || !state.StartTime.Time.Equal(time.Date(2026, 8, 7, 10, 1, 0, 0, time.UTC)) {
		t.Errorf("start time = %v", state.StartTime)
	}
	if len(state.Reasons) != 1 || state.Reasons[0].Code != v1.ReasonInsufficientCPU {
		t.Errorf("reasons = %#v", state.Reasons)
	}
}

func TestAnApplicationThatCannotStartSaysWhy(t *testing.T) {
	tests := []struct {
		name    string
		objects []client.Object
		code    string
	}{
		{
			name: "driver image pull",
			objects: []client.Object{&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "spark-driver", Namespace: "tenant-default", Labels: map[string]string{ApplicationNameLabel: "spark-pi", JobIDLabel: "uid-1"}},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
					Name: DriverContainerName, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image \"nope:1\": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image \"docker.io/library/nope:1\": failed to resolve reference \"docker.io/library/nope:1\": docker.io/library/nope:1: not found"}},
				}}},
			}},
			code: v1.ReasonImagePullFailed,
		},
		{
			name: "operator scheduling event",
			objects: []client.Object{&corev1.Event{
				ObjectMeta: metav1.ObjectMeta{Name: "spark-warning", Namespace: "tenant-default"},
				Type:       corev1.EventTypeWarning, Reason: "FailedScheduling", Message: "0/1 nodes are available: 1 Insufficient memory",
				InvolvedObject: corev1.ObjectReference{Name: "spark-pi"},
			}},
			code: v1.ReasonInsufficientMemory,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := testutil.Scheme(t, corev1.AddToScheme)
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(test.objects...).Build()
			application := NewApplication("tenant-default", "spark-pi")
			application.SetLabels(map[string]string{JobIDLabel: "uid-1"})
			state := k8spod.Enrich(context.Background(), cl, k8spod.Observed{Namespace: application.GetNamespace(), Objects: []string{application.GetName()},
				State: backend.JobState{Phase: v1.PhaseSubmitted}, Pods: ApplicationPods(context.Background(), cl, application), Container: DriverContainerName})
			if len(state.Reasons) != 1 || state.Reasons[0].Code != test.code {
				t.Fatalf("state = %#v", state)
			}
		})
	}
}

func TestMapReleasedStatePreservesFailure(t *testing.T) {
	application := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"currentState": map[string]any{"currentStateSummary": "ResourceReleased"},
			"stateTransitionHistory": map[string]any{
				"1": map[string]any{"currentStateSummary": "RunningHealthy"},
				"2": map[string]any{"currentStateSummary": "Failed"},
			},
		},
	}}
	if got := MapJobState(application); got.Phase != v1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Phase)
	}
}

func TestMapReleasedStateAfterHealthyRunIsSucceeded(t *testing.T) {
	application := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"currentState": map[string]any{"currentStateSummary": "ResourceReleased"},
			"stateTransitionHistory": map[string]any{
				"1": map[string]any{"currentStateSummary": "Submitted"},
				"2": map[string]any{"currentStateSummary": "RunningHealthy"},
			},
		},
	}}
	if got := MapJobState(application); got.Phase != v1.PhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", got.Phase)
	}
}

func TestMapRetainedStateUsesSuccessfulNativeOutcome(t *testing.T) {
	application := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"currentState": map[string]any{
				"currentStateSummary": "TerminatedWithoutReleaseResources",
				"message":             "Application is terminated without releasing resources as configured.",
				"lastTransitionTime":  "2026-08-07T10:03:01Z",
			},
			"stateTransitionHistory": map[string]any{
				"1": map[string]any{"currentStateSummary": "RunningHealthy", "lastTransitionTime": "2026-08-07T10:02:00Z"},
				"2": map[string]any{
					"currentStateSummary": "Succeeded", "lastTransitionTime": "2026-08-07T10:03:00Z",
					"message": "Spark application completed successfully.",
				},
				"3": map[string]any{
					"currentStateSummary": "TerminatedWithoutReleaseResources", "lastTransitionTime": "2026-08-07T10:03:01Z",
				},
			},
		},
	}}
	state := MapJobState(application)
	if state.Phase != v1.PhaseSucceeded || len(state.Reasons) != 0 {
		t.Fatalf("state = %#v", state)
	}
	if state.Message != "Spark application completed successfully." {
		t.Errorf("message = %q", state.Message)
	}
	if state.CompletionTime == nil || !state.CompletionTime.Time.Equal(time.Date(2026, 8, 7, 10, 3, 0, 0, time.UTC)) {
		t.Errorf("completion time = %v", state.CompletionTime)
	}
}

func TestMapRetainedStatePreservesFailedNativeOutcome(t *testing.T) {
	application := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"currentState": map[string]any{"currentStateSummary": "TerminatedWithoutReleaseResources"},
			"stateTransitionHistory": map[string]any{
				"1": map[string]any{"currentStateSummary": "RunningHealthy", "lastTransitionTime": "2026-08-07T10:02:00Z"},
				"2": map[string]any{
					"currentStateSummary": "Failed", "lastTransitionTime": "2026-08-07T10:03:00Z", "message": "driver exited 1",
				},
				"3": map[string]any{
					"currentStateSummary": "TerminatedWithoutReleaseResources", "lastTransitionTime": "2026-08-07T10:03:01Z",
				},
			},
		},
	}}
	state := MapJobState(application)
	if state.Phase != v1.PhaseFailed || len(state.Reasons) != 1 {
		t.Fatalf("state = %#v", state)
	}
}
