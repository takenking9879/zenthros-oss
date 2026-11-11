// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

func TestMapPhase(t *testing.T) {
	cases := []struct {
		name       string
		deployment rayv1.JobDeploymentStatus
		job        rayv1.JobStatus
		want       v1.Phase
	}{
		{"succeeded job wins", rayv1.JobDeploymentStatusRunning, rayv1.JobStatusSucceeded, v1.PhaseSucceeded},
		{"failed job", rayv1.JobDeploymentStatusRunning, rayv1.JobStatusFailed, v1.PhaseFailed},
		{"stopped job", rayv1.JobDeploymentStatusRunning, rayv1.JobStatusStopped, v1.PhaseCancelled},
		{"deployment failed", rayv1.JobDeploymentStatusFailed, rayv1.JobStatusNew, v1.PhaseFailed},
		{"validation failed", rayv1.JobDeploymentStatusValidationFailed, rayv1.JobStatusNew, v1.PhaseFailed},
		{"deployment complete", rayv1.JobDeploymentStatusComplete, rayv1.JobStatusNew, v1.PhaseSucceeded},
		{"running", rayv1.JobDeploymentStatusRunning, rayv1.JobStatusRunning, v1.PhaseRunning},
		{"suspended", rayv1.JobDeploymentStatusSuspended, rayv1.JobStatusNew, v1.PhaseCancelled},
		{"initializing", rayv1.JobDeploymentStatusInitializing, rayv1.JobStatusNew, v1.PhaseSubmitted},
		{"waiting", rayv1.JobDeploymentStatusWaiting, rayv1.JobStatusPending, v1.PhaseSubmitted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mapPhase(tc.deployment, tc.job); got != tc.want {
				t.Errorf("mapPhase(%q,%q) = %q, want %q", tc.deployment, tc.job, got, tc.want)
			}
		})
	}
}

func TestMapJobStateUnschedulable(t *testing.T) {
	rayJob := &rayv1.RayJob{}
	rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusInitializing
	rayJob.Status.JobStatus = rayv1.JobStatusPending
	rayJob.Status.Message = "0/1 nodes are available: 1 Insufficient cpu, 1 Insufficient nvidia.com/gpu."

	state := MapJobState(rayJob)

	if state.Phase != v1.PhaseUnschedulable {
		t.Fatalf("phase = %q, want %q", state.Phase, v1.PhaseUnschedulable)
	}
	if len(state.Reasons) == 0 || state.Reasons[0].Code != v1.ReasonInsufficientCPU {
		t.Errorf("expected INSUFFICIENT_CPU reason, got %+v", state.Reasons)
	}
}

func TestMapJobStateRunningNoReasons(t *testing.T) {
	rayJob := &rayv1.RayJob{}
	rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusRunning
	rayJob.Status.JobStatus = rayv1.JobStatusRunning

	state := MapJobState(rayJob)

	if state.Phase != v1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", state.Phase)
	}
	if len(state.Reasons) != 0 {
		t.Errorf("expected no reasons while running, got %+v", state.Reasons)
	}
}

func TestReasonCode(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{"image pull backoff", "ImagePullBackOff Back-off pulling image \"ray:nope\": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image \"docker.io/library/ray:nope\": failed to resolve reference \"docker.io/library/ray:nope\": docker.io/library/ray:nope: not found", v1.ReasonImagePullFailed},
		{"err image pull", "ErrImagePull: rpc error: code = Unknown desc = pull access denied", v1.ReasonImagePullFailed},
		{"failed to pull image", "Failed to pull image ray:latest: 401 Unauthorized", v1.ReasonImagePullFailed},
		// The network's, which the kubelet pulls again (diagnostics_test in pkg/backend).
		{"a pull the network cut", "ErrImagePull: rpc error: code = Unknown desc = failed to copy: unexpected EOF", ""},
		{"exceeded quota", "exceeded quota: cpu", v1.ReasonQuotaExceeded},
		{"forbidden exceeded", "forbidden: exceeded quota: pods", v1.ReasonQuotaExceeded},
		{"admission webhook", "admission webhook denied the request", v1.ReasonAdmissionRejected},
		{"denied the request", "policy denied the request: image not allowed", v1.ReasonAdmissionRejected},
		{"insufficient cpu", "0/3 nodes are available: 3 Insufficient cpu", v1.ReasonInsufficientCPU},
		{"evicted", "The node was low on resource: Evicted", v1.ReasonPodEvicted},
		{"preempted", "Preempted by a higher priority pod", v1.ReasonPodEvicted},
		{"unknown", "something else entirely", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := backend.DiagnosticReasonCode(tc.text); got != tc.want {
				t.Errorf("DiagnosticReasonCode(%q) = %q, want %q", tc.text, got, tc.want)
			}
		})
	}
}

// Found in the local E2E: a Ray program that exited 2 was reported as
// SUBMISSION_FAILED. The job ran; the user's process failed.
func TestMapJobStateSeparatesAppFailureFromSubmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason rayv1.JobFailedReason
		status rayv1.JobStatus
		want   string
	}{
		{"app failed", rayv1.AppFailed, rayv1.JobStatusFailed, v1.ReasonProcessFailed},
		{"job failed without a reason", "", rayv1.JobStatusFailed, v1.ReasonProcessFailed},
		{"submission failed", rayv1.SubmissionFailed, rayv1.JobStatusNew, v1.ReasonSubmissionFailed},
		{"deadline exceeded", rayv1.DeadlineExceeded, rayv1.JobStatusRunning, v1.ReasonSubmissionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rayJob := &rayv1.RayJob{}
			rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
			rayJob.Status.JobStatus = tc.status
			rayJob.Status.Reason = tc.reason
			rayJob.Status.Message = "Job entrypoint command failed with exit code 2"

			state := MapJobState(rayJob)
			if state.Phase != v1.PhaseFailed || len(state.Reasons) == 0 || state.Reasons[0].Code != tc.want {
				t.Fatalf("phase=%s reasons=%+v, want Failed %s", state.Phase, state.Reasons, tc.want)
			}
		})
	}
}
