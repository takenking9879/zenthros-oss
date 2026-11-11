// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"strings"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// MapJobState translates KubeRay status into a Zenthros JobState. Zenthros only
// reflects what the backend reports; it never computes resources or scheduling.
func MapJobState(rayJob *rayv1.RayJob) backend.JobState {
	message := rayJob.Status.Message
	failedReason := string(rayJob.Status.Reason)

	phase := mapPhase(rayJob.Status.JobDeploymentStatus, rayJob.Status.JobStatus)
	reasons := deriveReasons(phase, message, failedReason, appFailed(rayJob))

	// A non-terminal job whose backend reports a scheduling problem is unschedulable.
	if !phase.Terminal() && hasSchedulingReason(reasons) {
		phase = v1.PhaseUnschedulable
	}

	return backend.JobState{
		Phase:          phase,
		Reasons:        reasons,
		Message:        message,
		StartTime:      rayJob.Status.StartTime,
		CompletionTime: rayJob.Status.EndTime,
	}
}

func mapPhase(deployment rayv1.JobDeploymentStatus, job rayv1.JobStatus) v1.Phase {
	// Job-level terminal states are authoritative for completion.
	switch job {
	case rayv1.JobStatusSucceeded:
		return v1.PhaseSucceeded
	case rayv1.JobStatusFailed:
		return v1.PhaseFailed
	case rayv1.JobStatusStopped:
		return v1.PhaseCancelled
	}

	// Deployment-level states cover provisioning failures and in-progress runs.
	switch deployment {
	case rayv1.JobDeploymentStatusFailed, rayv1.JobDeploymentStatusValidationFailed:
		return v1.PhaseFailed
	case rayv1.JobDeploymentStatusComplete:
		return v1.PhaseSucceeded
	case rayv1.JobDeploymentStatusSuspending, rayv1.JobDeploymentStatusSuspended:
		return v1.PhaseCancelled
	case rayv1.JobDeploymentStatusRunning:
		return v1.PhaseRunning
	}

	if job == rayv1.JobStatusRunning {
		return v1.PhaseRunning
	}
	return v1.PhaseSubmitted
}

// appFailed reports that the job ran and the user's program failed, as
// opposed to a job Ray never started.
func appFailed(rayJob *rayv1.RayJob) bool {
	reason := rayJob.Status.Reason
	return reason == rayv1.AppFailed || (reason == "" && rayJob.Status.JobStatus == rayv1.JobStatusFailed)
}

func deriveReasons(phase v1.Phase, message, failedReason string, appFailed bool) []v1.Reason {
	text := strings.TrimSpace(message + " " + failedReason)
	if text == "" {
		return nil
	}

	code := backend.DiagnosticReasonCode(text)
	if code == "" {
		if phase != v1.PhaseFailed && phase != v1.PhaseUnschedulable {
			return nil
		}
		code = v1.ReasonSubmissionFailed
		if phase == v1.PhaseFailed && appFailed {
			code = v1.ReasonProcessFailed
		}
	}
	return []v1.Reason{{Code: code, Detail: text}}
}

func hasSchedulingReason(reasons []v1.Reason) bool {
	for _, reason := range reasons {
		if backend.PhaseForDiagnosticReason(reason.Code) == v1.PhaseUnschedulable {
			return true
		}
	}
	return false
}
