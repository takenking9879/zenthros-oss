// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
)

// DiagnosticReasonCode maps Kubernetes and native-operator messages to the stable
// reason taxonomy shared across workload adapters.
func DiagnosticReasonCode(text string) string {
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "insufficient cpu"):
		return v1.ReasonInsufficientCPU
	case strings.Contains(lower, "insufficient memory"):
		return v1.ReasonInsufficientMemory
	case strings.Contains(lower, "insufficient nvidia.com/gpu"), strings.Contains(lower, "insufficient gpu"):
		return v1.ReasonInsufficientGPU
	case strings.Contains(lower, "unschedulable"), strings.Contains(lower, "failedscheduling"), strings.Contains(lower, "nodes are available"):
		return v1.ReasonUnschedulable
	case strings.Contains(lower, "imagepullbackoff"), strings.Contains(lower, "errimagepull"),
		strings.Contains(lower, "image pull"), strings.Contains(lower, "failed to pull image"):
		// Only the registry's refusal ends the job; anything else the
		// kubelet pulls again, whose back-off carries the last pull's error.
		if !PullRefused(lower) {
			return ""
		}
		return v1.ReasonImagePullFailed
	case strings.Contains(lower, "exceeded quota"), strings.Contains(lower, "quota exceeded"), strings.Contains(lower, "forbidden: exceeded"):
		return v1.ReasonQuotaExceeded
	case strings.Contains(lower, "admission webhook"), strings.Contains(lower, "denied the request"), strings.Contains(lower, "admissionrejected"):
		return v1.ReasonAdmissionRejected
	case strings.Contains(lower, "evicted"), strings.Contains(lower, "preempted"),
		strings.Contains(lower, "preempting"), strings.Contains(lower, "disruptiontarget"):
		return v1.ReasonPodEvicted
	}
	return ""
}

// PhaseForDiagnosticReason returns the stable phase implied by a backend reason.
func PhaseForDiagnosticReason(code string) v1.Phase {
	switch code {
	case v1.ReasonInsufficientCPU, v1.ReasonInsufficientMemory,
		v1.ReasonInsufficientGPU, v1.ReasonUnschedulable, v1.ReasonNodeResourceMissing:
		return v1.PhaseUnschedulable
	default:
		return v1.PhaseFailed
	}
}
