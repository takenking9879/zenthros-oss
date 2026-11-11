// SPDX-License-Identifier: Apache-2.0

package spark

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// MapJobState maps the operator's current state summary into the stable Zenthros phases.
func MapJobState(application *unstructured.Unstructured) backend.JobState {
	summary, _, _ := unstructured.NestedString(application.Object, "status", "currentState", "currentStateSummary")
	message, _, _ := unstructured.NestedString(application.Object, "status", "currentState", "message")
	phase := mapPhase(summary)
	if isTerminalWrapper(summary) {
		phase, message = terminalOutcome(application)
	}
	state := backend.JobState{Phase: phase, Message: message, Restarts: restartCount(application)}
	if state.Message == "" {
		state.Message = fmt.Sprintf("SparkApplication state: %s", displayState(summary))
	}
	state.StartTime, state.CompletionTime = transitionTimes(application)
	transition, _, _ := unstructured.NestedString(application.Object, "status", "currentState", "lastTransitionTime")
	if state.Phase.Terminal() && state.CompletionTime == nil {
		state.CompletionTime = parseTime(transition)
	}
	if code := backend.DiagnosticReasonCode(state.Message); code != "" {
		state.Reasons = []v1.Reason{{Code: code, Detail: state.Message}}
		state.Phase = backend.PhaseForDiagnosticReason(code)
	} else if state.Phase == v1.PhaseFailed {
		state.Reasons = []v1.Reason{{Code: v1.ReasonSubmissionFailed, Detail: state.Message}}
	}
	return state
}

func mapPhase(summary string) v1.Phase {
	switch summary {
	case "Submitted", "DriverRequested", "ScheduledToRestart":
		return v1.PhaseSubmitted
	case "DriverStarted", "DriverReady", "InitializedBelowThresholdExecutors",
		"RunningHealthy", "RunningWithBelowThresholdExecutors", "RunningWithPartialCapacity":
		return v1.PhaseRunning
	case "Succeeded":
		return v1.PhaseSucceeded
	case "SchedulingFailure":
		return v1.PhaseUnschedulable
	case "DriverEvicted", "DriverReadyTimedOut", "DriverStartTimedOut", "ExecutorsStartTimedOut",
		"Failed":
		return v1.PhaseFailed
	default:
		return v1.PhasePending
	}
}

func restartCount(application *unstructured.Unstructured) int32 {
	value, found, _ := unstructured.NestedInt64(
		application.Object, "status", "currentAttemptSummary", "attemptInfo", "restartCounter",
	)
	if !found || value < 0 {
		return 0
	}
	return int32(value)
}

func transitionTimes(application *unstructured.Unstructured) (start, completion *metav1.Time) {
	histories := [][]string{
		{"status", "stateTransitionHistory"},
		{"status", "currentAttemptSummary", "stateTransitionHistory"},
	}
	for _, fields := range histories {
		history, found, _ := unstructured.NestedMap(application.Object, fields...)
		if !found {
			continue
		}
		for _, raw := range history {
			entry, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			summary, _ := entry["currentStateSummary"].(string)
			value, _ := entry["lastTransitionTime"].(string)
			transition := parseTime(value)
			if transition == nil {
				continue
			}
			phase := mapPhase(summary)
			if isTerminalWrapper(summary) {
				continue
			}
			if phase == v1.PhaseRunning && (start == nil || transition.Time.Before(start.Time)) {
				start = transition
			}
			if phase.Terminal() && (completion == nil || transition.After(completion.Time)) {
				completion = transition
			}
		}
	}
	return start, completion
}

func terminalOutcome(application *unstructured.Unstructured) (v1.Phase, string) {
	history, _, _ := unstructured.NestedMap(application.Object, "status", "stateTransitionHistory")
	foundSuccess := false
	foundRunning := false
	foundFailure := false
	var successMessage, runningMessage, failureMessage string
	var latestPhase v1.Phase
	var latestMessage string
	var latestTime *metav1.Time
	for _, raw := range history {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		summary, _ := entry["currentStateSummary"].(string)
		if isTerminalWrapper(summary) {
			continue
		}
		message, _ := entry["message"].(string)
		phase := mapPhase(summary)
		if phase.Terminal() {
			transition, _ := entry["lastTransitionTime"].(string)
			parsed := parseTime(transition)
			if parsed != nil && (latestTime == nil || parsed.After(latestTime.Time)) {
				latestTime = parsed
				latestPhase = phase
				latestMessage = message
			}
		}
		switch phase {
		case v1.PhaseFailed:
			foundFailure = true
			failureMessage = message
		case v1.PhaseSucceeded:
			foundSuccess = true
			successMessage = message
		case v1.PhaseRunning:
			foundRunning = true
			runningMessage = message
		}
	}
	if latestTime != nil {
		return latestPhase, latestMessage
	}
	if foundFailure {
		return v1.PhaseFailed, failureMessage
	}
	if foundSuccess || foundRunning {
		if foundSuccess {
			return v1.PhaseSucceeded, successMessage
		}
		return v1.PhaseSucceeded, runningMessage
	}
	return v1.PhasePending, ""
}

func isTerminalWrapper(summary string) bool {
	return summary == "ResourceReleased" || summary == "TerminatedWithoutReleaseResources"
}

func displayState(summary string) string {
	if summary == "" {
		return "Pending"
	}
	return summary
}

func parseTime(value string) *metav1.Time {
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	result := metav1.NewTime(parsed)
	return &result
}
