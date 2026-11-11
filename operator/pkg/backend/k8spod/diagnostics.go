// SPDX-License-Identifier: Apache-2.0

package k8spod

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// Observed is what one status read found of a workload: the state its kind
// mapped from the native object, and the pods that run it.
type Observed struct {
	Namespace string
	// Objects are the native objects whose events speak for the workload:
	// the one the agent made and, for a RayJob, its cluster.
	Objects []string
	State   backend.JobState
	Pods    []corev1.Pod
	// Container is the workload container in Pods, whose image digest is
	// reported; "" for a kind with no container of its own (Manifest).
	Container string
	// DetailOnly is a kind whose operator decides its fate from its pods
	// (Manifest): a failed pod, an eviction, even an image that will not pull
	// may be retried by it, so a diagnosis only explains.
	DetailOnly bool
}

// Settled is a state no pod can change: Succeeded or Cancelled. Its pods
// are not read.
func Settled(state backend.JobState) bool {
	return state.Phase == v1.PhaseSucceeded || state.Phase == v1.PhaseCancelled
}

// Enrich is the one rule for what a workload's pods and warning events add
// to the state its kind mapped, for every kind:
//   - a Settled state stays as it is;
//   - the workload container's image digest is reported;
//   - Failed stays Failed: the pods' detail is appended, and a reason that
//     names no cause (SUBMISSION_FAILED, or none) takes the pods' diagnosis;
//   - a workload that has not ended takes a diagnosis of why it cannot start
//     (image pull, quota, admission, scheduling), never a failed container
//     or an evicted pod: the native controller retries those, and only its
//     own terminal state ends the job (C-03);
//   - events are listed only while no pod runs: a running pod has started,
//     and its events were listed on every poll;
//   - DetailOnly appends the diagnosis to a workload that is not Running and
//     changes nothing else.
func Enrich(ctx context.Context, cl client.Client, o Observed) backend.JobState {
	state := o.State
	if Settled(state) {
		return state
	}
	if o.Container != "" {
		state.ResolvedImage = ResolvedImage(o.Pods, o.Container)
		if state.Phase == v1.PhaseFailed {
			state.ExitCode = exitCode(o.Pods, o.Container)
		}
	}
	if o.DetailOnly && state.Phase == v1.PhaseRunning {
		return state
	}
	code, detail := diagnose(ctx, cl, o)
	switch {
	case detail == "":
		return state
	case o.DetailOnly:
		return withDetail(state, detail)
	case state.Phase == v1.PhaseFailed:
		if code != "" && !hasAuthoritativeReason(state) {
			state.Reasons = []v1.Reason{{Code: code, Detail: detail}}
			state.Message = withDetail(backend.JobState{Message: state.Message}, detail).Message
			return state
		}
		return withDetail(state, detail)
	case code == "" || nativeRetries(code) || hasAuthoritativeReason(state):
		return state
	}
	return ApplyDiagnostic(state, code, detail)
}

// exitCode is what the workload container of a failed workload returned: the
// first pod where it exited other than 0 (a pod that was killed has none).
func exitCode(pods []corev1.Pod, container string) *int32 {
	for i := range pods {
		for _, status := range pods[i].Status.ContainerStatuses {
			if status.Name != container || status.State.Terminated == nil || status.State.Terminated.ExitCode == 0 {
				continue
			}
			code := status.State.Terminated.ExitCode
			return &code
		}
	}
	return nil
}

// diagnose reads the pods first, then, while none runs, the warning events
// on the workload's objects and pods.
func diagnose(ctx context.Context, cl client.Client, o Observed) (code, detail string) {
	if code, detail := DiagnosePods(o.Pods); detail != "" {
		return code, detail
	}
	for i := range o.Pods {
		if o.Pods[i].Status.Phase == corev1.PodRunning {
			return "", ""
		}
	}
	owned := map[string]bool{}
	for _, name := range o.Objects {
		owned[name] = true
	}
	for _, pod := range o.Pods {
		owned[pod.Name] = true
	}
	events := ListWarningEvents(ctx, cl, o.Namespace, owned)
	if o.DetailOnly {
		return "", LatestWarning(events, owned)
	}
	return DiagnoseWarningEvents(events, owned)
}

// nativeRetries is a diagnosis the native controller handles itself while
// the workload runs: a container that exited, a pod that was evicted.
func nativeRetries(code string) bool {
	return code == v1.ReasonProcessFailed || code == v1.ReasonPodEvicted
}

func withDetail(state backend.JobState, detail string) backend.JobState {
	if state.Message == "" {
		state.Message = detail
	} else {
		state.Message += "; " + detail
	}
	if len(state.Reasons) > 0 {
		state.Reasons[0].Detail += "; " + detail
	}
	return state
}

func hasAuthoritativeReason(state backend.JobState) bool {
	for _, reason := range state.Reasons {
		switch reason.Code {
		case v1.ReasonProcessFailed, v1.ReasonPodEvicted, v1.ReasonImagePullFailed,
			v1.ReasonQuotaExceeded, v1.ReasonAdmissionRejected,
			v1.ReasonInsufficientCPU, v1.ReasonInsufficientMemory, v1.ReasonInsufficientGPU,
			v1.ReasonUnschedulable, v1.ReasonNodeResourceMissing,
			v1.ReasonAgentDead, v1.ReasonNetworkPartition, v1.ReasonVMFailed, v1.ReasonProviderGone:
			return true
		}
	}
	return false
}

// DiagnosePods maps waiting, terminated, eviction, and condition failures to the
// stable reason taxonomy. Eviction is checked before container exit so a killed
// container is not reported as PROCESS_FAILED.
func DiagnosePods(pods []corev1.Pod) (code, detail string) {
	for _, pod := range pods {
		if code, detail := diagnosePodEviction(pod); code != "" {
			return code, detail
		}
		statuses := append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
		for _, containerStatus := range statuses {
			if waiting := containerStatus.State.Waiting; waiting != nil {
				text := strings.TrimSpace(waiting.Reason + " " + waiting.Message)
				if mapped := backend.DiagnosticReasonCode(text); mapped != "" {
					return mapped, fmt.Sprintf("pod %s: %s", pod.Name, text)
				}
			}
			if terminated := containerStatus.State.Terminated; terminated != nil && terminated.ExitCode != 0 {
				text := strings.TrimSpace(terminated.Reason + " " + terminated.Message)
				if text == "" {
					text = fmt.Sprintf("container exited with code %d", terminated.ExitCode)
				}
				return v1.ReasonProcessFailed, fmt.Sprintf("pod %s: %s", pod.Name, text)
			}
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Status != corev1.ConditionFalse || condition.Message == "" {
				continue
			}
			text := strings.TrimSpace(condition.Reason + " " + condition.Message)
			if mapped := backend.DiagnosticReasonCode(text); mapped != "" {
				return mapped, fmt.Sprintf("pod %s: %s", pod.Name, text)
			}
		}
	}
	return "", ""
}

func diagnosePodEviction(pod corev1.Pod) (code, detail string) {
	reason := strings.TrimSpace(pod.Status.Reason)
	lower := strings.ToLower(reason)
	if lower == "evicted" || lower == "preempting" || lower == "preempted" {
		return v1.ReasonPodEvicted, fmt.Sprintf("pod %s: %s %s", pod.Name, reason, strings.TrimSpace(pod.Status.Message))
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == "DisruptionTarget" && condition.Status == corev1.ConditionTrue {
			text := strings.TrimSpace(condition.Reason + " " + condition.Message)
			return v1.ReasonPodEvicted, fmt.Sprintf("pod %s: %s", pod.Name, text)
		}
	}
	return "", ""
}

// ListWarningEvents prefers a field selector per owned name and falls back to a
// namespace list when the client has no Event indexer (unit-test fake clients).
func ListWarningEvents(ctx context.Context, cl client.Client, namespace string, owned map[string]bool) []corev1.Event {
	if cl == nil || namespace == "" || len(owned) == 0 {
		return nil
	}
	var collected []corev1.Event
	indexed := true
	for name := range owned {
		var part corev1.EventList
		if err := cl.List(ctx, &part, client.InNamespace(namespace), client.MatchingFields{"involvedObject.name": name}); err != nil {
			indexed = false
			break
		}
		collected = append(collected, part.Items...)
	}
	if indexed {
		return collected
	}
	var events corev1.EventList
	if err := cl.List(ctx, &events, client.InNamespace(namespace)); err != nil {
		return nil
	}
	return events.Items
}

// DiagnoseWarningEvents returns the first mapped warning on an owned object.
func DiagnoseWarningEvents(events []corev1.Event, owned map[string]bool) (code, detail string) {
	for _, event := range events {
		if event.Type != corev1.EventTypeWarning || !owned[event.InvolvedObject.Name] {
			continue
		}
		text := strings.TrimSpace(event.Reason + " " + event.Message)
		if mapped := backend.DiagnosticReasonCode(text); mapped != "" {
			return mapped, event.Message
		}
	}
	return "", ""
}

// ApplyDiagnostic stamps a reason onto a JobState.
func ApplyDiagnostic(state backend.JobState, code, detail string) backend.JobState {
	state.Phase = backend.PhaseForDiagnosticReason(code)
	state.Reasons = []v1.Reason{{Code: code, Detail: detail}}
	state.Message = detail
	return state
}

// LatestWarning is the newest Warning event on an owned object, as its
// operator or Kubernetes wrote it: "Warning <Reason> <Kind>/<name>: <message>".
// Whatever an operator could not do (a pod a quota refused, an image it could
// not pull) is what the user needs while the job waits.
func LatestWarning(events []corev1.Event, owned map[string]bool) string {
	var newest *corev1.Event
	var newestAt time.Time
	for i := range events {
		event := &events[i]
		if event.Type != corev1.EventTypeWarning || !owned[event.InvolvedObject.Name] {
			continue
		}
		at := eventTime(event)
		if newest == nil || at.After(newestAt) {
			newest, newestAt = event, at
		}
	}
	if newest == nil {
		return ""
	}
	return fmt.Sprintf("Warning %s %s/%s: %s", newest.Reason, newest.InvolvedObject.Kind, newest.InvolvedObject.Name, newest.Message)
}

func eventTime(event *corev1.Event) time.Time {
	switch {
	case !event.EventTime.IsZero():
		return event.EventTime.Time
	case !event.LastTimestamp.IsZero():
		return event.LastTimestamp.Time
	}
	return event.CreationTimestamp.Time
}

// ResolvedImage is the container's image pinned by digest (repo@sha256:...),
// from the imageID of the first pod that pulled it. "" until one has, or
// when the runtime reports only a local image ID.
func ResolvedImage(pods []corev1.Pod, container string) string {
	for _, pod := range pods {
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != container {
				continue
			}
			id := strings.TrimPrefix(strings.TrimPrefix(status.ImageID, "docker-pullable://"), "docker://")
			if strings.Contains(id, "@sha256:") {
				return id
			}
		}
	}
	return ""
}

// pullFailures are the kubelet's waiting reasons for an image it could not
// pull.
var pullFailures = map[string]bool{
	"ErrImagePull": true, "ImagePullBackOff": true, "InvalidImageName": true,
	"ErrImageNeverPull": true, "RegistryUnavailable": true,
}

// PullFailed reports a container waiting because its image could not be
// pulled.
func PullFailed(reason string) bool { return pullFailures[reason] }
