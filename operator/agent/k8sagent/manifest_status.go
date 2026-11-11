// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// The default status contract reads status.conditions the way batch/v1 Jobs
// and the Kubeflow training jobs write them. An operator that reports a phase
// string instead (Volcano, for one) is read through phasePath.
var (
	defaultSuccessWhen = []v1.ConditionMatch{{Type: "Succeeded", Status: "True"}, {Type: "Complete", Status: "True"}}
	defaultFailureWhen = []v1.ConditionMatch{{Type: "Failed", Status: "True"}}
	// Restarting is the training operators' "a replica failed and is being
	// restarted within its policy": the job still runs.
	defaultRunningWhen = []v1.ConditionMatch{{Type: "Running", Status: "True"}, {Type: "Restarting", Status: "True"}}
)

type condition struct {
	Type, Status, Reason, Message string
}

func conditionsOf(obj *unstructured.Unstructured) []condition {
	raw, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	out := make([]condition, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		str := func(k string) string { s, _ := m[k].(string); return s }
		out = append(out, condition{Type: str("type"), Status: str("status"), Reason: str("reason"), Message: str("message")})
	}
	return out
}

func matching(conds []condition, matches []v1.ConditionMatch) (condition, bool) {
	for _, m := range matches {
		want := m.Status
		if want == "" {
			want = "True"
		}
		for _, c := range conds {
			if c.Type == m.Type && c.Status == want {
				return c, true
			}
		}
	}
	return condition{}, false
}

func orDefault(matches, fallback []v1.ConditionMatch) []v1.ConditionMatch {
	if len(matches) == 0 {
		return fallback
	}
	return matches
}

// manifestState maps the object's status onto a Zenthros phase by the
// contract. Success is checked first, as a Job's Complete wins over a stale
// Failed. A failure's message is the operator's condition as written:
// "<Kind>/<name> condition <Type>=<Status> reason=<reason>: <message>".
func manifestState(obj *unstructured.Unstructured, c manifestContract) backend.JobState {
	label := obj.GetKind() + "/" + obj.GetName()
	if c.PhasePath != nil {
		return phaseState(obj, label, c.PhasePath)
	}
	conds := conditionsOf(obj)
	if _, ok := matching(conds, orDefault(c.SuccessWhen, defaultSuccessWhen)); ok {
		return backend.JobState{Phase: v1.PhaseSucceeded}
	}
	if cond, ok := matching(conds, orDefault(c.FailureWhen, defaultFailureWhen)); ok {
		msg := fmt.Sprintf("%s condition %s=%s reason=%s: %s", label, cond.Type, cond.Status, cond.Reason, cond.Message)
		return failedState(msg)
	}
	if cond, ok := matching(conds, orDefault(c.RunningWhen, defaultRunningWhen)); ok {
		return backend.JobState{Phase: v1.PhaseRunning, Message: fmt.Sprintf("%s condition %s=%s", label, cond.Type, cond.Status)}
	}
	if active, found, _ := unstructured.NestedInt64(obj.Object, "status", "active"); found && active > 0 {
		return backend.JobState{Phase: v1.PhaseRunning, Message: fmt.Sprintf("%s status.active=%d", label, active)}
	}
	return backend.JobState{Phase: v1.PhaseSubmitted, Message: label + " applied; waiting for its operator to report it running"}
}

func failedState(msg string) backend.JobState {
	return backend.JobState{Phase: v1.PhaseFailed, Message: msg,
		Reasons: []v1.Reason{{Code: v1.ReasonProcessFailed, Detail: msg}}}
}

// phaseState reads a phase string at a dotted path. A failure carries the
// object's status.message, if it has one, after the phase.
func phaseState(obj *unstructured.Unstructured, label string, p *v1.PhasePath) backend.JobState {
	phase, _, _ := unstructured.NestedString(obj.Object, strings.Split(p.Path, ".")...)
	seen := fmt.Sprintf("%s %s=%q", label, p.Path, phase)
	switch {
	case phase == "":
		return backend.JobState{Phase: v1.PhaseSubmitted, Message: label + " applied; " + p.Path + " not set yet"}
	case slices.Contains(p.Succeeded, phase):
		return backend.JobState{Phase: v1.PhaseSucceeded}
	case slices.Contains(p.Failed, phase):
		if detail, _, _ := unstructured.NestedString(obj.Object, "status", "message"); detail != "" {
			seen += ": " + detail
		}
		return failedState(seen)
	case slices.Contains(p.Running, phase):
		return backend.JobState{Phase: v1.PhaseRunning, Message: seen}
	}
	return backend.JobState{Phase: v1.PhaseSubmitted, Message: seen}
}

func readManifest(e *Executor, ctx context.Context, _ client.Client, r ref) (client.Object, backend.JobState, error) {
	obj, err := e.getManifest(ctx, r)
	if err != nil {
		return nil, backend.JobState{}, err
	}
	contract := readContract(obj)
	return obj, manifestEndpoints(obj, contract, manifestState(obj, contract)), nil
}

// manifestPodsOf are the object's pods (manifestPods); a list that failed
// reads as none.
func manifestPodsOf(e *Executor, ctx context.Context, _ client.Client, obj client.Object) []corev1.Pod {
	manifest := obj.(*unstructured.Unstructured)
	pods, _ := e.manifestPods(ctx, manifest, readContract(manifest))
	return pods
}

// manifestEndpoints reads the URLs a running Service object publishes, by
// its endpointFrom. All of them or none: a path not set yet, or not an
// absolute URL, is why there are none.
func manifestEndpoints(obj *unstructured.Unstructured, c manifestContract, state backend.JobState) backend.JobState {
	if state.Phase != v1.PhaseRunning || len(c.EndpointFrom) == 0 {
		return state
	}
	label := obj.GetKind() + "/" + obj.GetName()
	endpoints := make([]v1.Endpoint, 0, len(c.EndpointFrom))
	for _, from := range c.EndpointFrom {
		raw, _, _ := unstructured.NestedString(obj.Object, strings.Split(from.Path, ".")...)
		if raw == "" {
			state.EndpointProblem = fmt.Sprintf("WaitingForPort: %s has no %s yet", label, from.Path)
			return state
		}
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" {
			state.EndpointProblem = fmt.Sprintf("EndpointUnresolved: %s %s=%q is not an absolute URL", label, from.Path, raw)
			return state
		}
		endpoints = append(endpoints, v1.Endpoint{Name: from.Name, URL: raw, Scope: urlScope(u.Hostname())})
	}
	state.Endpoints = endpoints
	return state
}

// urlScope: a cluster-local name is reached inside the cluster only.
func urlScope(host string) string {
	if strings.HasSuffix(host, ".svc") || strings.Contains(host, ".svc.") || strings.HasSuffix(host, ".cluster.local") {
		return v1.EndpointScopeCluster
	}
	return v1.EndpointScopePublic
}
