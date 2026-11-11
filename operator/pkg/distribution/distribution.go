// SPDX-License-Identifier: Apache-2.0

// Package distribution decides what a job split into Independent tasks does:
// the env each task sees and what the tasks' phases add up to (whether its
// seats keep the placement rules is pkg/placement's taskSeatRule). It knows no Kubernetes
// client; the controller feeds it what it read and writes what it says.
package distribution

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
)

// FansOut reports whether a distributed job runs its tasks as jobs of
// their own: on machines. A job whose every target is a Kubernetes cluster
// goes to that cluster whole, as one Indexed Job. isCluster tells whether a
// backend is one. The controller makes the tasks of a job that fans out,
// and the Scheduler places the tasks, never the job.
func FansOut(spec v1.ZenthrosJobSpec, isCluster func(backendID string) bool) bool {
	if !spec.Distributed() {
		return false
	}
	if spec.NamesPool() {
		return true
	}
	names := spec.Target.NamedBackends()
	if len(names) == 0 || slices.Contains(spec.Target.Backends, v1.BackendFallbackAny) {
		return true
	}
	return slices.ContainsFunc(names, func(id string) bool { return !isCluster(id) })
}

// MemberName is the task job of rank: <job>-<rank>.
func MemberName(job string, rank int32) string {
	return fmt.Sprintf("%s-%d", job, rank)
}

// RankEnv is the env a task sees.
func RankEnv(d *v1.Distribution, rank int32) map[string]string {
	return map[string]string{
		v1.EnvTaskIndex: strconv.Itoa(int(rank)),
		v1.EnvTaskCount: strconv.Itoa(int(d.Count)),
	}
}

// Member is what a member job says about itself.
type Member struct {
	Rank    int32
	Present bool
	Phase   v1.Phase
	// Reason is the member's reason code.
	Reason  string
	Message string
}

// Outcome is what the members add up to.
type Outcome struct {
	Phase   v1.Phase
	Reason  string
	Message string
	// CancelRest: stop every task still running.
	CancelRest                 bool
	Running, Succeeded, Failed int32
}

// Aggregate adds the tasks up. They fail fast by default (the first task
// that fails, by rank, speaks for the job), or run to the end with
// ContinueOnFailure; the job succeeds when every task has.
func Aggregate(d *v1.Distribution, members []Member) Outcome {
	sorted := append([]Member(nil), members...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Rank < sorted[j].Rank })
	var out Outcome
	var failed []Member
	terminal := int32(0)
	for _, m := range sorted {
		switch {
		case !m.Present:
		case m.Phase == v1.PhaseSucceeded:
			out.Succeeded++
			terminal++
		case m.Phase.Terminal():
			out.Failed++
			terminal++
			failed = append(failed, m)
		case m.Phase == v1.PhaseRunning:
			out.Running++
		}
	}
	count := d.Count
	if len(failed) > 0 && (d.EffectiveFailurePolicy() == v1.FailurePolicyFailFast || terminal == count) {
		first := failed[0]
		out.Phase, out.Reason, out.CancelRest = v1.PhaseFailed, memberReason(first), true
		out.Message = memberMessage(first)
		if len(failed) > 1 {
			out.Message = fmt.Sprintf("%d of %d tasks failed; first: %s", len(failed), count, out.Message)
		}
		return out
	}
	switch {
	case out.Succeeded == count:
		out.Phase, out.Message = v1.PhaseSucceeded, fmt.Sprintf("all %d tasks succeeded", count)
	case out.Running > 0 || terminal > 0:
		out.Phase = v1.PhaseRunning
		out.Message = fmt.Sprintf("%d running, %d succeeded, %d failed of %d tasks", out.Running, out.Succeeded, out.Failed, count)
	default:
		out.Phase, out.Message = v1.PhasePending, fmt.Sprintf("0 of %d tasks running", count)
	}
	return out
}

func memberReason(m Member) string {
	if m.Reason == "" {
		return v1.ReasonMemberFailed
	}
	return m.Reason
}

// memberMessage keeps the member's words and says which member spoke.
func memberMessage(m Member) string {
	return fmt.Sprintf("member rank %d %s: %s", m.Rank, strings.ToLower(string(m.Phase)), m.Message)
}
