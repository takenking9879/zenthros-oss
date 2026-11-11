// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	v1 "github.com/zenthros/operator/api/v1"
)

// RayClusterIdleID is the backend_id label for one exclusive rayClusterRef unit.
func RayClusterIdleID(namespace, name string) string {
	if namespace == "" || name == "" {
		return ""
	}
	return "raycluster/" + namespace + "/" + name
}

// IdleSnapshotsFromRayClusterJobs emits one exclusive idle unit per distinct
// rayClusterRef seen on ZenthrosJobs. Occupied when a non-terminal job holds
// the cluster (running, submitted, or exclusive-hold). Ephemeral jobs without
// a ref are ignored.
func IdleSnapshotsFromRayClusterJobs(jobs []v1.ZenthrosJob) []IdleSnapshot {
	occupied := map[string]int{}
	seen := make([]string, 0)
	index := map[string]bool{}
	for i := range jobs {
		id, ok := rayClusterUnitID(jobs[i])
		if !ok {
			continue
		}
		if !index[id] {
			index[id] = true
			seen = append(seen, id)
		}
		if rayClusterJobOccupies(jobs[i]) {
			occupied[id]++
		}
	}
	out := make([]IdleSnapshot, 0, len(seen))
	for _, id := range seen {
		out = append(out, IdleSnapshot{
			ID:          id,
			Type:        v1.BackendKindK8s,
			Ownership:   "External",
			SharedPool:  false,
			RunningJobs: occupied[id],
		})
	}
	return out
}

func rayClusterUnitID(job v1.ZenthrosJob) (string, bool) {
	ref := job.Spec.Target.RayCluster
	if ref == nil || ref.Name == "" {
		return "", false
	}
	namespace := ref.Namespace
	if namespace == "" {
		namespace = job.Namespace
	}
	id := RayClusterIdleID(namespace, ref.Name)
	return id, id != ""
}

func rayClusterJobOccupies(job v1.ZenthrosJob) bool {
	if job.Status.Phase.Terminal() {
		return false
	}
	if job.Status.Phase == v1.PhaseRunning || job.Status.Phase == v1.PhaseSubmitted {
		return true
	}
	if job.Status.BackendJobName != "" {
		return true
	}
	if job.Annotations != nil && job.Annotations[v1.AnnotationExclusiveHold] != "" {
		return true
	}
	return false
}
