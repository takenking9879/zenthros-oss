// SPDX-License-Identifier: Apache-2.0

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Fan-out: a running job spawns child jobs, each a ZenthrosJob of its own,
// named <parent>-<group>-<index> (docs/fan-out.md).
const (
	// LabelChildOf on a child names the job that spawned it.
	LabelChildOf = "zenthros.ai/child-of"
	// LabelChildGroup is the spawn the child belongs to.
	LabelChildGroup = "zenthros.ai/child-group"
	// LabelChildIndex is the child's TASK_INDEX in its group.
	LabelChildIndex = "zenthros.ai/child-index"
)

// Env a child sees, besides TASK_INDEX and TASK_COUNT.
const (
	// EnvParentJobID is the UID of the job that spawned the child.
	EnvParentJobID = "ZENTHROS_PARENT_JOB_ID"
	// EnvChildGroup is the name of the child's group.
	EnvChildGroup = "ZENTHROS_CHILD_GROUP"
)

// Codes a refused spawn carries, with the controller's words.
const (
	SpawnQuotaExceeded = "SPAWN_QUOTA_EXCEEDED"
	SpawnLeaseNotLive  = "LEASE_NOT_LIVE"
	// SpawnLeasePending: the parent's workload runs but the controller has
	// not recorded its lease yet (a first seat, or a replacement's). The
	// agent answers it as 503, so the job asks again.
	SpawnLeasePending  = "LEASE_PENDING"
	SpawnNoGrandchild  = "NO_GRANDCHILDREN"
	SpawnGroupMismatch = "GROUP_MISMATCH"
	SpawnInvalid       = "SPAWN_INVALID"
	SpawnParentGone    = "PARENT_NOT_FOUND"
	SpawnCreateFailed  = "CHILD_CREATE_FAILED"
	// SpawnUntrustedHost refuses a spawn from a host a third party runs, for
	// a job that does not say it accepts that (AnnotationSpawnOnUntrustedHost).
	SpawnUntrustedHost = "SPAWN_ON_UNTRUSTED_HOST"
)

// ChildGroup is one spawn of child jobs. It is recorded on the parent
// before any child exists, so a replay of the spawn (a replacement parent
// after its machine was lost) finds it and creates nothing twice.
type ChildGroup struct {
	Name  string `json:"name"`
	Count int32  `json:"count"`
	// SpecHash is what the group runs (command, same, env): a replay that
	// asks for something else under the same name is refused.
	SpecHash  string      `json:"specHash"`
	SpawnedAt metav1.Time `json:"spawnedAt"`
	// Generation is the parent's lease generation that spawned it.
	// +optional
	Generation int64 `json:"generation,omitempty"`
	Running    int32 `json:"running,omitempty"`
	Succeeded  int32 `json:"succeeded,omitempty"`
	Failed     int32 `json:"failed,omitempty"`
	// Children are the group's children by index, as each says.
	// +optional
	// +listType=map
	// +listMapKey=index
	Children []ChildStatus `json:"children,omitempty"`
}

// ChildStatus is one child as its own job says.
type ChildStatus struct {
	Index int32  `json:"index"`
	Job   string `json:"job"`
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// Message is the child's status.message, verbatim up to 4 KiB (head
	// and tail kept); the child job has it whole.
	// +optional
	Message string `json:"message,omitempty"`
}
