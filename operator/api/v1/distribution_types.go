// SPDX-License-Identifier: Apache-2.0

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// DistributionMode says how the tasks of a distributed job relate. Only
// Independent exists: tightly coupled multi-node work runs on Kubernetes
// with a real operator (kind Manifest: PyTorchJob, KubeRay). The field is
// kept so another mode can be added without breaking the API.
// +kubebuilder:validation:Enum=Independent
type DistributionMode string

const (
	// DistributionIndependent runs count tasks that never talk to each other:
	// each reads its shard (TASK_INDEX of TASK_COUNT) and writes its own
	// output. A lost task is re-seated alone; the others keep running.
	DistributionIndependent DistributionMode = "Independent"
)

// FailurePolicy values for Distribution.
const (
	// FailurePolicyFailFast fails the job, and cancels the other tasks, as
	// soon as one task has failed for good.
	FailurePolicyFailFast = "FailFast"
	// FailurePolicyContinueOnFailure lets every task finish; the job fails at
	// the end if any task did.
	FailurePolicyContinueOnFailure = "ContinueOnFailure"
)

// Distribution splits a Job into count Independent tasks, fixed when the job
// is created. For work a job splits while it runs, see spawn (fan-out).
//
// On machines each task is a ZenthrosJob of its own, named <job>-<index>,
// with its own seat, lease and logs. On a Kubernetes cluster the job becomes
// one Indexed batch Job.
type Distribution struct {
	// Mode is Independent.
	Mode DistributionMode `json:"mode"`

	// Count is how many tasks run.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=256
	Count int32 `json:"count"`

	// TasksPerNode is how many tasks may share one machine; the machine must
	// run that many jobs at once. Default 1.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	TasksPerNode int32 `json:"tasksPerNode,omitempty"`

	// FailurePolicy is FailFast (default) or ContinueOnFailure.
	// +optional
	// +kubebuilder:validation:Enum=FailFast;ContinueOnFailure
	FailurePolicy string `json:"failurePolicy,omitempty"`
}

// EffectiveTasksPerNode is TasksPerNode with its default.
func (d *Distribution) EffectiveTasksPerNode() int32 {
	if d == nil || d.TasksPerNode <= 0 {
		return 1
	}
	return d.TasksPerNode
}

// EffectiveFailurePolicy is FailurePolicy with its default.
func (d *Distribution) EffectiveFailurePolicy() string {
	if d == nil || d.FailurePolicy == "" {
		return FailurePolicyFailFast
	}
	return d.FailurePolicy
}

// Distributed reports whether the job runs as more than one task.
func (s ZenthrosJobSpec) Distributed() bool {
	d := s.Workload.Template.Distribution
	return d != nil && d.Count > 1
}

// Labels and annotations that tie a task to its distributed job.
const (
	// LabelMemberOf on a task names the distributed job it belongs to.
	LabelMemberOf = "zenthros.ai/member-of"
	// LabelMemberRank is the task's index (TASK_INDEX).
	LabelMemberRank = "zenthros.ai/member-rank"
	// AnnotationQueuedAt on a task (or a spawned child) is when its parent
	// queued, so it queues in its parent's place, not its own.
	AnnotationQueuedAt = "zenthros.ai/queued-at"
)

// Env a task sees.
const (
	EnvTaskIndex = "TASK_INDEX"
	EnvTaskCount = "TASK_COUNT"
)

// ReasonMemberFailed: a task failed; its words follow.
const ReasonMemberFailed = "MEMBER_FAILED"

// MemberStatus is one task of a distributed job, as the task job says.
type MemberStatus struct {
	Rank int32 `json:"rank"`
	// Job is the task's ZenthrosJob.
	Job string `json:"job"`
	// BackendID is the machine the task is on.
	// +optional
	BackendID string `json:"backendID,omitempty"`
	// +optional
	CapacityRequest string `json:"capacityRequest,omitempty"`
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// Message is the task's status.message, verbatim.
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	Zone string `json:"zone,omitempty"`
	// +optional
	Region string `json:"region,omitempty"`
	// +optional
	PreemptedAt *metav1.Time `json:"preemptedAt,omitempty"`
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`
}

// DistributionStatus counts the tasks by phase.
type DistributionStatus struct {
	Running   int32 `json:"running,omitempty"`
	Succeeded int32 `json:"succeeded,omitempty"`
	Failed    int32 `json:"failed,omitempty"`
}
