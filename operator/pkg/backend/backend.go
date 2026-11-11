// SPDX-License-Identifier: Apache-2.0

// Package backend defines the ExecutionBackend contract and the values exchanged
// across it. The Controller factory registers only AgentBackend. The k8s
// subpackage translates JobSpec to RayJob/Spark for spoke Agents; the
// Controller never applies those resources to the hub apiserver.
package backend

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

// Backend type identifiers: the API's backend kinds, defined once there
// (v1.BackendKind*). Secret negotiation reads these and the traits
// (TraitsOf); metrics and enrollment claims, which stay free of this
// package's dependencies, read the API's. None keeps its own strings.
const (
	// TypeK8s is the Agent-advertised spoke label for a Kubernetes cluster
	// (target.rayCluster / Spark). It is not a Factory execution type.
	TypeK8s = v1.BackendKindK8s
	// TypeVM is the Agent-advertised label for a machine running jobs under
	// systemd.
	TypeVM = v1.BackendKindVM
	// TypeContainer is the Agent-advertised label for an agent running
	// inside one unprivileged container, running jobs as its own child
	// processes. See docs/design/container-agent.md.
	TypeContainer = v1.BackendKindContainer
	// TypeAgent is the only Controller Factory execution type.
	TypeAgent = "agent"
)

// JobHandle identifies a submitted job in a backend. It is the reference for
// Status and Cancel.
type JobHandle struct {
	// ID is the idempotency key (the ZenthrosJob UID).
	ID string

	// TargetBackend is the backend id (spec.target.backend) the job was routed to.
	// Agent-backed jobs use it to find the owning agent on Status/Cancel.
	TargetBackend string

	// Namespace the native resource lives in.
	Namespace string

	// Name of the native resource (e.g. the RayJob name).
	Name string

	// Kind of the native resource.
	Kind string

	// ResolvedImage is the job's image pinned by digest (repo@sha256:...),
	// when the destination pulled it during Submit.
	ResolvedImage string
}

// InstalledJob is a workload an agent's executor finds already installed:
// the job it runs for and the handle Status and Cancel take.
type InstalledJob struct {
	JobID  string
	Handle string
}

// JobState is the observed state of a backend job, expressed in Zenthros terms.
// Zenthros reflects what the backend reports; it never computes resources itself.
type JobState struct {
	// Phase in Zenthros terms.
	Phase v1.Phase

	// Reasons explain the phase (e.g. INSUFFICIENT_CPU).
	Reasons []v1.Reason

	// Message is a short human-readable summary.
	Message string

	// StartTime when the backend job started running.
	StartTime *metav1.Time

	// CompletionTime when the backend job reached a terminal state.
	CompletionTime *metav1.Time

	// ExitCode is what the workload's process returned, once it ended and
	// where the executor can tell; nil otherwise (killed by a signal, a lost
	// machine, a kind with no one process).
	ExitCode *int32

	// Restarts is the backend-native restart count observed for this workload
	// (a Job's failed pods, KubeRay, Spark). A machine reports none: the
	// controller re-runs a failed process there itself.
	Restarts int32

	// ResolvedImage is the image the workload runs, pinned by digest
	// (repo@sha256:...), once the destination has pulled it.
	ResolvedImage string

	// Endpoints are where a Service answers, once it does (Name, Port, URL,
	// Scope). Empty for any other job, and for a Service not ready yet.
	Endpoints []v1.Endpoint

	// EndpointProblem says why a running Service publishes no endpoint
	// yet: "WaitingForPort: ..." or "EndpointUnresolved: ...".
	EndpointProblem string
}

// LogChunk is a fragment of user-facing log output from a backend job. The agent
// produces these from the backend-native log source (journalctl, pod logs) and the
// controller proxies them to zctl.
type LogChunk struct {
	// Stream is "stdout" or "stderr".
	Stream string

	// Data is the raw log bytes for this chunk.
	Data []byte

	// Timestamp when the log line was produced (zero if unknown).
	Timestamp time.Time
}

// ExecutionBackend is implemented by each backend adapter. Submit is idempotent
// by JobSpec.ID; Cancel and Status are idempotent on the JobHandle. State
// observation is deliberately not part of this contract: the controller uses
// owned-resource events, agent-pushed updates, or Status polling according to
// the backend topology.
type ExecutionBackend interface {
	// Submit creates or reconciles the native resource for the job.
	Submit(ctx context.Context, spec jobspec.JobSpec) (JobHandle, error)

	// Cancel deletes the native resource. Idempotent: a missing job is not an error.
	Cancel(ctx context.Context, handle JobHandle) error

	// Status reads the current state of the native resource.
	Status(ctx context.Context, handle JobHandle) (JobState, error)
}

// Resubmitter is a backend that tells a job's first Submit from the Submit
// of a job it already holds: the workload was submitted to that backend and
// the job has its lease there. It is for the agent to adopt the run with
// what changed since, not new work, so what admits new work (the backend's
// free slots) is not asked of it: the job is what fills its slot.
type Resubmitter interface {
	Resubmit(ctx context.Context, spec jobspec.JobSpec) (JobHandle, error)
}
