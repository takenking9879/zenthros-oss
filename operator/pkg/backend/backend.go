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
// reads it from the job's log (its file on a machine or in a container, the pod's
// log in Kubernetes) and the Gateway forwards it to whoever reads.
type LogChunk struct {
	// Stream is "stdout" or "stderr".
	Stream string

	// Data is the raw log bytes for this chunk.
	Data []byte

	// Timestamp when the log line was produced (zero if unknown).
	Timestamp time.Time

	// Cursor is where a read goes on right after this chunk (LogRequest.After):
	// a reader that lost its connection asks for it and gets every byte once.
	// Empty where the source has none.
	Cursor string

	// Gap is how many bytes of the log, right before Data, never come, and
	// GapReason why: Uncounted when the source no longer knows how many.
	Gap       int64
	GapReason GapReason

	// Omitted, on a read's first chunk, is how many bytes of the log come
	// before where the read started: what its history left out. Uncounted
	// when the source cannot say whether any do (the Kubernetes API serves
	// only the kubelet's latest file of a log).
	Omitted int64
}

// Uncounted, as LogChunk.Gap or Omitted, is a part of the log the source
// cannot count.
const Uncounted = -1

// GapReason is why a part of a log never comes.
type GapReason int

const (
	// GapFreed: the machine freed it, past the cap of the job's file.
	GapFreed GapReason = iota
	// GapBudget: the machine's hourly log budget ran out, and the read went
	// on from where the log was when it came back.
	GapBudget
)

// LogRequest is what a read of a job's log asks for: a history, the end of
// the log as it is, then, when it follows, what the job writes next.
type LogRequest struct {
	// Follow keeps the read open after the history until the job ends.
	Follow bool
	// TailLines makes the history the last TailLines lines; -1 leaves it to
	// HistoryBytes.
	TailLines int
	// HistoryBytes is the most of the log's end the history holds, from the
	// start of a line: 0 is DefaultHistoryBytes, AllHistory all the source
	// keeps.
	HistoryBytes int64
	// After goes on right after a cursor an earlier read gave, with no
	// history. Empty starts with the history.
	After string
}

// DefaultHistoryBytes is the history a read starts with when it names none:
// ~3,600-4,600 lines of training output, more than any terminal keeps
// (benchmarks/loglab/PLAN.md, decision 4).
const DefaultHistoryBytes = 512 << 10

// AllHistory, as LogRequest.HistoryBytes, asks for all the source keeps.
const AllHistory = -1

// History is how many bytes of the log's end the history of req holds, or
// AllHistory.
func (req LogRequest) History() int64 {
	if req.HistoryBytes == 0 {
		return DefaultHistoryBytes
	}
	return req.HistoryBytes
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
