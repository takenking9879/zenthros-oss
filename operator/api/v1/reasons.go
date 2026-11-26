// SPDX-License-Identifier: Apache-2.0

package v1

import "slices"

// Phase is the lifecycle phase of a ZenthrosJob, derived from the native
// phase of the backend resource (RayJob, SparkApplication) that the agent observes.
type Phase string

const (
	PhasePending       Phase = "Pending"
	PhaseSubmitted     Phase = "Submitted"
	PhaseRunning       Phase = "Running"
	PhaseSucceeded     Phase = "Succeeded"
	PhaseFailed        Phase = "Failed"
	PhaseUnschedulable Phase = "Unschedulable"
	PhaseCancelled     Phase = "Cancelled"
)

// Terminal reports whether the phase is final and needs no further reconciliation.
func (p Phase) Terminal() bool {
	return slices.Contains(TerminalPhases, p)
}

// TerminalPhases are the final phases, the one list every selector of the
// jobs that have or have not ended is built from.
var TerminalPhases = []Phase{PhaseSucceeded, PhaseFailed, PhaseCancelled}

// Reason codes explain why a ZenthrosJob is in its current phase. They mirror
// what Kubernetes/KubeRay report; Zenthros reflects them, never computes resources.
const (
	ReasonInsufficientCPU     = "INSUFFICIENT_CPU"
	ReasonInsufficientMemory  = "INSUFFICIENT_MEMORY"
	ReasonInsufficientGPU     = "INSUFFICIENT_GPU"
	ReasonNodeResourceMissing = "NODE_RESOURCE_MISSING"
	ReasonUnschedulable       = "UNSCHEDULABLE"
	ReasonSpecMismatch        = "SPEC_MISMATCH"
	ReasonUnsupportedKind     = "UNSUPPORTED_WORKLOAD_KIND"
	ReasonUnsupportedBackend  = "UNSUPPORTED_BACKEND"
	ReasonBackendUnavailable  = "BACKEND_UNAVAILABLE"
	ReasonBackendForbidden    = "BACKEND_FORBIDDEN"
	ReasonDependencyNotReady  = "DEPENDENCY_NOT_READY"
	// ReasonDependencyFailed ends a job whose dependsOn upstream ended
	// without succeeding; the detail is the upstream's phase and message.
	ReasonDependencyFailed = "DEPENDENCY_FAILED"
	// ReasonDependencyNotFound ends a job whose dependsOn upstream does not
	// exist once the creation grace has passed (deleted, or never created).
	ReasonDependencyNotFound = "DEPENDENCY_NOT_FOUND"
	ReasonSubmissionFailed   = "SUBMISSION_FAILED"
	// ReasonSubmitRetriesExhausted ends a job whose Submit kept failing with
	// transient errors for the whole submit budget (15 minutes); the detail
	// is the last error.
	ReasonSubmitRetriesExhausted = "SUBMIT_RETRIES_EXHAUSTED"
	ReasonQuotaExceeded          = "QUOTA_EXCEEDED"
	ReasonBudgetExceeded         = "BUDGET_EXCEEDED"
	ReasonProvisioningSuspended  = "PROVISIONING_SUSPENDED"
	ReasonAdmissionRejected      = "ADMISSION_REJECTED"
	ReasonImagePullFailed        = "IMAGE_PULL_FAILED"
	// ReasonWorkloadForbidden, ReasonWorkloadInvalid and ReasonWorkloadNotFound
	// carry an API server or operator refusal of the workload itself; the
	// reason's detail is that refusal word for word.
	ReasonWorkloadForbidden        = "WORKLOAD_FORBIDDEN"
	ReasonWorkloadInvalid          = "WORKLOAD_INVALID"
	ReasonWorkloadNotFound         = "WORKLOAD_NOT_FOUND"
	ReasonInvalidSecretSource      = "INVALID_SECRET_SOURCE"
	ReasonUnsupportedSecretSource  = "UNSUPPORTED_SECRET_SOURCE"
	ReasonSecretNotFound           = "SECRET_SOURCE_NOT_FOUND"
	ReasonSecretKeyNotFound        = "SECRET_KEY_NOT_FOUND"
	ReasonSecretNamespaceForbidden = "SECRET_NAMESPACE_FORBIDDEN"
	ReasonSecretAccessDenied       = "SECRET_ACCESS_DENIED"
	ReasonSecretResolutionFailed   = "SECRET_SOURCE_RESOLUTION_FAILED"

	// ReasonProcessFailed reports a workload process that exited non-zero or hit
	// its restart limit: a VM unit (from systemd) or a Ray job's entrypoint
	// (KubeRay AppFailed). Reflected from the backend, never computed by Zenthros.
	ReasonProcessFailed = "PROCESS_FAILED"

	// ReasonOOMKilled reports a job the kernel killed for memory, on
	// evidence of the job's own: its systemd unit's Result (oom-kill), its
	// pod's container (the kubelet's OOMKilled). Where the evidence is a
	// cgroup the job shares (a container's processes), the job is
	// PROCESS_FAILED and its message says it probably was (procagent).
	ReasonOOMKilled = "OOM_KILLED"

	// ReasonCheckpointIncomplete reports a process that exited 0 while a phase
	// it checkpointed never acked its COMPLETE: its work may be unfinished.
	ReasonCheckpointIncomplete = "CHECKPOINT_INCOMPLETE"

	// ReasonLogsNotSaved reports a job whose log its agent saves no more to
	// spec.workload.template.logs, for good: the bucket or its credentials
	// refused it, and no END.json will come. The job runs on.
	ReasonLogsNotSaved = "LOGS_NOT_SAVED"

	// ReasonLogsIncomplete reports a job whose log's copy in its bucket
	// misses a part: lost before it was saved (a queue that stayed full, a
	// machine's file that freed it), or past logs.maxBytes. Its END.json
	// says so. The job runs on.
	ReasonLogsIncomplete = "LOGS_INCOMPLETE"

	// ReasonOutputsNotSaved reports a job whose outputs folder its agent
	// could not save, whole, to its output URI (ZENTHROS_DATA_OUTPUT_URI):
	// the bucket or its credentials refused it, or a file was not one to
	// send. The detail names what was left. It never fails the job.
	ReasonOutputsNotSaved = "OUTPUTS_NOT_SAVED"

	// ReasonProcessRestarting reports a failed process the controller re-runs
	// on a machine, within restartPolicy.backoffLimit; the detail is how the
	// failed attempt ended.
	ReasonProcessRestarting = "PROCESS_RESTARTING"

	// ReasonProviderGone reports the provider instance is gone (Describe
	// NotFound). Overlay on worker-lost when optional Describe says the
	// machine vanished.
	ReasonProviderGone = "PROVIDER_GONE"

	// ReasonPreempted reports a provider interruption (Spot/preempt). Overlay
	// on worker-lost when an agent IMDS/GCP notice (or observe) supplies it.
	ReasonPreempted = "PREEMPTED"

	// ReasonPodEvicted reports a Kubernetes pod removed by eviction, preemption,
	// or a DisruptionTarget condition — not a voluntary process exit.
	ReasonPodEvicted = "POD_EVICTED"

	// ReasonAgentDead reports the destination Agent process is gone (stream
	// closed, no Running agent pod). Distinct from a partition: nothing is left
	// to reconnect.
	ReasonAgentDead = "AGENT_DEAD"

	// ReasonNetworkPartition reports the Agent is believed still running (k8s
	// agent pod Ready, or CapacityRequest still Ready) but the control channel
	// is down.
	ReasonNetworkPartition = "NETWORK_PARTITION"

	// ReasonVMFailed reports the machine still exists at the provider but is
	// Unhealthy/stopped — not a missing instance.
	ReasonVMFailed = "VM_FAILED"

	// Backpressure reasons. These are transient: the job is held Pending
	// and requeued until capacity frees, never terminally failed.

	// ReasonBackendBusy reports a healthy backend that refused a submission because
	// it is at capacity (its agent reports active jobs at/above its advertised max).
	ReasonBackendBusy = "BACKEND_BUSY"

	// ReasonRateLimited reports a submission throttled by a rate limit.
	ReasonRateLimited = "RATE_LIMITED"

	// ReasonDestinationUnavailable reports a submission the destination's
	// own cluster or API turned away for now (429, 5xx, a timeout).
	ReasonDestinationUnavailable = "DESTINATION_UNAVAILABLE"

	// ReasonQueueFull reports a submission refused because the controller's
	// submission queue is at depth.
	ReasonQueueFull = "QUEUE_FULL"

	// Managed-capacity reasons. They are transient while the
	// CapacityRequest is progressing: the job stays Pending and rechecks.

	// ReasonCapacityProvisioning reports managed capacity being created by the
	// provider for the job's CapacityRequest.
	ReasonCapacityProvisioning = "CAPACITY_PROVISIONING"

	// ReasonPlacementUnsatisfied reports a job no seat satisfies
	// spec.placement for; the detail names the rule and why each candidate
	// failed it. The job waits for one.
	ReasonPlacementUnsatisfied = "PLACEMENT_UNSATISFIED"

	// ReasonCapacityBooting reports the provisioned machine booting.
	ReasonCapacityBooting = "CAPACITY_BOOTING"

	// ReasonCapacityAwaitingEnrollment reports the machine waiting to enroll
	// as the job's backend.
	ReasonCapacityAwaitingEnrollment = "CAPACITY_AWAITING_ENROLLMENT"

	// ReasonCapacityResolving reports an ambiguous capacity outcome being
	// resolved through provider describe/find.
	ReasonCapacityResolving = "CAPACITY_RESOLVING"

	// ReasonCapacityFailed reports the job's CapacityRequest reaching a dead
	// end (Failed or quarantined Orphaned); the job cannot run on it.
	ReasonCapacityFailed = "CAPACITY_FAILED"

	// ReasonExclusiveQueued reports a job waiting in FIFO for an exclusive
	// unit: a managed VM slot, a dedicated VM, or a referenced RayCluster.
	ReasonExclusiveQueued = "EXCLUSIVE_QUEUED"

	// ReasonCancelled reports a job whose CRD was deleted. The controller
	// persists PhaseCancelled before removing the backend-cleanup finalizer
	// so clients can observe CANCELLED before the object is gone.
	ReasonCancelled = "CANCELLED"

	// ReasonWorkerLost reports a submitted job whose worker stayed silent past
	// jobGrace: a loss after the job started. Fail policy is terminal;
	// Reschedule substitutes a slot.
	ReasonWorkerLost = "WORKER_LOST"

	// ReasonMachineFailed reports a job whose machine failed to start or to
	// enroll before the job was submitted to it: a loss before the job ran,
	// so it cannot have run twice. It gets another machine within the same
	// workerLostRetryLimit, and onWorkerLost: Fail does not apply. Once the
	// job is submitted, a loss is WORKER_LOST.
	ReasonMachineFailed = "MACHINE_FAILED"

	// ReasonMachineLifetime reports a job whose machine reached its pool's
	// spec.scaling.maxLifetime while it ran. It ends or moves as a lost
	// worker does (onWorkerLost), under its own code.
	ReasonMachineLifetime = "MACHINE_LIFETIME"

	// ReasonServiceEnded reports a Service (a job that runs until cancelled)
	// that its backend reported finished on its own.
	ReasonServiceEnded = "SERVICE_ENDED"
)

// OutOfMemoryAdvice is what a job that ran out of memory says to do, after
// how it is known.
const OutOfMemoryAdvice = "use less memory (read the data in parts, smaller types) or a machine with more"

// ProcessFailure reports a reason of the job's own process ending, as
// opposed to its machine, its lease or its image: what a restartPolicy runs
// again. A Service's process that exited at all (SERVICE_ENDED, even with
// code 0) failed.
func ProcessFailure(code string) bool {
	return code == ReasonProcessFailed || code == ReasonOOMKilled || code == ReasonServiceEnded
}

const (
	// ReasonDeadlineExceeded reports a job that stayed active past
	// spec.activeDeadlineSeconds. The worker-lost lease does not extend it.
	ReasonDeadlineExceeded = "DEADLINE_EXCEEDED"

	// ReasonStopAfter reports a Service the controller stopped (Cancelled)
	// once it had served its spec.workload.template.service.stopAfterSeconds.
	ReasonStopAfter = "STOP_AFTER"
)
