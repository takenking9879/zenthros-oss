// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/zenthros/operator/internal/grpc/envelope"
	"github.com/zenthros/operator/internal/grpc/errorclass"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/errtext"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/metrics"
	"github.com/zenthros/operator/pkg/owner"
)

// Dispatch routes a command to its handler and wraps the result in a response that
// echoes the command's request id. Returns nil for an unrecognized message or a
// message that requires no response (e.g. HeartbeatAck).
//
// ServeStream calls it through commandLanes: the Submits and Cancels of one
// job run one at a time, in arrival order, and Prepares likewise among
// themselves; different jobs' commands run beside each other, bounded. Status
// runs on its own bounded lane. An executor must therefore take concurrent
// Submits and Cancels of different jobs: whatever they share (a namespace, a
// systemd daemon-reload) has to tolerate a concurrent writer.
func (a *Agent) Dispatch(ctx context.Context, cmd *agentpb.RegisterResponse) *agentpb.RegisterRequest {
	switch {
	case cmd.GetSubmit() != nil:
		return &agentpb.RegisterRequest{SubmitResponse: a.Submit(ctx, cmd.GetSubmit())}
	case cmd.GetCancel() != nil:
		return &agentpb.RegisterRequest{CancelResponse: a.Cancel(ctx, cmd.GetCancel())}
	case cmd.GetStatus() != nil:
		return &agentpb.RegisterRequest{StatusResponse: a.Status(ctx, cmd.GetStatus())}
	case cmd.GetPrepare() != nil:
		return &agentpb.RegisterRequest{PrepareResponse: a.Prepare(ctx, cmd.GetPrepare())}
	case cmd.GetPowerOff() != nil:
		a.powerOff()
		return nil
	case cmd.GetHeartbeatAck() != nil:
		return nil
	case cmd.GetExecutionReportAck() != nil:
		if a.forwarder != nil {
			a.forwarder.HandleAck(ctx, cmd.GetExecutionReportAck())
		}
		return nil
	case cmd.GetCheckpointAck() != nil:
		if a.checkpoints != nil {
			a.checkpoints.HandleAck(cmd.GetCheckpointAck())
		}
		return nil
	case cmd.GetChildSpawnAck() != nil:
		if a.children != nil {
			a.children.HandleAck(cmd.GetChildSpawnAck())
		}
		return nil
	default:
		a.logger.Info("ignoring unrecognized control message")
		return nil
	}
}

// Submit decodes reference-only intent and delegates destination-specific handling
// to the executor. Kubernetes SecretSources are resolved by the Kubernetes executor.
// AWS Secrets Manager references are resolved by the VM executor using worker identity.
func (a *Agent) Submit(ctx context.Context, req *agentpb.SubmitRequest) *agentpb.SubmitResponse {
	spec, err := envelope.Decode(req.GetJob())
	if err != nil {
		return submitFailure(req.GetRequestId(), err)
	}
	if err := a.admitKind(ctx, spec.Kind); err != nil {
		return submitFailure(req.GetRequestId(), err)
	}
	// The controller checks capacity on the last heartbeat; two of its
	// workers can both see the last slot free. The agent counts its own.
	release, ok := a.tracker.Claim(spec.ID, a.cfg.MaxJobs)
	if !ok {
		return submitFailure(req.GetRequestId(), fmt.Errorf("%w: all %d job slots are taken", backend.ErrBackendBusy, a.cfg.MaxJobs))
	}
	defer release()

	name, err := a.executor.Submit(ctx, spec, a.injectReportEnv(ctx, &spec, spec.Env))
	if err != nil {
		metrics.RecordAgentExecutorError(metrics.OpSubmit)
		return submitFailure(req.GetRequestId(), err)
	}
	a.tracker.Add(spec.ID, name)
	a.slotsChanged()
	if spec.Service != nil {
		if err := a.services.Put(name, spec.Service); err != nil {
			// It runs and publishes now; a restarted agent would not.
			a.logger.Error(err, "failed to persist the Service's ports", "handle", name)
		}
	}
	a.logger.Info("submitted job", "job_id", spec.ID, "handle", name)
	return &agentpb.SubmitResponse{RequestId: req.GetRequestId(), HandleName: name, ResolvedImage: a.resolvedImage(ctx, spec)}
}

// imageResolver is an executor that can say which digest a job's image
// resolved to once Submit pulled it.
type imageResolver interface {
	ResolvedImage(ctx context.Context, spec jobspec.JobSpec) string
}

// resolvedImage is the job's image pinned by digest, when the executor knows
// it at Submit. The controller sends it to the job's later attempts, so a
// replacement or a retry runs the same code as the first run (F11).
func (a *Agent) resolvedImage(ctx context.Context, spec jobspec.JobSpec) string {
	resolver, ok := a.executor.(imageResolver)
	if !ok || spec.Image == "" {
		return ""
	}
	return resolver.ResolvedImage(ctx, spec)
}

// handleCancel stops and removes the job's unit. Idempotent. When the workload had
// already reached a terminal phase, the elapsed time since then is recorded as
// teardown duration (terminal → resources cleaned up).
func (a *Agent) Cancel(ctx context.Context, req *agentpb.CancelRequest) *agentpb.CancelResponse {
	if req.GetHandleName() == "" {
		// A Submit whose reply was lost: the controller knows the job, not
		// the handle. Cancel what runs under that
		// job; nothing is nothing to cancel.
		handle, ok := a.tracker.HandleOf(req.GetId())
		if req.GetId() == "" || !ok {
			a.logger.Info("cancel by job ID: no job of that ID runs here", "job_id", req.GetId())
			return &agentpb.CancelResponse{RequestId: req.GetRequestId(), Cancelled: true}
		}
		req = &agentpb.CancelRequest{RequestId: req.GetRequestId(), BackendId: req.GetBackendId(), Id: req.GetId(), HandleName: handle}
	}
	if req.GetId() == "" {
		// The job's ID is whose the handle's objects are; without it a
		// Kubernetes executor could go only by name (K1/06#1).
		err := fmt.Errorf("%w: cancel of %q without the job's ID", backend.ErrInvalid, req.GetHandleName())
		return &agentpb.CancelResponse{RequestId: req.GetRequestId(), Error: err.Error(), ErrorClass: errorclass.Encode(err)}
	}
	// Stopping the unit ends its process with SIGTERM, and until the unit is
	// removed a status read shows that end. The watcher must not push it: the
	// controller cancelled this job and records how it ended.
	a.tracker.BeginCancel(req.GetHandleName())
	defer a.tracker.EndCancel(req.GetHandleName())
	if err := a.executor.Cancel(ctx, req.GetHandleName(), owner.Ref{Kind: "ZenthrosJob", JobID: req.GetId()}); err != nil {
		metrics.RecordAgentExecutorError(metrics.OpCleanup)
		// A Cancel or Status error without a class says nothing about the
		// job: it stays unclassified, so the controller retries or resubmits.
		// Only Submit treats an executor's own error as a refusal.
		return &agentpb.CancelResponse{RequestId: req.GetRequestId(), Error: errtext.Bound(err.Error(), errtext.MaxWire), ErrorClass: errorclass.Encode(err)}
	}
	if finishedAt, ok := a.tracker.FinishedAt(req.GetHandleName()); ok {
		metrics.ObserveAgentJobTeardown(time.Since(finishedAt))
		a.tracker.Forget(req.GetHandleName())
	}
	// A job cancelled while it ran frees its slot here; one that had ended
	// freed it when its end was delivered.
	held := a.tracker.Tracked(req.GetHandleName())
	a.tracker.Remove(req.GetHandleName())
	if held {
		a.slotsChanged()
	}
	a.services.Delete(req.GetHandleName())
	if a.cfg.PreemptDir != "" && req.GetId() != "" {
		_ = os.Remove(preemptFile(a.cfg.PreemptDir, req.GetId()))
	}
	// The job's reports end with its last run here: the cancel of an old
	// attempt while the next one runs leaves its token and sequence.
	if a.spool != nil && req.GetId() != "" && !a.tracker.RunsJob(req.GetId()) {
		if err := a.spool.EndJob(ctx, req.GetId()); err != nil {
			a.logger.Error(err, "failed to end the job's report spool", "job_id", req.GetId())
		}
	}
	a.logger.Info("cancelled job", "job_id", req.GetId(), "handle", req.GetHandleName())
	return &agentpb.CancelResponse{RequestId: req.GetRequestId(), Cancelled: true}
}

// handleStatus asks the executor for the job's observed state and wraps it for the
// wire. Failure enrichment (e.g. the systemd journal) is the executor's responsibility.
func (a *Agent) Status(ctx context.Context, req *agentpb.StatusRequest) *agentpb.StatusResponse {
	cancels := a.tracker.Cancels()
	state, err := a.executor.Status(ctx, req.GetHandleName())
	if err != nil {
		return &agentpb.StatusResponse{RequestId: req.GetRequestId(), Error: errtext.Bound(err.Error(), errtext.MaxWire), ErrorClass: errorclass.Encode(err)}
	}
	if !state.Phase.Terminal() && req.GetId() != "" {
		// A job this agent lost track of (it restarted, or a spool was lost)
		// is running: watch and count it again.
		a.tracker.Retrack(req.GetId(), req.GetHandleName(), cancels)
	}
	state = a.withEndpoints(ctx, req.GetHandleName(), state)
	a.tracker.Answered(req.GetHandleName(), statusKey(state))
	return &agentpb.StatusResponse{RequestId: req.GetRequestId(), Status: toProtoStatus(state)}
}

// toProtoStatus converts a backend.JobState to the wire JobStatus.
func toProtoStatus(state backend.JobState) *agentpb.JobStatus {
	status := &agentpb.JobStatus{
		Phase:           string(state.Phase),
		Message:         state.Message,
		RetryCount:      state.Restarts,
		ResolvedImage:   state.ResolvedImage,
		EndpointProblem: state.EndpointProblem,
		ExitCode:        state.ExitCode,
	}
	for _, e := range state.Endpoints {
		status.Endpoints = append(status.Endpoints, &agentpb.Endpoint{Name: e.Name, Port: e.Port, Url: e.URL, Scope: e.Scope})
	}
	for _, reason := range state.Reasons {
		status.Reasons = append(status.Reasons, &agentpb.Reason{Code: reason.Code, Detail: reason.Detail})
	}
	if state.StartTime != nil {
		status.StartTimeUnix = state.StartTime.Unix()
	}
	if state.CompletionTime != nil {
		status.CompletionTimeUnix = state.CompletionTime.Unix()
	}
	return status
}

// submitFailure keeps the human text and stamps the stable class so the
// controller can errors.Is after decode. An error without a sentinel goes
// UNSPECIFIED: the controller retries it within the submit budget (U2).
// Known refusals carry their sentinel (c4-A2).
func submitFailure(requestID string, err error) *agentpb.SubmitResponse {
	reason, _ := backend.SecretSourceReason(err)
	return &agentpb.SubmitResponse{
		RequestId:   requestID,
		Error:       errtext.Bound(err.Error(), errtext.MaxWire),
		ErrorClass:  errorclass.Encode(err),
		ErrorReason: reason,
	}
}
