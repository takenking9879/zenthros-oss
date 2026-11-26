// SPDX-License-Identifier: Apache-2.0

// Package errorclass encodes and decodes the stable Submit error class on the
// control channel. The class is the pkg/backend sentinel set, not a
// second taxonomy: every sentinel a Submit can return crosses the wire intact,
// transient ones included, so the controller retries what the agent said was
// temporary and fails only what the agent refused.
package errorclass

import (
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "github.com/takenking9879/zenthros-oss/operator/internal/grpc/gen"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
	"github.com/takenking9879/zenthros-oss/operator/pkg/fault"
)

// classes pairs each wire class with its sentinel. Order matters only for
// errors that wrap more than one sentinel: the first match wins.
var classes = []struct {
	class    agentpb.ErrorClass
	sentinel error
}{
	{agentpb.ErrorClass_SPEC_MISMATCH, backend.ErrSpecMismatch},
	{agentpb.ErrorClass_UNSUPPORTED_WORKLOAD_KIND, backend.ErrUnsupportedKind},
	{agentpb.ErrorClass_UNSUPPORTED_BACKEND, backend.ErrUnsupportedBackend},
	{agentpb.ErrorClass_IMAGE_PULL_FAILED, backend.ErrImagePull},
	{agentpb.ErrorClass_QUOTA_EXCEEDED, backend.ErrQuotaExceeded},
	{agentpb.ErrorClass_ADMISSION_REJECTED, backend.ErrAdmissionRejected},
	{agentpb.ErrorClass_INVALID_SECRET_SOURCE, backend.ErrSecretSource},
	{agentpb.ErrorClass_FORBIDDEN, backend.ErrForbidden},
	{agentpb.ErrorClass_INVALID, backend.ErrInvalid},
	{agentpb.ErrorClass_NOT_FOUND, backend.ErrNotFound},
	{agentpb.ErrorClass_REJECTED, backend.ErrRejected},
	{agentpb.ErrorClass_BACKEND_UNAVAILABLE, backend.ErrBackendUnavailable},
	{agentpb.ErrorClass_DESTINATION_UNAVAILABLE, backend.ErrDestinationUnavailable},
	{agentpb.ErrorClass_BACKEND_BUSY, backend.ErrBackendBusy},
	{agentpb.ErrorClass_RATE_LIMITED, backend.ErrRateLimited},
	{agentpb.ErrorClass_QUEUE_FULL, backend.ErrQueueFull},
}

// Encode maps a local Submit error onto the wire class. An error without a
// sentinel says nothing certain about the job (a dbus timeout, a full disk, a
// pull the network cut: c4-A2) and stays UNSPECIFIED, which the controller
// retries within its submit budget. A refusal of the job itself carries its
// sentinel (ErrRejected and the rest).
func Encode(err error) agentpb.ErrorClass {
	if err == nil {
		return agentpb.ErrorClass_UNSPECIFIED
	}
	for _, entry := range classes {
		if errors.Is(err, entry.sentinel) {
			return entry.class
		}
	}
	return agentpb.ErrorClass_UNSPECIFIED
}

// Decode rebuilds a local error the controller can errors.Is. UNSPECIFIED and
// unknown classes stay a plain gRPC Internal so Classify remains transient.
func Decode(class agentpb.ErrorClass, message string) error {
	if message == "" {
		return nil
	}
	sentinel := sentinel(class)
	if sentinel == nil {
		return status.Error(codes.Internal, message)
	}
	return &wireError{sentinel: sentinel, message: message}
}

func sentinel(class agentpb.ErrorClass) error {
	for _, entry := range classes {
		if entry.class == class {
			return entry.sentinel
		}
	}
	return nil
}

// wireError keeps the human message and unwraps to the backend sentinel.
type wireError struct {
	sentinel error
	message  string
}

func (e *wireError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func (e *wireError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.sentinel
}

func (e *wireError) GRPCStatus() *status.Status {
	if e == nil {
		return status.New(codes.Internal, "")
	}
	return status.New(codes.Internal, e.message)
}

// StreamClosed is the Gateway's answer to a command whose agent stream
// closed after it was sent: the agent may have acted on it, so a Submit that
// meets it is in doubt, a transient error the controller retries (the agent
// adopts the run), never ErrBackendUnavailable (pipeclient
// TestSubmitLostAfterDeliveryIsRetried). The soak and the controller's
// tests lose a Submit with this very error.
var StreamClosed = status.Error(codes.Unavailable, "agent stream closed while awaiting response")

// StreamClosedBy is StreamClosed with why the stream closed, its code
// unchanged: in doubt as ever. "agent stream closed while awaiting response"
// alone told nobody why a first submit to a machine just enrolled failed
// (23.2).
func StreamClosedBy(cause error) error {
	if cause == nil {
		return StreamClosed
	}
	return fault.GRPC(fault.Wrap(fault.Transient, status.Convert(StreamClosed).Message(), cause))
}

// jobAbsent marks the Gateway's answer about a ZenthrosJob that is not
// there (never created, deleted, collected after its TTL), apart from any
// other NotFound: zctl exits with its own code for it, which the SDK reads.
var jobAbsent = &errdetails.ErrorInfo{Domain: "zenthros.io", Reason: "JOB_ABSENT"}

// JobAbsent is the Gateway's answer about job namespace/name, which is not
// there.
func JobAbsent(namespace, name string) error {
	absent, err := status.New(codes.NotFound, fmt.Sprintf("zenthrosjob %q not found in namespace %q", name, namespace)).WithDetails(jobAbsent)
	if err != nil {
		// WithDetails fails only for codes.OK.
		panic(err)
	}
	return absent.Err()
}

// IsJobAbsent reports whether err is JobAbsent's answer: by its detail,
// never its words.
func IsJobAbsent(err error) bool { return hasDetail(err, jobAbsent) }

// logGone marks a log its machine no longer keeps (27): a machine keeps the
// logs of its last jobs, and frees the older ones.
var logGone = &errdetails.ErrorInfo{Domain: "zenthros.io", Reason: "LOG_GONE"}

// LogEnded is the Gateway's answer to a log read its agent ended with an
// error of code, in the agent's words: NotFound is a log the machine no
// longer keeps (IsLogGone), which zenthros-api answers 410; any other is the
// agent's failure, Internal. The lab's old job read a 502 (27).
func LogEnded(code codes.Code, said string) error {
	if code != codes.NotFound {
		return status.Error(codes.Internal, said)
	}
	gone, err := status.New(codes.NotFound, said).WithDetails(logGone)
	if err != nil {
		// WithDetails fails only for codes.OK.
		panic(err)
	}
	return gone.Err()
}

// IsLogGone reports whether err is LogEnded's answer about a log the machine
// no longer keeps: by its detail, never its words.
func IsLogGone(err error) bool { return hasDetail(err, logGone) }

// noMachine marks the Gateway's refusal of a log of a job that runs on no
// machine yet: zenthros-api holds a followed one until the job changes.
var noMachine = &errdetails.ErrorInfo{Domain: "zenthros.io", Reason: "NO_MACHINE_YET"}

// NoMachine is the Gateway's refusal of a log of a job that runs on no
// machine yet (seats.RunsOn): not submitted to one, or its lease fenced
// after its machine was lost.
func NoMachine() error {
	refused, err := status.New(codes.FailedPrecondition, "the job runs on no machine yet").WithDetails(noMachine)
	if err != nil {
		// WithDetails fails only for codes.OK.
		panic(err)
	}
	return refused.Err()
}

// IsNoMachine reports whether err is NoMachine's refusal: by its detail,
// never its words.
func IsNoMachine(err error) bool { return hasDetail(err, noMachine) }

// machineAway marks a log read of a machine whose agent no replica holds:
// the machine is detached, stopped, or between two connections. It is an
// answer about the machine, not the Gateway or the hub failing: the hub
// says so at once (zenthros-api 409) instead of having its callers wait for
// it to come back, as they wait out a restart.
var machineAway = &errdetails.ErrorInfo{Domain: "zenthros.io", Reason: "MACHINE_AWAY"}

// MachineAway is a replica's answer to a log read of backendID when the
// backend's agent is not connected to it. Unavailable as before, so a
// replica asks the next one (pipeclient's fan-out).
func MachineAway(backendID string) error {
	away, err := status.New(codes.Unavailable, fmt.Sprintf("no agent connected for backend %q", backendID)).WithDetails(machineAway)
	if err != nil {
		// WithDetails fails only for codes.OK.
		panic(err)
	}
	return away.Err()
}

// IsMachineAway reports whether err is MachineAway's answer: by its detail,
// never its words.
func IsMachineAway(err error) bool { return hasDetail(err, machineAway) }

// notThisReplica marks a Gateway replica's refusal of a LogChannel when
// another replica holds the agent's Register session.
var notThisReplica = &errdetails.ErrorInfo{Domain: "zenthros.io", Reason: "NOT_THIS_REPLICA"}

// NotThisReplica is the Gateway's refusal of a LogChannel that reached a
// replica without the agent's session. Behind a Service, each new connection
// lands on a replica at random, so it is a routing miss to retry at once, not
// a failure to back off from.
func NotThisReplica(backendID string) error {
	refused, err := status.New(codes.Unavailable, fmt.Sprintf("no connected agent for backend %q", backendID)).WithDetails(notThisReplica)
	if err != nil {
		// WithDetails fails only for codes.OK.
		panic(err)
	}
	return refused.Err()
}

// IsNotThisReplica reports whether err is NotThisReplica's refusal, as the
// agent receives it: by its detail, never its words.
func IsNotThisReplica(err error) bool { return hasDetail(err, notThisReplica) }

// hasDetail reports whether err is a gRPC status that carries mark.
func hasDetail(err error, mark *errdetails.ErrorInfo) bool {
	var grpcErr interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &grpcErr) {
		return false
	}
	for _, detail := range grpcErr.GRPCStatus().Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetDomain() == mark.GetDomain() && info.GetReason() == mark.GetReason() {
			return true
		}
	}
	return false
}
