// SPDX-License-Identifier: Apache-2.0

package errorclass

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
)

func TestEncodeDecodePermanentClasses(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		class agentpb.ErrorClass
	}{
		{"spec mismatch", backend.ErrSpecMismatch, agentpb.ErrorClass_SPEC_MISMATCH},
		{"spec mismatch wrapped", backend.NewSpecMismatchError("image", "a", "b"), agentpb.ErrorClass_SPEC_MISMATCH},
		{"unsupported kind", backend.ErrUnsupportedKind, agentpb.ErrorClass_UNSUPPORTED_WORKLOAD_KIND},
		{"unsupported backend", backend.ErrUnsupportedBackend, agentpb.ErrorClass_UNSUPPORTED_BACKEND},
		{"image pull", backend.ErrImagePull, agentpb.ErrorClass_IMAGE_PULL_FAILED},
		{"quota", backend.ErrQuotaExceeded, agentpb.ErrorClass_QUOTA_EXCEEDED},
		{"admission", backend.ErrAdmissionRejected, agentpb.ErrorClass_ADMISSION_REJECTED},
		{"secret source", backend.ErrSecretSource, agentpb.ErrorClass_INVALID_SECRET_SOURCE},
		{"secret source typed", backend.NewSecretSourceError(v1.ReasonSecretKeyNotFound, "missing"), agentpb.ErrorClass_INVALID_SECRET_SOURCE},
		{"rejected", fmt.Errorf("%w: argument list too long", backend.ErrRejected), agentpb.ErrorClass_REJECTED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Encode(tc.err)
			if got != tc.class {
				t.Fatalf("Encode = %v, want %v", got, tc.class)
			}
			decoded := Decode(got, tc.err.Error())
			if !errors.Is(decoded, sentinel(tc.class)) {
				t.Fatalf("Decode errors.Is = %v, want %v", decoded, sentinel(tc.class))
			}
			if decoded.Error() != tc.err.Error() {
				t.Fatalf("Decode message = %q, want %q", decoded.Error(), tc.err.Error())
			}
			if backend.Classify(decoded) != backend.ClassPermanent {
				t.Fatalf("Classify(%v) = %v, want permanent", decoded, backend.Classify(decoded))
			}
		})
	}
}

func TestDecodeUnknownStaysTransient(t *testing.T) {
	cases := []struct {
		name    string
		class   agentpb.ErrorClass
		message string
	}{
		{"unspecified", agentpb.ErrorClass_UNSPECIFIED, "rpc error: code = Internal desc = connection reset"},
		{"unknown class value", agentpb.ErrorClass(99), "boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Decode(tc.class, tc.message)
			if err == nil {
				t.Fatal("expected an error")
			}
			if backend.Classify(err) != backend.ClassTransient {
				t.Fatalf("Classify = %v, want transient", backend.Classify(err))
			}
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.Internal {
				t.Fatalf("status = %v ok=%v, want Internal", err, ok)
			}
		})
	}
}

// A transient refusal from the agent must stay transient after the wire.
// Losing the class turned "runtime still installing" and "busy" into a
// permanent job failure.
func TestTransientSentinelsSurviveTheWire(t *testing.T) {
	for _, sentinel := range []error{
		backend.ErrBackendUnavailable,
		backend.ErrDestinationUnavailable,
		backend.ErrBackendBusy,
		backend.ErrRateLimited,
		backend.ErrQueueFull,
	} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			local := fmt.Errorf("%w: guest runtime is still installing", sentinel)
			decoded := Decode(Encode(local), local.Error())
			if !errors.Is(decoded, sentinel) {
				t.Fatalf("decoded %v does not wrap %v", decoded, sentinel)
			}
			if backend.Classify(decoded) != backend.ClassTransient {
				t.Fatalf("Classify = %v, want transient", backend.Classify(decoded))
			}
		})
	}
}

// Every sentinel a Submit can return has its own class, so adding one to
// pkg/backend without a wire class fails here instead of in production.
func TestEverySubmitSentinelHasAWireClass(t *testing.T) {
	for _, sentinel := range []error{
		backend.ErrSpecMismatch, backend.ErrUnsupportedKind, backend.ErrUnsupportedBackend,
		backend.ErrBackendUnavailable, backend.ErrQuotaExceeded, backend.ErrAdmissionRejected,
		backend.ErrImagePull, backend.ErrSecretSource, backend.ErrBackendBusy,
		backend.ErrRateLimited, backend.ErrQueueFull, backend.ErrRejected,
		backend.ErrDestinationUnavailable, backend.ErrForbidden, backend.ErrInvalid, backend.ErrNotFound,
	} {
		if Encode(sentinel) == agentpb.ErrorClass_UNSPECIFIED {
			t.Errorf("%q has no wire class", sentinel)
		}
	}
}

func TestEncodeUnknownIsUnspecified(t *testing.T) {
	if got := Encode(errors.New("connection reset")); got != agentpb.ErrorClass_UNSPECIFIED {
		t.Fatalf("Encode unknown = %v, want UNSPECIFIED", got)
	}
	if got := Encode(nil); got != agentpb.ErrorClass_UNSPECIFIED {
		t.Fatalf("Encode nil = %v, want UNSPECIFIED", got)
	}
}

// An executor error without a sentinel says nothing certain about the job (a
// dbus timeout, a full disk, a cut pull): it crosses as UNSPECIFIED, which the
// controller retries within its submit budget, never as a refusal (c4-A2).
func TestAnUnclassifiedExecutorErrorStaysTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want agentpb.ErrorClass
	}{
		{"unclassified executor error", errors.New("start unit: dbus: connection timed out"), agentpb.ErrorClass_UNSPECIFIED},
		{"deadline", fmt.Errorf("pull image: %w", context.DeadlineExceeded), agentpb.ErrorClass_UNSPECIFIED},
		{"cancelled", fmt.Errorf("pull image: %w", context.Canceled), agentpb.ErrorClass_UNSPECIFIED},
		{"refusal", fmt.Errorf("%w: source.hostPath is outside this machine", backend.ErrRejected), agentpb.ErrorClass_REJECTED},
		{"classified", fmt.Errorf("%w: busy", backend.ErrBackendBusy), agentpb.ErrorClass_BACKEND_BUSY},
		{"nil", nil, agentpb.ErrorClass_UNSPECIFIED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Encode(tc.err)
			if got != tc.want {
				t.Fatalf("Encode = %v, want %v", got, tc.want)
			}
			if tc.err != nil && got == agentpb.ErrorClass_UNSPECIFIED && backend.Classify(Decode(got, tc.err.Error())) != backend.ClassTransient {
				t.Fatalf("%v decodes to a permanent error", tc.err)
			}
		})
	}
}

func TestDecodeEmptyMessage(t *testing.T) {
	if err := Decode(agentpb.ErrorClass_SPEC_MISMATCH, ""); err != nil {
		t.Fatalf("empty message should decode to nil, got %v", err)
	}
}

// A job the Gateway does not have is known by JobAbsent's detail, through
// any wrapping, never by its words; NotThisReplica is not it, nor the other
// way round.
func TestJobAbsentIsKnownByItsDetail(t *testing.T) {
	absent := JobAbsent("tenant-risk", "train")
	if status.Code(absent) != codes.NotFound || !IsJobAbsent(absent) || !IsJobAbsent(fmt.Errorf("logs: %w", absent)) {
		t.Fatalf("JobAbsent = %v: not known for what it is", absent)
	}
	for name, err := range map[string]error{
		"its words, without its detail": status.Error(codes.NotFound, absent.Error()),
		"another replica's session":     NotThisReplica("ct-1"),
		"no gRPC status":                errors.New(`zenthrosjob "train" not found in namespace "tenant-risk"`),
	} {
		if IsJobAbsent(err) {
			t.Errorf("%s taken for a job absent: %v", name, err)
		}
	}
	if IsNotThisReplica(absent) {
		t.Error("a job absent taken for another replica's session")
	}
}
