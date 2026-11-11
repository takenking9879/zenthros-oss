// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/liveness"
	"github.com/zenthros/operator/pkg/owner"
)

// blockingStream simulates a zombie control stream: Recv never returns (a half-open
// TCP connection with no keepalive) and Send is a no-op.
type blockingStream struct{}

func (blockingStream) Send(*agentpb.RegisterRequest) error { return nil }
func (blockingStream) Recv() (*agentpb.RegisterResponse, error) {
	select {} // block forever, like a dead controller with no FIN
}

type registrationStream struct {
	sent *agentpb.RegisterRequest
}

func (s *registrationStream) Send(request *agentpb.RegisterRequest) error {
	s.sent = request
	return nil
}

func (*registrationStream) Recv() (*agentpb.RegisterResponse, error) {
	return &agentpb.RegisterResponse{Accepted: true}, nil
}

func TestRegisterAdvertisesOwnership(t *testing.T) {
	tests := []struct {
		name      string
		ownership infrav1alpha1.BackendOwnership
		want      string
	}{
		{name: "manual defaults external", want: "External"},
		{name: "managed explicit", ownership: infrav1alpha1.OwnershipManaged, want: "Managed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm", Ownership: tt.ownership}, SystemdExecutor{}, logr.Discard())
			stream := &registrationStream{}
			if err := a.Register(context.Background(), stream); err != nil {
				t.Fatalf("Register: %v", err)
			}
			if got := stream.sent.GetAdvertise().GetOwnership(); got != tt.want {
				t.Fatalf("ownership = %q, want %q", got, tt.want)
			}
		})
	}
}

// The agent advertises the wire it was built from: a Gateway of another
// refuses it at Register (envelope.Protocol).
func TestRegisterAdvertisesItsWire(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, SystemdExecutor{}, logr.Discard())
	stream := &registrationStream{}
	if err := a.Register(context.Background(), stream); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := stream.sent.GetAdvertise().GetWireProtocol(); got != envelope.Protocol() {
		t.Fatalf("wire protocol = %q, want %q", got, envelope.Protocol())
	}
}

func TestValidateForRunRejectsUnknownOwnership(t *testing.T) {
	cfg := Config{ControllerAddr: "controller:9443", BackendID: "vm-test", BackendType: "vm", Ownership: "Borrowed"}
	err := cfg.ValidateForRun()
	if err == nil || !strings.Contains(err.Error(), "ownership must be External or Managed") {
		t.Fatalf("ValidateForRun() error = %v", err)
	}
}

// TestServeStreamUnblocksOnContextCancel guards the reconnect path: with keepalive the
// connection context is cancelled on failure, and serveStream must return promptly
// instead of hanging in Recv so Run can redial.
func TestServeStreamUnblocksOnContextCancel(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, SystemdExecutor{}, logr.Discard())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- a.ServeStream(ctx, blockingStream{}) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ServeStream returned %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStream did not return after context cancellation; it would leave the agent on a zombie stream")
	}
}

// TestServeStreamRecvPumpUnblocksOnCancel: Recv keeps delivering while Dispatch
// is blocked in Submit. Cancel must return ServeStream even if the pump is
// mid-send into the one-slot result channel.
func TestServeStreamRecvPumpUnblocksOnCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	job, err := envelope.Encode(jobspec.JobSpec{
		ID: "uid-pump", Kind: v1.WorkloadKindJob, Entrypoint: "echo hi",
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, blockingExecutor{started: started, release: release}, logr.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- a.ServeStream(ctx, &floodSubmitStream{job: job}) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Dispatch never entered Submit")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ServeStream returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStream stayed blocked in the Recv pump after cancel")
	}
}

type floodSubmitStream struct {
	job   *agentpb.JobEnvelope
	recvs atomic.Int32
}

func (s *floodSubmitStream) Send(*agentpb.RegisterRequest) error { return nil }

func (s *floodSubmitStream) Recv() (*agentpb.RegisterResponse, error) {
	s.recvs.Add(1)
	return &agentpb.RegisterResponse{Submit: &agentpb.SubmitRequest{RequestId: "r-pump", Job: s.job}}, nil
}

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (e blockingExecutor) Submit(ctx context.Context, _ jobspec.JobSpec, _ map[string]string) (string, error) {
	select {
	case <-e.started:
	default:
		close(e.started)
	}
	select {
	case <-e.release:
		return "handle", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (blockingExecutor) Cancel(context.Context, string, owner.Ref) error { return nil }
func (blockingExecutor) Status(context.Context, string) (backend.JobState, error) {
	return backend.JobState{}, nil
}
func (blockingExecutor) Logs(context.Context, string, bool, int) (<-chan backend.LogChunk, error) {
	ch := make(chan backend.LogChunk)
	close(ch)
	return ch, nil
}

// refusedStream answers the advertisement with a refused ack, then ends as a
// full Gateway does.
type refusedStream struct{ acked bool }

func (*refusedStream) Send(*agentpb.RegisterRequest) error { return nil }

func (s *refusedStream) Recv() (*agentpb.RegisterResponse, error) {
	if !s.acked {
		s.acked = true
		return &agentpb.RegisterResponse{Accepted: false, Message: liveness.GatewayFull}, nil
	}
	return nil, liveness.FullError("register: " + liveness.GatewayFull)
}

// A full Gateway refuses in the ack, then ends the stream with the typed
// refusal. The agent reads that end, so it retries soon by the reason and
// not by the ack's text (c4-E7).
func TestARefusedAckReadsTheGatewaysReason(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, SystemdExecutor{}, logr.Discard())
	err := a.Register(context.Background(), &refusedStream{})
	if !liveness.RefusedFull(err) {
		t.Fatalf("Register = %v, want a refusal of a full Gateway", err)
	}
}
