// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/capability"
	"github.com/zenthros/operator/pkg/capability/capwire"
)

// declaringExecutor declares whatever the test sets.
type declaringExecutor struct {
	mismatchExecutor
	set  atomic.Pointer[capability.Set]
	host capability.Host
}

func (e *declaringExecutor) Declare(context.Context) (capability.Set, capability.Host) {
	return *e.set.Load(), e.host
}

func newDeclaring(set capability.Set) *declaringExecutor {
	e := &declaringExecutor{host: capability.Host{Arch: "arm64", OSID: "ubuntu", OSVersion: "24.04"}}
	e.set.Store(&set)
	return e
}

// The VM declares containerd only where the probe found it active: a host
// without it must not be offered container jobs.
func TestVMAgentAdvertisesContainerdOnlyWhenProbed(t *testing.T) {
	without, _ := capability.VM(false, capability.RuntimeContainerd).Find(v1.WorkloadKindJob)
	with, _ := capability.VM(true, "").Find(v1.WorkloadKindJob)
	if slices.Contains(without.Runtimes, capability.RuntimeContainerd) {
		t.Fatalf("no containerd: runtimes %v", without.Runtimes)
	}
	if without.DefaultRuntime != capability.RuntimeSystemd {
		t.Fatalf("no containerd must not default to it: %+v", without)
	}
	if !slices.Contains(with.Runtimes, capability.RuntimeContainerd) || with.DefaultRuntime != capability.RuntimeSystemd {
		t.Fatalf("containerd: %+v", with)
	}
	if w, _ := capability.VM(true, capability.RuntimeContainerd).Find(v1.WorkloadKindJob); w.DefaultRuntime != capability.RuntimeContainerd {
		t.Fatalf("declared default dropped: %+v", w)
	}
}

// Register advertises the executor's declaration and its hash.
func TestRegisterAdvertisesDeclaration(t *testing.T) {
	exec := newDeclaring(capability.Kubernetes(v1.WorkloadKindRayJob))
	a := NewWithExecutor(Config{BackendID: "cluster-b", BackendType: "k8s", TailscaleWait: 1}, exec, logr.Discard())
	stream := newRecordingStream()
	stream.recv <- &agentpb.RegisterResponse{Accepted: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Register(ctx, stream); err != nil {
		t.Fatal(err)
	}
	adv := stream.sent[0].GetAdvertise()
	if got := capwire.FromProto(adv.GetCapabilities()).Kinds(); !slices.Equal(got, []string{v1.WorkloadKindRayJob}) {
		t.Fatalf("advertised kinds %v", got)
	}
	if adv.GetHost().GetArch() != "arm64" || adv.GetCapabilityHash() == "" {
		t.Fatalf("advertise host %+v hash %q", adv.GetHost(), adv.GetCapabilityHash())
	}
}

// The job grace the provider set for this agent's link
// (ZENTHROS_JOB_GRACE) is declared with the host facts, in whole seconds,
// so the control plane can wait that long before calling its jobs lost.
func TestRegisterDeclaresTheLinksJobGrace(t *testing.T) {
	exec := newDeclaring(capability.Container())
	a := NewWithExecutor(Config{BackendID: "ct-1", BackendType: "container", TailscaleWait: 1, JobGrace: 75 * time.Second}, exec, logr.Discard())
	stream := newRecordingStream()
	stream.recv <- &agentpb.RegisterResponse{Accepted: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Register(ctx, stream); err != nil {
		t.Fatal(err)
	}
	if got := stream.sent[0].GetAdvertise().GetHost().GetJobGraceSeconds(); got != 75 {
		t.Fatalf("declared job grace %ds, want 75", got)
	}
	if _, host, _ := NewWithExecutor(Config{BackendID: "vm-1"}, newDeclaring(nil), logr.Discard()).declaration(context.Background()); host.JobGraceSeconds != 0 {
		t.Fatalf("an agent with no ZENTHROS_JOB_GRACE declared %ds", host.JobGraceSeconds)
	}
}

// A heartbeat carries the declaration only when it changed since the
// gateway last received one: a cluster where the Spark operator was just
// installed declares it within one heartbeat, and an unchanged one costs
// nothing.
func TestHeartbeatCapabilityRefreshOnHashChange(t *testing.T) {
	exec := newDeclaring(capability.Kubernetes(v1.WorkloadKindRayJob))
	a := NewWithExecutor(Config{BackendID: "cluster-b"}, exec, logr.Discard())
	_, _, first := a.declaration(context.Background())
	a.advertisedHash.Store(first)
	stream := newRecordingStream()
	hb := newHeartbeatSender(a.tracker, newStreamSender(stream), "cluster-b", 0, nil, logr.Discard())
	hb.declare, hb.advertised = a.declaration, &a.advertisedHash

	hb.send()
	if caps := stream.sent[0].GetHeartbeat().GetCapabilities(); len(caps) != 0 {
		t.Fatalf("unchanged declaration resent: %v", caps)
	}

	grown := capability.Kubernetes(v1.WorkloadKindRayJob, v1.WorkloadKindSparkApplication)
	exec.set.Store(&grown)
	hb.send()
	sent := stream.sent[1].GetHeartbeat()
	if got := capwire.FromProto(sent.GetCapabilities()).Kinds(); !slices.Contains(got, v1.WorkloadKindSparkApplication) {
		t.Fatalf("changed declaration not sent: %v", got)
	}
	if sent.GetCapabilityHash() == first {
		t.Fatal("hash did not change with the declaration")
	}

	hb.send()
	if caps := stream.sent[2].GetHeartbeat().GetCapabilities(); len(caps) != 0 {
		t.Fatalf("declaration resent after the gateway had it: %v", caps)
	}
}

// The agent refuses a kind its executor did not declare, and says what it
// runs; the executor never sees the job.
func TestAgentRefusesUndeclaredKind(t *testing.T) {
	exec := newDeclaring(capability.VM(false, ""))
	a := NewWithExecutor(Config{BackendID: "vm-a"}, exec, logr.Discard())
	err := a.admitKind(context.Background(), v1.WorkloadKindRayJob)
	if !errors.Is(err, backend.ErrUnsupportedKind) || !strings.Contains(err.Error(), "runs Job") {
		t.Fatalf("admitKind = %v", err)
	}
	if err := a.admitKind(context.Background(), v1.WorkloadKindJob); err != nil {
		t.Fatalf("declared kind refused: %v", err)
	}
}

// A clock stepped back gave the reconnect an epoch below the session the
// gateway still held: refused as stale until the old stream was reaped
// (09#6). A process's sessions only go forward.
func TestASessionAfterTheClockSteppedBackIsStillNewer(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-1", BackendType: "vm", TailscaleWait: 1}, newDeclaring(nil), logr.Discard())
	ahead := time.Now().Add(time.Minute).UnixNano() // the session before the step
	a.sessionEpoch.Store(ahead)
	stream := newRecordingStream()
	stream.recv <- &agentpb.RegisterResponse{Accepted: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Register(ctx, stream); err != nil {
		t.Fatal(err)
	}
	if got := stream.sent[0].GetAdvertise().GetSessionEpoch(); got <= ahead {
		t.Fatalf("session epoch %d after the clock stepped back, want above the last %d", got, ahead)
	}
}
