// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// powerOffAgent is an agent whose power switch counts its pulls.
func powerOffAgent(ownership infrav1alpha1.BackendOwnership, result error) (*Agent, chan struct{}) {
	pulled := make(chan struct{}, 8)
	a := NewWithExecutor(Config{
		BackendID: "vm-test", BackendType: "vm", Ownership: ownership,
		PowerOff: func(context.Context) error {
			pulled <- struct{}{}
			return result
		},
	}, nil, logr.Discard())
	return a, pulled
}

func askPowerOff(a *Agent) *agentpb.RegisterRequest {
	return a.Dispatch(context.Background(), &agentpb.RegisterResponse{PowerOff: &agentpb.PowerOff{}})
}

func pulls(t *testing.T, pulled chan struct{}, want int) {
	t.Helper()
	for i := 0; i < want; i++ {
		select {
		case <-pulled:
		case <-time.After(2 * time.Second):
			t.Fatalf("the machine was powered off %d times, want %d", i, want)
		}
	}
	select {
	case <-pulled:
		t.Fatalf("the machine was powered off more than %d times", want)
	case <-time.After(50 * time.Millisecond):
	}
}

// The installation's own machine powers off when the hub asks, once however
// many times it is asked, and answers nothing: its going is the answer.
func TestManagedMachinePowersOffWhenTheHubAsks(t *testing.T) {
	a, pulled := powerOffAgent(infrav1alpha1.OwnershipManaged, nil)
	for i := 0; i < 3; i++ {
		if answer := askPowerOff(a); answer != nil {
			t.Fatalf("power off answered %v, want nothing", answer)
		}
	}
	pulls(t, pulled, 1)
}

// A machine somebody joined is theirs: the hub has no say over its power.
func TestJoinedMachineIgnoresPowerOff(t *testing.T) {
	for _, ownership := range []infrav1alpha1.BackendOwnership{infrav1alpha1.OwnershipExternal, ""} {
		a, pulled := powerOffAgent(ownership, nil)
		askPowerOff(a)
		pulls(t, pulled, 0)
	}
}

// An agent with no power switch (a container, a simulator) ignores the ask.
func TestAgentWithoutAPowerSwitchIgnoresPowerOff(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm", Ownership: infrav1alpha1.OwnershipManaged}, nil, logr.Discard())
	if answer := askPowerOff(a); answer != nil {
		t.Fatalf("power off answered %v, want nothing", answer)
	}
}

// A power off that failed is tried again when the hub asks again.
func TestFailedPowerOffIsTriedAgainAtTheNextAsk(t *testing.T) {
	a, pulled := powerOffAgent(infrav1alpha1.OwnershipManaged, errors.New("systemctl: access denied"))
	askPowerOff(a)
	pulls(t, pulled, 1)
	deadline := time.Now().Add(2 * time.Second)
	for a.poweringOff.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	askPowerOff(a)
	pulls(t, pulled, 1)
}
