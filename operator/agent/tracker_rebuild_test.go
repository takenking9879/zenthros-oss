// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/systemd"
)

// unitsRunner answers systemctl as a machine with the given job units
// installed, each active or not.
type unitsRunner struct {
	active map[string]bool
	order  []string
}

func (r unitsRunner) Run(_ context.Context, cmd string) (string, error) {
	if strings.Contains(cmd, systemd.JobTarget+".wants") {
		return strings.Join(r.order, " ") + "\n", nil
	}
	for _, unit := range r.order {
		if strings.Contains(cmd, "show "+unit) {
			if r.active[unit] {
				return "ActiveState=active\nSubState=running\nLoadState=loaded\n", nil
			}
			return "ActiveState=inactive\nSubState=dead\nLoadState=loaded\nResult=success\n", nil
		}
	}
	return "", nil
}

// An agent that restarts while a job runs keeps tracking it: the running
// units (a re-run's included, under its job's id) are in running_jobs before
// the first heartbeat, and a finished unit kept by Retain is not.
func TestTrackerRebuiltFromUnits(t *testing.T) {
	runner := unitsRunner{
		order:  []string{"zenthros-uid-1.service", "zenthros-uid-2-a1.service", "zenthros-uid-3.service"},
		active: map[string]bool{"zenthros-uid-1.service": true, "zenthros-uid-2-a1.service": true},
	}
	manager := systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem}, runner, logr.Discard())
	a := NewWithExecutor(Config{BackendID: "vm-a", BackendType: "vm"}, SystemdExecutor{Manager: manager}, logr.Discard())
	// Run tracks them first thing, before it even loads its TLS identity.
	if err := a.Run(context.Background()); err == nil {
		t.Fatal("Run without a TLS identity should fail")
	}
	if a.tracker.Count() != 2 {
		t.Fatalf("tracked %d jobs, want the 2 running units", a.tracker.Count())
	}
	byHandle := map[string]string{}
	for _, job := range a.tracker.Snapshot() {
		byHandle[job.handleName] = job.jobID
	}
	if byHandle["zenthros-uid-1.service"] != "uid-1" || byHandle["zenthros-uid-2-a1.service"] != "uid-2" {
		t.Fatalf("tracked %v", byHandle)
	}
	if a.tracker.rebuild([]InstalledJob{{JobID: "uid-1", Handle: "zenthros-uid-1.service"}}) != 0 {
		t.Fatal("a second rebuild tracked a job twice")
	}
}

type runningExecutor struct {
	mismatchExecutor
	phase v1.Phase
}

func (e runningExecutor) Status(context.Context, string) (backend.JobState, error) {
	return backend.JobState{Phase: e.phase}, nil
}

// The controller asking about a running job the agent does not track (it
// restarted, or its listing failed) makes the agent track it again; a
// finished one is not re-tracked.
func TestStatusReaddsTrackedHandle(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-a"}, runningExecutor{phase: v1.PhaseRunning}, logr.Discard())
	a.Status(context.Background(), &agentpb.StatusRequest{Id: "uid-1", HandleName: "zenthros-uid-1.service"})
	if !a.tracker.Watching("zenthros-uid-1.service") || a.tracker.Count() != 1 {
		t.Fatal("a running job the controller asked about is not tracked")
	}
	done := NewWithExecutor(Config{BackendID: "vm-a"}, runningExecutor{phase: v1.PhaseSucceeded}, logr.Discard())
	done.Status(context.Background(), &agentpb.StatusRequest{Id: "uid-2", HandleName: "zenthros-uid-2.service"})
	if done.tracker.Count() != 0 {
		t.Fatal("a finished job was tracked again")
	}
}

func TestJobOfUnitDropsAttempt(t *testing.T) {
	uid := "f93abbcc-1117-40e1-9860-5309fc85ec73"
	for unit, want := range map[string]string{
		systemd.UnitName(uid):         uid,
		systemd.UnitName(uid + "-a3"): uid,
		"zenthros-agent.service":      "agent",
	} {
		if got := systemd.JobOfUnit(unit); got != want {
			t.Errorf("JobOfUnit(%q) = %q, want %q", unit, got, want)
		}
	}
	if got := systemd.JobIDFromUnit(systemd.UnitName(uid + "-a3")); got != uid+"-a3" {
		t.Errorf("JobIDFromUnit keeps the attempt's own id for its work dir, got %q", got)
	}
}
