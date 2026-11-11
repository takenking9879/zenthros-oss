// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	ctd "github.com/zenthros/operator/pkg/containerd"
	"github.com/zenthros/operator/pkg/systemd"
)

// netnsRunner answers `ip netns list` and lists job units, active unless
// they are in stopped.
type netnsRunner struct {
	recordingRunner
	netns, units string
	stopped      []string
}

func (r *netnsRunner) Run(ctx context.Context, cmd string) (string, error) {
	r.commands = append(r.commands, cmd)
	switch {
	case strings.Contains(cmd, "ip netns list"):
		return r.netns, nil
	case strings.Contains(cmd, ".wants"):
		return r.units, nil
	case strings.Contains(cmd, "systemctl") && strings.Contains(cmd, "show"):
		for _, unit := range r.stopped {
			if strings.Contains(cmd, unit) {
				return "ActiveState=failed\nLoadState=loaded\n", nil
			}
		}
		return "ActiveState=active\nLoadState=loaded\n", nil
	}
	return "", nil
}

// An agent killed while a job's network was being set up, or a unit that
// stopped without its ExecStopPost, leaves the namespace, veth and rules
// behind; the agent removes those of its own units that no longer run when
// it starts, and keeps the rest. A namespace none of its units names is
// another agent's on the host (one in the other systemd scope, in attach
// mode), and is not its to remove (N20).
func TestSweepRemovesOnlyItsOwnNetworksNoUnitRuns(t *testing.T) {
	live := ctd.JobNetName("job-live")
	stale := ctd.JobNetName("job-stopped")
	foreign := ctd.JobNetName("job-of-another-agent")
	runner := &netnsRunner{
		netns:   live + " (id: 0)\n" + stale + " (id: 1)\n" + foreign + " (id: 2)\nother-ns\n",
		units:   systemd.UnitName("job-live") + "\n" + systemd.UnitName("job-stopped") + "\n",
		stopped: []string{systemd.UnitName("job-stopped")},
	}
	exec := SystemdExecutor{Manager: systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem}, runner, logr.Discard())}
	removed, err := exec.SweepJobNetworks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(removed, []string{stale}) {
		t.Fatalf("removed %v, want only %s (live %s, another agent's %s)", removed, stale, live, foreign)
	}
}
