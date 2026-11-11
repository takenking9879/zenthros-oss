// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	ctd "github.com/zenthros/operator/pkg/containerd"
	"github.com/zenthros/operator/pkg/systemd"
)

// SweepJobNetworks removes the networks of this agent's container jobs whose
// unit no longer runs: a unit's ExecStopPost removes its own, but an agent
// killed mid-setup, or a unit that stopped without it, leaves namespaces,
// veths and rules behind. A network is this agent's when one of its own
// units names it; the namespaces are the host's, and another agent's (one in
// the other systemd scope) are not its to remove (N20). A network whose unit
// is gone too is left: nothing says whose it is. It returns the namespaces
// it removed.
func (e SystemdExecutor) SweepJobNetworks(ctx context.Context) ([]string, error) {
	runner := e.Manager.Runner()
	sudo := e.Manager.Config().SudoPrefix()
	out, err := runner.Run(ctx, sudo+"ip netns list 2>/dev/null || true")
	if err != nil {
		return nil, err
	}
	units, err := e.Manager.JobUnits(ctx)
	if err != nil {
		return nil, err
	}
	stopped := map[string]bool{}
	for _, unit := range units {
		active, err := e.Manager.UnitActive(ctx, unit)
		if err != nil {
			return nil, err
		}
		if !active {
			stopped[ctd.JobNetName(systemd.JobIDFromUnit(unit))] = true
		}
	}
	var removed []string
	for _, ns := range staleJobNets(out, stopped) {
		script := base64.StdEncoding.EncodeToString([]byte(ctd.JobNetDownScript(ns)))
		if _, err := runner.Run(ctx, fmt.Sprintf(`printf '%%s' '%s' | base64 -d | %sbash`, script, sudo)); err != nil {
			return removed, fmt.Errorf("remove job network %s: %w", ns, err)
		}
		removed = append(removed, ns)
	}
	return removed, nil
}

// staleJobNets are the namespaces in `ip netns list` output that are
// stopped's.
func staleJobNets(list string, stopped map[string]bool) []string {
	var out []string
	for _, line := range strings.Split(list, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !stopped[fields[0]] {
			continue
		}
		out = append(out, fields[0])
	}
	return out
}
