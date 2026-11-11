// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/systemd"
)

// showAllRunner answers `systemctl show` for any number of units, in order.
type showAllRunner struct {
	active    map[string]string
	failBatch bool
	shows     []string
}

func (r *showAllRunner) Run(_ context.Context, cmd string) (string, error) {
	fields := strings.Fields(cmd)
	if len(fields) < 2 || fields[1] != "show" {
		return "", nil // the finished-state record, the journal tail
	}
	r.shows = append(r.shows, cmd)
	var blocks []string
	for _, unit := range fields[2:] {
		if strings.HasPrefix(unit, "--") {
			continue
		}
		active, ok := r.active[unit]
		if !ok {
			blocks = append(blocks, "Id="+unit+"\nLoadState=not-found\n")
			continue
		}
		blocks = append(blocks, "Id="+unit+"\nLoadState=loaded\nActiveState="+active+"\nSubState=running\nResult=success\n")
	}
	if r.failBatch && len(blocks) > 1 {
		return "", errors.New("Failed to get properties")
	}
	return strings.Join(blocks, "\n"), nil
}

// The VM executor reads every job's unit in one `systemctl show`, and each
// job's state is its own unit's: a missing unit is NotFound for that job
// alone. A batch that fails is read unit by unit.
func TestSystemdStatusAllReadsEveryUnitInOneCommand(t *testing.T) {
	runner := &showAllRunner{active: map[string]string{"zenthros-a.service": "active", "zenthros-c.service": "active"}}
	exec := SystemdExecutor{Manager: systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem}, runner, logr.Discard())}
	handles := []string{"zenthros-a.service", "zenthros-b.service", "zenthros-c.service"}

	states, errs := exec.StatusAll(context.Background(), handles)
	if len(runner.shows) != 1 {
		t.Fatalf("%d show commands for 3 jobs, want 1: %v", len(runner.shows), runner.shows)
	}
	if errs[0] != nil || states[0].Phase != v1.PhaseRunning || errs[2] != nil || states[2].Phase != v1.PhaseRunning {
		t.Errorf("running units read as %v/%v and %v/%v", states[0], errs[0], states[2], errs[2])
	}
	if errs[1] == nil {
		t.Errorf("the missing unit read as %v", states[1])
	}

	runner.failBatch, runner.shows = true, nil
	states, errs = exec.StatusAll(context.Background(), handles)
	if len(runner.shows) != 4 {
		t.Errorf("a failed batch was followed by %d commands, want one per unit", len(runner.shows)-1)
	}
	if errs[0] != nil || states[0].Phase != v1.PhaseRunning || errs[1] == nil {
		t.Errorf("after a failed batch: %v %v / %v", states[0], errs[0], errs[1])
	}

	// Every follow stream of the machine waits on one watch.
	if exec.Manager.TerminalWatch() != exec.Manager.TerminalWatch() {
		t.Error("each follow stream gets a watch of its own")
	}
}
