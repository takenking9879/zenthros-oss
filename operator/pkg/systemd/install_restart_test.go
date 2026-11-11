// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

// An installed unit with the same spec that never ran (no result record) must
// be started. Reusing it returns the old handle and the job never runs.
func TestInstallRestartsFinishedUnitWithSameSpec(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	runner := &fakeRunner{}
	manager := NewManager(cfg, runner, logr.Discard())
	spec := jobspec.JobSpec{
		ID:         "uid-finished",
		Name:       "hello",
		Namespace:  "tenant-default",
		Kind:       v1.WorkloadKindRayJob,
		Entrypoint: "echo hi",
		Image:      "ray:latest",
	}
	env := map[string]string{"A": "1"}
	runner.unitContent = RenderUnit(spec, env, cfg, "")

	if _, err := manager.Install(context.Background(), spec, env); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if runner.count("enable --now") == 0 {
		t.Fatal("Install reused a leftover unit without starting it; a finished unit must be reinstalled")
	}
}

// Found in the floci E2E, where a job that exited 7 was reported Succeeded:
// reset-failed turns a failed unit into inactive/Result=success, which is
// what a finished successful unit also looks like (systemd collects it at
// once). A status read between reset-failed and the start took the old
// failure for a success. A unit that is started again (installed, failed,
// and no result record) is started directly; job units set
// StartLimitIntervalSec=0, so there is no start limit to clear.
func TestRestartOfFinishedUnitNeverResetsItsState(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	runner := &fakeRunner{showOutput: "LoadState=loaded\nActiveState=failed\nResult=exit-code\nExecMainStatus=7\n"}
	manager := NewManager(cfg, runner, logr.Discard())
	spec := jobspec.JobSpec{ID: "uid-failed", Name: "hello", Namespace: "tenant-default",
		Kind: v1.WorkloadKindRayJob, Entrypoint: "exit 7", Image: "ray:latest"}
	env := map[string]string{"A": "1"}
	runner.unitContent = RenderUnit(spec, env, cfg, "")
	if !strings.Contains(runner.unitContent, "StartLimitIntervalSec=0") {
		t.Fatal("job units must disable the start limit; the restart path relies on it")
	}

	if _, err := manager.Install(context.Background(), spec, env); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if runner.count("enable --now") != 1 {
		t.Fatalf("a finished unit must be started again; commands: %v", runner.commands)
	}
	if runner.ran("reset-failed") {
		t.Fatalf("restart must not reset the unit's state before starting it; commands: %v", runner.commands)
	}
}
