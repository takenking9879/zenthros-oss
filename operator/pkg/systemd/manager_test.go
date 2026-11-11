// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

// fakeRunner records the commands it is asked to run and returns canned output.
type fakeRunner struct {
	mu       sync.Mutex
	commands []string
	// showOutput is returned for `systemctl show` commands.
	showOutput string
	// unitContent is returned for unit-file reads (`cat <unit>`); empty means the
	// unit does not exist yet (the read fails, as a real cat of a missing file).
	unitContent string
	// markerExists controls the retain-marker probe (`test -e`); false fails it.
	markerExists bool
	// record is returned for result-record reads; empty means no record.
	record string
	// stdin records, per command, what RunInput fed it.
	stdin map[string]string
}

func (f *fakeRunner) RunInput(ctx context.Context, cmd string, stdin io.Reader) (string, error) {
	data, err := io.ReadAll(stdin)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	if f.stdin == nil {
		f.stdin = map[string]string{}
	}
	f.stdin[cmd] = string(data)
	f.mu.Unlock()
	return f.Run(ctx, cmd)
}

func (f *fakeRunner) Run(_ context.Context, cmd string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, cmd)
	if strings.Contains(cmd, "systemctl") && strings.Contains(cmd, "show") {
		return f.showOutput, nil
	}
	if strings.Contains(cmd, "test -e") {
		if f.markerExists {
			return "", nil
		}
		return "", fmt.Errorf("test: file does not exist")
	}
	if strings.Contains(cmd, "cat ") && strings.Contains(cmd, "/zenthros/results/") {
		return f.record, nil
	}
	if strings.Contains(cmd, "cat ") {
		if f.unitContent == "" {
			return "", fmt.Errorf("cat: no such file or directory")
		}
		return f.unitContent, nil
	}
	return "", nil
}

func (f *fakeRunner) ran(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.commands {
		if strings.Contains(c, substr) {
			return true
		}
	}
	return false
}

func (f *fakeRunner) count(substr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.commands {
		if strings.Contains(c, substr) {
			n++
		}
	}
	return n
}

func testSpec() jobspec.JobSpec {
	return jobspec.JobSpec{
		ID:         "uid-123",
		Name:       "hello",
		Namespace:  "tenant-default",
		Kind:       v1.WorkloadKindRayJob,
		Entrypoint: "echo hi",
		Image:      "ray:latest",
	}
}

func TestInstallRunsExpectedSequence(t *testing.T) {
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())

	name, err := m.Install(context.Background(), testSpec(), map[string]string{"A": "1"})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if name != "zenthros-uid-123.service" {
		t.Errorf("unit name = %q", name)
	}
	for _, want := range []string{"mkdir -p", "base64 -d", "daemon-reload", "reset-failed zenthros-uid-123.service", "enable --now zenthros-uid-123.service"} {
		if !runner.ran(want) {
			t.Errorf("expected a command containing %q; ran %v", want, runner.commands)
		}
	}
}

// TestInstallIsIdempotentForSameSpec guards the create-or-reuse contract: the
// reconciler resubmits a job on every reconcile, so a second Install with an
// unchanged spec must reuse the existing unit and NOT re-run `enable --now`
// (re-starting a fast-completing unit trips systemd's start-limit-hit).
func TestInstallIsIdempotentForSameSpec(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	m := NewManager(cfg, runner, logr.Discard())
	spec := testSpec()
	env := map[string]string{"A": "1"}

	if _, err := m.Install(context.Background(), spec, env); err != nil {
		t.Fatalf("first Install: %v", err)
	}
	if got := runner.count("enable --now"); got != 1 {
		t.Fatalf("first install enable --now count = %d, want 1", got)
	}

	// The unit now exists and is still running. A finished unit is a different
	// case: TestInstallRestartsFinishedUnitWithSameSpec must enable --now.
	runner.unitContent = RenderUnit(spec, env, cfg, "")
	runner.showOutput = "ActiveState=active\n"

	name, err := m.Install(context.Background(), spec, env)
	if err != nil {
		t.Fatalf("second Install: %v", err)
	}
	if name != "zenthros-uid-123.service" {
		t.Errorf("unit name = %q", name)
	}
	if got := runner.count("enable --now"); got != 1 {
		t.Errorf("resubmit must reuse the unit, not re-start it; enable --now count = %d, want 1", got)
	}
}

// TestInstallRejectsChangedSpecOnResubmit guards the other half of the contract:
// a resubmit whose spec differs from the installed unit is a conflict, surfaced
// as ErrSpecMismatch (and never started).
func TestInstallRejectsChangedSpecOnResubmit(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	m := NewManager(cfg, runner, logr.Discard())

	other := testSpec()
	other.Entrypoint = "echo something-else"
	runner.unitContent = RenderUnit(other, nil, cfg, "")

	_, err := m.Install(context.Background(), testSpec(), nil)
	if !errors.Is(err, ErrSpecMismatch) {
		t.Fatalf("err = %v, want ErrSpecMismatch", err)
	}
	if runner.ran("enable --now") {
		t.Errorf("spec mismatch must not start the unit; ran %v", runner.commands)
	}
}

func TestUninstallIsIdempotentSequence(t *testing.T) {
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())

	if err := m.Uninstall(context.Background(), "zenthros-uid-123.service"); err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	for _, want := range []string{"disable --now", "rm -f", "daemon-reload", "reset-failed zenthros-uid-123.service"} {
		if !runner.ran(want) {
			t.Errorf("expected a command containing %q; ran %v", want, runner.commands)
		}
	}
}

func TestStatusParsesShowOutput(t *testing.T) {
	runner := &fakeRunner{showOutput: "ActiveState=inactive\nResult=success\nSubState=exited\n"}
	m := NewManager(Config{Scope: ScopeUser}, runner, logr.Discard())

	props, err := m.Status(context.Background(), "zenthros-x.service")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	state := MapState(props)
	if state.Phase != v1.PhaseSucceeded {
		t.Errorf("phase = %q, want Succeeded", state.Phase)
	}
}

func TestUserScopeUsesUserFlagAndNoSudo(t *testing.T) {
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeUser, UseSudo: false}, runner, logr.Discard())

	if _, err := m.Install(context.Background(), testSpec(), nil); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !runner.ran("systemctl --user") {
		t.Errorf("expected --user flag; ran %v", runner.commands)
	}
	if runner.ran("sudo ") {
		t.Errorf("did not expect sudo; ran %v", runner.commands)
	}
	if !runner.ran("$HOME/.config/systemd/user") {
		t.Errorf("expected user unit dir; ran %v", runner.commands)
	}
}

func TestLocalRunnerRunsRealCommand(t *testing.T) {
	out, err := LocalRunner{}.Run(context.Background(), "echo zenthros-probe")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.TrimSpace(out) != "zenthros-probe" {
		t.Errorf("output = %q", out)
	}
}

func TestLocalRunnerReportsExitCode(t *testing.T) {
	_, err := LocalRunner{}.Run(context.Background(), "exit 3")
	if err == nil {
		t.Fatal("expected an error for non-zero exit")
	}
	exitErr, ok := err.(*ExitError)
	if !ok {
		t.Fatalf("error type = %T, want *ExitError", err)
	}
	if exitErr.Code != 3 {
		t.Errorf("code = %d, want 3", exitErr.Code)
	}
}

// TestRemoveWorkDirDeletesJobDir covers the VM half of Phase L GC: Cancel must free
// the prepared source, not just the unit.
func TestRemoveWorkDirDeletesJobDir(t *testing.T) {
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())

	if err := m.RemoveWorkDir(context.Background(), "uid-123"); err != nil {
		t.Fatalf("RemoveWorkDir: %v", err)
	}
	if !runner.ran(`rm -rf "/var/lib/zenthros/jobs/uid-123"`) {
		t.Errorf("expected rm -rf of the job dir; ran %v", runner.commands)
	}
}

func TestRemoveWorkDirUserScope(t *testing.T) {
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeUser}, runner, logr.Discard())

	if err := m.RemoveWorkDir(context.Background(), "uid-123"); err != nil {
		t.Fatalf("RemoveWorkDir: %v", err)
	}
	if !runner.ran(`rm -rf "$HOME/.local/share/zenthros/jobs/uid-123"`) {
		t.Errorf("expected user-scope job dir removal; ran %v", runner.commands)
	}
}

// TestRetainWorkdirReadsUnitFlag proves the GC flag is read off the installed unit
// (so it survives agent restarts) and defaults to false when the unit is absent.
func TestRetainWorkdirReadsUnitFlag(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}

	retained := testSpec()
	retained.RetainWorkdir = true
	runner := &fakeRunner{unitContent: RenderUnit(retained, nil, cfg, "")}
	m := NewManager(cfg, runner, logr.Discard())
	if !m.RetainWorkdir(context.Background(), "zenthros-uid-123.service") {
		t.Error("unit rendered with RetainWorkdir must report true")
	}

	runner = &fakeRunner{unitContent: RenderUnit(testSpec(), nil, cfg, "")}
	m = NewManager(cfg, runner, logr.Discard())
	if m.RetainWorkdir(context.Background(), "zenthros-uid-123.service") {
		t.Error("unit without the flag must report false")
	}

	runner = &fakeRunner{} // cat fails: unit already gone, no marker either
	m = NewManager(cfg, runner, logr.Discard())
	if m.RetainWorkdir(context.Background(), "zenthros-uid-123.service") {
		t.Error("missing unit and marker must report false")
	}
}

// TestRetainWorkdirFallsBackToMarker covers the repeat-Cancel case: the GC
// retention pass uninstalls the unit first, so the finalizer's later Cancel must
// still recover the retain decision from the marker inside the job dir.
func TestRetainWorkdirFallsBackToMarker(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	runner := &fakeRunner{markerExists: true} // cat fails: unit already uninstalled
	m := NewManager(cfg, runner, logr.Discard())

	if !m.RetainWorkdir(context.Background(), "zenthros-uid-123.service") {
		t.Error("marker present with the unit gone must report true")
	}
	if !runner.ran(`for f in "/var/lib/zenthros/jobs/uid-123/.zenthros-retain" /var/lib/zenthros/workspaces/*/jobs/uid-123/.zenthros-retain; do test -e "$f"`) {
		t.Errorf("expected a marker probe in the job's directory of any Workspace; ran %v", runner.commands)
	}
}

// TestInstallWritesRetainMarker proves Install persists the retain flag inside the
// job dir (alongside the unit comment) when a source prepared a work dir.
func TestInstallWritesRetainMarker(t *testing.T) {
	retained := testSpec()
	retained.RetainWorkdir = true
	retained.Workspace = "team-a"
	retained.Source.Inline = inlineTarball(map[string]string{"main.py": "print(1)"}, nil)

	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())
	if _, err := m.Install(context.Background(), retained, nil); err != nil {
		t.Fatalf("Install: %v", err)
	}
	// In the Workspace's job directory, the one the source made: the one of
	// no Workspace is never made, and touch there failed the job.
	if !runner.ran(`touch "/var/lib/zenthros/workspaces/team-a/jobs/uid-123/.zenthros-retain"`) {
		t.Errorf("expected the retain marker in the job's directory; ran %v", runner.commands)
	}

	runner = &fakeRunner{}
	m = NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())
	if _, err := m.Install(context.Background(), testSpec(), nil); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if runner.ran(".zenthros-retain") {
		t.Errorf("no marker without the retain flag; ran %v", runner.commands)
	}

	// Retain without a source prepares no work dir, so there is nothing to mark.
	noSource := testSpec()
	noSource.RetainWorkdir = true
	runner = &fakeRunner{}
	m = NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())
	if _, err := m.Install(context.Background(), noSource, nil); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if runner.ran(".zenthros-retain") {
		t.Errorf("no marker without a prepared work dir; ran %v", runner.commands)
	}
}

// The job units are the links `enable` put in zenthros-job.target.wants,
// read with a real shell. The target has no unit file, so systemd itself
// reports no Wants for it; a lister that asks systemctl finds nothing.
func TestJobUnitsFromEnabledLinks(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	wants := filepath.Join(home, ".config/systemd/user", JobTarget+".wants")
	if err := os.MkdirAll(wants, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"zenthros-uid-1.service", "zenthros-uid-2-a1.service", "not-a-job.service"} {
		if err := os.Symlink("../"+name, filepath.Join(wants, name)); err != nil {
			t.Fatal(err)
		}
	}
	m := NewManager(Config{Scope: ScopeUser}, LocalRunner{}, logr.Discard())
	units, err := m.JobUnits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(units, " ") != "zenthros-uid-1.service zenthros-uid-2-a1.service" {
		t.Fatalf("units = %v", units)
	}

	// A machine that never ran a job has no .wants directory.
	t.Setenv("HOME", t.TempDir())
	if units, err := m.JobUnits(context.Background()); err != nil || len(units) != 0 {
		t.Fatalf("no jobs: units %v err %v", units, err)
	}
}

// The signal a job asked for on a notice is in its unit file, so an agent
// that restarted still sends it, to the main process; a signal outside the
// allowed set is never rendered, and a job that asked for none gets none.
func TestPreemptSignalRecordedInUnit(t *testing.T) {
	spec := jobspec.JobSpec{ID: "uid-1", Entrypoint: "echo hi", PreemptSignal: "SIGUSR1"}
	unit := RenderUnit(spec, nil, Config{}, "")
	if parsePreemptSignal(unit) != "SIGUSR1" {
		t.Fatalf("unit does not record the signal:\n%s", unit)
	}
	if specHash(spec, nil) == specHash(jobspec.JobSpec{ID: "uid-1", Entrypoint: "echo hi"}, nil) {
		t.Fatal("asking for a signal did not change the spec hash")
	}
	if bad := RenderUnit(jobspec.JobSpec{ID: "uid-1", Entrypoint: "x", PreemptSignal: "SIGKILL; rm -rf /"}, nil, Config{}, ""); parsePreemptSignal(bad) != "" || strings.Contains(bad, "rm -rf") {
		t.Fatalf("an unknown signal reached the unit:\n%s", bad)
	}

	runner := &fakeRunner{unitContent: unit}
	m := NewManager(Config{Scope: ScopeSystem}, runner, logr.Discard())
	sent, err := m.SignalPreempt(context.Background(), "zenthros-uid-1.service")
	if err != nil || !sent || !runner.ran("systemctl kill --kill-whom=main --signal=SIGUSR1 zenthros-uid-1.service") {
		t.Fatalf("sent=%v err=%v commands %v", sent, err, runner.commands)
	}
	quiet := &fakeRunner{unitContent: RenderUnit(jobspec.JobSpec{ID: "uid-2", Entrypoint: "x"}, nil, Config{}, "")}
	if sent, _ := NewManager(Config{}, quiet, logr.Discard()).SignalPreempt(context.Background(), "zenthros-uid-2.service"); sent || quiet.ran("kill") {
		t.Fatal("a job that asked for no signal got one")
	}
}
