// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// indexOf is the position of the first command containing substr, or -1.
func (f *fakeRunner) indexOf(substr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.IndexFunc(f.commands, func(c string) bool { return strings.Contains(c, substr) })
}

// The unit writes systemd's own end-of-run variables to its record. The
// ExecStopPost line is run here the way systemd runs it: its escapes undone
// ($$ is a literal $, %% a literal %), then the line split into words by
// its quotes (sh splits single quotes like systemd) and executed with the
// variables systemd sets for ExecStopPost. Found in the floci E2E: a record
// whose printf format was single-quoted inside the single-quoted sh -c
// ended the word early, no record was written, and every finished job
// looked as if it had never run.
func TestUnitRecordsHowItsProcessEnded(t *testing.T) {
	spec := testSpec()
	home := t.TempDir()
	unit := RenderUnit(spec, nil, Config{Scope: ScopeUser}, "")
	var line string
	for _, l := range strings.Split(unit, "\n") {
		if v, ok := strings.CutPrefix(l, "ExecStopPost=-+"); ok {
			line = v
		}
	}
	if line == "" {
		t.Fatalf("unit has no ExecStopPost record:\n%s", unit)
	}
	line = strings.NewReplacer("$$", "$", "%%", "%").Replace(line)
	for _, end := range []struct{ result, code, status string }{
		{"success", "exited", "0"}, {"exit-code", "exited", "7"}, {"success", "killed", "TERM"},
	} {
		cmd := exec.Command("sh", "-c", line)
		cmd.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH"),
			"SERVICE_RESULT=" + end.result, "EXIT_CODE=" + end.code, "EXIT_STATUS=" + end.status}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("ExecStopPost failed: %v\n%s", err, out)
		}
		data, err := os.ReadFile(filepath.Join(home, ".local/share/zenthros/results", UnitName(spec.ID)))
		if err != nil {
			t.Fatalf("no record after ExecStopPost: %v", err)
		}
		record, ok := parseRecorded(string(data))
		if !ok || record != (Recorded{Result: end.result, Code: end.code, Status: end.status}) {
			t.Fatalf("record = %q, want %+v", data, end)
		}
	}
	system := RenderUnit(spec, nil, Config{Scope: ScopeSystem}, "")
	if !strings.Contains(system, `> "/var/lib/zenthros/results/zenthros-uid-123.service.tmp"`) {
		t.Fatalf("system-scope record must live under /var/lib/zenthros/results:\n%s", system)
	}
	if ParseSpecHash(unit) != SpecHash(spec, nil) {
		t.Fatal("the record must not change the spec hash: a resubmit after an agent upgrade would be SPEC_MISMATCH")
	}
}

// Only a main process that exited 0 succeeded. systemd reports
// Result=success for a service stopped with SIGTERM (a reboot, a stop) too.
func TestRecordedStateOnlyExitZeroSucceeds(t *testing.T) {
	cases := []struct {
		record string
		phase  v1.Phase
		detail string
	}{
		{"result=success code=exited status=0", v1.PhaseSucceeded, ""},
		{"result=exit-code code=exited status=7", v1.PhaseFailed, "exitCode=7"},
		{"result=success code=killed status=TERM", v1.PhaseFailed, "killed by signal TERM"},
		{"result=signal code=dumped status=SEGV", v1.PhaseFailed, "dumped by signal SEGV"},
		{"result=resources code= status=", v1.PhaseFailed, "result=resources"},
	}
	for _, tc := range cases {
		record, ok := parseRecorded(tc.record)
		if !ok {
			t.Fatalf("%q: not parsed as a record", tc.record)
		}
		state := record.State()
		if state.Phase != tc.phase || !strings.Contains(state.Message, tc.detail) {
			t.Errorf("%q = %s %q, want %s containing %q", tc.record, state.Phase, state.Message, tc.phase, tc.detail)
		}
		if tc.phase == v1.PhaseFailed && (len(state.Reasons) != 1 || state.Reasons[0].Code != v1.ReasonProcessFailed) {
			t.Errorf("%q: reasons %v, want PROCESS_FAILED", tc.record, state.Reasons)
		}
	}
	for _, empty := range []string{"", "  \n", "result= code= status="} {
		if _, ok := parseRecorded(empty); ok {
			t.Errorf("%q parsed as a record", empty)
		}
	}
}

// The job's exit code is what its process returned; a signal or a unit that
// never started has none.
func TestRecordedStateCarriesTheExitCode(t *testing.T) {
	code := func(n int32) *int32 { return &n }
	for _, tc := range []struct {
		record string
		want   *int32
	}{
		{"result=success code=exited status=0", code(0)},
		{"result=exit-code code=exited status=7", code(7)},
		{"result=success code=killed status=TERM", nil},
		{"result=signal code=dumped status=SEGV", nil},
		{"result=resources code= status=", nil},
	} {
		record, ok := parseRecorded(tc.record)
		if !ok {
			t.Fatalf("%q: not parsed", tc.record)
		}
		got := record.State().ExitCode
		if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
			t.Errorf("%q: exit code %v, want %v", tc.record, got, tc.want)
		}
	}
}

// An inactive unit is resolved from its record; with none, an installed
// unit has not run (an error, so the controller resubmits and the unit is
// started), even when systemd infers a success.
func TestFinishedStateComesFromTheRecord(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	unit := RenderUnit(testSpec(), nil, cfg, "")
	name := UnitName(testSpec().ID)
	inferred := backend.JobState{Phase: v1.PhaseSucceeded, Message: "process exited successfully"}

	runner := &fakeRunner{unitContent: unit, record: "result=exit-code code=exited status=7"}
	state, err := NewManager(cfg, runner, logr.Discard()).FinishedState(context.Background(), name, inferred)
	if err != nil || state.Phase != v1.PhaseFailed {
		t.Fatalf("recorded exit 7 = %s, %v; want Failed", state.Phase, err)
	}

	runner = &fakeRunner{unitContent: unit}
	_, err = NewManager(cfg, runner, logr.Discard()).FinishedState(context.Background(), name, inferred)
	if !errors.Is(err, ErrNotStarted) {
		t.Fatalf("installed unit without a record = %v, want ErrNotStarted", err)
	}

	runner = &fakeRunner{}
	if _, err = NewManager(cfg, runner, logr.Discard()).FinishedState(context.Background(), name, inferred); err == nil {
		t.Fatal("a unit removed meanwhile (Cancel) must be an error, not a state")
	}
}

// A resubmit (after a failed status read) adopts a unit that ran to an end
// it recorded, whatever the end: starting it again runs the job twice.
func TestResubmitAdoptsUnitThatRan(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	for _, record := range []string{"result=success code=exited status=0", "result=exit-code code=exited status=7"} {
		runner := &fakeRunner{showOutput: "LoadState=loaded\nActiveState=inactive\nResult=success\n", record: record}
		runner.unitContent = RenderUnit(testSpec(), nil, cfg, "")
		name, err := NewManager(cfg, runner, logr.Discard()).Install(context.Background(), testSpec(), nil)
		if err != nil || name != UnitName(testSpec().ID) {
			t.Fatalf("%s: Install = %q, %v", record, name, err)
		}
		if runner.ran("enable --now") {
			t.Fatalf("%s: a unit that already ran was started again: %v", record, runner.commands)
		}
	}
}

// A fresh install removes a record left by an earlier unit of the same name
// before the start, and Uninstall removes it after the stop (which writes it
// one last time).
func TestRecordIsClearedAroundTheRun(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	runner := &fakeRunner{}
	manager := NewManager(cfg, runner, logr.Discard())
	if _, err := manager.Install(context.Background(), testSpec(), nil); err != nil {
		t.Fatalf("Install: %v", err)
	}
	clear, start := runner.indexOf("rm -f \"/var/lib/zenthros/results/"), runner.indexOf("enable --now")
	if clear < 0 || clear > start {
		t.Fatalf("record cleared at %d, unit started at %d; want cleared first: %v", clear, start, runner.commands)
	}

	runner = &fakeRunner{}
	if err := NewManager(cfg, runner, logr.Discard()).Uninstall(context.Background(), UnitName(testSpec().ID)); err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	stop, clear := runner.indexOf("disable --now"), runner.indexOf("rm -f \"/var/lib/zenthros/results/")
	if clear < 0 || clear < stop {
		t.Fatalf("record cleared at %d, unit stopped at %d; want cleared after the stop: %v", clear, stop, runner.commands)
	}
}

// A unit still running its stop path (deactivating) has not written its
// record yet: it is the job's attempt still running, as MapState says, and a
// resubmit adopts it rather than start it again (19#6).
func TestResubmitAdoptsAUnitStillStopping(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	runner := &fakeRunner{showOutput: "LoadState=loaded\nActiveState=deactivating\nSubState=stop-post\n"}
	runner.unitContent = RenderUnit(testSpec(), nil, cfg, "")
	manager := NewManager(cfg, runner, logr.Discard())
	if _, err := manager.Install(context.Background(), testSpec(), nil); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if runner.ran("enable --now") {
		t.Fatalf("a unit still stopping was started again: %v", runner.commands)
	}
	if active, err := manager.UnitActive(context.Background(), UnitName(testSpec().ID)); err != nil || !active {
		t.Fatalf("UnitActive = %t, %v; MapState says %s", active, err, MapState(ParseShow(runner.showOutput)).Phase)
	}
}

// A start still queued (an enable --now cut off by its timeout, the start
// job waiting behind others) is a unit about to run: inactive, with a Job.
// Read as finished, the agent took back the secrets it was started with and
// the unit ran without them (R4 ronda 5, P4).
func TestAUnitWithAStartQueuedMayRun(t *testing.T) {
	runner := &fakeRunner{showOutput: "LoadState=loaded\nActiveState=inactive\nSubState=dead\nJob=4211\n"}
	manager := NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())
	if active, err := manager.UnitActive(context.Background(), UnitName(testSpec().ID)); err != nil || !active {
		t.Fatalf("UnitActive = %t, %v with a start queued", active, err)
	}
}
