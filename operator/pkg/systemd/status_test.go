// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
)

func TestParseShow(t *testing.T) {
	props := ParseShow("ActiveState=active\nResult=success\nJunk\n")
	if props["ActiveState"] != "active" || props["Result"] != "success" {
		t.Errorf("props = %+v", props)
	}
	if _, ok := props["Junk"]; ok {
		t.Error("a line without '=' must not parse as a property")
	}
}

// A unit that failed stays "failed" in systemd, and the job's exit code comes
// from there, not from the result record. Found on a real GCP VM: the message
// said exitCode=3 and status.exitCode was empty. A signal's number is no exit code.
func TestAFailedUnitCarriesItsExitCode(t *testing.T) {
	failed := MapState(map[string]string{"ActiveState": "failed", "Result": "exit-code", "ExecMainStatus": "3"})
	if failed.ExitCode == nil || *failed.ExitCode != 3 {
		t.Fatalf("exit code %v, want 3", failed.ExitCode)
	}
	killed := MapState(map[string]string{"ActiveState": "failed", "Result": "signal", "ExecMainStatus": "9"})
	if killed.ExitCode != nil {
		t.Fatalf("a signal has no exit code; got %d", *killed.ExitCode)
	}
	running := MapState(map[string]string{"ActiveState": "active", "ExecMainStatus": "0"})
	if running.ExitCode != nil {
		t.Fatalf("a running unit has no exit code; got %d", *running.ExitCode)
	}
}

func TestMapStateRunning(t *testing.T) {
	state := MapState(map[string]string{"ActiveState": "active", "SubState": "running"})
	if state.Phase != v1.PhaseRunning {
		t.Errorf("phase = %q, want Running", state.Phase)
	}
}

func TestMapStateSucceeded(t *testing.T) {
	state := MapState(map[string]string{"ActiveState": "inactive", "Result": "success", "SubState": "exited"})
	if state.Phase != v1.PhaseSucceeded {
		t.Errorf("phase = %q, want Succeeded", state.Phase)
	}
}

// systemd shows a unit it has no file for as inactive/Result=success (values
// read from systemd 255 on the floci E2E guest). That is not a success.
func TestUnitMissing(t *testing.T) {
	missing := map[string]string{"LoadState": "not-found", "ActiveState": "inactive", "Result": "success"}
	collected := map[string]string{"LoadState": "loaded", "ActiveState": "inactive", "Result": "success"}
	if !UnitMissing(missing) {
		t.Error("LoadState=not-found must be a missing unit")
	}
	if UnitMissing(collected) {
		t.Error("a collected finished unit still has its file (LoadState=loaded)")
	}
}

func TestMapStateInactiveNotSuccessIsSubmitted(t *testing.T) {
	state := MapState(map[string]string{"ActiveState": "inactive", "Result": ""})
	if state.Phase != v1.PhaseSubmitted {
		t.Errorf("phase = %q, want Submitted", state.Phase)
	}
}

func TestMapStateActivatingIsSubmitted(t *testing.T) {
	state := MapState(map[string]string{"ActiveState": "activating"})
	if state.Phase != v1.PhaseSubmitted {
		t.Errorf("phase = %q, want Submitted", state.Phase)
	}
}

func TestMapStateFailedCarriesReason(t *testing.T) {
	state := MapState(map[string]string{"ActiveState": "failed", "Result": "exit-code", "ExecMainStatus": "1"})
	if state.Phase != v1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", state.Phase)
	}
	if len(state.Reasons) != 1 || state.Reasons[0].Code != v1.ReasonProcessFailed {
		t.Errorf("reasons = %+v, want a single %s", state.Reasons, v1.ReasonProcessFailed)
	}
}

func TestSystemdTimeParsesAndIgnoresNA(t *testing.T) {
	if got := systemdTime("n/a"); got != nil {
		t.Errorf("systemdTime(n/a) = %v, want nil", got)
	}
	if got := systemdTime(""); got != nil {
		t.Errorf("systemdTime(\"\") = %v, want nil", got)
	}
	if got := systemdTime("Sun 2026-07-26 12:34:56 UTC"); got == nil {
		t.Error("systemdTime failed to parse a valid timestamp")
	}
}
