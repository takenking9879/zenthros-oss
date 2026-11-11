// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// ShowProperties are the `systemctl show` fields the status mapping relies on.
const ShowProperties = "LoadState,ActiveState,SubState,Result,ExecMainStatus,ActiveEnterTimestamp,InactiveEnterTimestamp,Job"

// ParseShow turns `systemctl show` output ("Key=Value" lines) into a map.
func ParseShow(stdout string) map[string]string {
	props := make(map[string]string)
	for _, line := range strings.Split(stdout, "\n") {
		key, value, found := strings.Cut(line, "=")
		if found {
			props[key] = value
		}
	}
	return props
}

// MapState maps systemd unit properties to a Zenthros JobState. Zenthros reflects
// what systemd reports; it never infers resources or recomputes resilience.
func MapState(props map[string]string) backend.JobState {
	state := backend.JobState{
		StartTime:      systemdTime(props["ActiveEnterTimestamp"]),
		CompletionTime: systemdTime(props["InactiveEnterTimestamp"]),
	}

	switch props["ActiveState"] {
	case "active", "deactivating":
		state.Phase = v1.PhaseRunning
	case "failed":
		state.Phase = v1.PhaseFailed
		state.Reasons = []v1.Reason{{Code: v1.ReasonProcessFailed, Detail: failureDetail(props)}}
		state.Message = failureDetail(props)
		if props["Result"] == "exit-code" {
			state.ExitCode = Recorded{Code: "exited", Status: props["ExecMainStatus"]}.exitCode()
		}
	case "inactive":
		// A finished successful unit is garbage-collected at once, and systemd
		// then shows it exactly like one that has not run yet (inactive,
		// Result=success, no timestamps). Callers must not read a unit in that
		// state before starting it (Manager.InstallWithWrapper), and a missing
		// unit is not a state at all (UnitMissing).
		if props["Result"] == "success" {
			state.Phase = v1.PhaseSucceeded
			state.Message = "process exited successfully"
		} else {
			// Present but not successfully run yet (e.g. awaiting first start).
			state.Phase = v1.PhaseSubmitted
		}
	default:
		// activating, reloading, maintenance, or unknown: not yet running.
		state.Phase = v1.PhaseSubmitted
	}
	return state
}

// UnitMissing reports a unit systemd has no file for. `systemctl show`
// succeeds on it and reports inactive/Result=success, which MapState would
// read as a success. Found in the floci E2E, where a job that exited 7 was
// reported Succeeded: a missing unit (removed by Cancel while the status
// watcher still tracked it) is no evidence of how the job ended.
func UnitMissing(props map[string]string) bool {
	return props["LoadState"] == "not-found"
}

// failureDetail summarizes why a unit failed, from systemd's own fields.
func failureDetail(props map[string]string) string {
	result := props["Result"]
	if result == "" {
		result = "unknown"
	}
	return fmt.Sprintf("process failed (result=%s, exitCode=%s)", result, props["ExecMainStatus"])
}

// systemdTime parses a systemd timestamp, returning nil for empty or "n/a" values.
// Best-effort: status times are informational.
func systemdTime(value string) *metav1.Time {
	value = strings.TrimSpace(value)
	if value == "" || value == "n/a" {
		return nil
	}
	layouts := []string{
		"Mon 2006-01-02 15:04:05 MST",
		"Mon 2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05 MST",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			t := metav1.NewTime(parsed)
			return &t
		}
	}
	return nil
}
