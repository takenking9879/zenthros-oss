// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// A job unit records how its process ended, because systemd cannot tell
// afterwards. A unit that exited 0 is garbage-collected at once, and
// `systemctl show` then reports it exactly like a unit that never started,
// one just reset, or one that does not exist: inactive, Result=success, no
// timestamps. Found in the floci E2E, where a job that exited 7 was reported
// Succeeded. The unit's ExecStopPost writes systemd's own $SERVICE_RESULT,
// $EXIT_CODE and $EXIT_STATUS to a file that outlives that runtime state.

// ErrNotStarted is the status of a unit that records its result, is
// installed, and has not run: there is no state to report yet. The
// controller then resubmits, and the agent starts the unit.
var ErrNotStarted = fmt.Errorf("unit has not run")

// resultDir holds the records, outside the job dir so a retained work
// directory and the record are cleaned up independently.
func resultDir(cfg Config) string {
	if cfg.Scope == ScopeUser {
		return "$HOME/.local/share/zenthros/results"
	}
	return "/var/lib/zenthros/results"
}

// ResultPath is the shell path expression of a unit's result record.
func ResultPath(cfg Config, name string) string {
	return resultDir(cfg) + "/" + name
}

// recordResultCommand renders the unit's ExecStopPost. It runs after every
// stop, including a failed start. The record is written to a temporary file
// and renamed, so a reader never sees a partial one. The leading "-" keeps a
// failed write from failing the unit; "+" runs it as root and outside the
// job's confinement, where /var/lib/zenthros is read-only.
func recordResultCommand(cfg Config, name string) string {
	path := ResultPath(cfg, name)
	script := fmt.Sprintf(`mkdir -p "%s" && printf "result=%%s code=%%s status=%%s" "$SERVICE_RESULT" "$EXIT_CODE" "$EXIT_STATUS" > "%s.tmp" && mv -f "%s.tmp" "%s"`,
		resultDir(cfg), path, path, path)
	// systemd expands $NAME in Exec lines itself; $$ hands the shell a
	// literal $, and the shell reads what systemd set for ExecStopPost.
	return "-+/bin/sh -c " + systemdEscape(strings.ReplaceAll("'"+script+"'", "$", "$$"))
}

// Recorded is how a unit's process ended, as its ExecStopPost wrote it.
type Recorded struct {
	// Result is systemd's $SERVICE_RESULT (success, exit-code, signal, ...).
	Result string
	// Code is $EXIT_CODE (exited, killed, dumped); Status is $EXIT_STATUS,
	// an exit status or a signal name.
	Code, Status string
}

// parseRecorded reads a record; ok is false when there is none.
func parseRecorded(out string) (Recorded, bool) {
	fields := map[string]string{}
	for _, field := range strings.Fields(out) {
		if key, value, found := strings.Cut(field, "="); found {
			fields[key] = value
		}
	}
	if fields["result"] == "" {
		return Recorded{}, false
	}
	return Recorded{Result: fields["result"], Code: fields["code"], Status: fields["status"]}, true
}

// State maps the record. Only a main process that exited 0 succeeded: systemd
// also reports Result=success for a service stopped with SIGTERM (a shutdown,
// a stop), and that job did not finish.
func (r Recorded) State() backend.JobState {
	exitCode := r.exitCode()
	if r.Result == "success" && r.Code == "exited" && r.Status == "0" {
		return backend.JobState{Phase: v1.PhaseSucceeded, Message: "process exited successfully", ExitCode: exitCode}
	}
	detail := fmt.Sprintf("process failed (result=%s, exitCode=%s)", r.Result, r.Status)
	if r.Code == "killed" || r.Code == "dumped" {
		detail = fmt.Sprintf("process %s by signal %s (result=%s)", r.Code, r.Status, r.Result)
	}
	return backend.JobState{
		Phase:    v1.PhaseFailed,
		Message:  detail,
		Reasons:  []v1.Reason{{Code: v1.ReasonProcessFailed, Detail: detail}},
		ExitCode: exitCode,
	}
}

// exitCode is what the process returned; a signal, or a start that failed,
// returned nothing.
func (r Recorded) exitCode() *int32 {
	if r.Code != "exited" {
		return nil
	}
	code, err := strconv.ParseInt(r.Status, 10, 32)
	if err != nil {
		return nil
	}
	exit := int32(code)
	return &exit
}

// Recorded reads the unit's result record; ok is false when it has none.
func (m *Manager) Recorded(ctx context.Context, name string) (Recorded, bool, error) {
	out, err := m.runner.Run(ctx, fmt.Sprintf(`%scat "%s" 2>/dev/null || true`, m.cfg.SudoPrefix(), ResultPath(m.cfg, name)))
	if err != nil {
		return Recorded{}, false, fmt.Errorf("read result of %s: %w", name, err)
	}
	record, ok := parseRecorded(out)
	return record, ok, nil
}

// FinishedState resolves an inactive unit, which systemd alone cannot: from
// its record when it has one, with systemd's times (inferred); as not
// started when it is installed and has none yet.
func (m *Manager) FinishedState(ctx context.Context, name string, inferred backend.JobState) (backend.JobState, error) {
	record, ok, err := m.Recorded(ctx, name)
	if err != nil {
		return backend.JobState{}, err
	}
	if ok {
		state := record.State()
		state.StartTime, state.CompletionTime = inferred.StartTime, inferred.CompletionTime
		return state, nil
	}
	if _, err := m.runner.Run(ctx, fmt.Sprintf(`%scat "%s"`, m.cfg.SudoPrefix(), m.cfg.UnitPath(name))); err != nil {
		return backend.JobState{}, fmt.Errorf("unit %s is not installed: %w", name, err)
	}
	return backend.JobState{}, fmt.Errorf("%w: %s", ErrNotStarted, name)
}

// Adoptable reports whether an installed unit with the job's spec already is
// the job's attempt: still starting or running, or run to an end it recorded.
// A resubmit adopts it. Starting it again would run the job twice; only a
// unit that never ran is started.
func (m *Manager) Adoptable(ctx context.Context, name string) (bool, error) {
	active, err := m.UnitActive(ctx, name)
	if err != nil || active {
		return active, err
	}
	_, ran, err := m.Recorded(ctx, name)
	return ran, err
}

// clearRecord removes the unit's result record.
func (m *Manager) clearRecord(ctx context.Context, name string) error {
	path := ResultPath(m.cfg, name)
	if _, err := m.runner.Run(ctx, fmt.Sprintf(`%srm -f "%s" "%s.tmp"`, m.cfg.SudoPrefix(), path, path)); err != nil {
		return fmt.Errorf("remove result of %s: %w", name, err)
	}
	return nil
}
