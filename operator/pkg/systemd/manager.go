// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/jobspec"
)

// ErrSpecMismatch indicates a resubmit whose spec differs from the unit already
// installed for a job. The agent maps it onto backend.ErrSpecMismatch so the
// controller treats it as a terminal spec conflict (matching the k8s backend).
var ErrSpecMismatch = errors.New("spec mismatch")

// LocalRunner runs commands on the controller's own host via bash. Used by the
// agent (which runs on the target VM) and by dev/validation.
type LocalRunner struct{}

// Run implements Runner with an in-process bash invocation.
func (r LocalRunner) Run(ctx context.Context, cmd string) (string, error) {
	return r.RunInput(ctx, cmd, nil)
}

// RunInput implements InputRunner: Run with stdin fed to the command.
func (LocalRunner) RunInput(ctx context.Context, cmd string, stdin io.Reader) (string, error) {
	execCmd := exec.CommandContext(ctx, "bash", "-c", cmd)
	execCmd.Stdin = stdin
	var stdout, stderr strings.Builder
	execCmd.Stdout = &stdout
	execCmd.Stderr = &stderr
	err := execCmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code := -1
			if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				code = ws.ExitStatus()
			}
			return stdout.String(), &ExitError{Command: cmd, Stderr: stderr.String(), Code: code}
		}
		return stdout.String(), err
	}
	return stdout.String(), nil
}

// Manager installs, removes and inspects per-job systemd units on a host. It is the
// agent's execution engine: stateless (live state lives in the units). Job units
// run once (Restart=no); retries are Submit/BackoffLimit, not a systemd crash loop.
// It speaks to systemd through a Runner so it is transport-agnostic and testable.
type Manager struct {
	cfg    Config
	runner Runner
	logger logr.Logger
	// watch is the follow streams' shared TerminalWatch, made on first use.
	watchOnce sync.Once
	watch     *TerminalWatch
	// usersMu makes one job user at a time (EnsureJobUser).
	usersMu sync.Mutex
}

// NewManager builds a Manager for the given scope/privilege and command runner.
func NewManager(cfg Config, runner Runner, logger logr.Logger) *Manager {
	return &Manager{cfg: cfg, runner: runner, logger: logger.WithName("systemd-manager")}
}

// Config returns the Manager's configuration (scope, privilege).
func (m *Manager) Config() Config {
	return m.cfg
}

// Runner returns the Manager's command runner. Exposed so the log streamer can poll
// unit state through the same transport the Manager uses.
func (m *Manager) Runner() Runner {
	return m.runner
}

// MatchingUnit reports whether a unit for this spec is already installed.
func (m *Manager) MatchingUnit(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (string, bool, error) {
	name := UnitName(spec.ID)
	path := m.cfg.UnitPath(name)
	existing, err := m.runner.Run(ctx, fmt.Sprintf("%scat \"%s\"", m.cfg.SudoPrefix(), path))
	if err != nil {
		return "", false, nil
	}
	got := ParseSpecHash(existing)
	if got == "" {
		return "", false, nil
	}
	want := SpecHash(spec, env)
	if got == want {
		return name, true, nil
	}
	return "", false, fmt.Errorf("%w: unit %s has spec hash %s, resubmit has %s", ErrSpecMismatch, name, got, want)
}

// Install writes the unit file for a job, reloads systemd, and enables+starts it.
// The file is piped base64-encoded so newlines and quotes survive the shell intact.
func (m *Manager) Install(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (string, error) {
	return m.InstallWithWrapper(ctx, spec, env, "")
}

// InstallWithWrapper installs the unit with a destination-local exec wrapper.
func (m *Manager) InstallWithWrapper(ctx context.Context, spec jobspec.JobSpec, env map[string]string, execWrapper string) (string, error) {
	if err := ValidateUnitInputs(spec, env); err != nil {
		return "", err
	}
	name := UnitName(spec.ID)
	path := m.cfg.UnitPath(name)

	// A unit with the same spec that is running, or ran to an end it
	// recorded, is reused (Adoptable): a second enable --now while it is
	// active trips start-limit, and one after it ended runs the job twice.
	// A unit that never ran is started. A missing unit (cat fails) falls
	// through to the full install below. A different spec is a conflict.
	if existing, ok, err := m.MatchingUnit(ctx, spec, env); err != nil {
		return "", err
	} else if ok {
		adopt, adoptErr := m.Adoptable(ctx, existing)
		if adoptErr != nil {
			return "", adoptErr
		}
		if adopt {
			m.logger.Info("unit already installed for spec; reusing", "unit", existing)
			return existing, nil
		}
		// Start it directly, without reset-failed: that turns a failed unit
		// into inactive/Result=success, which a status read before the start
		// takes for a success (MapState). Job units set
		// StartLimitIntervalSec=0, so there is no start limit to clear.
		if _, err := m.runner.Run(ctx, m.cfg.Systemctl("enable --now "+name)); err != nil {
			return "", fmt.Errorf("start unit %s: %w", name, err)
		}
		m.logger.Info("started installed unit", "unit", existing)
		return existing, nil
	}

	workDir, err := PrepareSource(ctx, m.runner, spec, m.cfg)
	if err != nil {
		return "", fmt.Errorf("prepare source for %s: %w", name, err)
	}
	// Persist the retain flag inside the job dir as well as in the unit comment:
	// a repeat Cancel finds the unit already uninstalled and must still know.
	if spec.RetainWorkdir && workDir != "" {
		markerCmd := fmt.Sprintf(`%stouch "%s"`, m.cfg.SudoPrefix(), RetainMarkerPath(spec.Workspace, spec.ID, m.cfg))
		if _, err := m.runner.Run(ctx, markerCmd); err != nil {
			return "", fmt.Errorf("write retain marker for %s: %w", name, err)
		}
	}

	// Given to the job's user after the source is in place, as root, and
	// before the unit starts.
	var jail *confinement
	if m.cfg.Scope == ScopeSystem {
		if jail, err = m.confine(ctx, spec, workDir, execWrapper); err != nil {
			return "", fmt.Errorf("prepare %s for its user: %w", name, err)
		}
		jobEnv := env
		if jail != nil {
			jobEnv = jail.env(spec, env)
		}
		if err := m.writeEnvFile(ctx, name, jobEnv); err != nil {
			return "", err
		}
	}
	unit := renderUnit(spec, env, m.cfg, workDir, execWrapper, jail)

	if _, err := m.runner.Run(ctx, fmt.Sprintf(`%smkdir -p "%s"`, m.cfg.SudoPrefix(), m.cfg.UnitDir())); err != nil {
		return "", fmt.Errorf("create unit dir for %s: %w", name, err)
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(unit))
	writeCmd := fmt.Sprintf(`printf '%%s' '%s' | base64 -d | %stee "%s" >/dev/null`, encoded, m.cfg.SudoPrefix(), path)
	if _, err := m.runner.Run(ctx, writeCmd); err != nil {
		return "", fmt.Errorf("write unit %s: %w", name, err)
	}
	if _, err := m.runner.Run(ctx, m.cfg.Systemctl("daemon-reload")); err != nil {
		return "", fmt.Errorf("daemon-reload for %s: %w", name, err)
	}
	// A record left by an earlier unit of this name would report its end
	// for this run.
	if err := m.clearRecord(ctx, name); err != nil {
		return "", err
	}
	// Clear a leftover failed/start-limit counter if this unit name was used
	// before. Missing units make reset-failed non-zero; || true keeps install going.
	if _, err := m.runner.Run(ctx, m.cfg.Systemctl("reset-failed "+name)+" || true"); err != nil {
		return "", fmt.Errorf("reset-failed for %s: %w", name, err)
	}
	if _, err := m.runner.Run(ctx, m.cfg.Systemctl("enable --now "+name)); err != nil {
		return "", fmt.Errorf("start unit %s: %w", name, err)
	}
	m.logger.Info("installed unit", "unit", name)
	return name, nil
}

// writeEnvFile writes a system unit's environment (EnvFilePath): created
// 0600 under umask 077, in a directory only root reads, so no job sees it
// even for a moment.
func (m *Manager) writeEnvFile(ctx context.Context, name string, env map[string]string) error {
	sudo := m.cfg.SudoPrefix()
	encoded := base64.StdEncoding.EncodeToString([]byte(formatEnvFile(env)))
	cmd := fmt.Sprintf(`%[1]smkdir -p -m 0700 "%[2]s" && printf '%%s' '%[3]s' | %[1]ssh -c 'umask 077 && base64 -d > "$1"' sh "%[4]s"`,
		sudo, envFileDir, encoded, EnvFilePath(name))
	if _, err := m.runner.Run(ctx, cmd); err != nil {
		return fmt.Errorf("write the environment of %s: %w", name, err)
	}
	return nil
}

// Uninstall stops, disables and removes a unit, then reloads systemd. Idempotent:
// a missing unit is not an error.
func (m *Manager) Uninstall(ctx context.Context, name string) error {
	if _, err := m.runner.Run(ctx, m.cfg.Systemctl("disable --now "+name)); err != nil {
		m.logger.Info("disable reported (unit may be absent)", "unit", name, "error", err.Error())
	}
	removeCmd := fmt.Sprintf(`%srm -f "%s"`, m.cfg.SudoPrefix(), m.cfg.UnitPath(name))
	if m.cfg.Scope == ScopeSystem {
		removeCmd += fmt.Sprintf(` "%s"`, EnvFilePath(name))
	}
	if _, err := m.runner.Run(ctx, removeCmd); err != nil {
		return fmt.Errorf("remove unit %s: %w", name, err)
	}
	// After the stop above, which wrote the record one last time.
	if err := m.clearRecord(ctx, name); err != nil {
		return err
	}
	if _, err := m.runner.Run(ctx, m.cfg.Systemctl("daemon-reload")); err != nil {
		m.logger.Info("daemon-reload after uninstall reported", "unit", name, "error", err.Error())
	}
	if _, err := m.runner.Run(ctx, m.cfg.Systemctl("reset-failed "+name)+" || true"); err != nil {
		m.logger.Info("reset-failed after uninstall reported", "unit", name, "error", err.Error())
	}
	m.logger.Info("uninstalled unit", "unit", name)
	return nil
}

// RemoveWorkDir deletes the job's state directory (prepared source): JobDir,
// where a systemd job or one with no Workspace keeps it, and the Workspace's
// (IsolatedJobDir). Idempotent: a missing directory is not an error.
func (m *Manager) RemoveWorkDir(ctx context.Context, jobID string) error {
	sudo := m.cfg.SudoPrefix()
	jobDir := JobDir(jobID, m.cfg)
	isolated := workspacesRoot(m.cfg) + "/*/jobs/" + jobID
	removeCmd := fmt.Sprintf(`%srm -rf "%s" %s`, sudo, jobDir, isolated)
	if _, err := m.runner.Run(ctx, removeCmd); err != nil {
		return fmt.Errorf("remove work dir for %s: %w", jobID, err)
	}
	m.logger.Info("removed work dir", "job_id", jobID)
	return nil
}

// RetainWorkdir reports whether the job carries the retain-workdir GC flag. The
// primary source is the installed unit's comment; when the unit is already gone
// (a repeat Cancel after the GC retention pass uninstalled it) the marker file
// inside the job dir still carries the decision. Best-effort: a missing unit and
// marker report false (nothing left to retain).
func (m *Manager) RetainWorkdir(ctx context.Context, name string) bool {
	if stdout, err := m.runner.Run(ctx, fmt.Sprintf("%scat \"%s\"", m.cfg.SudoPrefix(), m.cfg.UnitPath(name))); err == nil {
		if ParseRetainWorkdir(stdout) {
			return true
		}
	}
	if id := JobIDFromUnit(name); id != "" {
		if _, err := m.runner.Run(ctx, retainMarkerProbe(id, m.cfg)); err == nil {
			return true
		}
	}
	return false
}

// JobTarget is the target every job unit is wanted by.
const JobTarget = "zenthros-job.target"

// JobUnits lists the installed job units, running or finished: the links
// `enable` put in the target's .wants directory. The target itself has no
// unit file (it is never started), and systemd reports no Wants for a unit
// it cannot load, so `systemctl show` on it lists nothing.
func (m *Manager) JobUnits(ctx context.Context) ([]string, error) {
	stdout, err := m.runner.Run(ctx, fmt.Sprintf(`ls -1 "%s/%s.wants" 2>/dev/null || true`, m.cfg.UnitDir(), JobTarget))
	if err != nil {
		return nil, fmt.Errorf("list job units: %w", err)
	}
	var units []string
	for _, name := range strings.Fields(stdout) {
		if jobIDFromUnit(name) != "" {
			units = append(units, name)
		}
	}
	return units, nil
}

// SignalPreempt sends the unit's process the signal its job asked for on an
// interruption notice (see preemptSignalKey), to the main process only. It
// reports whether one was sent; a job that asked for none gets none.
func (m *Manager) SignalPreempt(ctx context.Context, name string) (bool, error) {
	unit, err := m.runner.Run(ctx, fmt.Sprintf("%scat \"%s\"", m.cfg.SudoPrefix(), m.cfg.UnitPath(name)))
	if err != nil {
		return false, nil
	}
	sig := parsePreemptSignal(unit)
	if sig == "" {
		return false, nil
	}
	if _, err := m.runner.Run(ctx, m.cfg.Systemctl("kill --kill-whom=main --signal="+sig+" "+name)); err != nil {
		return false, fmt.Errorf("signal %s to %s: %w", sig, name, err)
	}
	return true, nil
}

// UnitActive reports whether the unit is still starting, running or
// stopping: anything but finished (UnitFinished), or a unit with a job
// queued (Job: a start waiting to run is inactive until it does).
func (m *Manager) UnitActive(ctx context.Context, name string) (bool, error) {
	props, err := m.Status(ctx, name)
	if err != nil {
		return false, err
	}
	return !UnitFinished(props["ActiveState"]) || props["Job"] != "", nil
}

// UnitFinished reports an ActiveState after which the unit runs nothing:
// inactive, failed, or none. A deactivating unit still runs its stop path,
// which writes its record: MapState reads it as Running, and so does every
// caller of UnitActive (19#6).
func UnitFinished(activeState string) bool {
	return activeState == "inactive" || activeState == "failed" || activeState == ""
}

// Status reads the unit's systemd properties as a raw map (suitable for MapState).
func (m *Manager) Status(ctx context.Context, name string) (map[string]string, error) {
	stdout, err := m.runner.Run(ctx, m.cfg.Systemctl("show "+name+" --property="+ShowProperties))
	if err != nil {
		return nil, fmt.Errorf("show unit %s: %w", name, err)
	}
	return ParseShow(stdout), nil
}

// JournalTail returns the last few journal lines for a unit, best-effort.
func (m *Manager) JournalTail(ctx context.Context, name string, lines int) string {
	cmd := "journalctl"
	if m.cfg.Scope == ScopeUser {
		cmd += " --user"
	}
	if m.cfg.UseSudo {
		cmd = "sudo " + cmd
	}
	stdout, err := m.runner.Run(ctx, fmt.Sprintf("%s -u %s -n %d --no-pager", cmd, name, lines))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(stdout)
}
