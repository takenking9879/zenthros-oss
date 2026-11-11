// SPDX-License-Identifier: Apache-2.0

// Package systemd renders and maps per-job systemd units. It is the shared
// execution core for the VM backend: both the SSH-direct controller path (M1) and
// the agent on the host (M2) render the identical unit from a JobSpec and map the
// identical systemd state to a Zenthros JobState. Keeping it here avoids
// duplicating the rendering/escaping/status logic across the two paths.
package systemd

import (
	"strings"

	"github.com/zenthros/operator/pkg/jobspec"
)

// Scope selects where units live and how systemctl is invoked.
const (
	// ScopeSystem manages system units under /etc/systemd/system (a real VM/server).
	ScopeSystem = "system"
	// ScopeUser manages user units under $HOME/.config/systemd/user (no root needed).
	ScopeUser = "user"
)

// Config describes how to address systemd on a host: the scope and whether file
// and systemctl operations need a sudo prefix. It carries no connection details;
// those live with the transport (SSH client or local runner).
type Config struct {
	// Scope is ScopeSystem or ScopeUser.
	Scope string
	// UseSudo prefixes systemctl and file operations with sudo (typical for system
	// scope where the acting user has passwordless sudo).
	UseSudo bool
	// RuntimeImage, when set, runs the job inside containerd (ctr run) while the
	// Agent stays on the host. Empty keeps ExecStart on the host binary.
	RuntimeImage string
	// HostRoots are the folders the machine's owner opened to jobs
	// (HostRootsEnv).
	HostRoots []string
}

// unitDir returns the shell path expression of the unit directory for the scope.
// User scope uses $HOME so it expands in the acting shell without depending on a
// literal home directory at config time.
func (c Config) unitDir() string {
	if c.Scope == ScopeUser {
		return "$HOME/.config/systemd/user"
	}
	return "/etc/systemd/system"
}

// UnitDir returns the shell path expression of the unit directory for the scope.
func (c Config) UnitDir() string {
	return c.unitDir()
}

// UnitPath returns a shell path expression for a unit file in the configured scope.
// Callers double-quote it so $HOME expands and any spaces stay safe.
func (c Config) UnitPath(name string) string {
	return c.unitDir() + "/" + name
}

// Systemctl returns a systemctl command line for the configured scope and privilege.
func (c Config) Systemctl(args string) string {
	cmd := "systemctl"
	if c.Scope == ScopeUser {
		cmd += " --user"
	}
	if c.UseSudo {
		cmd = "sudo " + cmd
	}
	return cmd + " " + args
}

// SudoPrefix returns the privilege prefix for non-systemctl file operations.
func (c Config) SudoPrefix() string {
	if c.UseSudo {
		return "sudo "
	}
	return ""
}

// UnitName maps a job id to a stable, systemd-safe unit name. Job ids are k8s
// UIDs; any unexpected character is normalized so the name is always valid.
func UnitName(jobID string) string {
	return unitName(jobID)
}

// RenderUnit produces the systemd unit file for a job. The rendered text is a pure
// function of (spec, env, scope, workDir) so it is deterministic and directly testable.
// workDir overrides the WorkingDirectory when source preparation resolved a path
// (git clone or inline extraction); pass "" to use the spec's hostPath as before.
func RenderUnit(spec jobspec.JobSpec, env map[string]string, cfg Config, workDir string) string {
	return renderUnit(spec, env, cfg, workDir, "", nil)
}

// RenderUnitWithWrapper prepends a destination-local exec wrapper. The wrapper
// path must not contain secret values; it only exports files written on the host.
func RenderUnitWithWrapper(spec jobspec.JobSpec, env map[string]string, cfg Config, workDir, execWrapper string) string {
	return renderUnit(spec, env, cfg, workDir, execWrapper, nil)
}

// SpecHash fingerprints the fields that define what runs, so a resubmit with an
// unchanged spec is idempotent and a changed spec is rejected as a SpecMismatch.
func SpecHash(spec jobspec.JobSpec, env map[string]string) string {
	return specHash(spec, env)
}

// ParseSpecHash extracts the spec fingerprint comment from a rendered unit, or "".
func ParseSpecHash(unitText string) string {
	return parseSpecHash(unitText)
}

// ParseRetainWorkdir reports whether a rendered unit carries the retain-workdir
// GC flag.
func ParseRetainWorkdir(unitText string) bool {
	return parseRetainWorkdir(unitText)
}

// JobIDFromUnit recovers the id a unit was installed under (the reverse of
// UnitName) when it is systemd-safe, else "". For a re-run that is the
// attempt's own id, <job id>-a<n>, which keys its work directory.
func JobIDFromUnit(name string) string {
	return jobIDFromUnit(name)
}

// JobOfUnit is the job a unit runs for: JobIDFromUnit without a re-run's
// -a<n>. A UID never ends in -a<digits> (its last group is 12 hex digits), so
// the suffix is always an attempt.
func JobOfUnit(name string) string {
	id := jobIDFromUnit(name)
	if i := strings.LastIndex(id, "-a"); i > 0 && isDigits(id[i+2:]) {
		return id[:i]
	}
	return id
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
