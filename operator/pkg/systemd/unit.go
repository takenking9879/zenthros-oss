// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"encoding/base64"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/jobspec/fingerprint"
)

const (
	// unitPrefix namespaces Zenthros units on the host.
	unitPrefix = "zenthros-"

	// specHashKey marks the unit-file comment carrying the spec fingerprint used
	// for idempotent Submit. The value never appears in any k8s manifest.
	specHashKey = "zenthros-spec-hash"

	// retainWorkdirKey marks the unit-file comment carrying the GC flag:
	// keep the work directory when the job is cleaned up. Stored in the unit file
	// (not agent memory) so it survives agent restarts, like the spec fingerprint.
	retainWorkdirKey = "zenthros-retain-workdir"

	// preemptSignalKey marks the signal the job asked for on an interruption
	// notice. In the unit file, so an agent that restarted still sends it.
	preemptSignalKey = "zenthros-preempt-signal"
)

// ContainerRunScript is the containerd executor's per-job wrapper
// (containerd.RunScriptName); ContainerNetDownArg makes it remove the job's
// own network (containerd.NetDownArg).
const (
	ContainerRunScript  = "ctr-run"
	ContainerNetDownArg = "--zenthros-net-down"
)

// preemptSignals are the signals a job may ask for; nothing else reaches
// systemctl.
var preemptSignals = map[string]bool{"SIGTERM": true, "SIGINT": true, "SIGUSR1": true, "SIGUSR2": true}

// unitName maps a job id to a stable, systemd-safe unit name.
func unitName(jobID string) string {
	var b strings.Builder
	for _, r := range jobID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return unitPrefix + b.String() + ".service"
}

// renderUnit produces the systemd unit file for a job. jail, when set,
// holds a host job (confine); a container's unit and a user manager's run
// as they are.
func renderUnit(spec jobspec.JobSpec, env map[string]string, cfg Config, workDir, execWrapper string, jail *confinement) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# %s=%s\n", specHashKey, specHash(spec, env))
	if spec.RetainWorkdir {
		fmt.Fprintf(&b, "# %s=true\n", retainWorkdirKey)
	}
	if preemptSignals[spec.PreemptSignal] {
		fmt.Fprintf(&b, "# %s=%s\n", preemptSignalKey, spec.PreemptSignal)
	}
	b.WriteString("[Unit]\n")
	fmt.Fprintf(&b, "Description=Zenthros job %s (%s)\n", systemdEscape(spec.Name), systemdEscape(spec.ID))
	if runtimeImage(cfg) != "" {
		b.WriteString("After=network-online.target containerd.service\n")
		b.WriteString("Wants=network-online.target containerd.service\n")
	} else {
		b.WriteString("After=network-online.target\n")
		b.WriteString("Wants=network-online.target\n")
	}
	// Jobs are one-shot. systemd Restart=on-failure plus StartLimitBurst=
	// BackoffLimit+1 (0+1=1 when the CR omits backoff) turns the first non-zero
	// exit into "Start request repeated too quickly" and hides the real failure.
	// Sequential playground experiments then look like start-limit on a healthy VM.
	// Control-plane BackoffLimit retries as a new Submit, not a crash loop here.
	b.WriteString("StartLimitIntervalSec=0\n")

	b.WriteString("\n[Service]\n")
	b.WriteString("Type=simple\n")
	if cfg.Scope == ScopeSystem {
		// The system manager shows a unit's Environment= to every user;
		// the Manager writes the job's to this file (writeEnvFile).
		fmt.Fprintf(&b, "EnvironmentFile=%s\n", systemdEscape(EnvFilePath(unitName(spec.ID))))
	} else {
		for _, line := range envLines(env) {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	dir := effectiveWorkDir(spec, workDir)
	if jail != nil {
		for _, line := range jail.directives() {
			b.WriteString(line)
			b.WriteByte('\n')
		}
		switch {
		case dir == "":
			dir = jail.home
		case dir == spec.Source.HostPath && jail.hostPath != "":
			dir = jail.hostPath
		}
	}
	if dir != "" {
		fmt.Fprintf(&b, "WorkingDirectory=%s\n", systemdEscape(dir))
	}
	fmt.Fprintf(&b, "ExecStart=%s\n", execStart(spec, execWrapper))
	b.WriteString("Restart=no\n")
	fmt.Fprintf(&b, "ExecStopPost=%s\n", recordResultCommand(cfg, unitName(spec.ID)))
	if containerUnit(execWrapper) {
		// After the result is recorded: the container's own network, if it
		// had one (a shared backend), goes with the unit.
		fmt.Fprintf(&b, "ExecStopPost=-\"%s\" %s\n", execTokenEscape(execWrapper), ContainerNetDownArg)
	}

	// enable --now needs [Install]. multi-user.target would start the job
	// again on reboot, and the agent does not adopt that process.
	b.WriteString("\n[Install]\n")
	b.WriteString("WantedBy=zenthros-job.target\n")
	return b.String()
}

// execStart renders the ExecStart line for a job. A structured Command runs
// directly, without any shell in between; a shell Entrypoint takes the
// base64+bash path.
func execStart(spec jobspec.JobSpec, execWrapper string) string {
	inner := execStartCommand(spec)
	if execWrapper == "" {
		return inner
	}
	// The unit's own container run script is made executable where it
	// lives. Any other wrapper (the secrets one) may live on a noexec
	// tmpfs (/dev/shm, /run in a container host): the shell reads it.
	if !containerUnit(execWrapper) {
		return `/bin/sh "` + execTokenEscape(execWrapper) + `" ` + inner
	}
	return `"` + execTokenEscape(execWrapper) + `" ` + inner
}

// containerUnit reports a unit that runs its job in a container: its
// wrapper is the container run script.
func containerUnit(execWrapper string) bool {
	return path.Base(execWrapper) == ContainerRunScript
}

func runtimeImage(cfg Config) string {
	if v := strings.TrimSpace(cfg.RuntimeImage); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("ZENTHROS_RUNTIME_IMAGE")); v != "" {
		return v
	}
	data, err := os.ReadFile("/var/lib/zenthros/runtime-image")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func execStartCommand(spec jobspec.JobSpec) string {
	if len(spec.Command) > 0 {
		return execStartStructured(spec.Command, spec.Args)
	}
	return execStartShell(spec.Entrypoint)
}

// execStartShell renders the ExecStart line for a shell entrypoint. The
// entrypoint is base64-encoded and decoded at start time so it always occupies a
// single, safe unit line: systemd rejects literal newlines in an ExecStart value,
// and user scripts routinely contain newlines, quotes, pipes and '%' that would
// otherwise need fragile escaping. The decoded script runs in a login bash
// (matching the prior `bash -lc`).
//
// The rendered command is systemd-escaped: the literal '%' in `printf %s` must reach
// the unit file as '%%' or systemd expands it as a unit specifier, corrupting the
// base64 payload before the shell ever sees it (manifests as `base64: invalid input`).
func execStartShell(entrypoint string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(entrypoint))
	cmd := fmt.Sprintf("/bin/bash -c 'printf %%s %s | base64 -d | /bin/bash -l'", encoded)
	return systemdEscape(cmd)
}

// execStartStructured renders Command+Args as a direct ExecStart: systemd executes
// the argv itself, so no shell touches the tokens (no quoting bugs, no '%' specifier
// corruption — the class of failures the shell-string entrypoint caused). Each token
// is double-quoted so whitespace stays literal; backslashes, quotes and '%' are
// escaped for systemd's parser.
func execStartStructured(command, args []string) string {
	tokens := make([]string, 0, len(command)+len(args))
	tokens = append(tokens, command...)
	tokens = append(tokens, args...)
	quoted := make([]string, len(tokens))
	for i, token := range tokens {
		quoted[i] = `"` + execTokenEscape(token) + `"`
	}
	line := strings.Join(quoted, " ")
	if bareProgram(command[0]) {
		line = envProgram + " " + line
	}
	return line
}

// envProgram looks a bare program name up on the unit's PATH. systemd does
// not: it resolves a bare ExecStart name on its compile-time search path, so
// `python3` would be /usr/bin/python3 even on a machine whose toolchain
// (/opt/zenthros/bin, first on the PATH install-agent.sh gives every unit) is
// the python3 every shell and `bash -c` there sees.
const envProgram = "/usr/bin/env"

// bareProgram: a name for env to look up. A path is run as it is; a name env
// would read as an option or an assignment is left to systemd.
func bareProgram(name string) bool {
	return name != "" && !strings.ContainsAny(name, "/=") && !strings.HasPrefix(name, "-")
}

// execTokenEscape prepares a token for a double-quoted systemd ExecStart
// argument. systemd unquotes it with C escapes, so a newline in the token
// (a `python3 -c` script) is written as \n: a literal one makes the unit
// invalid and systemd never starts it. `$` is doubled, or systemd expands
// $NAME from the unit's environment before the program sees it.
func execTokenEscape(value string) string {
	value = cEscape(value)
	value = strings.ReplaceAll(value, "$", "$$")
	return systemdEscape(value)
}

// cEscape escapes what systemd's C-unescaping quoted-word parser reads
// specially: backslash, double quote, and the line breaks a unit line
// cannot hold.
func cEscape(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, "\r", `\r`)
	return value
}

// effectiveWorkDir resolves the WorkingDirectory: an explicit spec.WorkingDir wins,
// then the prepared workDir (git/inline), then the spec's hostPath.
func effectiveWorkDir(spec jobspec.JobSpec, workDir string) string {
	if spec.WorkingDir != "" {
		return spec.WorkingDir
	}
	if workDir != "" {
		return workDir
	}
	return spec.Source.HostPath
}

// specHash fingerprints the fields that define what runs (the same
// fingerprint as the process executor's).
func specHash(spec jobspec.JobSpec, env map[string]string) string {
	return fingerprint.Of(spec, env)
}

// parseSpecHash extracts the spec fingerprint comment from a rendered unit, or "".
func parseSpecHash(unitText string) string {
	prefix := "# " + specHashKey + "="
	for _, line := range strings.Split(unitText, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}

// parsePreemptSignal is the signal the rendered unit asked for on an
// interruption notice, or "".
func parsePreemptSignal(unitText string) string {
	prefix := "# " + preemptSignalKey + "="
	for _, line := range strings.Split(unitText, "\n") {
		if strings.HasPrefix(line, prefix) {
			if sig := strings.TrimPrefix(line, prefix); preemptSignals[sig] {
				return sig
			}
		}
	}
	return ""
}

// parseRetainWorkdir reports whether the rendered unit carries the retain-workdir flag.
func parseRetainWorkdir(unitText string) bool {
	prefix := "# " + retainWorkdirKey + "="
	for _, line := range strings.Split(unitText, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix) == "true"
		}
	}
	return false
}

// jobIDFromUnit recovers the job id from a rendered unit name when the id consists
// solely of systemd-safe characters (always true for Kubernetes UIDs). Returns ""
// when the name is not a Zenthros unit or the id cannot round-trip.
func jobIDFromUnit(name string) string {
	id, ok := strings.CutPrefix(name, unitPrefix)
	if !ok {
		return ""
	}
	id, ok = strings.CutSuffix(id, ".service")
	if !ok || id == "" {
		return ""
	}
	if unitName(id) != name {
		return ""
	}
	return id
}

// envLines renders sorted, quoted systemd Environment assignments.
func envLines(env map[string]string) []string {
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)

	lines := make([]string, 0, len(names))
	for _, name := range names {
		// The whole assignment is quoted: a value with a space is
		// otherwise cut there, and systemd drops the rest.
		lines = append(lines, fmt.Sprintf(`Environment="%s=%s"`, name, envValueEscape(env[name])))
	}
	return lines
}

// systemdEscape doubles '%' so values are not interpreted as systemd specifiers.
func systemdEscape(value string) string {
	return strings.ReplaceAll(value, "%", "%%")
}

// envValueEscape prepares a value for a quoted systemd Environment
// assignment. `$` means nothing there and stays as it is.
func envValueEscape(value string) string {
	return systemdEscape(cEscape(value))
}
