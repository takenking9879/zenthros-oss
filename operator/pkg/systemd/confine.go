// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/shellword"
)

// A job never runs as root on a machine (docs/design/machine-privileges.md).
// Each Workspace has a user of its own there, JobUser, which its jobs run
// as: a host job's unit says User=, a container's process takes its uid. It
// writes only its own directories (its work, its home, its Workspace's
// function environments) and the folders the machine's owner opened to jobs
// (HostRootsEnv). What the agent writes as root stays outside all of them.

// JobUser is the user a Workspace's jobs run as on a machine: zj- and the
// first 8 hex of the SHA-256 of the Workspace's name. One per Workspace,
// not per job: its function environments (EnvsDir) are shared by its jobs
// and would change owner on every job.
func JobUser(workspace string) string {
	sum := sha256.Sum256([]byte(workspace))
	return "zj-" + hex.EncodeToString(sum[:4])
}

// EnsureJobUser makes the Workspace's job user on its first job: a system
// user and group of that name, with no home and no login shell. One at a
// time: useradd locks the user database, and refuses a second one at once.
// It returns the user's name.
func (m *Manager) EnsureJobUser(ctx context.Context, workspace string) (string, error) {
	m.usersMu.Lock()
	defer m.usersMu.Unlock()
	name := JobUser(workspace)
	cmd := fmt.Sprintf(`id -u %[1]s >/dev/null 2>&1 || %[2]suseradd --system --user-group --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin %[1]s`,
		name, m.cfg.SudoPrefix())
	if _, err := m.runner.Run(ctx, cmd); err != nil {
		return "", fmt.Errorf("make the job user %s: %w", name, err)
	}
	return name, nil
}

// EnvsRoot holds the function environments of every Workspace on a machine.
const EnvsRoot = "/var/lib/zenthros/envs"

// EnvsDir is a Workspace's function environments on this machine: the
// runner's venv, a function's own (packages=), the machine's base (zenthros
// install), by the SDK's hashes, and the installers' cache. They outlive a
// job. Its job user owns it, 0700: another Workspace's job does not read it.
// A container job sees it at EnvsRoot.
func EnvsDir(workspace string) string {
	if workspace == "" {
		workspace = "_"
	}
	return filepath.Join(EnvsRoot, filepath.Base(filepath.Clean("/"+workspace)))
}

// HostEnvsDir is where a host job builds its environments: a directory of
// its own in EnvsDir. A container job's are EnvsDir's top level, built with
// its image's Python: a venv names the interpreter it was made from, and the
// host's is another.
func HostEnvsDir(workspace string) string {
	return EnvsDir(workspace) + "/host"
}

// InstallCacheDir is the installers' cache (uv, pip) a Workspace's jobs
// share, as XDG_CACHE_HOME: what one job downloaded the next one reuses.
func InstallCacheDir(workspace string) string {
	return InstallCacheIn(EnvsDir(workspace))
}

// InstallCacheIn is that cache in a Workspace's environments directory where
// a job sees it: EnvsDir on the host, where it is bound in a container.
func InstallCacheIn(envs string) string {
	return envs + "/cache"
}

// HomeDir is a job's own home, in its job directory.
func HomeDir(workspace, jobID string, cfg Config) string {
	return IsolatedJobDir(workspace, jobID, cfg) + "/home"
}

// HostRootsEnv names the folders of a machine its owner opened to jobs
// (zenadm join --folder): colon-separated absolute paths. A job's
// source.hostPath must be under one of them, and a host job writes there,
// with what the folder's own permissions give its user. Unset opens none.
const HostRootsEnv = "ZENTHROS_HOSTPATH_ROOTS"

// HostRoots parses HostRootsEnv's value.
func HostRoots(value string) []string {
	var roots []string
	for _, root := range strings.Split(value, ":") {
		if root = strings.TrimSpace(root); filepath.IsAbs(root) {
			roots = append(roots, filepath.Clean(root))
		}
	}
	return roots
}

// ConfineHostPath is a job's source.hostPath resolved through resolve
// (symlinks) and checked to be under one of roots, the machine's owner's
// folders: the path a job may use. Using the resolved one, not the name,
// keeps a link swapped in after the check from taking the job elsewhere.
func ConfineHostPath(hostPath string, roots []string, resolve func(string) (string, error)) (string, error) {
	if !filepath.IsAbs(hostPath) {
		return "", fmt.Errorf("%w: source.hostPath %q is not absolute", backend.ErrRejected, hostPath)
	}
	resolved, err := resolve(filepath.Clean(hostPath))
	if err != nil {
		return "", fmt.Errorf("resolve source.hostPath %q: %w", hostPath, err)
	}
	if !UnderAny(resolved, roots) {
		return "", fmt.Errorf("%w: source.hostPath %q is outside the folders this machine opened to jobs (zenadm join --folder, %s)",
			backend.ErrRejected, hostPath, HostRootsEnv)
	}
	return resolved, nil
}

// UnderAny reports whether path is one of roots or inside one.
func UnderAny(path string, roots []string) bool {
	for _, root := range roots {
		if path == root || strings.HasPrefix(path, strings.TrimSuffix(root, "/")+"/") {
			return true
		}
	}
	return false
}

// AgentHomes are what the agent's unit hides behind an empty folder of its
// own (ProtectHome=tmpfs, zenadm's agentConfinement): its jobs see them, so
// what the agent wrote there for a job would not be where the job looks. The
// machine's open folders are bound into its view, as its jobs see them.
var AgentHomes = []string{"/home", "/root", "/run/user"}

// AgentSees reports whether the agent sees path as the machine's jobs do:
// outside the homes, or in one of the open folders.
func AgentSees(path string, folders []string) bool {
	path = filepath.Clean(path)
	return !UnderAny(path, AgentHomes) || UnderAny(path, folders)
}

// confinement is how a host job runs: as its Workspace's user, writing only
// its own directories and the machine's open folders.
type confinement struct {
	user string
	// home is the job's HOME, made for it; envs its Workspace's
	// environments.
	home, envs string
	// hostPath is the job's source.hostPath resolved (ConfineHostPath), or "".
	hostPath string
	// writable are the job's own directories it writes.
	writable []string
	// folders are the machine's open folders: written where they exist.
	folders []string
}

// GiveToJobUser gives paths, recursively and without following a link, to
// the Workspace's job user, making the user first. It returns the user.
func (m *Manager) GiveToJobUser(ctx context.Context, workspace string, paths ...string) (string, error) {
	user, err := m.EnsureJobUser(ctx, workspace)
	if err != nil {
		return "", err
	}
	if err := m.giveTo(ctx, user, "-hR", paths...); err != nil {
		return "", err
	}
	return user, nil
}

// giveTo chowns paths to user, with chown's flags; none is nothing to do.
func (m *Manager) giveTo(ctx context.Context, user, flags string, paths ...string) error {
	if len(paths) == 0 {
		return nil
	}
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = shellword.Quote(p)
	}
	cmd := fmt.Sprintf(`%schown %s %s %s`, m.cfg.SudoPrefix(), flags, shellword.Quote(user+":"+user), strings.Join(quoted, " "))
	if _, err := m.runner.Run(ctx, cmd); err != nil {
		return fmt.Errorf("give %s to %s: %w", strings.Join(paths, ", "), user, err)
	}
	return nil
}

// confine makes what a job needs to run as its Workspace's user, after its
// source is prepared as root and before its unit starts: the user, its
// Workspace's environments, the job's work directory given to it and, for
// a host job, a home of its own. The job directory itself stays root's: a
// job that owned it could plant a link where the agent, root, writes (the
// retain marker). A container's unit runs ctr as root and is not held
// itself (nil): its process is, by the container.
func (m *Manager) confine(ctx context.Context, spec jobspec.JobSpec, workDir, execWrapper string) (*confinement, error) {
	host := !containerUnit(execWrapper)
	jail := &confinement{
		home:    HomeDir(spec.Workspace, spec.ID, m.cfg),
		envs:    EnvsDir(spec.Workspace),
		folders: m.cfg.HostRoots,
	}
	if host && spec.Source.HostPath != "" {
		resolved, err := ConfineHostPath(spec.Source.HostPath, m.cfg.HostRoots, filepath.EvalSymlinks)
		if err != nil {
			return nil, err
		}
		jail.hostPath = resolved
	}
	user, err := m.EnsureJobUser(ctx, spec.Workspace)
	if err != nil {
		return nil, err
	}
	sudo := m.cfg.SudoPrefix()
	mkdir := fmt.Sprintf(`%[1]smkdir -p -m 0700 %[2]s && %[1]schmod 0700 %[2]s`, sudo, shellword.Quote(jail.envs))
	var owned []string
	if host {
		mkdir += fmt.Sprintf(` && %smkdir -p %s`, sudo, shellword.Quote(jail.home))
		owned = append(owned, jail.home)
	}
	if workDir != "" {
		owned = append(owned, workDir)
	}
	if _, err := m.runner.Run(ctx, mkdir); err != nil {
		return nil, fmt.Errorf("make the job's directories: %w", err)
	}
	// The environments' directory alone, not what is in it: its jobs made
	// that, as its user, and it can be large.
	if err := m.giveTo(ctx, user, "-h", jail.envs); err != nil {
		return nil, err
	}
	if err := m.giveTo(ctx, user, "-hR", owned...); err != nil {
		return nil, err
	}
	if !host {
		return nil, nil
	}
	jail.user = user
	jail.writable = append(owned, jail.envs)
	return jail, nil
}

// env is what a confined job's environment adds, unless the job set it: a
// home of its own, its Workspace's environments, and the installers' cache.
func (j *confinement) env(spec jobspec.JobSpec, env map[string]string) map[string]string {
	out := make(map[string]string, len(env)+3)
	for k, v := range env {
		out[k] = v
	}
	for name, value := range map[string]string{
		"HOME":             j.home,
		jobspec.EnvEnvsDir: HostEnvsDir(spec.Workspace),
		"XDG_CACHE_HOME":   InstallCacheDir(spec.Workspace),
	} {
		if _, ok := out[name]; !ok {
			out[name] = value
		}
	}
	return out
}

// directives are the unit's [Service] lines that hold the job: its user, no
// way to gain privileges, the whole file system read-only but its own
// directories, the open folders and the machine's /tmp and /var/tmp, and no
// other user's processes in its /proc. No PrivateDevices: it hides
// /dev/nvidia*. No PrivateTmp: a job's /tmp would go with its unit, and with
// it a file:// checkpoint or output its next attempt reads; /tmp and
// /var/tmp are what every local user may write, sticky.
func (j *confinement) directives() []string {
	lines := []string{
		"User=" + j.user,
		"Group=" + j.user,
		"NoNewPrivileges=yes",
		"CapabilityBoundingSet=",
		"AmbientCapabilities=",
		"ProtectSystem=strict",
		"ProtectHome=read-only",
		"ProtectProc=invisible",
		"RestrictSUIDSGID=yes",
		"RestrictNamespaces=yes",
		"ProtectKernelTunables=yes",
		"ProtectKernelModules=yes",
		"ProtectKernelLogs=yes",
		"ProtectControlGroups=yes",
	}
	paths := make([]string, 0, len(j.writable)+len(j.folders)+len(scratch))
	for _, dir := range append(append([]string{}, j.writable...), scratch...) {
		paths = append(paths, unitPathWord(dir))
	}
	// "-": an open folder the machine does not have is no reason to fail
	// the job.
	for _, dir := range j.folders {
		paths = append(paths, "-"+unitPathWord(dir))
	}
	return append(lines, "ReadWritePaths="+strings.Join(paths, " "))
}

// scratch are the machine's own places for anyone's temporary files.
var scratch = []string{"/tmp", "/var/tmp"}

// unitPathWord is a path as one word of a unit's path list: quoted, '%'
// doubled. The paths are the job's own directories, made of names
// ValidateUnitInputs checked, and the folders zenadm join took
// (zenadm.ValidFolder): none holds a quote, a backslash or a line break.
func unitPathWord(path string) string {
	return `"` + systemdEscape(path) + `"`
}

// envFileDir holds the system manager's job units' environments: root's,
// 0700.
const envFileDir = "/var/lib/zenthros/units"

// EnvFilePath is a system unit's environment file. A unit's Environment=
// lines are readable by every local user: the unit file is 0644, and
// systemd shows them to anyone over D-Bus. The file is root's and 0600, and
// systemd reads it as root.
func EnvFilePath(name string) string {
	return envFileDir + "/" + name + ".env"
}

// formatEnvFile renders env as systemd reads an EnvironmentFile: one
// NAME="value" per variable, sorted. In double quotes systemd keeps a
// newline and takes \ " $ and ` escaped; it expands nothing.
func formatEnvFile(env map[string]string) string {
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		value := env[name]
		for _, special := range []string{`\`, `"`, "$", "`"} {
			value = strings.ReplaceAll(value, special, `\`+special)
		}
		fmt.Fprintf(&b, "%s=\"%s\"\n", name, value)
	}
	return b.String()
}
