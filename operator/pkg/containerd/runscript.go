// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"fmt"
	"github.com/zenthros/operator/pkg/shellword"
	"path"
	"path/filepath"
	"sort"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/systemd"
)

// RunScriptName is the per-job wrapper systemd ExecStart invokes.
const RunScriptName = systemd.ContainerRunScript

// RunScriptPath is JobDir/ctr-run.
func RunScriptPath(jobID string, cfg systemd.Config) string {
	return RunScriptPathFor("", jobID, cfg)
}

// RunScriptPathFor uses the workspace-scoped JobDir when workspace is set.
func RunScriptPathFor(workspace, jobID string, cfg systemd.Config) string {
	return systemd.IsolatedJobDir(workspace, jobID, cfg) + "/" + RunScriptName
}

// BindMount is a host path mounted at the same path in the container.
type BindMount struct {
	Source string
	Dest   string
	// ReadOnly binds it read-only.
	ReadOnly bool
}

// PreemptDir is where the agent writes interruption notices
// (agent.DefaultPreemptDir).
const PreemptDir = "/var/run/zenthros/preempt"

// RunRoot is the machine's /var/run/zenthros. A container never sees the
// host's: its RunRoot is its own job's run directory (RunOpts.RunDir), and
// the host's report socket directory and notices are bound into it
// read-only. The host's holds every job's state.
const RunRoot = "/var/run/zenthros"

// RunOpts is the filesystem and image mapping for one job.
type RunOpts struct {
	Image     string
	Container string
	Cwd       string
	WorkDir   string
	TmpDir    string
	// RunDir is the job's own directory the container sees as RunRoot.
	RunDir string
	// ReportSocketDir holds the agent's report socket and nothing else; it
	// is bound read-only at the same path, so a job reaches the socket and
	// cannot replace it.
	ReportSocketDir string
	HostPath        string
	// HostPathSource is HostPath resolved (ConfineHostMounts): what the
	// container binds at HostPath.
	HostPathSource string
	// CwdSource is the host directory behind Cwd when Cwd is outside the
	// job's own directories; see ConfineHostMounts.
	CwdSource    string
	ExtraMounts  []BindMount
	Env          map[string]string
	EnvFile      string
	InnerWrapper string
	Remove       bool
	// GPUs is how many of the machine's GPUs the container gets (ctr run
	// --gpus 0..GPUs-1, through the NVIDIA container toolkit).
	GPUs int
	// NetNS, when set, is the network namespace of the job's own (a
	// backend other Workspaces share, jobnet.go); empty runs with the
	// host's network. ResolvConf is the file written for its DNS.
	NetNS      string
	ResolvConf string
	// User is the job user its process runs as (systemd.JobUser), by its
	// uid and gid on this machine: the image knows no such name. Empty runs
	// as the image says.
	User string
	// UserNS runs the job in a user namespace of its Workspace's (userns.go)
	// when it has a User and no GPUs; the machine can (UserNamespaces).
	UserNS bool
}

// RunOptsFromSpec builds mounts from the same path rules as the systemd executor:
// JobDir/work for git/inline, source.hostPath at the same path, JobDir/tmp as /tmp.
func RunOptsFromSpec(spec jobspec.JobSpec, cfg systemd.Config, env map[string]string, innerWrapper string) RunOpts {
	jobDir := systemd.IsolatedJobDir(spec.Workspace, spec.ID, cfg)
	workDir := ""
	if spec.Source.Git != "" || spec.Source.Inline != "" {
		workDir = jobDir + "/work"
	}
	cwd := spec.WorkingDir
	if cwd == "" {
		cwd = workDir
	}
	if cwd == "" {
		cwd = spec.Source.HostPath
	}
	_, _, gpus := v1.SizesOf(spec.Workers, spec.Compute)
	opts := RunOpts{
		Image:        NormalizeImageRef(spec.Image),
		Container:    ContainerName(spec.Workspace, spec.ID),
		Cwd:          cwd,
		WorkDir:      workDir,
		TmpDir:       jobDir + "/tmp",
		RunDir:       jobDir + "/run",
		HostPath:     spec.Source.HostPath,
		Env:          env,
		EnvFile:      jobDir + "/ctr-env",
		InnerWrapper: innerWrapper,
		Remove:       spec.Lifespan != v1.LifespanRetain,
		GPUs:         int(gpus),
	}
	if spec.IsolatedNetwork {
		opts.NetNS = JobNetName(spec.ID)
		opts.ResolvConf = jobDir + "/resolv.conf"
	}
	return opts
}

// ConfineHostMounts limits what the job's container sees of the host. The
// container is the isolation boundary; a rw bind of any host path is root on
// the host. source.hostPath must resolve, through resolve
// (symlinks), under one of roots, which the machine's owner sets. A
// workingDir outside the job's directories is backed by a private directory
// under jobDir instead of the host directory of the same name.
func (o *RunOpts) ConfineHostMounts(jobDir string, roots []string, resolve func(string) (string, error)) error {
	if o.HostPath != "" {
		// The path checked is the one bound: the name may be a symlink
		// pointed elsewhere after the check (07#1).
		resolved, err := systemd.ConfineHostPath(o.HostPath, roots, resolve)
		if err != nil {
			return err
		}
		o.HostPathSource = resolved
	}
	if o.Cwd != "" && o.Cwd != "/tmp" && o.Cwd != o.WorkDir && o.Cwd != o.HostPath && !systemd.UnderAny(filepath.Clean(o.Cwd), []string{jobDir}) {
		o.CwdSource = filepath.Join(jobDir, "cwd", filepath.Clean("/"+o.Cwd))
	}
	// ctr reads a mount comma-separated: a path with a comma adds options,
	// and one with a newline breaks the script's line (07#3).
	for _, m := range mounts(*o) {
		if strings.ContainsAny(m.Source, ",\n") || strings.ContainsAny(m.Dest, ",\n") {
			return fmt.Errorf("%w: a container mount cannot take the path %q: it holds a comma or a newline", backend.ErrRejected, m.Dest)
		}
	}
	return nil
}

// ContainerName is zenthros-ws-<workspace>-<jobid> when a Workspace is set so a
// Retain corpse cannot collide with the next task. Without a Workspace it matches
// the systemd unit name.
func ContainerName(workspace, jobID string) string {
	unit := strings.TrimSuffix(systemd.UnitName(jobID), ".service")
	if workspace == "" {
		return unit
	}
	return "zenthros-ws-" + workspace + "-" + jobID
}

// ctrEnvReadLoop loads the env file into `ctr run --env` arguments. A quoted
// value may span lines; the closing quote is real only when an even number of
// backslashes precedes it, so a newline inside a PEM is one variable.
const ctrEnvReadLoop = `  quote_closed() {
    local s="$1" i bs=0
    [ "${s: -1}" = '"' ] || return 1
    i=$(( ${#s} - 2 ))
    while [ "$i" -ge 0 ] && [ "${s:i:1}" = '\' ]; do
      bs=$((bs + 1))
      i=$((i - 1))
    done
    [ $((bs % 2)) -eq 0 ]
  }
  while IFS= read -r line || [ -n "${line:-}" ]; do
    [ -n "$line" ] || continue
    case "$line" in
      *'="'*)
        env_key="${line%%=*}"
        env_value="${line#*=}"
        env_value="${env_value#\"}"
        while ! quote_closed "$env_value"; do
          IFS= read -r line || break
          env_value="${env_value}"$'\n'"${line}"
        done
        env_value="${env_value%\"}"
        env_plain=""
        env_src="$env_value"
        env_len=${#env_src}
        if [ "$env_len" -gt 0 ]; then
          for ((env_i=0; env_i<env_len; env_i++)); do
            env_c="${env_src:env_i:1}"
            if [ "$env_c" = '\' ]; then
              env_n="${env_src:env_i+1:1}"
              env_plain+="$env_n"
              env_i=$((env_i+1))
            else
              env_plain+="$env_c"
            fi
          done
        fi
        args+=(--env "${env_key}=${env_plain}")
        ;;
      *)
        args+=(--env "$line")
        ;;
    esac
`

// FormatEnvFile writes KEY=VALUE lines for `ctr run --env`. Values stay in this
// 0600 file instead of the world-readable wrapper or sudo argv. A newline stays
// inside a quoted value so a PEM or kubeconfig is not flattened.
func FormatEnvFile(env map[string]string) string {
	if len(env) == 0 {
		return ""
	}
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "%s=%s\n", name, formatEnvValue(env[name]))
	}
	return b.String()
}

func formatEnvValue(value string) string {
	if !strings.Contains(value, "\n") && !strings.Contains(value, `\`) && !strings.Contains(value, `"`) {
		return value
	}
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`
}

// DroppedCapabilities are containerd's default capabilities
// (oci.defaultUnixCaps), each dropped by name: what --cap-drop ALL would
// mean, without counting on ctr reading ALL.
var DroppedCapabilities = []string{
	"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FSETID", "CAP_FOWNER", "CAP_MKNOD", "CAP_NET_RAW", "CAP_SETGID",
	"CAP_SETUID", "CAP_SETFCAP", "CAP_SETPCAP", "CAP_NET_BIND_SERVICE", "CAP_SYS_CHROOT", "CAP_KILL", "CAP_AUDIT_WRITE",
}

// RenderRunScript is a bash wrapper systemd starts. It bind-mounts job paths and
// execs `ctr run` with the unit's original argv so journalctl still captures logs.
// The unit already runs as root; the wrapper does not sudo (avoids logging env).
func RenderRunScript(opts RunOpts) string {
	var b strings.Builder
	b.WriteString("#!/bin/bash\n")
	b.WriteString("set -euo pipefail\n")
	b.WriteString("# Generated by Zenthros containerd executor. Do not edit.\n")
	if opts.NetNS != "" {
		b.WriteString(jobNetFunctions(opts.NetNS))
		fmt.Fprintf(&b, "if [ \"${1:-}\" = %s ]; then zj_down; exit 0; fi\n", NetDownArg)
	} else {
		fmt.Fprintf(&b, "if [ \"${1:-}\" = %s ]; then exit 0; fi\n", NetDownArg)
	}
	fmt.Fprintf(&b, "image=%s\n", shellword.Quote(opts.Image))
	fmt.Fprintf(&b, "name=%s\n", shellword.Quote(opts.Container))
	if opts.User != "" {
		fmt.Fprintf(&b, "job_uid=$(id -u %[1]s)\njob_gid=$(id -g %[1]s)\n", shellword.Quote(opts.User))
	}
	userns := opts.userNS()
	if userns {
		b.WriteString(idMapFunction)
		b.WriteString("job_uidmap=$(zj_idmap \"$job_uid\")\njob_gidmap=$(zj_idmap \"$job_gid\")\n")
	}
	run := "run"
	if opts.Remove {
		run += " --rm"
	}
	// Read-only binds of directories the agent fills later: they must exist
	// before runc mounts them.
	fmt.Fprintf(&b, "mkdir -p %s\n", shellword.Quote(PreemptDir))
	if opts.NetNS != "" {
		fmt.Fprintf(&b, "mkdir -p %s\n", shellword.Quote(path.Dir(opts.ResolvConf)))
		b.WriteString(jobNetUp(opts.ResolvConf, userns))
		joins := ""
		if userns {
			joins = " --with-ns \"user:$zj_userns\""
		}
		b.WriteString("args=(/usr/local/bin/ctr -n " + Namespace + " " + run + " --read-only" + joins + " --with-ns \"network:/var/run/netns/$zj_ns\")\n")
	} else {
		b.WriteString("args=(/usr/local/bin/ctr -n " + Namespace + " " + run + " --read-only --net-host)\n")
		if userns {
			// The host's network is the host user namespace's: its /sys,
			// bound, in place of the sysfs runc could not mount. Its
			// cgroup tree comes with it; an empty tmpfs covers it, as
			// a job's own sysfs shows none.
			fmt.Fprintf(&b, "args+=(--mount %s)\n", shellword.Quote("type=bind,src=/sys,dst=/sys,options=rbind:rro:nosuid:nodev:noexec"))
			fmt.Fprintf(&b, "args+=(--mount %s)\n", shellword.Quote("type=tmpfs,src=tmpfs,dst=/sys/fs/cgroup,options=ro:nosuid:nodev:noexec"))
		}
	}
	for _, m := range mounts(opts) {
		mode := "ro"
		if !m.ReadOnly {
			// A rw bind is a directory the job writes; a read-only one may
			// be a file (a secret, the agent binary) and must exist already.
			fmt.Fprintf(&b, "mkdir -p %s\n", shellword.Quote(m.Source))
			mode = "rw"
		}
		fmt.Fprintf(&b, "args+=(--mount %s)\n", shellword.Quote("type=bind,src="+m.Source+",dst="+m.Dest+",options=rbind:"+mode))
	}
	if opts.Cwd != "" {
		fmt.Fprintf(&b, "args+=(--cwd %s)\n", shellword.Quote(opts.Cwd))
	}
	if opts.User != "" {
		b.WriteString("args+=(--user \"$job_uid:$job_gid\")\n")
	}
	if userns {
		b.WriteString("for zj_m in $job_uidmap; do args+=(--uidmap \"$zj_m\"); done\n")
		b.WriteString("for zj_m in $job_gidmap; do args+=(--gidmap \"$zj_m\"); done\n")
		b.WriteString("args+=(--remap-labels)\n")
	}
	// containerd's default seccomp profile, and none of its default
	// capabilities: a job is a user with nothing to raise. No new
	// privileges is ctr's default (never --allow-new-privs), and never
	// --privileged.
	b.WriteString("args+=(--seccomp")
	for _, c := range DroppedCapabilities {
		b.WriteString(" --cap-drop " + c)
	}
	b.WriteString(")\n")
	for i := 0; i < opts.GPUs; i++ {
		fmt.Fprintf(&b, "args+=(--gpus %d)\n", i)
	}
	if opts.EnvFile != "" {
		fmt.Fprintf(&b, "if [ -f %s ]; then\n", shellword.Quote(opts.EnvFile))
		b.WriteString(ctrEnvReadLoop)
		fmt.Fprintf(&b, "  done < %s\n", shellword.Quote(opts.EnvFile))
		b.WriteString("fi\n")
	}
	b.WriteString("args+=(\"$image\" \"$name\")\n")
	if opts.InnerWrapper != "" {
		fmt.Fprintf(&b, "args+=(%s)\n", shellword.Quote(opts.InnerWrapper))
	}
	b.WriteString("args+=(\"$@\")\n")
	b.WriteString("exec \"${args[@]}\"\n")
	return b.String()
}

func mounts(opts RunOpts) []BindMount {
	seen := map[string]struct{}{}
	var out []BindMount
	add := func(src, dst string) {
		if src == "" || dst == "" {
			return
		}
		key := src + "->" + dst
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, BindMount{Source: src, Dest: dst})
	}
	add(opts.TmpDir, "/tmp")
	add(opts.WorkDir, opts.WorkDir)
	hostSource := opts.HostPath
	if opts.HostPathSource != "" {
		hostSource = opts.HostPathSource
	}
	add(hostSource, opts.HostPath)
	// The job's own RunRoot, never the host's: that one holds every job's
	// state.
	add(opts.RunDir, RunRoot)
	// Job process publishes ExecutionReports on the agent unix socket, in a
	// directory of its own, read-only: connecting needs no write, and the
	// socket cannot be replaced. Never RunRoot itself.
	if dir := filepath.Clean(opts.ReportSocketDir); opts.ReportSocketDir != "" && dir != RunRoot && dir != "/" {
		out = append(out, BindMount{Source: dir, Dest: dir, ReadOnly: true})
	}
	// Interruption notices: the job reads them, and cannot write one, its own
	// or another job's. After the directory above, so it binds over it.
	out = append(out, BindMount{Source: PreemptDir, Dest: PreemptDir, ReadOnly: true})
	if opts.Cwd != "" && opts.Cwd != "/tmp" && opts.Cwd != opts.WorkDir && opts.Cwd != opts.HostPath {
		source := opts.Cwd
		if opts.CwdSource != "" {
			source = opts.CwdSource
		}
		add(source, opts.Cwd)
	}
	for _, m := range opts.ExtraMounts {
		if m.ReadOnly {
			out = append(out, m)
			continue
		}
		add(m.Source, m.Dest)
	}
	if opts.NetNS != "" && opts.ResolvConf != "" {
		out = append(out, BindMount{Source: opts.ResolvConf, Dest: "/etc/resolv.conf", ReadOnly: true})
	}
	if opts.InnerWrapper != "" {
		// Secret wrapper and files stay at the same host path inside the
		// container, read-only: the job's own secret root.
		dir := opts.InnerWrapper
		if i := strings.LastIndex(dir, "/"); i > 0 {
			dir = dir[:i]
		}
		out = append(out, BindMount{Source: dir, Dest: dir, ReadOnly: true})
	}
	return out
}
