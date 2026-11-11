// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/systemd"
)

func TestRunOptsFromSpecMatchesSystemdPaths(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	spec := jobspec.JobSpec{
		ID:         "uid-123",
		Image:      "python:3.13.14-slim-bookworm",
		Source:     v1.Source{Git: "https://example.com/r.git"},
		WorkingDir: "",
	}
	opts := RunOptsFromSpec(spec, cfg, map[string]string{"A": "1"}, "")
	if opts.WorkDir != "/var/lib/zenthros/jobs/uid-123/work" {
		t.Errorf("WorkDir = %q", opts.WorkDir)
	}
	if opts.TmpDir != "/var/lib/zenthros/jobs/uid-123/tmp" {
		t.Errorf("TmpDir = %q", opts.TmpDir)
	}
	if opts.Cwd != opts.WorkDir {
		t.Errorf("Cwd = %q, want work dir", opts.Cwd)
	}
	if opts.Image != "docker.io/library/python:3.13.14-slim-bookworm" {
		t.Errorf("Image = %q", opts.Image)
	}
	if opts.Container != "zenthros-uid-123" {
		t.Errorf("Container = %q", opts.Container)
	}
	if !opts.Remove {
		t.Error("ephemeral default must --rm")
	}
}

func TestRunOptsWorkspaceNameAndJobDir(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem}
	spec := jobspec.JobSpec{
		ID:        "uid-123",
		Workspace: "research",
		Image:     "python:3.13.14-slim-bookworm",
		Source:    v1.Source{Git: "https://example.com/r.git"},
		Lifespan:  v1.LifespanRetain,
	}
	opts := RunOptsFromSpec(spec, cfg, nil, "")
	if opts.Container != "zenthros-ws-research-uid-123" {
		t.Errorf("Container = %q", opts.Container)
	}
	if opts.WorkDir != "/var/lib/zenthros/workspaces/research/jobs/uid-123/work" {
		t.Errorf("WorkDir = %q", opts.WorkDir)
	}
	if opts.Remove {
		t.Error("Retain must not --rm")
	}
}

func TestRenderRunScriptMountsWorkAndTmp(t *testing.T) {
	script := RenderRunScript(RunOpts{
		Image:     "docker.io/library/python:3.13.14-slim-bookworm",
		Container: "zenthros-uid-123",
		Cwd:       "/var/lib/zenthros/jobs/uid-123/work",
		WorkDir:   "/var/lib/zenthros/jobs/uid-123/work",
		TmpDir:    "/var/lib/zenthros/jobs/uid-123/tmp",
		RunDir:    "/var/lib/zenthros/jobs/uid-123/run",
		HostPath:  "/tmp/bench",
		EnvFile:   "/var/lib/zenthros/jobs/uid-123/ctr-env",
		Remove:    true,
	})
	for _, want := range []string{
		"args=(/usr/local/bin/ctr -n zenthros run --rm --read-only --net-host)",
		"mkdir -p '/var/lib/zenthros/jobs/uid-123/tmp'",
		"mkdir -p '/var/lib/zenthros/jobs/uid-123/work'",
		"src=/var/lib/zenthros/jobs/uid-123/tmp,dst=/tmp",
		"src=/var/lib/zenthros/jobs/uid-123/work,dst=/var/lib/zenthros/jobs/uid-123/work",
		"src=/tmp/bench,dst=/tmp/bench",
		"src=/var/lib/zenthros/jobs/uid-123/run,dst=/var/run/zenthros,options=rbind:rw",
		"--cwd '/var/lib/zenthros/jobs/uid-123/work'",
		"ctr-env",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q:\n%s", want, script)
		}
	}
	if !strings.Contains(script, "--net-host") {
		t.Error("playground jobs need host net for GCS/metadata; no CNI on the VM")
	}
	if strings.Contains(script, "sudo") {
		t.Error("unit already runs as root; wrapper must not sudo ctr")
	}
}

func TestRenderRunScriptInnerWrapperMountsSecretDir(t *testing.T) {
	script := RenderRunScript(RunOpts{
		Image:        "docker.io/library/python:latest",
		Container:    "zenthros-j",
		TmpDir:       "/var/lib/zenthros/jobs/j/tmp",
		InnerWrapper: "/dev/shm/zenthros/secrets/j/wrapper",
	})
	if !strings.Contains(script, "src=/dev/shm/zenthros/secrets/j,dst=/dev/shm/zenthros/secrets/j,options=rbind:ro") {
		t.Errorf("secret dir not mounted:\n%s", script)
	}
	if !strings.Contains(script, "'/dev/shm/zenthros/secrets/j/wrapper'") {
		t.Errorf("inner wrapper not in argv:\n%s", script)
	}
}

// A containerd job reads interruption notices and cannot write one: the
// notice directory is bound read-only over the job's rw run directory, after it.
func TestPreemptDirBoundReadOnly(t *testing.T) {
	script := RenderRunScript(RunOpts{Image: "img", Container: "c", RunDir: "/var/lib/zenthros/jobs/c/run"})
	rw := strings.Index(script, "type=bind,src=/var/lib/zenthros/jobs/c/run,dst="+RunRoot+",options=rbind:rw")
	ro := strings.Index(script, "type=bind,src="+PreemptDir+",dst="+PreemptDir+",options=rbind:ro")
	if rw < 0 || ro < 0 || ro < rw {
		t.Fatalf("run dir at %d, notice dir (ro) at %d, want the ro bind after:\n%s", rw, ro, script)
	}
}

// The host's /var/run/zenthros held every job's default file secrets and
// the report socket all of them share, and was bound rw into every
// container: a tenant read another's secrets and could replace the socket.
// A container sees its own run directory there, the socket's directory
// read-only, and never the host's directory.
func TestContainerdJobMountsOnlyItsOwnRunDir(t *testing.T) {
	opts := RunOpts{Image: "img", Container: "c", RunDir: "/var/lib/zenthros/jobs/c/run",
		ReportSocketDir: "/var/run/zenthros/agent", TmpDir: "/var/lib/zenthros/jobs/c/tmp"}
	for _, m := range mounts(opts) {
		if m.Source == RunRoot {
			t.Fatalf("the host's %s is bound into the container: %+v", RunRoot, m)
		}
		if m.Source == opts.ReportSocketDir && !m.ReadOnly {
			t.Fatalf("report socket directory bound rw: %+v", m)
		}
	}
	script := RenderRunScript(opts)
	for _, want := range []string{
		"src=/var/lib/zenthros/jobs/c/run,dst=/var/run/zenthros,options=rbind:rw",
		"src=/var/run/zenthros/agent,dst=/var/run/zenthros/agent,options=rbind:ro",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q:\n%s", want, script)
		}
	}
	// A socket configured in RunRoot itself is not bound: that would be the
	// host's directory again.
	opts.ReportSocketDir = RunRoot
	for _, m := range mounts(opts) {
		if m.Source == RunRoot {
			t.Fatalf("socket dir %s bound: %+v", RunRoot, m)
		}
	}
}

// A read-only bind may be a file (a secret, the agent binary): the wrapper
// runs under set -e, and mkdir -p on an existing file fails, so only the rw
// (directory) sources are created.
func TestRunScriptCreatesOnlyWritableSources(t *testing.T) {
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, []byte("v"), 0o600); err != nil {
		t.Fatal(err)
	}
	argv := runWrapper(t, RunOpts{Image: "img", Container: "c", TmpDir: filepath.Join(t.TempDir(), "tmp"),
		ExtraMounts: []BindMount{{Source: file, Dest: "/run/secrets/token", ReadOnly: true}}})
	want := "type=bind,src=" + file + ",dst=/run/secrets/token,options=rbind:ro"
	if !slices.Contains(argv, want) {
		t.Fatalf("argv lacks %q:\n%s", want, strings.Join(argv, "\n"))
	}
}

// Every directory the script binds must exist before ctr runs, or runc
// refuses the container. The preempt directory is bound read-only and the
// agent creates it only for a first notice: once it stopped being part of
// a read-write bind of /var/run/zenthros, nothing created it and every
// containerd job failed to start ("open /var/run/zenthros/preempt").
func TestRunScriptCreatesThePreemptDirItBinds(t *testing.T) {
	for _, opts := range []RunOpts{
		{Image: "img", Container: "c"},
		{Image: "img", Container: "c", NetNS: JobNetName("j"), ResolvConf: "/var/lib/zenthros/jobs/j/resolv.conf"},
	} {
		script := RenderRunScript(opts)
		mkdir := strings.Index(script, "mkdir -p '"+PreemptDir+"'")
		if mkdir < 0 || mkdir > strings.Index(script, "exec \"${args[@]}\"") {
			t.Errorf("the script binds %s without creating it first:\n%s", PreemptDir, script)
		}
		if opts.ResolvConf != "" && !strings.Contains(script, "mkdir -p '/var/lib/zenthros/jobs/j'") {
			t.Errorf("the resolv.conf directory is not created:\n%s", script)
		}
	}
}

// gpu= of the SDK is spec.compute. The container read only workers and ran
// without the GPU the job asked for: the same rule as a Job's pod (v1.SizesOf).
func TestAContainerHasTheGPUsSpecComputeAsked(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	base := jobspec.JobSpec{ID: "u", Image: "python:3.13"}
	asked := base
	asked.Compute = &v1.ComputeConstraints{GPUCount: 2}
	if got := RunOptsFromSpec(asked, cfg, nil, "").GPUs; got != 2 {
		t.Errorf("compute.gpuCount=2 runs with %d GPUs", got)
	}
	workers := base
	workers.Workers.GPUsPerNode = 1
	if got := RunOptsFromSpec(workers, cfg, nil, "").GPUs; got != 1 {
		t.Errorf("workers.gpusPerNode=1 runs with %d GPUs", got)
	}
	if got := RunOptsFromSpec(base, cfg, nil, "").GPUs; got != 0 {
		t.Errorf("asking for none runs with %d GPUs", got)
	}
}
