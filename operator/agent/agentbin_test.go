// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/systemd"
)

// A job finds the agent binary, for `$ZENTHROS_AGENT_BIN job ...`: the
// host's path as a process, the bound path in a container (read-only, so a
// job cannot replace the agent). E2E TestShellJobCheckpointsWithAgentCLI.
func TestJobSeesAgentBin(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}

	runner := &recordingRunner{}
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard()), AgentBin: "/opt/z/agent"}
	spec := vmTestSpec()
	spec.Runtime = "systemd"
	if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
		t.Fatal(err)
	}
	if env := writtenTeeBody(t, runner.commands, ".service.env"); !strings.Contains(env, "ZENTHROS_AGENT_BIN=\"/opt/z/agent\"\n") {
		t.Fatalf("process job: its environment has no ZENTHROS_AGENT_BIN:\n%s", env)
	}

	runner = &recordingRunner{}
	exec.Manager = systemd.NewManager(cfg, runner, logr.Discard())
	if _, err := exec.Submit(context.Background(), containerdSpec(), nil); err != nil {
		t.Fatal(err)
	}
	if env := writtenTeeBody(t, runner.commands, "ctr-env"); !strings.Contains(env, "ZENTHROS_AGENT_BIN="+ContainerAgentBin) {
		t.Fatalf("container env: %q", env)
	}
	script := writtenTeeBody(t, runner.commands, "ctr-run")
	if !strings.Contains(script, "src=/opt/z/agent,dst="+ContainerAgentBin+",options=rbind:ro") {
		t.Fatalf("container run script does not bind the agent read-only:\n%s", script)
	}
}

// Where the agent is installed is not what runs: a unit an agent installed
// before it set ZENTHROS_AGENT_BIN (or from another path) is adopted, not
// failed SPEC_MISMATCH, by an upgraded agent.
func TestAgentBinOutsideSpecHash(t *testing.T) {
	if !jobspec.ControllerDerivedEnv(jobspec.EnvAgentBin) {
		t.Fatal("ZENTHROS_AGENT_BIN is part of the spec hash")
	}
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	spec := vmTestSpec()
	spec.Runtime = "systemd"
	before := systemd.RenderUnit(spec, nil, cfg, "")
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, mismatchRunner{unitContent: before}, logr.Discard()), AgentBin: "/usr/local/bin/zenthros-agent"}
	if _, err := exec.Submit(context.Background(), spec, nil); errors.Is(err, backend.ErrSpecMismatch) {
		t.Fatalf("upgraded agent refused its own unit: %v", err)
	}
}

// A containerd job's function environments outlive it: its Workspace's
// directory on the host, bound read-write where the SDK looks
// (ZENTHROS_ENV_CACHE), made before the job starts. Under HOME=/tmp every
// job built the runner's venv again (lab, 2026-10-18: 36 s against 6 s on
// systemd). Another Workspace's job gets another directory.
func TestContainerdJobsKeepTheirEnvironments(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	for ws, host := range map[string]string{"risk": "/var/lib/zenthros/envs/risk", "other": "/var/lib/zenthros/envs/other", "": "/var/lib/zenthros/envs/_"} {
		runner := &recordingRunner{}
		exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}
		spec := containerdSpec()
		spec.Workspace = ws
		if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
			t.Fatal(err)
		}
		if env := writtenTeeBody(t, runner.commands, "ctr-env"); !strings.Contains(env, "ZENTHROS_ENV_CACHE="+ContainerEnvsDir) {
			t.Fatalf("workspace %q: the job is not told where its environments are: %q", ws, env)
		}
		script := writtenTeeBody(t, runner.commands, "ctr-run")
		if !strings.Contains(script, "src="+host+",dst="+ContainerEnvsDir+",options=rbind:rw") {
			t.Fatalf("workspace %q: %s is not bound read-write at %s:\n%s", ws, host, ContainerEnvsDir, script)
		}
		// Made 0700 and given to the Workspace's job user, the container
		// process's: another Workspace's job reads none of it.
		user := systemd.JobUser(ws)
		made, given := -1, -1
		for i, c := range runner.commands {
			if strings.Contains(c, `mkdir -p -m 0700 '`+host+`'`) {
				made = i
			}
			if strings.Contains(c, `chown -h '`+user+":"+user+`' '`+host+`'`) {
				given = i
			}
		}
		if made < 0 || given < made {
			t.Fatalf("workspace %q: %s is not made, then given to %s: %v", ws, host, user, runner.commands)
		}
	}
	if got := systemd.EnvsDir("../../etc"); got != "/var/lib/zenthros/envs/etc" {
		t.Fatalf("a Workspace name leaves the envs root: %s", got)
	}
}

// A container job finds the machine's uv first on its PATH, read-only (a job
// cannot replace what the next one runs), in an image with no uv of its own
// (Alpine's among them: the static build); its packages come from the
// Workspace's installers' cache, as a host job's. A machine without uv binds
// none. E2E test_attach_flow (containerd: slim-bookworm and alpine).
func TestContainerJobsHaveTheMachinesUV(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	for _, uv := range []string{HostUV, ""} {
		runner := &recordingRunner{}
		exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard()), UV: uv}
		if _, err := exec.Submit(context.Background(), containerdSpec(), nil); err != nil {
			t.Fatal(err)
		}
		script := writtenTeeBody(t, runner.commands, "ctr-run")
		bound := strings.Contains(script, "src="+HostUV+",dst="+ContainerUV+",options=rbind:ro")
		if bound != (uv != "") || strings.Contains(script, "dst="+ContainerUV+",options=rbind:rw") {
			t.Errorf("uv %q: bound read-only %v:\n%s", uv, bound, script)
		}
		if env := writtenTeeBody(t, runner.commands, "ctr-env"); !strings.Contains(env, "XDG_CACHE_HOME="+ContainerEnvsDir+"/cache\n") {
			t.Errorf("uv %q: the installers' cache is not the Workspace's: %q", uv, env)
		}
	}
	if ContainerUV != "/usr/local/bin/uv" || systemd.InstallCacheIn(ContainerEnvsDir) != ContainerEnvsDir+"/cache" ||
		systemd.InstallCacheDir("risk") != systemd.InstallCacheIn(systemd.EnvsDir("risk")) {
		t.Error("a container job's cache is not where a host job's is in the same environments")
	}
}
