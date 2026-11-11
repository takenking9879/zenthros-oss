// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/agent/awssecret"
	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/shellword"
	"github.com/zenthros/operator/pkg/systemd"
)

func indexOf(commands []string, substr string) int {
	for i, c := range commands {
		if strings.Contains(c, substr) {
			return i
		}
	}
	return -1
}

func secretJob(runtime string) (jobspec.JobSpec, SystemdExecutor, *recordingRunner, string) {
	root, _ := os.MkdirTemp("", "zenthros-secrets-")
	runner := &recordingRunner{}
	spec := vmTestSpec()
	spec.Workspace = "team-a"
	spec.Runtime = runtime
	spec.SecretSources = []v1.SecretSourceRef{{
		Type:              v1.SecretSourceTypeAWSSecretsManager,
		AWSSecretsManager: &v1.AWSSecretsManagerRef{SecretID: "workload/token", Region: "us-east-2"},
		Materialization:   v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "TOKEN"},
	}}
	exec := SystemdExecutor{
		Manager: systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}, runner, logr.Discard()),
		Secrets: awssecret.Config{
			Root:     root,
			Identity: func(context.Context) (bool, error) { return true, nil },
			Factory: func(context.Context, string) (awssecret.Getter, error) {
				return getterStub(func(context.Context, awssecret.GetRequest) (string, error) { return "v", nil }), nil
			},
		},
	}
	return spec, exec, runner, root
}

// A job reads its secrets as its Workspace's user, on the host or in its
// container: the agent gives it its directory in the secret root once
// they are written, and before its unit starts. The root itself stays the
// agent's, one others pass through and cannot list.
func TestAJobsSecretsAreGivenToItsUserBeforeItStarts(t *testing.T) {
	for _, runtime := range []string{v1.RuntimeSystemd, v1.RuntimeContainerd} {
		spec, exec, runner, root := secretJob(runtime)
		t.Cleanup(func() { _ = os.RemoveAll(root) })
		if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
			t.Fatalf("%s: Submit: %v", runtime, err)
		}
		user := systemd.JobUser("team-a")
		given := indexOf(runner.commands, "chown -hR "+shellword.Quote(user+":"+user)+" "+shellword.Quote(secretfile.Dir(root, spec.ID)))
		start := indexOf(runner.commands, "enable --now")
		if given < 0 || start < given {
			t.Fatalf("%s: the secrets are not the job user's before it starts (chown %d, start %d): %v", runtime, given, start, runner.commands)
		}
		if _, err := os.Stat(filepath.Join(secretfile.Dir(root, spec.ID), "wrapper")); err != nil {
			t.Fatalf("%s: no wrapper: %v", runtime, err)
		}
		info, err := os.Stat(root)
		if err != nil || info.Mode().Perm() != secretfile.RootMode {
			t.Fatalf("%s: secret root mode %v (%v), want %o", runtime, info.Mode().Perm(), err, secretfile.RootMode)
		}
	}
}

// A container job's own directories are its user's, made before its unit
// starts; its process takes that user's uid in the container.
func TestAContainerJobsDirectoriesAreItsUsers(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &recordingRunner{}
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}
	spec := containerdSpec()
	spec.Workspace = "team-a"
	if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
		t.Fatal(err)
	}
	user := systemd.JobUser("team-a")
	dir := systemd.IsolatedJobDir("team-a", spec.ID, cfg)
	given := indexOf(runner.commands, "chown -hR "+shellword.Quote(user+":"+user)+" "+shellword.Quote(dir+"/tmp")+" "+shellword.Quote(dir+"/run"))
	made := indexOf(runner.commands, "useradd")
	if made < 0 || given < made || indexOf(runner.commands, "enable --now") < given {
		t.Fatalf("tmp and run not given to %s after it is made and before the start: %v", user, runner.commands)
	}
	if script := writtenTeeBody(t, runner.commands, "ctr-run"); !strings.Contains(script, "id -u '"+user+"'") {
		t.Fatalf("the container does not run as %s:\n%s", user, script)
	}
}

// A machine that can (UserNamespaces) runs its container jobs in a user
// namespace of their Workspace's; one that cannot runs them as before.
func TestAContainerJobRunsInAUserNamespaceWhereTheMachineCan(t *testing.T) {
	for _, can := range []bool{true, false} {
		cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
		runner := &recordingRunner{}
		exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard()), UserNS: can}
		spec := containerdSpec()
		spec.Workspace = "team-a"
		if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
			t.Fatal(err)
		}
		if script := writtenTeeBody(t, runner.commands, "ctr-run"); strings.Contains(script, "--remap-labels") != can {
			t.Fatalf("machine can %v, the job's run script:\n%s", can, script)
		}
	}
}

// A spec whose ID or Workspace is a path, from a hub that is not what it
// says, is refused before any directory or user is made of it.
func TestAJobNamedAsAPathIsRefusedBeforeAnythingIsMade(t *testing.T) {
	for _, spec := range []jobspec.JobSpec{
		func() jobspec.JobSpec { s := containerdSpec(); s.Workspace = "../../etc"; return s }(),
		func() jobspec.JobSpec { s := vmTestSpec(); s.Runtime = v1.RuntimeSystemd; s.ID = "../x"; return s }(),
	} {
		runner := &recordingRunner{}
		exec := SystemdExecutor{Manager: systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}, runner, logr.Discard())}
		if _, err := exec.Submit(context.Background(), spec, nil); !errors.Is(err, backend.ErrInvalid) {
			t.Errorf("id %q workspace %q: %v, want ErrInvalid", spec.ID, spec.Workspace, err)
		}
		if len(runner.commands) != 0 {
			t.Errorf("id %q workspace %q: ran %v before refusing", spec.ID, spec.Workspace, runner.commands)
		}
	}
}
