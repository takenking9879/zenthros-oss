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

	"github.com/zenthros/operator/agent/agepayload"
	v1 "github.com/zenthros/operator/api/v1"
	pkgage "github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/systemd"
)

func sealedFile(t *testing.T, bundle agepayload.Bundle, path string) v1.SecretSourceRef {
	t.Helper()
	ct, err := pkgage.Encrypt([]byte("tenant value"), bundle.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	return v1.SecretSourceRef{
		Type:             v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{Ciphertext: ct, Recipients: bundle.Recipients()},
		Materialization:  v1.SecretMaterialization{Mode: v1.SecretMaterializationFile, Path: path},
	}
}

// A containerd job's file secrets are bound read-only at the path it asked
// for, inside the container; on the host they stay in the job's secret
// root. The agent once wrote the path on the host, as root, and the
// container saw the host's /var/run/zenthros.
func TestContainerdFileSecretIsMountedReadOnlyAtItsPath(t *testing.T) {
	bundle, err := agepayload.Generate()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	hostPath := filepath.Join(t.TempDir(), "etc", "shadow")
	runner := &recordingRunner{}
	exec := SystemdExecutor{
		Manager:         systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem}, runner, logr.Discard()),
		Age:             agepayload.Config{Root: root, Bundle: bundle},
		ReportSocketDir: "/var/run/zenthros/agent",
	}
	spec := containerdSpec()
	spec.SecretSources = []v1.SecretSourceRef{sealedFile(t, bundle, hostPath), sealedFile(t, bundle, "")}
	spec.SecretSources[1].EncryptedPayload.KeyID = "k1"
	if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(hostPath); err == nil {
		t.Fatalf("the agent wrote %s on the host", hostPath)
	}
	script := writtenTeeBody(t, runner.commands, "ctr-run")
	jobRoot := filepath.Join(root, spec.ID)
	for _, want := range []string{
		"src=" + jobRoot + "/mount" + hostPath + ",dst=" + hostPath + ",options=rbind:ro",
		"src=" + jobRoot + "/mount/var/run/zenthros/secrets/k1/value,dst=/var/run/zenthros/secrets/k1/value,options=rbind:ro",
		"dst=/var/run/zenthros,options=rbind:rw",
		"src=/var/run/zenthros/agent,dst=/var/run/zenthros/agent,options=rbind:ro",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("run script lacks %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "src=/var/run/zenthros,") {
		t.Errorf("the host's /var/run/zenthros is bound:\n%s", script)
	}
	if env := writtenTeeBody(t, runner.commands, "ctr-env"); !strings.Contains(env, "ZENTHROS_SECRETS_DIR=/var/run/zenthros/secrets") {
		t.Errorf("container env lacks ZENTHROS_SECRETS_DIR: %q", env)
	}
}

// A systemd job sees the host: a file secret path outside its own
// directories is refused before it runs, with nothing written there.
func TestSystemdFileSecretOutsideTheJobIsRefused(t *testing.T) {
	bundle, err := agepayload.Generate()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	hostPath := filepath.Join(t.TempDir(), "etc", "shadow")
	exec := SystemdExecutor{
		Manager: systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem}, &recordingRunner{}, logr.Discard()),
		Age:     agepayload.Config{Root: root, Bundle: bundle},
	}
	spec := vmTestSpec()
	spec.Runtime = v1.RuntimeSystemd
	spec.SecretSources = []v1.SecretSourceRef{sealedFile(t, bundle, hostPath)}
	_, err = exec.Submit(context.Background(), spec, nil)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), "working directory") {
		t.Fatalf("Submit = %v, want ErrInvalid naming the working directory", err)
	}
	if _, err := os.Lstat(hostPath); err == nil {
		t.Fatalf("%s was created", hostPath)
	}
	if _, err := os.Stat(filepath.Join(root, spec.ID)); !os.IsNotExist(err) {
		t.Fatalf("the refused job's secret root was left: %v", err)
	}
}

// A systemd job whose working directory is in a home the machine did not
// open has no file secret linked there: the agent's own home is an empty
// one, and the link would be where the job does not look. It is refused
// before the job runs, saying how to open the folder; nothing is left.
func TestSystemdFileSecretInAHomeNotOpenedIsRefused(t *testing.T) {
	bundle, err := agepayload.Generate()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	runner := &recordingRunner{}
	exec := SystemdExecutor{
		Manager: systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem, HostRoots: []string{"/home/u/data"}}, runner, logr.Discard()),
		Age:     agepayload.Config{Root: root, Bundle: bundle},
	}
	spec := vmTestSpec()
	spec.Runtime = v1.RuntimeSystemd
	spec.WorkingDir = "/home/u/notes"
	spec.SecretSources = []v1.SecretSourceRef{sealedFile(t, bundle, "/home/u/notes/conf/token")}
	_, err = exec.Submit(context.Background(), spec, nil)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), "has not opened") || !strings.Contains(err.Error(), "zenadm join --folder") {
		t.Fatalf("Submit = %v, want ErrInvalid saying the home is not opened and how to open it", err)
	}
	if _, err := os.Stat(filepath.Join(root, spec.ID)); !os.IsNotExist(err) {
		t.Fatalf("the refused job's secret root was left: %v", err)
	}
	for _, cmd := range runner.commands {
		if strings.Contains(cmd, "enable --now") {
			t.Fatalf("the refused job's unit was started: %s", cmd)
		}
	}
}
