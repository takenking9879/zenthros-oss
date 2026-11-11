// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/agent/awssecret"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/systemd"
)

// secretOrderRunner records whether secret files still exist at the moment
// uninstall is requested. Cancel must not delete them first.
type secretOrderRunner struct {
	*recordingRunner
	secretPath               string
	sawUninstall             bool
	secretExistedAtUninstall bool
}

func (r *secretOrderRunner) Run(ctx context.Context, cmd string) (string, error) {
	if strings.Contains(cmd, "disable --now") {
		r.sawUninstall = true
		_, err := os.Stat(r.secretPath)
		r.secretExistedAtUninstall = err == nil
	}
	return r.recordingRunner.Run(ctx, cmd)
}

func cancelWithSecret(t *testing.T) (existedAtUninstall bool, sawUninstall bool, secretPath string) {
	t.Helper()
	root := t.TempDir()
	spec := vmTestSpec()
	secretDir := filepath.Join(root, spec.ID)
	if err := os.MkdirAll(secretDir, 0o700); err != nil {
		t.Fatal(err)
	}
	secretPath = filepath.Join(secretDir, "token")
	if err := os.WriteFile(secretPath, []byte("line1\nline2"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &secretOrderRunner{
		recordingRunner: &recordingRunner{unitContent: systemd.RenderUnit(spec, nil, cfg, "")},
		secretPath:      secretPath,
	}
	exec := SystemdExecutor{
		Manager: systemd.NewManager(cfg, runner, logr.Discard()),
		Secrets: awssecret.Config{Root: root},
	}
	if err := exec.Cancel(context.Background(), systemd.UnitName(spec.ID), owner.Ref{}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := os.Stat(secretPath); !os.IsNotExist(err) {
		t.Fatal("secrets remained after Cancel")
	}
	return runner.secretExistedAtUninstall, runner.sawUninstall, secretPath
}

// TestCancelUninstallsBeforeDeletingSecrets pins SystemdExecutor.Cancel.
// Uninstall must be requested while the secret files still exist. Removing
// them before uninstall drops credentials out from under a running unit.
func TestCancelUninstallsBeforeDeletingSecrets(t *testing.T) {
	existed, sawUninstall, _ := cancelWithSecret(t)
	if !sawUninstall {
		t.Fatal("Cancel did not uninstall")
	}
	if !existed {
		t.Fatal("secrets were deleted before uninstall")
	}
}

// A Cancel that could not delete the job's secret files says so: the
// controller retries it, where an ignored error left credentials on the
// machine for the next job to find (07#4).
func TestACancelThatLeavesSecretsFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root removes through a read-only directory: here nothing can leave the secrets behind")
	}
	root := t.TempDir()
	spec := vmTestSpec()
	if err := os.MkdirAll(filepath.Join(root, spec.ID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, spec.ID, "token"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The root is read-only: the job's directory cannot be removed from it.
	if err := os.Chmod(root, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	exec := SystemdExecutor{
		Manager: systemd.NewManager(cfg, &recordingRunner{unitContent: systemd.RenderUnit(spec, nil, cfg, "")}, logr.Discard()),
		Secrets: awssecret.Config{Root: root},
	}
	if err := exec.Cancel(context.Background(), systemd.UnitName(spec.ID), owner.Ref{}); err == nil {
		t.Fatal("Cancel succeeded while the job's secrets stayed on the machine")
	}
}
