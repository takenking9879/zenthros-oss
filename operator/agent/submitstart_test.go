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
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/systemd"
)

// startTimesOutRunner is a machine whose `systemctl enable --now` answers
// with an error after the start may have been queued (a dbus timeout, the
// submit's context ending), and whose unit then reads as activeState.
type startTimesOutRunner struct {
	*recordingRunner
	activeState string
}

func (r *startTimesOutRunner) Run(ctx context.Context, cmd string) (string, error) {
	out, err := r.recordingRunner.Run(ctx, cmd)
	switch {
	case strings.Contains(cmd, "enable --now"):
		return "", errors.New("Failed to start: Connection timed out")
	case strings.Contains(cmd, "show "):
		return "ActiveState=" + r.activeState + "\n", nil
	}
	return out, err
}

// A start whose answer was lost may have started the unit: the job's secret
// files stay for it, and the resubmit adopts the running unit without
// writing them again. Removed, the job ran without its credentials (R4
// ronda 4, G2). A unit that surely did not start gives them back.
func TestAStartWhoseAnswerWasLostKeepsTheJobsSecrets(t *testing.T) {
	for state, keep := range map[string]bool{"activating": true, "active": true, "inactive": false} {
		root := t.TempDir()
		spec := vmTestSpec()
		spec.SecretSources = []v1.SecretSourceRef{{
			Type:              v1.SecretSourceTypeAWSSecretsManager,
			AWSSecretsManager: &v1.AWSSecretsManagerRef{SecretID: "workload/token", Region: "us-east-2", JSONKey: "token"},
			Materialization:   v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T_SECRET"},
		}}
		runner := &startTimesOutRunner{recordingRunner: &recordingRunner{}, activeState: state}
		exec := SystemdExecutor{
			Manager: systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}, runner, logr.Discard()),
			Secrets: awssecret.Config{
				Root:     root,
				Identity: func(context.Context) (bool, error) { return true, nil },
				Factory: func(context.Context, string) (awssecret.Getter, error) {
					return getterStub(func(context.Context, awssecret.GetRequest) (string, error) {
						return `{"token":"t"}`, nil
					}), nil
				},
			},
		}
		if _, err := exec.Submit(context.Background(), spec, nil); err == nil {
			t.Fatalf("%s: Submit after a start with no answer reported no error", state)
		}
		_, err := os.Stat(filepath.Join(root, spec.ID))
		if kept := err == nil; kept != keep {
			t.Errorf("unit %s after the lost start: secrets kept = %v, want %v", state, kept, keep)
		}
	}
}
