// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/systemd"
)

func submitPull(t *testing.T) []string {
	t.Helper()
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &recordingRunner{}
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}
	if _, err := exec.Submit(context.Background(), containerdSpec(), nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	sawPull := false
	for _, cmd := range runner.commands {
		if strings.Contains(cmd, "images pull") {
			sawPull = true
		}
	}
	if !sawPull {
		t.Fatalf("Submit did not pull; ran %v", runner.commands)
	}
	return runner.commands
}

// TestPullCommandDoesNotHardcodeAmd64 pins the ctr pull Submit runs.
// pullCommand is reached from EnsureImage, which installCtrWrapper calls.
func TestPullCommandDoesNotHardcodeAmd64(t *testing.T) {
	for _, cmd := range submitPull(t) {
		if strings.Contains(cmd, "linux/amd64") {
			t.Fatalf("ctr pull hardcoded linux/amd64: %s", cmd)
		}
	}
}
