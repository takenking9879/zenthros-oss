// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

func unitBodyFromInstall(t *testing.T, commands []string) string {
	t.Helper()
	const prefix = "printf '%s' '"
	const suffix = "' | base64 -d"
	for _, cmd := range commands {
		if !strings.Contains(cmd, ".service") || strings.Contains(cmd, ".service.env") || !strings.Contains(cmd, "base64 -d") {
			continue
		}
		start := strings.Index(cmd, prefix)
		if start < 0 {
			continue
		}
		rest := cmd[start+len(prefix):]
		end := strings.Index(rest, suffix)
		if end < 0 {
			t.Fatalf("unit write has no base64 payload: %s", cmd)
		}
		raw, err := base64.StdEncoding.DecodeString(rest[:end])
		if err != nil {
			t.Fatalf("decode unit: %v", err)
		}
		return string(raw)
	}
	t.Fatalf("Install did not write a unit; ran %v", commands)
	return ""
}

// TestInstallUnitIsNotEnabledAcrossRebootE2E enters through Manager.Install.
// The unit text that path writes must not contain WantedBy=multi-user.target,
// the same string TestJobUnitIsNotEnabledAcrossReboot rejects.
func TestInstallUnitIsNotEnabledAcrossRebootE2E(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	runner := &fakeRunner{}
	manager := NewManager(cfg, runner, logr.Discard())
	if _, err := manager.Install(context.Background(), jobspec.JobSpec{
		ID:         "uid-oneshot",
		Name:       "hello",
		Kind:       v1.WorkloadKindRayJob,
		Entrypoint: "echo hi",
	}, nil); err != nil {
		t.Fatalf("Install: %v", err)
	}
	unit := unitBodyFromInstall(t, runner.commands)
	if strings.Contains(unit, "WantedBy=multi-user.target") {
		t.Fatal("job unit is WantedBy multi-user.target; a reboot reruns a finished job the agent does not adopt")
	}
}
