// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/systemd"
)

// writtenTeeBody is the file body Submit asked the fake runner to write:
// installCtrWrapper writes the ctr env file as base64, and the Manager a
// unit's environment file (".service.env"), which are the files the guest
// would see.
func writtenTeeBody(t *testing.T, commands []string, pathSubstr string) string {
	t.Helper()
	const prefix = "printf '%s' '"
	const suffix = "' | "
	for _, cmd := range commands {
		if !strings.Contains(cmd, pathSubstr) || !strings.Contains(cmd, "base64 -d") {
			continue
		}
		start := strings.Index(cmd, prefix)
		if start < 0 {
			continue
		}
		rest := cmd[start+len(prefix):]
		end := strings.Index(rest, suffix)
		if end < 0 {
			t.Fatalf("write command for %s has no base64 payload: %s", pathSubstr, cmd)
		}
		raw, err := base64.StdEncoding.DecodeString(rest[:end])
		if err != nil {
			t.Fatalf("decode %s: %v", pathSubstr, err)
		}
		return string(raw)
	}
	t.Fatalf("Submit did not write %s; ran %v", pathSubstr, commands)
	return ""
}

func submitEnv(t *testing.T, env map[string]string) string {
	t.Helper()
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &recordingRunner{}
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}
	if _, err := exec.Submit(context.Background(), containerdSpec(), env); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return writtenTeeBody(t, runner.commands, "ctr-env")
}

// TestFormatEnvFileKeepsNewlines pins the env file Submit writes. A TOKEN
// value of line1\nline2 must still contain that newline. FormatEnvFile is
// what installCtrWrapper, called from Submit, uses to fill that file.
func TestFormatEnvFileKeepsNewlines(t *testing.T) {
	got := submitEnv(t, map[string]string{"TOKEN": "line1\nline2"})
	if !strings.Contains(got, "line1\nline2") {
		t.Fatalf("multiline secret was flattened: %q", got)
	}
}
