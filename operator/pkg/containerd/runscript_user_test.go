// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// A container job's process is its Workspace's user, by the uid and gid the
// machine gave it (the image knows no such name), under containerd's default
// seccomp profile and with none of its default capabilities. Never
// --privileged, never --allow-new-privs (ctr's no-new-privileges stays).
func TestAContainerJobRunsAsItsUserWithNothingToRaise(t *testing.T) {
	opts := RunOpts{Image: "docker.io/library/python:3.13-slim", Container: "c", TmpDir: "/var/lib/zenthros/j/tmp", User: "zj-0a1b2c3d"}
	script := RenderRunScript(opts)
	for _, want := range []string{
		"job_uid=$(id -u 'zj-0a1b2c3d')\n",
		"job_gid=$(id -g 'zj-0a1b2c3d')\n",
		"args+=(--user \"$job_uid:$job_gid\")\n",
		"args+=(--seccomp ",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("run script has no %q:\n%s", want, script)
		}
	}
	dropped := regexp.MustCompile(`--cap-drop (CAP_[A-Z_]+)`).FindAllStringSubmatch(script, -1)
	if len(dropped) != len(DroppedCapabilities) || len(DroppedCapabilities) != 14 {
		t.Errorf("%d capabilities dropped, want containerd's 14 defaults: %q", len(dropped), dropped)
	}
	for _, flag := range []string{"--privileged", "--allow-new-privs", "--cap-add"} {
		if strings.Contains(script, flag) {
			t.Errorf("run script raises privileges with %s:\n%s", flag, script)
		}
	}
	// Flags before the image: ctr reads what follows it as the command.
	if strings.Index(script, "--seccomp") > strings.Index(script, `args+=("$image" "$name")`) {
		t.Errorf("--seccomp comes after the image:\n%s", script)
	}
	if out, err := exec.Command("bash", "-n", "-c", script).CombinedOutput(); err != nil {
		t.Fatalf("run script is not valid bash: %v\n%s", err, out)
	}
	// set -e: a user the machine does not have fails the job there, before ctr.
	if !strings.HasPrefix(script, "#!/bin/bash\nset -euo pipefail\n") {
		t.Fatalf("run script does not stop on a failed lookup:\n%s", script)
	}
	if plain := RenderRunScript(RunOpts{Image: "i", Container: "c"}); strings.Contains(plain, "--user") || !strings.Contains(plain, "--seccomp") {
		t.Errorf("without a user: --user or no --seccomp:\n%s", plain)
	}
}
