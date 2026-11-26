// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// A container's agent is launched with its join token and tailnet key in its
// environment. Neither PID 1 nor the agent it starts keeps them in the
// environ /proc shows the container's jobs, and the agent still has them.
func TestContainerAgentEnvironHoldsNoSecret(t *testing.T) {
	if _, err := os.Stat("/proc/self/environ"); err != nil {
		t.Skip("no /proc")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		"ZENTHROS_TEST_HANDOFF=init",
		"ZENTHROS_JOIN_TOKEN=token-"+handoffMarker,
		EnvTailscaleAuthKey+"=tskey-"+handoffMarker,
	)
	raw, err := cmd.CombinedOutput()
	out := string(raw)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	for _, want := range []string{
		"INIT_ENVIRON clean",
		"CHILD_ENVIRON clean",
		"CHILD_TOKEN token-" + handoffMarker,
		"CHILD_AUTHKEY tskey-" + handoffMarker,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
