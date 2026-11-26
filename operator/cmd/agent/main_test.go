// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func TestDefaultMaxJobs(t *testing.T) {
	if got := defaultMaxJobs(agentModeVM); got != 1 {
		t.Fatalf("VM default MaxJobs = %d, want 1", got)
	}
	if got := defaultMaxJobs(""); got != 1 {
		t.Fatalf("empty mode default MaxJobs = %d, want 1 (VM)", got)
	}
	if got := defaultMaxJobs(agentModeContainer); got != 1 {
		t.Fatalf("container default MaxJobs = %d, want 1", got)
	}
	if got := defaultMaxJobs(agentModeK8s); got != 0 {
		t.Fatalf("k8s default MaxJobs = %d, want 0 (unlimited)", got)
	}
}

// The backend type follows from the mode; a configuration that says
// otherwise stops the agent at start rather than misrouting jobs.
func TestAgentRefusesModeTypeMismatch(t *testing.T) {
	for _, tc := range []struct{ mode, declared, want string }{
		{agentModeVM, "", "vm"},
		{"", "", "vm"},
		{agentModeK8s, "", "k8s"},
		{agentModeK8s, "k8s", "k8s"},
		{agentModeContainer, "", "container"},
		{agentModeContainer, "container", "container"},
	} {
		got, err := backendTypeFor(tc.mode, tc.declared)
		if err != nil || got != tc.want {
			t.Fatalf("backendTypeFor(%q, %q) = %q, %v; want %q", tc.mode, tc.declared, got, err, tc.want)
		}
	}
	for _, tc := range []struct{ mode, declared string }{{agentModeK8s, "vm"}, {agentModeVM, "k8s"}, {agentModeContainer, "vm"}, {agentModeVM, "container"}, {"docker", ""}} {
		if _, err := backendTypeFor(tc.mode, tc.declared); err == nil {
			t.Fatalf("backendTypeFor(%q, %q) accepted a contradiction", tc.mode, tc.declared)
		}
	}
}
