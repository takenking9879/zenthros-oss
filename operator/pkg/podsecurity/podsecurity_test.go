// SPDX-License-Identifier: Apache-2.0

package podsecurity

import "testing"

// An adopted namespace that already enforces a stricter level keeps it.
func TestPodSecurityStricterExistingLevelWins(t *testing.T) {
	if got := Labels("baseline", map[string]string{psaEnforce: "restricted"})[psaEnforce]; got != "restricted" {
		t.Fatalf("enforce = %s, want restricted kept", got)
	}
	if got := Labels("baseline", map[string]string{psaEnforce: "privileged"})[psaEnforce]; got != "baseline" {
		t.Fatalf("enforce = %s, want baseline over privileged", got)
	}
	if got := Labels("bogus", nil)[psaEnforce]; got != Default {
		t.Fatalf("unknown level = %s, want default", got)
	}
	if got := Labels("privileged", nil)[psaEnforce]; got != "privileged" {
		t.Fatalf("configured privileged = %s", got)
	}
}
