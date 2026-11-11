// SPDX-License-Identifier: Apache-2.0

package v1

import "testing"

func TestPhaseTerminal(t *testing.T) {
	cases := map[Phase]bool{
		PhasePending:       false,
		PhaseSubmitted:     false,
		PhaseRunning:       false,
		PhaseUnschedulable: false,
		PhaseSucceeded:     true,
		PhaseFailed:        true,
		PhaseCancelled:     true,
	}
	for phase, want := range cases {
		if got := phase.Terminal(); got != want {
			t.Errorf("Phase(%q).Terminal() = %v, want %v", phase, got, want)
		}
	}
}
