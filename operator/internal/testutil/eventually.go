// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"testing"
	"time"
)

// Eventually waits up to thirty seconds for cond to hold, and fails the test
// with what when it never does.
func Eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	// A bound on a hang, not a speed claim: under -race with every package
	// running, the machine stalled for seconds (2026-10-21), and a met
	// condition returns at once whatever the bound.
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("never: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
