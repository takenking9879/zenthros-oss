// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"testing"
	"time"
)

func TestHourlyBudgetTakeAllAndReset(t *testing.T) {
	now := time.Unix(0, 0)
	b := newHourlyBudget(5)
	b.now = func() time.Time { return now }
	if !b.takeAll(3) {
		t.Fatal("take 3 of 5")
	}
	if b.takeAll(3) {
		t.Fatal("took 3 with 2 left")
	}
	if !b.takeAll(2) {
		t.Fatal("take the 2 left")
	}
	now = now.Add(time.Hour)
	if !b.takeAll(4) {
		t.Fatal("after the window, take 4")
	}
}
