// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"testing"
	"time"
)

func TestHourlyBudgetTakeAndReset(t *testing.T) {
	now := time.Unix(0, 0)
	b := newHourlyBudget(5)
	b.now = func() time.Time { return now }
	if got := b.take(3); got != 3 {
		t.Fatalf("take 3 = %d", got)
	}
	if got := b.take(3); got != 2 {
		t.Fatalf("take remainder = %d, want 2", got)
	}
	if got := b.take(1); got != 0 {
		t.Fatalf("exhausted take = %d", got)
	}
	now = now.Add(time.Hour)
	if got := b.take(4); got != 4 {
		t.Fatalf("after window take = %d, want 4", got)
	}
}
