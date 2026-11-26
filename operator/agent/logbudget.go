// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"sync"
	"time"
)

// defaultHourlyLogBytes is the Agent-wide StreamLogs hourly budget.
const defaultHourlyLogBytes = 50 << 20

type hourlyBudget struct {
	mu    sync.Mutex
	used  int
	start time.Time
	limit int
	now   func() time.Time
}

func newHourlyBudget(limit int) *hourlyBudget {
	if limit <= 0 {
		limit = defaultHourlyLogBytes
	}
	return &hourlyBudget{limit: limit, now: time.Now}
}

// takeAll takes n bytes if they are all left this hour.
func (b *hourlyBudget) takeAll(n int) bool {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(now)
	if b.used+n > b.limit {
		return false
	}
	b.used += n
	return true
}

func (b *hourlyBudget) refill(now time.Time) {
	if b.start.IsZero() || now.Sub(b.start) >= time.Hour {
		b.start = now
		b.used = 0
	}
}
