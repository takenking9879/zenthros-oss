// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"sync"
	"time"
)

// defaultHourlyLogBytes is the Agent-wide StreamLogs hourly budget. Distinct
// from the per-request cap (defaultMaxLogBytes / X3).
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

func (b *hourlyBudget) take(n int) int {
	if b == nil || n <= 0 {
		return 0
	}
	if b.limit <= 0 {
		return n
	}
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.start.IsZero() || now.Sub(b.start) >= time.Hour {
		b.start = now
		b.used = 0
	}
	remain := b.limit - b.used
	if remain <= 0 {
		return 0
	}
	if n > remain {
		n = remain
	}
	b.used += n
	return n
}
