// SPDX-License-Identifier: Apache-2.0

package backend

import "time"

// RetryPolicy computes exponential backoff delays for transient submission failures.
// The delay doubles per attempt from BaseDelay up to MaxDelay, so a
// backend that is briefly busy is retried quickly while a prolonged condition backs
// off instead of hammering it.
type RetryPolicy struct {
	// BaseDelay is the delay before the first retry (attempt 0).
	BaseDelay time.Duration

	// MaxDelay caps the grown delay. Zero means no cap.
	MaxDelay time.Duration
}

// DefaultRetryPolicy returns the production backoff: a 5s base doubling to a 5m cap.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{BaseDelay: 5 * time.Second, MaxDelay: 5 * time.Minute}
}

// BusyRetryPolicy is the backoff for BACKEND_BUSY. Capacity is now pushed on
// Submit/terminal, so a 5s first wait was a stale-heartbeat stall, not true
// saturation. Still doubles up to 5s if the slot really stays full.
func BusyRetryPolicy() RetryPolicy {
	return RetryPolicy{BaseDelay: 250 * time.Millisecond, MaxDelay: 5 * time.Second}
}

// Delay returns the backoff before the (attempt+1)th try: attempt 0 → BaseDelay,
// attempt 1 → 2×BaseDelay, and so on, capped at MaxDelay. A non-positive BaseDelay
// disables backoff (returns 0).
func (p RetryPolicy) Delay(attempt int) time.Duration {
	if p.BaseDelay <= 0 {
		return 0
	}
	delay := p.BaseDelay
	for i := 0; i < attempt; i++ {
		if p.MaxDelay > 0 && delay >= p.MaxDelay {
			break
		}
		delay *= 2
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}
