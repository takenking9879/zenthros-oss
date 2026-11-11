// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"math/rand"
	"time"
)

const (
	reportBackoffInitial = 2 * time.Second
	reportBackoffCap     = 128 * time.Second
	reportSendTimeout    = 5 * time.Second
)

// reportCircuit is the Agent-wide report-send backoff. One failure delays
// every job's unacked snapshots; there is no per-job timer.
type reportCircuit struct {
	failures int
}

func (c *reportCircuit) reset() {
	c.failures = 0
}

func (c *reportCircuit) fail() {
	c.failures++
}

func (c *reportCircuit) wait() time.Duration {
	if c.failures <= 0 {
		return 0
	}
	shift := c.failures - 1
	if shift > 6 {
		return reportBackoffCap
	}
	return reportBackoffInitial << shift
}

// jitterWait subtracts 0–25% so a wait never exceeds its nominal step (the
// 128s cap stays a cap).
func jitterWait(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	max := int64(d / 4)
	if max <= 0 {
		return d
	}
	return d - time.Duration(rand.Int63n(max+1))
}
