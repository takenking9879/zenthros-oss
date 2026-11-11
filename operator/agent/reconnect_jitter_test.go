// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/liveness"
)

// Every agent redialed 5 s after its stream dropped: after a Gateway restart
// the whole fleet came back in the same second, each Register pulling the
// Hub, the Fence and a LogChannel with it. The
// delay is exponential with full jitter, up to a ceiling, and starts over
// after a session that held.
func TestAgentReconnectBackoffIsJittered(t *testing.T) {
	const base, ceiling = 5 * time.Second, 30 * time.Second
	rng := rand.New(rand.NewPCG(1, 2))
	firsts := map[time.Duration]bool{}
	for range 50 {
		r := reconnectSchedule{base: base, ceiling: ceiling, draw: rng.Int64N}
		d := r.next(0, false)
		if d < 0 || d > base {
			t.Fatalf("first delay %s, want within [0, %s]", d, base)
		}
		firsts[d] = true
	}
	if len(firsts) < 10 {
		t.Fatalf("50 agents drew %d distinct first delays: they come back together", len(firsts))
	}

	longest := func(n int64) int64 { return n - 1 }
	r := reconnectSchedule{base: base, ceiling: ceiling, draw: longest}
	var last time.Duration
	for range 10 {
		last = r.next(0, false)
		if last > ceiling {
			t.Fatalf("delay %s over the %s ceiling", last, ceiling)
		}
	}
	if last < ceiling-time.Second {
		t.Fatalf("after 10 failed dials the longest delay is %s, want it grown to the %s ceiling", last, ceiling)
	}
	if d := r.next(time.Minute, false); d > base {
		t.Fatalf("after a session that held a minute the delay is %s, want the schedule started over (<= %s)", d, base)
	}
}

// A restart of the controller and the gateways together gives every machine
// one job grace from the gateways' recovery (liveness.Tracker.Anchor), and
// the agent's Register through the gateway is its proof of life. A machine
// waits at most the ceiling to redial: if the ceiling reached the grace, a
// live machine would be judged lost before it could come back
// (hot-state-design §10.5).
func TestReconnectCeilingFitsInTheJobGrace(t *testing.T) {
	if ceiling := reconnectCeilingFactor * reconnectBackoff; ceiling >= liveness.DefaultJobGrace {
		t.Fatalf("the reconnect ceiling %v does not fit in the job grace %v", ceiling, liveness.DefaultJobGrace)
	}
}
