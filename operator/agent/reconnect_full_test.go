// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/liveness"
)

// A gateway died and its sibling is full: a refused machine has to try again
// soon, because the replica that comes back has room and each dial lands on
// either. Backing off as after any failure left 42 of 900 machines out for
// longer than their jobs' grace (scale lab, 2026-10-10).
func TestGatewayFullRefusalIsRetriedSoon(t *testing.T) {
	const base, ceiling = 5 * time.Second, 30 * time.Second
	longest := func(n int64) int64 { return n - 1 }
	r := reconnectSchedule{base: base, ceiling: ceiling, draw: longest}
	var waited time.Duration
	for i := range reconnectFullPatience {
		d := r.next(0, true)
		if d < reconnectFullMin || d > reconnectFullMax {
			t.Fatalf("refusal %d: delay %s, want within [%s, %s]", i+1, d, reconnectFullMin, reconnectFullMax)
		}
		waited += d
	}
	// The quick retries have to outlast a replica coming back, and to end
	// well inside a job's grace.
	if waited < 20*time.Second || waited >= liveness.DefaultJobGrace {
		t.Fatalf("the quick retries span up to %s, want at least 20 s and less than the job grace %s", waited, liveness.DefaultJobGrace)
	}
}

// With no room in any replica the surplus machines must not keep dialing
// every other second for ever: past the patience a full refusal backs off
// like any failure, up to the ceiling.
func TestGatewayFullRefusalsBackOffPastThePatience(t *testing.T) {
	const base, ceiling = 5 * time.Second, 30 * time.Second
	longest := func(n int64) int64 { return n - 1 }
	r := reconnectSchedule{base: base, ceiling: ceiling, draw: longest}
	for range reconnectFullPatience {
		r.next(0, true)
	}
	var last time.Duration
	for range 6 {
		last = r.next(0, true)
		if last > ceiling {
			t.Fatalf("delay %s over the %s ceiling", last, ceiling)
		}
	}
	if last < ceiling-time.Second {
		t.Fatalf("six refusals past the patience wait %s, want the delay grown to the %s ceiling", last, ceiling)
	}
	// A session that held gives the patience back.
	r.next(time.Minute, false)
	if d := r.next(0, true); d > reconnectFullMax {
		t.Fatalf("after a session that held, a full refusal waits %s, want a quick retry", d)
	}
}

// Only a full gateway is retried soon. Any other failure in between keeps
// its own backoff, and the quick retries do not reset it.
func TestOtherFailuresKeepTheirBackoffAmongFullRefusals(t *testing.T) {
	const base, ceiling = 5 * time.Second, 30 * time.Second
	longest := func(n int64) int64 { return n - 1 }
	r := reconnectSchedule{base: base, ceiling: ceiling, draw: longest}
	if d := r.next(0, false); d != base {
		t.Fatalf("first failure waits %s, want %s", d, base)
	}
	r.next(0, true)
	if d := r.next(0, false); d != 2*base {
		t.Fatalf("second failure waits %s, want %s: a full refusal in between reset the backoff", d, 2*base)
	}
}

// The agent's limit on Register is per certificate and per replica
// (grpcserver: 30 a minute). The quick retries, with the dial that started
// them, must stay under it even if every one lands on the same replica at
// the shortest delay: otherwise the gateway would answer them with its rate
// limit and the machine would back off after all.
func TestQuickRetriesFitInTheRegisterLimit(t *testing.T) {
	const registerLimitPerMinute = 30
	if dials := reconnectFullPatience + 1; dials >= registerLimitPerMinute/2 {
		t.Fatalf("%d dials in the quick retries, want under half the Register limit of %d a minute", dials, registerLimitPerMinute)
	}
	if reconnectFullMin < time.Second {
		t.Fatalf("quick retries as short as %s: a refused fleet would dial a full gateway more than once a second each", reconnectFullMin)
	}
}
