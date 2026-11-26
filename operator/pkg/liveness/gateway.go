// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"context"
	"sync"
	"time"
)

// GatewayHealth is the controller's view of whether every gateway replica
// answers (pipeclient.Client.AllAnswer). Leases are renewed by heartbeats
// that cross a gateway, so while one does not answer a lapsed lease of a
// machine it may hold says nothing about the worker behind it.
//
// A lease that was still live when the gateways went dark is not judged
// until they are back. A silence counts only while the gateways answer
// (Deadline): each outage adds what it hid, and a machine its own proof had
// alive when one began gets a full grace from the recovery for its
// heartbeats to resume. A lease already expired before the outage began is
// judged as usual.
type GatewayHealth struct {
	// OnChange, when set, hears every transition: reachable false when a
	// gateway stops answering, true when every one answers again.
	OnChange func(reachable bool, at time.Time)

	mu        sync.Mutex
	down      bool
	downSince time.Time
	// outages are the finished outages of the last outageMemory, oldest
	// first.
	outages []Outage
}

// outageMemory is how long a finished outage is remembered: longer than any
// silence counted through it (a lease's grace, the GC's orphan grace, the
// tracker's hour).
const outageMemory = 6 * time.Hour

// Outage is a window during which some gateway did not answer.
type Outage struct {
	From, To time.Time
}

// StartupGatewayHealth is the view a starting controller has: nothing renewed
// a lease while it was down (an upgrade, a failover), so its downtime is an
// outage that began before any lease it finds. Until the first probe nothing
// is judged; once one answers, every lease gets a full grace from it.
func StartupGatewayHealth(onChange func(reachable bool, at time.Time)) *GatewayHealth {
	return &GatewayHealth{down: true, OnChange: onChange}
}

// Observe records one probe of the gateways at now. One prober calls it
// (Watch).
//
// An outage shows before it is told, and a recovery is told before it shows:
// whoever hears it (the tracker's anchor) has given every machine its grace
// by the time a lease can be judged on gateways that answer again.
func (g *GatewayHealth) Observe(reachable bool, now time.Time) {
	g.mu.Lock()
	failing, recovering := !reachable && !g.down, reachable && g.down
	if failing {
		g.down, g.downSince = true, now
	}
	g.mu.Unlock()
	if (failing || recovering) && g.OnChange != nil {
		g.OnChange(reachable, now)
	}
	if recovering {
		g.mu.Lock()
		g.down = false
		kept := g.outages[:0]
		for _, o := range g.outages {
			if now.Sub(o.To) < outageMemory {
				kept = append(kept, o)
			}
		}
		g.outages = append(kept, Outage{From: g.downSince, To: now})
		g.mu.Unlock()
	}
}

// Down reports whether a gateway does not answer now, and since when. A lease still
// live at since cannot be judged while it is true. Whoever judges reads it
// before working out the lease's deadline: a recovery that comes in between
// is then in the deadline (see Observe).
func (g *GatewayHealth) Down() (bool, time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.down, g.downSince
}

// Deadline is when a silence that began with the proof at proof has lasted
// grace while the gateways answered. Each finished outage within it adds the
// time it hid; one that began while the proof itself still had the machine
// alive also gives a full grace from its recovery, for the heartbeats to
// resume. An outage still going on adds nothing: whoever judges waits while
// Down says so.
//
// A full grace from every recovery was what a replica that flapped faster
// than the grace gave a dead machine at each one, and it was never judged
// (10#4). Granted from the proof, not from the grace an earlier recovery
// gave, the extension ends.
func (g *GatewayHealth) Deadline(proof time.Time, grace time.Duration) time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	end := proof.Add(grace)
	for _, o := range g.outages {
		if !o.From.Before(end) {
			break
		}
		if !o.To.After(proof) {
			continue
		}
		end = end.Add(o.To.Sub(later(o.From, proof)))
		if o.From.Before(proof.Add(grace)) {
			if resumed := o.To.Add(grace); resumed.After(end) {
				end = resumed
			}
		}
	}
	return end
}

// Hidden is how much of [from, to) fell in finished outages.
func (g *GatewayHealth) Hidden(from, to time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	var hidden time.Duration
	for _, o := range g.outages {
		start, stop := later(o.From, from), o.To
		if to.Before(stop) {
			stop = to
		}
		if stop.After(start) {
			hidden += stop.Sub(start)
		}
	}
	return hidden
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// Watch probes every interval until ctx ends. A probe that returns false
// means some gateway replica did not answer.
func (g *GatewayHealth) Watch(ctx context.Context, interval time.Duration, now func() time.Time, probe func(context.Context) bool) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		probeCtx, cancel := context.WithTimeout(ctx, interval)
		reachable := probe(probeCtx)
		cancel()
		if ctx.Err() != nil {
			return // shutting down is not an outage
		}
		g.Observe(reachable, now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
