// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"testing"
	"time"
)

// The gateways are down from the first probe that fails. A lease live then
// runs one grace past the recovery; one that lapsed before the outage does
// not. (Which leases wait while they are down is the controller's:
// TestWorkerLostDeferredWhileNoGateway.)
func TestGatewayOutageDefersAndExtends(t *testing.T) {
	t0 := time.Unix(1000, 0)
	var changes []bool
	g := &GatewayHealth{OnChange: func(reachable bool, _ time.Time) { changes = append(changes, reachable) }}
	g.Observe(true, t0)
	g.Observe(false, t0.Add(10*time.Second))
	g.Observe(false, t0.Add(20*time.Second))

	if down, since := g.Down(); !down || !since.Equal(t0.Add(10*time.Second)) {
		t.Fatalf("down %v since %s, want down since the first probe that failed", down, since)
	}

	recovered := t0.Add(80 * time.Second)
	g.Observe(true, recovered)
	if down, _ := g.Down(); down {
		t.Fatal("down after the recovery")
	}
	grace := 45 * time.Second
	if got := g.Deadline(t0.Add(15*time.Second-grace), grace); !got.Equal(recovered.Add(grace)) {
		t.Fatalf("deadline = %s, want %s", got, recovered.Add(grace))
	}
	if got := g.Deadline(recovered.Add(time.Hour-grace), grace); !got.Equal(recovered.Add(time.Hour)) {
		t.Fatal("the outage moved a later deadline")
	}
	if got := g.Deadline(t0.Add(5*time.Second-grace), grace); !got.Equal(t0.Add(5 * time.Second)) {
		t.Fatal("the outage moved a lease that lapsed before it")
	}
	if len(changes) != 2 || changes[0] || !changes[1] {
		t.Fatalf("transitions %v, want [false true]", changes)
	}
}

// A recovery is told before it shows. The controller's memory of who lives
// is re-anchored by the telling (Tracker.Anchor): a job judged on gateways
// that already answer, with the anchor still to come, was lost with no grace
// at all (Floci, 2026-10-12). An outage shows before it is told, so nothing
// is judged on the way down either.
func TestGatewayRecoveryIsToldBeforeItShows(t *testing.T) {
	t0 := time.Unix(1000, 0)
	var g *GatewayHealth
	var told []string
	g = &GatewayHealth{OnChange: func(reachable bool, at time.Time) {
		down, _ := g.Down()
		switch {
		case reachable && !down:
			t.Errorf("the gateways already showed as answering when their recovery at %s was told", at)
		case !reachable && !down:
			t.Errorf("the gateways still showed as answering when their outage at %s was told", at)
		}
		if reachable {
			told = append(told, "back")
		} else {
			told = append(told, "down")
		}
	}}
	g.Observe(true, t0)
	g.Observe(false, t0.Add(10*time.Second))
	g.Observe(true, t0.Add(80*time.Second))
	if down, _ := g.Down(); down {
		t.Fatal("still down after the recovery was told")
	}
	if got := g.Hidden(t0, t0.Add(100*time.Second)); got != 70*time.Second {
		t.Fatalf("hidden %s, want the 70 s outage", got)
	}
	if len(told) != 2 || told[0] != "down" || told[1] != "back" {
		t.Fatalf("told %v, want [down back]", told)
	}

	// A starting controller's first answer is a recovery like any other.
	told = nil
	g = StartupGatewayHealth(g.OnChange)
	g.OnChange = func(reachable bool, at time.Time) {
		if down, _ := g.Down(); reachable && !down {
			t.Errorf("startup: the gateways showed as answering before it was told")
		}
		told = append(told, "back")
	}
	g.Observe(true, t0)
	if down, _ := g.Down(); down || len(told) != 1 {
		t.Fatalf("startup: down %v, told %v", down, told)
	}
}

// A replica that flaps faster than the grace does not keep a dead machine
// alive: each recovery gave it a full grace, so its silence was never long
// enough to judge (10#4). Its proof is what earns a full grace after an
// outage; past that, only the time each outage hid is added.
func TestAFlappingGatewayDoesNotKeepADeadMachineAlive(t *testing.T) {
	t0 := time.Unix(1000, 0)
	grace := 30 * time.Second
	g := &GatewayHealth{}
	g.Observe(true, t0)
	for k := range 30 {
		at := t0.Add(time.Duration(20*k) * time.Second)
		g.Observe(false, at.Add(5*time.Second))
		g.Observe(true, at.Add(10*time.Second))
	}
	// Up 15 s of every 20: two outages fall within its proof's grace and
	// earn a full grace from their recovery; the rest add what they hid.
	if got := g.Deadline(t0, grace); got.After(t0.Add(2*grace + 10*time.Second)) {
		t.Fatalf("a machine last heard at +0 is alive until +%s under a flapping gateway", got.Sub(t0))
	}
	// A machine heard during the flapping lives a grace of up-time past it.
	heard := t0.Add(300 * time.Second)
	if got := g.Deadline(heard, grace); got.Before(heard.Add(grace)) {
		t.Fatalf("a machine heard at +300s is judged at +%s, before its grace", got.Sub(t0))
	}
}
