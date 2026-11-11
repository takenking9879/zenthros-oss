// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/observe"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A gateway died with more machines on it than its sibling has room for: the
// ones left out keep calling and keep being refused until the dead gateway
// is back (the scale lab lost 42 jobs of 900 that way, 2026-10-10). A machine
// a gateway just refused for lack of room is alive; its jobs are not judged.
func TestRefusedMachineIsNotSuspectedWhileItKeepsCalling(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	t0 := time.Unix(1000, 0)
	tr.Listed(map[string]time.Time{"vm-1": t0})
	for _, after := range []time.Duration{20, 50, 80, 110} {
		tr.Refused("vm-1", t0.Add(after*time.Second))
		tr.Scan(t0.Add(after * time.Second))
	}
	if tr.Suspected("vm-1", t0.Add(150*time.Second)) {
		t.Fatal("a machine refused 40 s ago for lack of room was suspected")
	}
	if got := s.got(); len(got) != 0 {
		t.Fatalf("suspects = %v; a machine that keeps calling is not silent", got)
	}
	if got := tr.Since("vm-1"); !got.Equal(t0.Add(110 * time.Second)) {
		t.Fatalf("since = %v, want its last refusal", got)
	}
}

// The other half: a machine that stops calling is judged on its silence, a
// grace after the last time anybody heard it, refusal or not.
func TestRefusedMachineThatGoesSilentIsSuspected(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	t0 := time.Unix(1000, 0)
	tr.Listed(map[string]time.Time{"vm-1": t0})
	tr.Refused("vm-1", t0.Add(20*time.Second))
	tr.Scan(t0.Add(64 * time.Second))
	if got := s.got(); len(got) != 0 {
		t.Fatalf("suspects = %v a second before the grace from its last refusal", got)
	}
	tr.Scan(t0.Add(65 * time.Second))
	if got := s.got(); !slices.Equal(got, []string{"vm-1"}) {
		t.Fatalf("suspects = %v, want vm-1 a grace after it stopped calling", got)
	}
}

// A refusal is proof that the machine lives, not that anyone can reach it:
// no command gets to a machine with no session. Refusals alone carry a job
// for RefusedPatience from the last time the machine was really heard, and
// no further, however long it keeps calling.
func TestRefusalsCarryAMachineOnlySoLong(t *testing.T) {
	tr := NewTracker(45*time.Second, nil)
	t0 := time.Unix(1000, 0)
	tr.Anchor(t0.Add(-time.Second)) // the controller's start
	tr.Listed(map[string]time.Time{"vm-1": t0})
	for after := 20 * time.Second; after <= 10*time.Minute; after += 20 * time.Second {
		tr.Refused("vm-1", t0.Add(after))
	}
	limit := t0.Add(DefaultRefusedPatience)
	if got := tr.Since("vm-1"); !got.Equal(limit) {
		t.Fatalf("since = %v, want the limit %v: ten minutes of refusals carried it past the patience", got, limit)
	}
	if tr.Suspected("vm-1", limit.Add(44*time.Second)) {
		t.Fatal("suspected before a grace from the limit")
	}
	if !tr.Suspected("vm-1", limit.Add(45*time.Second)) {
		t.Fatal("not suspected a grace after the limit: refusals kept a job with no channel alive for ever")
	}

	// Heard for real again (a session, a message): the patience starts over.
	back := t0.Add(11 * time.Minute)
	tr.Seen("vm-1", back)
	tr.Refused("vm-1", back.Add(4*time.Minute))
	if got := tr.Since("vm-1"); !got.Equal(back.Add(4 * time.Minute)) {
		t.Fatalf("since = %v; a machine heard again did not get its patience back", got)
	}
}

// The patience counts from the anchor for a machine no list has shown since
// (the controller restarted while it was being refused), and a refusal never
// takes a newer proof back.
func TestRefusalCountsFromTheAnchorAndOnlyMovesForward(t *testing.T) {
	tr := NewTracker(45*time.Second, nil)
	if tr.Refused("vm-1", time.Unix(900, 0)); !tr.Since("vm-1").IsZero() {
		t.Fatal("a refusal invented a proof for a machine nobody ever heard, on an unanchored tracker")
	}
	start := time.Unix(1000, 0)
	tr.Anchor(start)
	tr.Refused("vm-1", start.Add(DefaultRefusedPatience+time.Minute))
	if got := tr.Since("vm-1"); !got.Equal(start.Add(DefaultRefusedPatience)) {
		t.Fatalf("since = %v, want the anchor plus the patience", got)
	}
	tr.Seen("vm-2", start.Add(30*time.Second))
	tr.Refused("vm-2", start.Add(10*time.Second))
	if got := tr.Since("vm-2"); !got.Equal(start.Add(30 * time.Second)) {
		t.Fatalf("since = %v; an older refusal took a newer proof back", got)
	}
}

// The sweep reads the refusals from the same lists as the sessions: a
// machine its gateway does not hold, but another refused, is not suspected.
func TestTrackerSweepCountsTheRefusedOfEveryGateway(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	now := time.Unix(1000, 0)
	tr.Seen("vm-refused", now.Add(-50*time.Second))
	tr.Seen("vm-silent", now.Add(-50*time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	list := func(context.Context) (observe.View[GatewayList], error) {
		return observe.View[GatewayList]{Complete: true, Items: []GatewayList{
			{Held: map[string]time.Time{"vm-a": now.Add(-10 * time.Second)}, RefusedFull: map[string]time.Time{"vm-refused": now.Add(-5 * time.Second)}},
			{Held: map[string]time.Time{}},
		}}, nil
	}
	go tr.Sweep(ctx, time.Hour, func() time.Time { return now }, list)
	deadline := time.After(5 * time.Second)
	for len(s.got()) == 0 {
		select {
		case <-deadline:
			t.Fatal("the sweep never scanned")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if got := s.got(); !slices.Equal(got, []string{"vm-silent"}) {
		t.Fatalf("suspects = %v, want only vm-silent", got)
	}
}

// The agent retries soon only what a full gateway refused, and stops its jobs
// only when the hub detached it: both by the refusal's reason, wrapped as
// the agent wraps it. The text alone is no refusal (c4-E7): an error that
// quoted "detached from the hub" stopped every job of the machine.
func TestRefusalsAreReadByTheirReason(t *testing.T) {
	full := FullError("register: " + GatewayFull)
	detached := DetachedError(`backend "vm-a" is revoked`)
	for name, tc := range map[string]struct {
		err            error
		full, detached bool
	}{
		"refused before the hub": {fmt.Errorf("recv register ack: %w", full), true, false},
		"refused by the ack":     {fmt.Errorf("controller rejected registration: %s: %w", GatewayFull, full), true, false},
		"its session closed":     {detached, false, true},
		"its redial refused":     {fmt.Errorf("recv register ack: %w", detached), false, true},
		"the text of a full ack": {errors.New("controller rejected registration: " + GatewayFull), false, false},
		"the text of a detach":   {status.Error(codes.PermissionDenied, Detached), false, false},
		"a job that quotes it":   {fmt.Errorf("submit: %s", Detached), false, false},
		"rate limited":           {status.Error(codes.ResourceExhausted, "register rate limit exceeded (30/1m0s)"), false, false},
		"gateway down":           {status.Error(codes.Unavailable, "connection refused"), false, false},
		"no error":               {nil, false, false},
	} {
		if got := RefusedFull(tc.err); got != tc.full {
			t.Errorf("%s: RefusedFull = %v, want %v", name, got, tc.full)
		}
		if got := RefusedDetached(tc.err); got != tc.detached {
			t.Errorf("%s: RefusedDetached = %v, want %v", name, got, tc.detached)
		}
	}
	if !strings.Contains(detached.Error(), Detached) || !strings.Contains(full.Error(), GatewayFull) {
		t.Error("a refusal lost the text people and older agents read")
	}
}

// A revoked machine still holds its certificate until it expires, so a full
// gateway matches its name and refuses it like any other. That refusal is
// not proof of anything the controller should wait for: the machine's job is
// judged a grace after the last time it was heard through a session. The
// refusal of a machine in good standing still counts.
func TestRefusalOfARevokedMachineIsNoProofOfLife(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	tr.Revoked = func(_ context.Context, backendID string) (bool, error) {
		return backendID == "vm-revoked", nil
	}
	t0 := time.Unix(1000, 0)
	tr.Listed(map[string]time.Time{"vm-revoked": t0, "vm-valid": t0})
	for _, after := range []time.Duration{20, 50, 80} {
		at := t0.Add(after * time.Second)
		tr.Refused("vm-revoked", at)
		tr.Told(GatewayList{RefusedFull: map[string]time.Time{"vm-revoked": at, "vm-valid": at}})
		tr.Scan(at)
	}
	if got := tr.Since("vm-revoked"); !got.Equal(t0) {
		t.Fatalf("since = %v, want %v: the refusals of a revoked machine moved its proof of life", got, t0)
	}
	if got := s.got(); !slices.Equal(got, []string{"vm-revoked"}) {
		t.Fatalf("suspects = %v, want only the revoked machine, a grace after its last session", got)
	}
	if got := tr.Since("vm-valid"); !got.Equal(t0.Add(80 * time.Second)) {
		t.Fatalf("since = %v, want the last refusal of the machine in good standing", got)
	}
}

// While the controller cannot tell whether a machine is revoked, its refusal
// waits: the gateway remembers it for two minutes and the next sweep reads
// it again.
func TestRefusalWaitsWhileRevocationCannotBeRead(t *testing.T) {
	tr := NewTracker(45*time.Second, nil)
	unreadable := true
	tr.Revoked = func(context.Context, string) (bool, error) {
		if unreadable {
			return false, errors.New("cache not synced")
		}
		return false, nil
	}
	t0 := time.Unix(1000, 0)
	tr.Listed(map[string]time.Time{"vm-1": t0})
	refused := GatewayList{RefusedFull: map[string]time.Time{"vm-1": t0.Add(20 * time.Second)}}
	if tr.Told(refused); !tr.Since("vm-1").Equal(t0) {
		t.Fatalf("since = %v: a refusal counted with the machine's standing unknown", tr.Since("vm-1"))
	}
	unreadable = false
	if tr.Told(refused); !tr.Since("vm-1").Equal(t0.Add(20 * time.Second)) {
		t.Fatalf("since = %v, want the refusal, read again by the next sweep", tr.Since("vm-1"))
	}
}
