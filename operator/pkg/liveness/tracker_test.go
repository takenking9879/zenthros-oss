// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"context"
	"errors"
	"github.com/zenthros/operator/pkg/observe"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type suspects struct {
	mu  sync.Mutex
	ids []string
}

func (s *suspects) add(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, id)
}

func (s *suspects) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ids)
}

func TestTrackerProofOnlyMovesForward(t *testing.T) {
	tr := NewTracker(45*time.Second, nil)
	t0 := time.Unix(1000, 0)
	tr.Seen("vm-1", t0.Add(10*time.Second))
	tr.Listed(map[string]time.Time{"vm-1": t0}) // a list read before the message
	if got := tr.Since("vm-1"); !got.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("since = %v; a late list took back a newer proof", got)
	}
}

// A machine no list has shown is alive one grace from the anchor (the
// controller's start, a gateway recovery), never judged on a silence the
// controller could not see (hot-state-design §10.5).
func TestTrackerAnchorIsTheOldestProof(t *testing.T) {
	tr := NewTracker(45*time.Second, nil)
	if !tr.Since("vm-unknown").IsZero() {
		t.Fatal("an unanchored tracker invented a proof")
	}
	start := time.Unix(1000, 0)
	tr.Seen("vm-old", start.Add(-time.Hour))
	tr.Anchor(start)
	for _, id := range []string{"vm-unknown", "vm-old"} {
		if got := tr.Since(id); !got.Equal(start) {
			t.Fatalf("%s: since = %v, want the anchor %v", id, got, start)
		}
		if tr.Suspected(id, start.Add(44*time.Second)) {
			t.Fatalf("%s suspected within a grace of the anchor", id)
		}
		if !tr.Suspected(id, start.Add(45*time.Second)) {
			t.Fatalf("%s not suspected a grace after the anchor", id)
		}
	}
	tr.Anchor(start.Add(-time.Minute))
	if got := tr.Since("vm-unknown"); !got.Equal(start) {
		t.Fatalf("an older anchor moved the proof back: %v", got)
	}
}

// Scan tells each silent machine once per silence; a new proof clears it.
func TestTrackerScanTellsOncePerSilence(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	t0 := time.Unix(1000, 0)
	tr.Listed(map[string]time.Time{"vm-dead": t0, "vm-live": t0})
	tr.Scan(t0.Add(44 * time.Second))
	if got := s.got(); len(got) != 0 {
		t.Fatalf("suspected within the grace: %v", got)
	}
	tr.Listed(map[string]time.Time{"vm-live": t0.Add(30 * time.Second)})
	tr.Scan(t0.Add(45 * time.Second))
	tr.Scan(t0.Add(60 * time.Second))
	if got := s.got(); !slices.Equal(got, []string{"vm-dead"}) {
		t.Fatalf("suspects = %v, want vm-dead once", got)
	}
	tr.Seen("vm-dead", t0.Add(61*time.Second)) // it came back
	tr.Scan(t0.Add(106 * time.Second))
	if got := s.got(); !slices.Contains(got[1:], "vm-dead") {
		t.Fatalf("a second silence after a proof was not told: %v", got)
	}
}

// A gateway's notice is a hint: it wakes the machine's jobs at once, and
// moves the proof to the heartbeat it names, never back.
func TestTrackerNoticeWakesWithoutTakingProofBack(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	t0 := time.Unix(1000, 0)
	tr.Seen("vm-1", t0.Add(20*time.Second))
	tr.Notified("vm-1", t0)
	if got := s.got(); !slices.Equal(got, []string{"vm-1"}) {
		t.Fatalf("suspects = %v, want vm-1 told at once", got)
	}
	if got := tr.Since("vm-1"); !got.Equal(t0.Add(20 * time.Second)) {
		t.Fatalf("since = %v; the notice took a newer proof back", got)
	}
}

func TestTrackerSweepListsAndScans(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	now := time.Unix(1000, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan struct{}, 10)
	list := func(context.Context) (observe.View[GatewayList], error) {
		calls <- struct{}{}
		// Two gateways: each lists only the machines it holds.
		return observe.View[GatewayList]{Complete: true, Items: []GatewayList{
			{Held: map[string]time.Time{"vm-a": now.Add(-10 * time.Second)}},
			{Held: map[string]time.Time{"vm-b": now.Add(-50 * time.Second)}},
		}}, nil
	}
	go tr.Sweep(ctx, time.Hour, func() time.Time { return now }, list)
	<-calls
	deadline := time.After(5 * time.Second)
	for len(s.got()) == 0 {
		select {
		case <-deadline:
			t.Fatal("the sweep never scanned")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if got := s.got(); !slices.Equal(got, []string{"vm-b"}) {
		t.Fatalf("suspects = %v, want only the silent vm-b", got)
	}
	if got := tr.Since("vm-a"); !got.Equal(now.Add(-10 * time.Second)) {
		t.Fatalf("vm-a since = %v", got)
	}
}

// With no gateway answering, nothing is suspected: GatewayHealth defers the
// verdicts and its recovery re-anchors the tracker.
func TestTrackerSweepThatFailsSuspectsNobody(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	now := time.Unix(1000, 0)
	tr.Seen("vm-a", now.Add(-time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	calls := make(chan struct{}, 10)
	go func() {
		defer close(done)
		tr.Sweep(ctx, time.Hour, func() time.Time { return now }, func(context.Context) (observe.View[GatewayList], error) {
			calls <- struct{}{}
			return observe.View[GatewayList]{}, errors.New("no gateway answers")
		})
	}()
	<-calls
	cancel()
	<-done
	if got := s.got(); len(got) != 0 {
		t.Fatalf("a failed list suspected %v", got)
	}
}

func TestTrackerForgetsMachinesGoneAnHour(t *testing.T) {
	tr := NewTracker(45*time.Second, nil)
	t0 := time.Unix(1000, 0)
	tr.Seen("vm-gone", t0)
	tr.Scan(t0.Add(2 * time.Hour))
	tr.mu.Lock()
	_, kept := tr.seen["vm-gone"]
	tr.mu.Unlock()
	if kept {
		t.Fatal("a machine gone two hours is still tracked")
	}
}

// Many machines going quiet together is a shared silence (the agents'
// ingress, a gateway replica); one machine's death among live ones is not,
// and machines that were already silent before it do not count.
func TestTrackerSharedSilence(t *testing.T) {
	grace := 30 * time.Second
	t0 := time.Unix(1_000_000, 0)
	now := t0.Add(time.Minute)
	for _, tc := range []struct {
		name   string
		proofs map[string]time.Time
		want   bool
	}{
		{"alone", map[string]time.Time{"a": t0}, false},
		{"one of three", map[string]time.Time{"a": t0, "b": now, "c": now}, false},
		{"half", map[string]time.Time{"a": t0, "b": t0, "c": now, "d": now}, false},
		{"two of two", map[string]time.Time{"a": t0, "b": t0.Add(-5 * time.Second)}, true},
		{"all of three", map[string]time.Time{"a": t0, "b": t0, "c": t0.Add(10 * time.Second)}, true},
		{"the others died long before", map[string]time.Time{"a": t0, "b": t0.Add(-time.Hour / 2), "c": t0.Add(-time.Hour / 2)}, false},
		{"not silent yet", map[string]time.Time{"a": now.Add(-10 * time.Second), "b": t0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewTracker(grace, nil)
			for id, at := range tc.proofs {
				tracker.Seen(id, at)
			}
			if got := tracker.SharedSilence("a", now); got != tc.want {
				t.Fatalf("SharedSilence = %v, want %v", got, tc.want)
			}
		})
	}
	if NewTracker(grace, nil).SharedSilence("unknown", now) {
		t.Fatal("a machine never seen has no shared silence")
	}
}

// Machines stopped on purpose (a pool scaling down, a test tearing its
// machines down) as another one dies are no shared silence: the dead one is
// judged on its grace, not five minutes later (local E2E 2026-10-16,
// TestContainerKilledIsLost). A stopped machine's own silence is never
// shared, and a machine seen again after it stopped counts again.
func TestStoppedMachinesAreNoSharedSilence(t *testing.T) {
	grace := 30 * time.Second
	t0 := time.Unix(1_000_000, 0)
	now := t0.Add(time.Minute)
	tracker := NewTracker(grace, nil)
	tracker.Seen("live", now)
	tracker.Seen("dead", t0)
	tracker.Departed("torn-down-1", t0.Add(-5*time.Second))
	tracker.Departed("torn-down-2", t0.Add(-10*time.Second))
	if tracker.SharedSilence("dead", now) {
		t.Fatal("two stopped machines made a death a shared silence")
	}
	if tracker.SharedSilence("torn-down-1", now) {
		t.Fatal("a stopped machine's silence is shared")
	}

	// Back from its restart: its next silence counts with the others'.
	tracker.Seen("torn-down-1", t0.Add(time.Second))
	tracker.Seen("torn-down-2", t0.Add(time.Second))
	if !tracker.SharedSilence("dead", now) {
		t.Fatal("machines seen again after they stopped are not counted")
	}

	// The notice is a hint like any other: the machine is looked at now.
	var told []string
	hinted := NewTracker(grace, func(id string) { told = append(told, id) })
	hinted.Departed("m", t0)
	if len(told) != 1 || told[0] != "m" || !hinted.Since("m").Equal(t0) {
		t.Fatalf("departed notice told %v, since %s", told, hinted.Since("m"))
	}
}

// Machines retired as another one dies were removed, not cut off: once a
// scan finds them silent and revoked, their silence is not shared, and the
// death is judged on its own grace (E2E: two retired containers held a
// killed one's verdict for the 5-minute shared-silence cap).
func TestRevokedMachinesAreNoSharedSilence(t *testing.T) {
	grace := 30 * time.Second
	t0 := time.Unix(1_000_000, 0)
	now := t0.Add(time.Minute)
	tracker := NewTracker(grace, nil)
	tracker.Revoked = func(_ context.Context, id string) (bool, error) {
		return strings.HasPrefix(id, "retired"), nil
	}
	tracker.Seen("live", now)
	tracker.Seen("dead", t0)
	tracker.Seen("retired-1", t0.Add(-5*time.Second))
	tracker.Seen("retired-2", t0.Add(-10*time.Second))
	if !tracker.SharedSilence("dead", now) {
		t.Fatal("before a scan, silent machines are counted")
	}
	tracker.Scan(now)
	if tracker.SharedSilence("dead", now) {
		t.Fatal("two revoked machines made a death a shared silence")
	}

	// Enrolled again under the same name: its next silence counts.
	tracker.Seen("retired-1", t0.Add(time.Second))
	tracker.Seen("retired-2", t0.Add(time.Second))
	tracker.Revoked = func(context.Context, string) (bool, error) { return false, nil }
	if !tracker.SharedSilence("dead", now) {
		t.Fatal("machines seen again after their revocation are not counted")
	}
}

// A forward proves its machine lived when the Gateway read it, never later
// than the Hub's now; a Gateway that does not stamp it falls back to now.
func TestReceivedAt(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	read := now.Add(-90 * time.Second)
	if got := ReceivedAt(read.UnixNano(), now); !got.Equal(read) {
		t.Fatalf("ReceivedAt(read) = %s, want %s", got, read)
	}
	if got := ReceivedAt(0, now); !got.Equal(now) {
		t.Fatalf("ReceivedAt(0) = %s, want now", got)
	}
	if got := ReceivedAt(now.Add(time.Hour).UnixNano(), now); !got.Equal(now) {
		t.Fatalf("a stamp from the future = %s, want now", got)
	}
}

// Silent is what the tracker knows: before its anchor no list has reached
// it and no machine is silent; after, a machine is silent a grace past its
// last proof or the anchor (the walk skips such a
// machine).
func TestTrackerSilentOnlyOnceAnchored(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	tracker := NewTracker(30*time.Second, nil)
	if tracker.Silent("vm-a", t0.Add(time.Hour)) {
		t.Fatal("a machine is silent before the tracker heard any list")
	}
	tracker.Anchor(t0)
	tracker.Seen("vm-b", t0.Add(time.Minute))
	now := t0.Add(time.Minute + 10*time.Second)
	if !tracker.Silent("vm-a", now) {
		t.Fatal("vm-a, unheard a minute past the anchor, is not silent")
	}
	if tracker.Silent("vm-b", now) {
		t.Fatal("vm-b, heard ten seconds ago, is silent")
	}
}

// K2: a list some gateway did not answer records who it shows, and finds
// nobody silent: a machine missing from it may be on the replica that did
// not answer.
func TestTrackerIncompleteSweepSuspectsNobody(t *testing.T) {
	var s suspects
	tr := NewTracker(45*time.Second, s.add)
	now := time.Unix(1000, 0)
	tr.Seen("vm-behind-the-silent-replica", now.Add(-time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	calls := make(chan struct{}, 10)
	go func() {
		defer close(done)
		tr.Sweep(ctx, time.Hour, func() time.Time { return now }, func(context.Context) (observe.View[GatewayList], error) {
			calls <- struct{}{}
			return observe.View[GatewayList]{Items: []GatewayList{{Held: map[string]time.Time{"vm-a": now}}}}, nil
		})
	}()
	<-calls
	// Cancelled once the sweep took the answer in: a sweep cancelled while
	// its list answers drops the answer, and the list answers after it
	// signals.
	for deadline := time.Now().Add(5 * time.Second); !tr.Since("vm-a").Equal(now); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("what the incomplete list showed was not recorded")
		}
	}
	cancel()
	<-done
	if got := s.got(); len(got) != 0 {
		t.Fatalf("an incomplete list suspected %v", got)
	}
}

// H11: a machine retired after it went silent departs on a later scan; its
// silence was counted as shared with the others' until it did.
func TestTrackerAsksRevokedOnEveryScan(t *testing.T) {
	tr := NewTracker(45*time.Second, nil)
	revoked := false
	tr.Revoked = func(context.Context, string) (bool, error) { return revoked, nil }
	t0 := time.Unix(1000, 0)
	tr.Seen("vm-a", t0)
	tr.Scan(t0.Add(time.Minute))
	tr.mu.Lock()
	_, left := tr.departed["vm-a"]
	tr.mu.Unlock()
	if left {
		t.Fatal("a machine not revoked departed")
	}
	revoked = true
	tr.Scan(t0.Add(2 * time.Minute))
	tr.mu.Lock()
	_, left = tr.departed["vm-a"]
	tr.mu.Unlock()
	if !left {
		t.Fatal("a machine revoked after its silence began never departed")
	}
}
