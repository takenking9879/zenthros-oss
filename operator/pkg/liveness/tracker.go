// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"context"
	"sync"
	"time"

	"github.com/zenthros/operator/pkg/observe"
)

// DefaultSweepInterval is how often the controller asks every gateway which
// machines it holds and when each last sent a heartbeat (Tracker.Sweep).
const DefaultSweepInterval = 15 * time.Second

// trackerForget is how long a machine nobody has seen stays in the tracker.
// Past it its proof falls back to the anchor, which is as old or older.
const trackerForget = time.Hour

// Tracker is the controller's memory of which machines are alive. The gateway
// that holds a machine's session sees its heartbeats; nobody writes them onto
// the machine's jobs any more (hot-state-design §10). The controller learns
// that a machine lives from:
//   - the gateways' lists of the machines they hold, with each one's last
//     heartbeat, every DefaultSweepInterval (Listed);
//   - any message the machine sends through the Hub, and any answer it gives
//     when asked (Seen);
//   - a gateway's notice that a machine went silent or its session closed
//     (Notified), which carries its last heartbeat and wakes its jobs early;
//   - the gateways' lists of the machines they refused for lack of room
//     (Refused): alive and calling, with no session to be asked through,
//     unless the machine is revoked.
//
// A machine is alive until its last proof plus a grace, and nothing is
// written while it is: only the verdict on a loss is. The lists are the
// level that does not depend on any notice arriving; a notice only makes the
// controller look sooner.
//
// The anchor is the oldest proof the controller assumes of anyone: its own
// start. A machine no list has shown is alive one grace from it, so nothing
// is judged on a silence the controller was not able to see (10.5); an
// outage of the gateways is the same kind of blindness, and Gateways leaves
// it out of every silence.
type Tracker struct {
	// Grace is how long a machine may be silent before its jobs are looked
	// at (OnSuspect). Each job is still judged on its own grace.
	Grace time.Duration
	// OnSuspect hears a machine whose last proof is older than Grace, once
	// per silence, and every machine a gateway gives notice of.
	OnSuspect func(backendID string)
	// RefusedPatience is how long refusals alone carry a machine from the
	// last time it was heard through a session; zero is
	// DefaultRefusedPatience.
	RefusedPatience time.Duration
	// Revoked tells whether a machine's access was withdrawn. A revoked
	// machine keeps its certificate until it expires, so a full gateway
	// still refuses it by name: that refusal is not counted. Nil counts
	// every refusal; an error counts none, and the next list brings the
	// refusal again.
	Revoked func(ctx context.Context, backendID string) (bool, error)
	// Gateways counts a silence only while they answer (Deadline); nil
	// counts all of it.
	Gateways *GatewayHealth

	mu     sync.Mutex
	anchor time.Time
	// seen is each machine's last proof of life, refusals included; heard
	// is the last one that came through a session.
	seen      map[string]time.Time
	heard     map[string]time.Time
	suspected map[string]bool
	// departed holds the machines whose agent closed its own session (it
	// was stopped) or that went silent revoked (retired), with their last
	// proof then. A newer proof takes the machine back.
	departed map[string]time.Time
}

// NewTracker returns a tracker anchored nowhere: until Anchor, a machine no
// list has shown has no proof at all.
func NewTracker(grace time.Duration, onSuspect func(backendID string)) *Tracker {
	if grace <= 0 {
		grace = DefaultJobGrace
	}
	return &Tracker{Grace: grace, OnSuspect: onSuspect, seen: map[string]time.Time{}, heard: map[string]time.Time{}, suspected: map[string]bool{}, departed: map[string]time.Time{}}
}

// Seen records proof that backendID was alive at at. Proof only moves
// forward: a late list cannot take back a newer message.
func (t *Tracker) Seen(backendID string, at time.Time) {
	if backendID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seenLocked(backendID, at)
}

func (t *Tracker) seenLocked(backendID string, at time.Time) {
	if at.After(t.heard[backendID]) {
		t.heard[backendID] = at
	}
	if at.After(t.seen[backendID]) {
		t.seen[backendID] = at
		delete(t.suspected, backendID)
	}
	if left, ok := t.departed[backendID]; ok && at.After(left) {
		delete(t.departed, backendID)
	}
}

// Refused records that a gateway refused backendID a session at at because
// it was full. The gateway matched the name to the connection's certificate
// first, so the machine is alive: its jobs are not judged on a silence that
// is only a lack of room, which lasts until a replica with room is back
// (scale lab, 2026-10-10).
//
// It is proof that the machine lives, not that anyone can reach it. Refusals
// alone move the proof no further than RefusedPatience past the last time
// the machine was heard through a session, or past the anchor. The refusal
// of a revoked machine (Revoked) proves nothing the controller waits for.
func (t *Tracker) Refused(backendID string, at time.Time) {
	if backendID == "" || !t.refusalCounts(context.Background(), backendID) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refusedLocked(backendID, at)
}

// refusalCounts asks Revoked, outside the lock: it reads the controller's
// cache.
func (t *Tracker) refusalCounts(ctx context.Context, backendID string) bool {
	if t.Revoked == nil {
		return true
	}
	revoked, err := t.Revoked(ctx, backendID)
	return err == nil && !revoked
}

func (t *Tracker) refusedLocked(backendID string, at time.Time) {
	heard := t.heard[backendID]
	if t.anchor.After(heard) {
		heard = t.anchor
	}
	if heard.IsZero() {
		return
	}
	patience := t.RefusedPatience
	if patience <= 0 {
		patience = DefaultRefusedPatience
	}
	if limit := heard.Add(patience); at.After(limit) {
		at = limit
	}
	if at.After(t.seen[backendID]) {
		t.seen[backendID] = at
		delete(t.suspected, backendID)
	}
}

// Anchor grants every machine proof of life at at: the controller started,
// and nothing it could see renewed anyone before. An outage of the gateways
// is not an anchor: Gateways counts the silence through it (10#4).
func (t *Tracker) Anchor(at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if at.After(t.anchor) {
		t.anchor = at
		t.suspected = map[string]bool{}
	}
}

// Since is backendID's last proof of life, never before the anchor. Zero
// when there is none.
func (t *Tracker) Since(backendID string) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sinceLocked(backendID)
}

// quietUntil is when a silence from since has lasted Grace.
func (t *Tracker) quietUntil(since time.Time) time.Time {
	if t.Gateways == nil {
		return since.Add(t.Grace)
	}
	return t.Gateways.Deadline(since, t.Grace)
}

func (t *Tracker) sinceLocked(backendID string) time.Time {
	if seen := t.seen[backendID]; seen.After(t.anchor) {
		return seen
	}
	return t.anchor
}

// Suspected reports whether backendID's silence passed Grace: its jobs were
// told (OnSuspect) and look again at their own deadlines. Before the anchor
// nothing is known, as for Silent (10#7).
func (t *Tracker) Suspected(backendID string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.anchor.IsZero() && !now.Before(t.quietUntil(t.sinceLocked(backendID)))
}

// Silent reports a machine the tracker knows is silent: it is anchored (its
// lists reach it), and the machine's last proof, or the anchor, is older
// than Grace. Before the anchor nothing is known, and nothing is silent.
func (t *Tracker) Silent(backendID string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.anchor.IsZero() && !now.Before(t.quietUntil(t.sinceLocked(backendID)))
}

// SharedSilence reports whether backendID's silence is shared: of the
// machines that gave proof of life within Grace before its last proof, at
// least two, more than half have been silent for Grace since. Many machines
// going quiet together is the path to them failing (the agents' ingress, a
// gateway replica that stopped answering), not each machine; one machine's
// death among live ones is never shared. A
// machine that was stopped (Departed) or retired (Scan) is neither:
// machines a pool scales down, or a user retires, as another one dies did
// not lose their path, and its own silence is never shared.
func (t *Tracker) SharedSilence(backendID string, now time.Time) bool {
	if backendID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, known := t.seen[backendID]; !known {
		return false
	}
	if _, left := t.departed[backendID]; left {
		return false
	}
	last := t.sinceLocked(backendID)
	if now.Before(t.quietUntil(last)) {
		return false
	}
	from := last.Add(-t.Grace)
	around, silent := 0, 0
	for id := range t.seen {
		since := t.sinceLocked(id)
		if since.Before(from) {
			continue
		}
		if _, left := t.departed[id]; left {
			continue
		}
		around++
		if !now.Before(t.quietUntil(since)) {
			silent++
		}
	}
	return around >= 2 && silent*2 > around
}

// ReceivedAt is the proof of life a forwarded message carries: when the
// Gateway read it from the agent's stream (receivedUnixNano), never later
// than now. A forward delivered late, by a slow Hub or from a queue drained
// after the stream ended, proves no more than that.
// Zero, from a Gateway that does not stamp it, is now.
func ReceivedAt(receivedUnixNano int64, now time.Time) time.Time {
	if receivedUnixNano <= 0 {
		return now
	}
	if at := time.Unix(0, receivedUnixNano); at.Before(now) {
		return at
	}
	return now
}

// Notified is a gateway's notice that backendID went silent or its session
// closed with nothing replacing it, last being its last heartbeat. It is a
// hint, never a verdict: the jobs are looked at now, and each is judged when
// its grace from the last proof runs out, after its owner is asked.
func (t *Tracker) Notified(backendID string, last time.Time) {
	if backendID == "" {
		return
	}
	t.mu.Lock()
	t.seenLocked(backendID, last)
	t.mu.Unlock()
	if t.OnSuspect != nil {
		t.OnSuspect(backendID)
	}
}

// Departed is a gateway's notice that backendID's agent closed its own
// session, last being its last heartbeat: the machine was stopped. Its jobs
// are looked at now, as on any notice, and judged on their own grace; its
// silence is never counted as shared with the others' (SharedSilence).
func (t *Tracker) Departed(backendID string, last time.Time) {
	if backendID == "" {
		return
	}
	t.mu.Lock()
	t.seenLocked(backendID, last)
	t.departLocked(backendID)
	t.mu.Unlock()
	if t.OnSuspect != nil {
		t.OnSuspect(backendID)
	}
}

// departLocked keeps backendID's silence out of SharedSilence until its next
// proof of life.
func (t *Tracker) departLocked(backendID string) {
	if t.departed == nil {
		t.departed = map[string]time.Time{}
	}
	t.departed[backendID] = t.seen[backendID]
}

// Listed records one gateway's list: each machine it holds, with its last
// heartbeat. A machine missing from a list proves nothing either way (it
// may be on another gateway); its proof simply stops moving.
func (t *Tracker) Listed(last map[string]time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, at := range last {
		t.seenLocked(id, at)
	}
}

// Told records one gateway's answer to the sweep: the machines it holds and
// the ones it refused for lack of room.
func (t *Tracker) Told(list GatewayList) {
	t.told(context.Background(), list)
}

func (t *Tracker) told(ctx context.Context, list GatewayList) {
	var refused []string
	for id := range list.RefusedFull {
		if t.refusalCounts(ctx, id) {
			refused = append(refused, id)
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, at := range list.Held {
		t.seenLocked(id, at)
	}
	for _, id := range refused {
		t.refusedLocked(id, list.RefusedFull[id])
	}
}

// Scan tells OnSuspect of every machine whose silence passed Grace at now
// and that it has not told since its last proof. A suspect whose access was
// withdrawn (Revoked) was retired, not cut off: it departs, as one that
// closed its own session. Revoked is asked on every scan of a suspect that
// has not departed: a machine retired after it went silent departs then,
// not never. It forgets machines nobody saw for an hour.
func (t *Tracker) Scan(now time.Time) {
	var suspects, undeparted []string
	t.mu.Lock()
	for id, seen := range t.seen {
		since := t.sinceLocked(id)
		if now.Sub(seen) > trackerForget && now.Sub(since) > trackerForget {
			delete(t.seen, id)
			delete(t.heard, id)
			delete(t.suspected, id)
			delete(t.departed, id)
			continue
		}
		if now.Before(t.quietUntil(since)) {
			continue
		}
		if !t.suspected[id] {
			t.suspected[id] = true
			suspects = append(suspects, id)
		}
		if _, left := t.departed[id]; !left {
			undeparted = append(undeparted, id)
		}
	}
	t.mu.Unlock()
	if t.Revoked != nil {
		for _, id := range undeparted {
			// Outside the lock: it reads the cache.
			if revoked, err := t.Revoked(context.Background(), id); err == nil && revoked {
				t.mu.Lock()
				t.departLocked(id)
				t.mu.Unlock()
			}
		}
	}
	if t.OnSuspect == nil {
		return
	}
	for _, id := range suspects {
		t.OnSuspect(id)
	}
}

// Sweep asks for the gateways' lists every interval until ctx ends, records
// what answered and scans. Every list that came is proof of life; only a
// view every replica answered may find a machine silent: one missing from
// it may be on the replica that did not answer. With no gateway answering,
// GatewayHealth defers the verdicts and its recovery re-anchors the tracker.
func (t *Tracker) Sweep(ctx context.Context, interval time.Duration, now func() time.Time, list func(context.Context) (observe.View[GatewayList], error)) {
	if interval <= 0 {
		interval = DefaultSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		listCtx, cancel := context.WithTimeout(ctx, interval)
		view, err := list(listCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			for _, l := range view.Items {
				t.told(ctx, l)
			}
			if view.Complete {
				t.Scan(now())
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
