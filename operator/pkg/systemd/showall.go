// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// showBatch is how many units one `systemctl show` reads: a sweep over many
// jobs is one command per hundred units, not one each.
const showBatch = 100

// StatusAll reads the units' ShowProperties, in the order named, in one
// `systemctl show` per showBatch units. A command per unit cost a bash and
// a systemctl, about 15 ms of CPU, every sweep.
func (m *Manager) StatusAll(ctx context.Context, names []string) ([]map[string]string, error) {
	return showAll(ctx, m.runner, m.cfg, names, ShowProperties)
}

// TerminalWatch returns the Manager's watch over the units its follow
// streams read: one for every stream, so their sweeps are one command.
func (m *Manager) TerminalWatch() *TerminalWatch {
	m.watchOnce.Do(func() { m.watch = NewTerminalWatch(m.cfg, m.runner) })
	return m.watch
}

func showAll(ctx context.Context, runner Runner, cfg Config, names []string, properties string) ([]map[string]string, error) {
	all := make([]map[string]string, 0, len(names))
	for start := 0; start < len(names); start += showBatch {
		chunk := names[start:min(len(names), start+showBatch)]
		stdout, err := runner.Run(ctx, cfg.Systemctl("show "+strings.Join(chunk, " ")+" --property=Id,"+properties))
		if err != nil {
			return nil, fmt.Errorf("show %d units: %w", len(chunk), err)
		}
		// systemctl answers each unit in the order named, a blank line
		// between them; the Id checks the order held.
		blocks := strings.Split(strings.TrimRight(stdout, "\n"), "\n\n")
		if len(blocks) != len(chunk) {
			return nil, fmt.Errorf("show %d units: %d answers", len(chunk), len(blocks))
		}
		for i, block := range blocks {
			props := ParseShow(block)
			if id := props["Id"]; id != "" && id != chunk[i] && id != chunk[i]+".service" {
				return nil, fmt.Errorf("show %s: the answer is for %s", chunk[i], id)
			}
			all = append(all, props)
		}
	}
	return all, nil
}

// unitEnded reports whether the unit's properties are a terminal Zenthros
// phase. A unit that never started is not.
func unitEnded(props map[string]string) bool {
	return MapState(props).Phase.Terminal()
}

// TerminalWatch tells follow streams when their units end. journalctl -f has
// no "stop when the unit stops", so something must ask systemd; one sweep
// asks for every followed unit in one `systemctl show`. A poll per stream
// cost two commands a second each, about 40 mCPU a follow.
type TerminalWatch struct {
	cfg      Config
	runner   Runner
	interval time.Duration

	mu      sync.Mutex
	waiters map[string][]*unitWaiter
	running bool
}

type unitWaiter struct {
	ctx   context.Context
	ended chan struct{}
}

// NewTerminalWatch builds a watch that asks through runner. A nil runner
// never reports an end.
func NewTerminalWatch(cfg Config, runner Runner) *TerminalWatch {
	return &TerminalWatch{cfg: cfg, runner: runner, interval: followPollInterval, waiters: map[string][]*unitWaiter{}}
}

// Ended returns a channel closed once the unit reaches a terminal phase. It
// is never closed when ctx ends first: the waiter is dropped at the next
// sweep.
func (w *TerminalWatch) Ended(ctx context.Context, unit string) <-chan struct{} {
	waiter := &unitWaiter{ctx: ctx, ended: make(chan struct{})}
	if w == nil || w.runner == nil {
		return waiter.ended
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waiters[unit] = append(w.waiters[unit], waiter)
	if !w.running {
		w.running = true
		go w.sweep()
	}
	return waiter.ended
}

// sweep runs while any stream waits: each tick it reads every waited unit
// at once and closes the waits of those that ended.
func (w *TerminalWatch) sweep() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for range ticker.C {
		units := w.waitedUnits()
		if len(units) == 0 {
			return
		}
		ended := w.ended(units)
		w.mu.Lock()
		for _, unit := range ended {
			for _, waiter := range w.waiters[unit] {
				close(waiter.ended)
			}
			delete(w.waiters, unit)
		}
		w.mu.Unlock()
	}
}

// waitedUnits drops the waits whose streams ended and lists the units still
// waited on; with none left the sweep stops, under the same lock a new wait
// takes to start one.
func (w *TerminalWatch) waitedUnits() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	units := make([]string, 0, len(w.waiters))
	for unit, waiters := range w.waiters {
		live := waiters[:0]
		for _, waiter := range waiters {
			if waiter.ctx.Err() == nil {
				live = append(live, waiter)
			}
		}
		if len(live) == 0 {
			delete(w.waiters, unit)
			continue
		}
		w.waiters[unit] = live
		units = append(units, unit)
	}
	if len(units) == 0 {
		w.running = false
	}
	return units
}

// ended is the units that reached a terminal phase. A failed read is not an
// end: a transient `systemctl show` failure must not cut a stream short. A
// batch that fails is read one unit at a time, so one bad name does not hold
// every other stream open.
func (w *TerminalWatch) ended(units []string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	all, err := showAll(ctx, w.runner, w.cfg, units, ShowProperties)
	if err != nil && len(units) > 1 {
		all = make([]map[string]string, len(units))
		for i, unit := range units {
			if one, err := showAll(ctx, w.runner, w.cfg, []string{unit}, ShowProperties); err == nil {
				all[i] = one[0]
			}
		}
	} else if err != nil {
		return nil
	}
	var ended []string
	for i, props := range all {
		if props != nil && unitEnded(props) {
			ended = append(ended, units[i])
		}
	}
	return ended
}
