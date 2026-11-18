// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
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
	m.watchOnce.Do(func() {
		m.watch = NewTerminalWatch(m.cfg, m.runner)
		m.watch.logger = m.logger
	})
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

// followPollInterval is how often a TerminalWatch asks whether its units
// ended while it cannot hear systemd.
const followPollInterval = 500 * time.Millisecond

// followFallbackInterval is how often it asks while it hears systemd, for a
// change it did not hear.
const followFallbackInterval = 10 * time.Second

// TerminalWatch tells follow streams when their units end: the end of a job
// comes from systemd, never from its log (P13). It hears from the manager's
// own socket which units changed (unitChanges) and reads every followed unit
// in one `systemctl show` when systemd says one of them changed, when a
// stream starts (its unit may have ended already), and every
// followFallbackInterval. Asking every followPollInterval instead cost ~30
// mCPU of a VM agent's ~46 with one reader (a bash and a systemctl each
// time; E10, 2026-11-14), and a job with logs= is followed its whole life;
// hearing systemd, the agent spends ~16 (E10, 2026-11-16). Without the socket (an agent that is not root)
// the watch asks every followPollInterval, and dials again each time.
type TerminalWatch struct {
	cfg    Config
	runner Runner
	logger logr.Logger
	// subscribe hears the units systemd says changed; interval is how
	// often the watch asks without it, fallback how often with it.
	subscribe func(context.Context) (<-chan string, error)
	interval  time.Duration
	fallback  time.Duration
	// started wakes the sweep for a stream that starts.
	started chan struct{}

	mu      sync.Mutex
	waiters map[string][]*unitWaiter
	running bool
}

type unitWaiter struct {
	ctx   context.Context
	ended chan struct{}
}

// NewTerminalWatch builds a watch that hears the manager cfg names and asks
// through runner. A nil runner never reports an end.
func NewTerminalWatch(cfg Config, runner Runner) *TerminalWatch {
	return &TerminalWatch{
		cfg: cfg, runner: runner, logger: logr.Discard(),
		subscribe: unitChanges(cfg.privateSocket()), interval: followPollInterval, fallback: followFallbackInterval,
		started: make(chan struct{}, 1), waiters: map[string][]*unitWaiter{},
	}
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
	select {
	case w.started <- struct{}{}:
	default:
	}
	if !w.running {
		w.running = true
		go w.sweep()
	}
	return waiter.ended
}

// sweep runs while any stream waits: it reads every waited unit at once
// and closes the waits of those that ended, then waits for the next read.
func (w *TerminalWatch) sweep() {
	// Its end closes the connection to systemd.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var changes <-chan string
	told := false
	next := time.NewTimer(w.interval)
	defer next.Stop()
	for {
		if changes == nil {
			// Before the read: a unit that ends after it is said.
			var err error
			if changes, err = w.subscribe(ctx); err != nil && !told {
				w.logger.Info("cannot hear systemd: follow streams ask whether their jobs ended every "+w.interval.String(), "error", err.Error())
				told = true
			}
		}
		// Every stream that started until here is in this read.
		select {
		case <-w.started:
		default:
		}
		units := w.waitedUnits()
		if len(units) == 0 {
			return
		}
		w.close(w.ended(units))
		if changes == nil {
			next.Reset(w.interval)
		} else {
			next.Reset(w.fallback)
		}
		w.await(next, &changes)
	}
}

// await returns when the next read is due: at the timer, when a stream
// starts, when systemd says a followed unit changed, or when the connection
// drops (*changes is then nil: what changed meanwhile was not said).
func (w *TerminalWatch) await(next *time.Timer, changes *<-chan string) {
	for {
		select {
		case <-next.C:
			return
		case <-w.started:
			return
		case unit, ok := <-*changes:
			if !ok {
				*changes = nil
				return
			}
			if w.follows(unit) {
				return
			}
		}
	}
}

// follows reports whether a stream waits on unit.
func (w *TerminalWatch) follows(unit string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.waiters[unit]
	return ok
}

// close tells the streams of the units that ended.
func (w *TerminalWatch) close(ended []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, unit := range ended {
		for _, waiter := range w.waiters[unit] {
			close(waiter.ended)
		}
		delete(w.waiters, unit)
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
