// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/takenking9879/zenthros-oss/operator/internal/testutil"
)

// fakeBus is systemd's bus as a TerminalWatch hears it: the units systemd
// says changed. Down, it refuses a connection.
type fakeBus struct {
	mu      sync.Mutex
	down    bool
	dials   int
	changes chan string
	// log, when set, records each connection, in order with the reads.
	log func(string)
}

func (b *fakeBus) subscribe(context.Context) (<-chan string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dials++
	if b.log != nil {
		b.log("subscribe")
	}
	if b.down {
		return nil, errors.New("dial unix /run/systemd/private: connect: no such file or directory")
	}
	b.changes = make(chan string, 16)
	return b.changes, nil
}

// say is systemd saying unit changed.
func (b *fakeBus) say(unit string) {
	b.mu.Lock()
	ch := b.changes
	b.mu.Unlock()
	ch <- unit
}

// drop ends the connection there is, as a `systemctl daemon-reexec` does.
func (b *fakeBus) drop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	close(b.changes)
	b.changes = nil
}

func (b *fakeBus) dialed() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dials
}

// noBus is a bus a test never reaches: its watch asks at its interval.
func noBus(context.Context) (<-chan string, error) { return nil, errors.New("no bus in this test") }

// busWatch is a watch that hears bus. Neither its interval nor its
// fallback comes within a test: what it reads, it reads because the bus or
// a new stream asked.
func busWatch(runner Runner, bus *fakeBus) *TerminalWatch {
	w := NewTerminalWatch(Config{Scope: ScopeSystem}, runner)
	w.subscribe, w.interval, w.fallback = bus.subscribe, time.Hour, time.Hour
	return w
}

func endedWithin(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: the stream was never told its unit ended", what)
	}
}

func stillOpen(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s: the stream was told its running unit ended", what)
	default:
	}
}

// A follow on a VM asked `systemctl show` twice a second for as long as it
// followed: a bash and a systemctl each time, ~35 mCPU of the agent's ~46
// with one reader (Cierre of the logs plan, 2026-11-15), and a job with
// logs= is followed its whole life. Hearing systemd, the watch reads once
// when the stream starts and again only when systemd says a followed unit
// changed; another unit's change asks nothing.
func TestAFollowAsksSystemdOnlyWhenItsUnitChanges(t *testing.T) {
	runner := &showRunner{active: map[string]string{}, result: map[string]string{}}
	unit := "zenthros-j001.service"
	runner.set(unit, "active", "success")
	bus := &fakeBus{}
	watch := busWatch(runner, bus)
	watch.interval = 5 * time.Millisecond // without the bus, it would ask every 5 ms

	ended := watch.Ended(context.Background(), unit)
	testutil.Eventually(t, "the first read", func() bool { return len(runner.seen()) >= 1 })
	time.Sleep(200 * time.Millisecond)
	if n := len(runner.seen()); n != 1 {
		t.Fatalf("%d reads in 200 ms of a running unit nobody said changed, want 1", n)
	}

	bus.say("zenthros-other.service")
	time.Sleep(50 * time.Millisecond)
	if n := len(runner.seen()); n != 1 {
		t.Fatalf("another unit's change made %d reads, want none", n-1)
	}
	stillOpen(t, ended, "another unit changed")

	runner.set(unit, "inactive", "success")
	bus.say(unit)
	endedWithin(t, ended, "systemd said the unit changed")
	if n := len(runner.seen()); n != 2 {
		t.Fatalf("%d reads, want 2: the first and the one systemd asked for", n)
	}
}

// The watch hears systemd before it reads: a unit that ends after the read
// is said on the bus, and one that ended before it is seen by the read. The
// other order loses an end that falls between the two until the fallback.
func TestAWatchHearsSystemdBeforeItReads(t *testing.T) {
	var mu sync.Mutex
	var order []string
	log := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, event)
	}
	shows := &showRunner{active: map[string]string{}, result: map[string]string{}}
	unit := "zenthros-j001.service"
	shows.set(unit, "active", "success")
	runner := RunnerFunc(func(ctx context.Context, cmd string) (string, error) {
		log("read")
		return shows.Run(ctx, cmd)
	})
	bus := &fakeBus{log: log}
	watch := busWatch(runner, bus)

	_ = watch.Ended(context.Background(), unit)
	testutil.Eventually(t, "the first read", func() bool { return len(shows.seen()) >= 1 })
	mu.Lock()
	defer mu.Unlock()
	if i := slices.Index(order, "subscribe"); i < 0 || i > slices.Index(order, "read") {
		t.Fatalf("events %v: the watch must hear systemd before its first read", order)
	}
}

// A stream that starts following a unit that already ended learns it at
// once: systemd says nothing more of that unit, and waiting for the
// fallback would hold the stream open for it. So does one that starts while
// the watch already hears systemd.
func TestAFollowOfAUnitThatAlreadyEndedEndsAtOnce(t *testing.T) {
	runner := &showRunner{active: map[string]string{}, result: map[string]string{}}
	runner.set("zenthros-done.service", "inactive", "success")
	runner.set("zenthros-running.service", "active", "success")
	runner.set("zenthros-failed.service", "failed", "exit-code")
	watch := busWatch(runner, &fakeBus{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	endedWithin(t, watch.Ended(ctx, "zenthros-done.service"), "the first stream")
	running := watch.Ended(ctx, "zenthros-running.service")
	testutil.Eventually(t, "the running unit read", func() bool { return len(runner.seen()) >= 2 })
	endedWithin(t, watch.Ended(ctx, "zenthros-failed.service"), "a stream while the watch hears systemd")
	stillOpen(t, running, "another stream started")
}

// Without systemd's bus (an agent that is not root, a manager without its
// socket) the watch asks at its interval, as before, and dials again each
// time. Once it hears systemd it stops asking. A connection that drops
// (`systemctl daemon-reexec`) is dialed again and read at once: what ended
// meanwhile was not said.
func TestWithoutSystemdsBusAWatchAsksAtItsInterval(t *testing.T) {
	runner := &showRunner{active: map[string]string{}, result: map[string]string{}}
	first, second := "zenthros-j001.service", "zenthros-j002.service"
	runner.set(first, "active", "success")
	runner.set(second, "active", "success")
	bus := &fakeBus{down: true}
	watch := busWatch(runner, bus)
	watch.interval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstEnded := watch.Ended(ctx, first)
	secondEnded := watch.Ended(ctx, second)
	testutil.Eventually(t, "reads at the interval", func() bool { return len(runner.seen()) >= 5 })
	runner.set(first, "inactive", "success")
	endedWithin(t, firstEnded, "no bus")
	if bus.dialed() < 2 {
		t.Fatalf("the watch dialed systemd %d times while it was down, want again at each read", bus.dialed())
	}

	bus.mu.Lock()
	bus.down = false
	bus.mu.Unlock()
	testutil.Eventually(t, "the watch hears systemd again", func() bool {
		bus.mu.Lock()
		defer bus.mu.Unlock()
		return bus.changes != nil
	})
	time.Sleep(20 * time.Millisecond)
	settled := len(runner.seen())
	time.Sleep(100 * time.Millisecond)
	if n := len(runner.seen()) - settled; n != 0 {
		t.Fatalf("%d reads in 100 ms while the watch hears systemd, want none", n)
	}

	runner.set(second, "failed", "exit-code") // while the connection is down: never said
	bus.drop()
	endedWithin(t, secondEnded, "the connection dropped")
}
