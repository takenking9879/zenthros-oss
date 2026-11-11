// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/internal/testutil"
)

// showRunner answers `systemctl show u1 u2 ...` as systemd does: one block a
// unit, in the order named, a blank line between them.
type showRunner struct {
	mu       sync.Mutex
	active   map[string]string // unit -> ActiveState; missing is not-found
	result   map[string]string
	broken   string // a unit whose name fails any command that names it
	commands []string
}

func (r *showRunner) Run(_ context.Context, cmd string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, cmd)
	fields := strings.Fields(cmd)
	if len(fields) < 2 || fields[0] != "systemctl" || fields[1] != "show" {
		return "", fmt.Errorf("unexpected command %q", cmd)
	}
	var units []string
	for _, f := range fields[2:] {
		if !strings.HasPrefix(f, "--") {
			units = append(units, f)
		}
	}
	if r.broken != "" && strings.Contains(cmd, r.broken) {
		return "", errors.New("Failed to get properties: Invalid unit name")
	}
	var blocks []string
	for _, unit := range units {
		active, ok := r.active[unit]
		if !ok {
			blocks = append(blocks, "Id="+unit+"\nLoadState=not-found\nActiveState=inactive\nResult=success\n")
			continue
		}
		blocks = append(blocks, fmt.Sprintf("Id=%s\nLoadState=loaded\nActiveState=%s\nResult=%s\n", unit, active, r.result[unit]))
	}
	return strings.Join(blocks, "\n"), nil
}

func (r *showRunner) set(unit, active, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active[unit], r.result[unit] = active, result
}

func (r *showRunner) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.commands...)
}

func unitNames(n int) []string {
	var names []string
	for i := 0; i < n; i++ {
		names = append(names, fmt.Sprintf("zenthros-j%03d.service", i))
	}
	return names
}

// A sweep over many jobs reads them in one `systemctl show`, each answer
// kept with its unit; one command per job was a bash and a systemctl, about
// 15 ms of CPU, every sweep (455 mCPU at 30
// jobs and 10 follows).
func TestStatusAllReadsEveryUnitInOneCommand(t *testing.T) {
	runner := &showRunner{active: map[string]string{}, result: map[string]string{}}
	names := unitNames(30)
	for i, name := range names {
		if i%3 == 0 {
			runner.set(name, "failed", "exit-code")
		} else {
			runner.set(name, "active", "success")
		}
	}
	missing := "zenthros-gone.service"
	names = append(names, missing)
	m := NewManager(Config{Scope: ScopeSystem}, runner, logr.Discard())

	all, err := m.StatusAll(context.Background(), names)
	if err != nil {
		t.Fatal(err)
	}
	if got := runner.seen(); len(got) != 1 {
		t.Fatalf("%d commands for %d units, want 1", len(got), len(names))
	}
	for i, props := range all[:30] {
		want := "active"
		if i%3 == 0 {
			want = "failed"
		}
		if props["Id"] != names[i] || props["ActiveState"] != want {
			t.Errorf("answer %d = %v, want %s %s", i, props, names[i], want)
		}
	}
	if !UnitMissing(all[30]) {
		t.Errorf("a missing unit reads %v", all[30])
	}

	// More than a batch is a command a batch, still in order.
	many := unitNames(showBatch*2 + 5)
	runner.commands = nil
	all, err = m.StatusAll(context.Background(), many)
	if err != nil || len(all) != len(many) || all[len(many)-1]["Id"] != many[len(many)-1] {
		t.Fatalf("%d units: %d answers, %v", len(many), len(all), err)
	}
	if got := len(runner.seen()); got != 3 {
		t.Errorf("%d units read in %d commands, want 3", len(many), got)
	}
}

// An answer that is not for the unit asked is refused, not attributed to it.
func TestShowAllRefusesAnswersOutOfOrder(t *testing.T) {
	swapped := RunnerFunc(func(context.Context, string) (string, error) {
		return "Id=b.service\nActiveState=active\n\nId=a.service\nActiveState=failed\n", nil
	})
	if _, err := showAll(context.Background(), swapped, Config{}, []string{"a.service", "b.service"}, ShowProperties); err == nil {
		t.Fatal("answers out of order were attributed")
	}
	short := RunnerFunc(func(context.Context, string) (string, error) {
		return "Id=a.service\nActiveState=active\n", nil
	})
	if _, err := showAll(context.Background(), short, Config{}, []string{"a.service", "b.service"}, ShowProperties); err == nil {
		t.Fatal("a missing answer was not noticed")
	}
}

// RunnerFunc adapts a function to Runner, for tests.
type RunnerFunc func(ctx context.Context, cmd string) (string, error)

func (f RunnerFunc) Run(ctx context.Context, cmd string) (string, error) { return f(ctx, cmd) }

// Every follow stream shares one sweep: a tick is one command naming every
// followed unit, not a command per stream. A unit that ends closes only its
// own streams; a stream whose reader left is dropped, and with none left the
// sweep stops asking.
func TestFollowStreamsShareOneSweep(t *testing.T) {
	runner := &showRunner{active: map[string]string{}, result: map[string]string{}}
	names := unitNames(10)
	for _, name := range names {
		runner.set(name, "active", "success")
	}
	watch := NewTerminalWatch(Config{Scope: ScopeSystem}, runner)
	watch.interval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	ended := map[string][]<-chan struct{}{}
	for _, name := range names {
		for range 2 { // two readers follow each unit
			ended[name] = append(ended[name], watch.Ended(ctx, name))
		}
	}
	testutil.Eventually(t, "len(runner.seen()) >= 5", func() bool { return len(runner.seen()) >= 5 })
	for _, cmd := range runner.seen() {
		for _, name := range names {
			if !strings.Contains(cmd, name) {
				t.Fatalf("sweep %q left out %s", cmd, name)
			}
		}
	}

	runner.set(names[0], "inactive", "success")
	runner.set(names[1], "failed", "exit-code")
	for _, name := range names[:2] {
		for _, ch := range ended[name] {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s ended but its stream was not told", name)
			}
		}
	}
	for _, name := range names[2:] {
		for _, ch := range ended[name] {
			select {
			case <-ch:
				t.Fatalf("%s is running but its stream was told it ended", name)
			default:
			}
		}
	}
	// Later sweeps ask only for the units still followed.
	before := len(runner.seen())
	testutil.Eventually(t, "len(runner.seen()) > before+1", func() bool { return len(runner.seen()) > before+1 })
	if last := runner.seen()[len(runner.seen())-1]; strings.Contains(last, names[0]) || !strings.Contains(last, names[2]) {
		t.Errorf("sweep after two ends = %q", last)
	}

	cancel()
	testutil.Eventually(t, "the sweep stops", func() bool {
		watch.mu.Lock()
		defer watch.mu.Unlock()
		return !watch.running
	})
	stopped := len(runner.seen())
	time.Sleep(30 * time.Millisecond)
	if got := len(runner.seen()); got != stopped {
		t.Errorf("the sweep kept asking with no stream: %d commands after it stopped", got-stopped)
	}

	// A new stream starts the sweep again.
	runner.set(names[5], "inactive", "success")
	select {
	case <-watch.Ended(context.Background(), names[5]):
	case <-time.After(5 * time.Second):
		t.Fatal("a stream after the sweep stopped was never told its unit ended")
	}
}

// A batch one bad unit name fails is read unit by unit: the other streams
// still learn their units ended, and a failed read is never an end.
func TestOneBadUnitDoesNotHoldEveryStreamOpen(t *testing.T) {
	runner := &showRunner{active: map[string]string{}, result: map[string]string{}, broken: "zenthros-bad.service"}
	runner.set("zenthros-ok.service", "inactive", "success")
	watch := NewTerminalWatch(Config{Scope: ScopeSystem}, runner)
	watch.interval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bad := watch.Ended(ctx, "zenthros-bad.service")
	select {
	case <-watch.Ended(ctx, "zenthros-ok.service"):
	case <-time.After(5 * time.Second):
		t.Fatal("a bad unit name held the other stream open")
	}
	select {
	case <-bad:
		t.Fatal("a unit systemd could not read was taken as ended")
	default:
	}
}
