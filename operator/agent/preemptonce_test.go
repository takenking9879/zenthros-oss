// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/cloudmeta"
)

// signalCounter counts the preempt signals each handle gets.
type signalCounter struct {
	mismatchExecutor
	mu    *sync.Mutex
	count map[string]int
}

func (s signalCounter) SignalPreempt(_ context.Context, handle string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count[handle]++
	return true, nil
}

// A pushed notice (a container's SIGTERM) is raised again by every control
// stream the agent opens after it. Each job is signalled once: a second
// signal can kill the checkpoint the first one started.
// A job that starts after the notice is told too.
func TestPushedNoticeSignalsOnceAcrossReconnect(t *testing.T) {
	rec := signalCounter{mu: &sync.Mutex{}, count: map[string]int{}}
	a := NewWithExecutor(Config{BackendID: "c-a", BackendType: "container", SkipMetadataNotices: true}, rec, logr.Discard())
	a.tracker.Add("uid-1", "unit-1")
	a.Interrupt(cloudmeta.Notice{Source: "sigterm", Action: "terminate"})

	stream := func() {
		w := newNoticeWatcher(a.logger)
		w.sources = nil
		w.pushed = a.interrupted.Load
		w.wake = a.interruptCh
		w.deliver = func(notice preemptionNotice) { a.deliverNotice(context.Background(), notice) }
		w.Run(context.Background())
	}
	stream()
	a.tracker.Add("uid-2", "unit-2")
	stream() // the reconnect

	if rec.count["unit-1"] != 1 || rec.count["unit-2"] != 1 {
		t.Fatalf("signals = %v, want one per job", rec.count)
	}
}

// failingOnce fails the first preempt signal of each handle.
type failingOnce struct {
	signalCounter
}

func (s failingOnce) SignalPreempt(ctx context.Context, handle string) (bool, error) {
	s.mu.Lock()
	first := s.count[handle] == 0
	s.mu.Unlock()
	s.signalCounter.SignalPreempt(ctx, handle)
	if first {
		return false, errors.New("unit busy")
	}
	return true, nil
}

// A signal that failed was recorded as told and never tried again: the job
// ran on to the machine's end without its notice (05#7). The next raise
// tries it again; one that went through is still not repeated.
func TestAFailedNoticeSignalIsTriedAgain(t *testing.T) {
	rec := failingOnce{signalCounter{mu: &sync.Mutex{}, count: map[string]int{}}}
	a := NewWithExecutor(Config{BackendID: "c-a", BackendType: "container", SkipMetadataNotices: true}, rec, logr.Discard())
	a.tracker.Add("uid-1", "unit-1")
	notice := preemptionNotice{Source: "sigterm", Action: "terminate"}
	for range 3 {
		a.deliverNotice(context.Background(), notice)
	}
	if rec.count["unit-1"] != 2 {
		t.Fatalf("%d signal attempts, want the failed one and one more", rec.count["unit-1"])
	}
}
