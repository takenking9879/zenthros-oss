// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/liveness"
)

// orderLog records what happened, in order, across the executor and the stream.
type orderLog struct {
	mu     sync.Mutex
	events []string
}

func (l *orderLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *orderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type orderExecutor struct {
	mismatchExecutor
	dir string
	log *orderLog
}

func (e orderExecutor) SignalPreempt(_ context.Context, handle string) (bool, error) {
	if _, err := os.Stat(preemptFile(e.dir, "uid-1")); err == nil {
		e.log.add("signal-after-file")
	} else {
		e.log.add("signal-before-file")
	}
	return true, nil
}

type orderStream struct {
	log *orderLog
}

func (s orderStream) Send(req *agentpb.RegisterRequest) error {
	if hb := req.GetHeartbeat(); hb != nil && hb.GetPreempted() {
		s.log.add("preempted-heartbeat:" + hb.GetPreemptSource())
	}
	return nil
}

func (orderStream) Recv() (*agentpb.RegisterResponse, error) {
	select {}
}

// A container's SIGTERM is its interruption notice. Pushed to the agent, it
// goes the way a metadata notice goes: the job's notice file, then its
// signal, then the heartbeat that ends its lease. A container asks no
// metadata service for one.
func TestPushedNoticeKeepsFileSignalHeartbeatOrder(t *testing.T) {
	for _, before := range []bool{false, true} {
		name := "during stream"
		if before {
			name = "before stream"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			log := &orderLog{}
			a := NewWithExecutor(Config{BackendID: "c-1", BackendType: "container", PreemptDir: dir, SkipMetadataNotices: true},
				orderExecutor{dir: dir, log: log}, logr.Discard())
			a.tracker.Add("uid-1", "uid-1")
			notice := cloudmeta.Notice{Source: liveness.NoticeContainerSIGTERM, Action: "terminate", Deadline: time.Now().Add(30 * time.Second)}
			if before {
				a.Interrupt(notice)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = a.ServeStream(ctx, orderStream{log: log}) }()
			if !before {
				time.Sleep(50 * time.Millisecond)
				a.Interrupt(notice)
			}
			deadline := time.Now().Add(3 * time.Second)
			for len(log.snapshot()) < 2 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			got := log.snapshot()
			want := []string{"signal-after-file", "preempted-heartbeat:" + liveness.NoticeContainerSIGTERM}
			if len(got) < 2 || got[0] != want[0] || got[1] != want[1] {
				t.Fatalf("events = %v, want %v", got, want)
			}
		})
	}
}

func TestOnlyTheFirstPushedNoticeCounts(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "c-1", BackendType: "container"}, mismatchExecutor{}, logr.Discard())
	a.Interrupt(cloudmeta.Notice{Source: "first"})
	a.Interrupt(cloudmeta.Notice{Source: "second"})
	if got := a.interrupted.Load(); got == nil || got.Source != "first" || !got.Preempted {
		t.Fatalf("interrupted = %+v", got)
	}
}
