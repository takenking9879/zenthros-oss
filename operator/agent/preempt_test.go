// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/infrastructure/provider/traits"
)

// scriptedNotices is a provider whose notice turns on after a number of polls.
type scriptedNotices struct {
	name   string
	after  int32
	polls  atomic.Int32
	notice cloudmeta.Notice
}

func (s *scriptedNotices) Name() string { return s.name }
func (s *scriptedNotices) Notice(context.Context) cloudmeta.Notice {
	if s.polls.Add(1) > s.after {
		return s.notice
	}
	return cloudmeta.Notice{Source: s.notice.Source}
}

// The watcher polls every registered provider and raises the first notice
// once, then stops polling.
func TestNoticeWatcherRaisesOnceFromAnyProvider(t *testing.T) {
	quiet := &scriptedNotices{name: "quiet", after: 1 << 30, notice: cloudmeta.Notice{Preempted: true, Source: "quiet"}}
	loud := &scriptedNotices{name: "loud", after: 2, notice: cloudmeta.Notice{Preempted: true, Source: noticeGCPMetadata, Action: "preempted"}}
	var raised atomic.Int32
	watcher := newNoticeWatcher(logr.Discard())
	watcher.sources = []cloudmeta.NoticeSource{quiet, loud}
	watcher.interval = 5 * time.Millisecond
	watcher.onNotice = func(preemptionNotice) { raised.Add(1) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	watcher.Run(ctx)

	if !watcher.Preempted() || raised.Load() != 1 || watcher.Notice().Source != noticeGCPMetadata {
		t.Fatalf("preempted=%v raised=%d notice=%+v", watcher.Preempted(), raised.Load(), watcher.Notice())
	}
	polled := loud.polls.Load()
	time.Sleep(30 * time.Millisecond)
	if loud.polls.Load() != polled {
		t.Fatal("watcher kept polling after the notice")
	}
}

func TestNoticeWatcherUsesRegisteredProviders(t *testing.T) {
	var names []string
	for _, source := range newNoticeWatcher(logr.Discard()).sources {
		names = append(names, source.Name())
	}
	if len(names) != 2 || names[0] != "aws" || names[1] != "gcp" {
		t.Fatalf("notice sources = %v", names)
	}
}

func TestNoticeWatcherDisabledDoesNotPoll(t *testing.T) {
	watcher := newNoticeWatcher(logr.Discard())
	watcher.interval = 0
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	watcher.Run(ctx)
	if watcher.Preempted() {
		t.Fatal("disabled watcher must not flag preempted")
	}
}

// scriptedRebalance recommends a rebalance after a number of polls.
type scriptedRebalance struct {
	after int32
	polls atomic.Int32
	at    time.Time
}

func (s *scriptedRebalance) Name() string { return "scripted" }
func (s *scriptedRebalance) Rebalance(context.Context) (time.Time, bool) {
	if s.polls.Add(1) > s.after {
		return s.at, true
	}
	return time.Time{}, false
}

// A rebalance recommendation is advice: the watcher records it, stops asking
// for it, and keeps polling for the notice.
func TestNoticeWatcherRecordsRebalanceAndKeepsPolling(t *testing.T) {
	notices := &scriptedNotices{name: "notices", after: 6, notice: cloudmeta.Notice{Preempted: true, Source: noticeAWSIMDS, Action: "terminate"}}
	rebalance := &scriptedRebalance{after: 1, at: time.Unix(1790000000, 0)}
	watcher := newNoticeWatcher(logr.Discard())
	watcher.sources = []cloudmeta.NoticeSource{notices}
	watcher.rebalances = []cloudmeta.RebalanceSource{rebalance}
	watcher.interval = 5 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	watcher.Run(ctx)
	if at := watcher.Rebalance(); at == nil || !at.Equal(rebalance.at) {
		t.Fatalf("rebalance = %v, want %v", at, rebalance.at)
	}
	if !watcher.Preempted() {
		t.Fatal("the recommendation stopped the notice polling")
	}
	if polls := rebalance.polls.Load(); polls != 2 {
		t.Fatalf("rebalance polled %d times, want 2: none after the recommendation", polls)
	}
}

const (
	noticeAWSIMDS     = traits.NoticeAWSIMDS
	noticeGCPMetadata = traits.NoticeGCPMetadata
)
