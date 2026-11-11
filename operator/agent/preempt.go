// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/zenthros/operator/pkg/cloudmeta"
)

// EventBridge Spot interruption is skipped: it needs a new AWS bus/rule/queue
// in this step. Heartbeat recovery does not require it.

const defaultNoticePoll = 5 * time.Second

// preemptionNotice is an interruption notice from a provider's metadata
// service (pkg/cloudmeta).
type preemptionNotice = cloudmeta.Notice

// noticeWatcher polls every registered provider's notice source until one
// announces an interruption, then stops. A notice can also be pushed to it
// (a container's SIGTERM): pushed holds it and wake says it arrived.
type noticeWatcher struct {
	sources []cloudmeta.NoticeSource
	// rebalances are polled with the notice until one recommends a
	// rebalance: advice, which the heartbeat carries, not a notice.
	rebalances []cloudmeta.RebalanceSource
	rebalance  atomic.Pointer[time.Time]
	interval   time.Duration
	logger     Logger
	// deliver tells the jobs (notice files, then signals) before the notice
	// is visible to heartbeats; onNotice runs after (the heartbeat itself).
	deliver  func(preemptionNotice)
	onNotice func(preemptionNotice)
	pushed   func() *preemptionNotice
	wake     <-chan struct{}
	flagged  atomic.Bool
	notice   atomic.Pointer[preemptionNotice]
}

func newNoticeWatcher(logger Logger) *noticeWatcher {
	return &noticeWatcher{
		sources:    cloudmeta.NoticeSources(),
		rebalances: cloudmeta.RebalanceSources(),
		interval:   defaultNoticePoll,
		logger:     logger.WithName("preempt"),
	}
}

func (w *noticeWatcher) Preempted() bool {
	return w.flagged.Load()
}

// Rebalance is when the provider recommended rebalancing the machine away,
// or nil.
func (w *noticeWatcher) Rebalance() *time.Time {
	return w.rebalance.Load()
}

// Notice is the notice heard, or nil.
func (w *noticeWatcher) Notice() *preemptionNotice {
	return w.notice.Load()
}

func (w *noticeWatcher) Run(ctx context.Context) {
	// A notice pushed before this stream existed is this stream's too.
	if w.raisePushed() {
		return
	}
	var tick <-chan time.Time
	if w.interval > 0 && len(w.sources) > 0 {
		w.poll(ctx)
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		tick = ticker.C
	} else if w.wake == nil {
		return
	}
	for {
		if w.flagged.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
			w.raisePushed()
		case <-tick:
			w.poll(ctx)
		}
	}
}

// raisePushed raises the pushed notice, if there is one.
func (w *noticeWatcher) raisePushed() bool {
	if w.pushed == nil {
		return false
	}
	if notice := w.pushed(); notice != nil {
		w.raise(*notice)
		return true
	}
	return false
}

func (w *noticeWatcher) poll(ctx context.Context) {
	if notice := cloudmeta.FirstNotice(ctx, w.sources); notice.Preempted {
		w.raise(notice)
		return
	}
	if w.rebalance.Load() != nil || len(w.rebalances) == 0 {
		return
	}
	if at, ok := cloudmeta.FirstRebalance(ctx, w.rebalances); ok {
		w.logger.Info("rebalance recommendation", "noticeTime", at)
		w.rebalance.Store(&at)
	}
}

func (w *noticeWatcher) raise(notice preemptionNotice) {
	if !w.flagged.CompareAndSwap(false, true) {
		return
	}
	w.logger.Info("preemption notice", "source", notice.Source, "action", notice.Action)
	// The jobs hear first. Only then may a heartbeat carry the notice: one
	// that did earlier (the stream's first, or a tick) would end the lease
	// before the job got its signal.
	if w.deliver != nil {
		w.deliver(notice)
	}
	w.notice.Store(&notice)
	if w.onNotice != nil {
		w.onNotice(notice)
	}
}
