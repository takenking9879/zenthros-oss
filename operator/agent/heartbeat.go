// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/capability"
	"github.com/zenthros/operator/pkg/capability/capwire"
	"github.com/zenthros/operator/pkg/liveness"
	"github.com/zenthros/operator/pkg/metrics"
	"github.com/zenthros/operator/pkg/runtimegate"
)

func guestRuntimeReady() bool { return runtimegate.Ready() }

// HeartbeatSender periodically sends a Heartbeat over the one control stream to
// prove agent liveness and report capacity. Job vs idle silence is applied by
// the controller (jobGrace vs idleGrace); miss count is ceil(grace/interval).
type HeartbeatSender struct {
	// mu sends one heartbeat at a time, each with the count when its turn
	// comes: the gateway keeps the last one, and a tick that read a job still
	// running must not land after the heartbeat of that job's end.
	mu            sync.Mutex
	tracker       *jobTracker
	sender        *streamSender
	backendID     string
	maxJobs       int
	ageRecipients []string
	logger        Logger
	interval      time.Duration
	preempted     func() *preemptionNotice
	// rebalance is when the provider recommended rebalancing the machine
	// away, or nil.
	rebalance func() *time.Time
	// declare is the executor's current declaration; advertised is the hash
	// the gateway holds. A heartbeat carries the declaration only when the
	// two differ.
	declare    func(context.Context) (capability.Set, capability.Host, string)
	advertised *atomic.Value
	// sentAt is when the heartbeat awaiting its ack was sent (UnixNano), 0
	// when none is out; observeAck records the send-to-ack time.
	sentAt     atomic.Int64
	observeAck func(time.Duration)
}

func newHeartbeatSender(tracker *jobTracker, sender *streamSender, backendID string, maxJobs int, ageRecipients []string, logger Logger) *HeartbeatSender {
	return &HeartbeatSender{
		tracker:       tracker,
		sender:        sender,
		backendID:     backendID,
		maxJobs:       maxJobs,
		ageRecipients: append([]string(nil), ageRecipients...),
		logger:        logger.WithName("heartbeat"),
		interval:      liveness.DefaultHeartbeatInterval,
		observeAck:    metrics.ObserveAgentHeartbeatAck,
	}
}

// acked times the heartbeat out, if one is; a second ack is not timed.
func (h *HeartbeatSender) acked() {
	if sent := h.sentAt.Swap(0); sent != 0 {
		h.observeAck(time.Since(time.Unix(0, sent)))
	}
}

// Run sends one heartbeat immediately so Gateway can promote RuntimeReady
// without waiting for the first ticker (which is a full interval late), then
// continues on the liveness interval. If the guest runtime is still installing,
// a short poll fires one extra heartbeat the moment runtime-ready appears.
func (h *HeartbeatSender) Run(ctx context.Context) {
	h.send()
	if !guestRuntimeReady() {
		go h.notifyWhenRuntimeReady(ctx)
	}
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.send()
		}
	}
}

func (h *HeartbeatSender) notifyWhenRuntimeReady(ctx context.Context) {
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if guestRuntimeReady() {
				h.send()
				return
			}
		}
	}
}

func (h *HeartbeatSender) send() {
	h.mu.Lock()
	defer h.mu.Unlock()
	maxJobs := int32(h.maxJobs)
	if !guestRuntimeReady() {
		// Sentinel: Agent is connected, guest runtime is still installing.
		// Capacity must not become Ready. 0 remains "unlimited".
		maxJobs = -1
	}
	msg := &agentpb.RegisterRequest{
		Heartbeat: &agentpb.Heartbeat{
			BackendId:      h.backendID,
			TimestampUnix:  time.Now().Unix(),
			RunningJobs:    int32(h.tracker.Count()),
			MaxJobs:        maxJobs,
			AgeRecipients:  h.ageRecipients,
			RuntimeFailure: runtimegate.Failure(),
		},
	}
	if notice := h.notice(); notice != nil {
		msg.Heartbeat.Preempted = true
		msg.Heartbeat.PreemptSource = notice.Source
		msg.Heartbeat.PreemptAction = notice.Action
		if !notice.Deadline.IsZero() {
			msg.Heartbeat.PreemptDeadlineUnix = notice.Deadline.Unix()
		}
	}
	if h.rebalance != nil {
		if at := h.rebalance(); at != nil {
			msg.Heartbeat.RebalanceRecommendedUnix = at.Unix()
		}
	}
	changed := h.attachDeclaration(msg.Heartbeat)
	// Stamped before Send: the ack can be read before Send returns.
	h.sentAt.Store(time.Now().UnixNano())
	if err := h.sender.Send(msg); err != nil {
		h.sentAt.Store(0)
		h.logger.Error(err, "failed to send heartbeat")
		return
	}
	if changed {
		h.advertised.Store(msg.Heartbeat.CapabilityHash)
	}
	metrics.RecordAgentHeartbeatSent()
}

// attachDeclaration puts the executor's declaration on hb when it differs
// from the advertised one, and reports whether it did.
func (h *HeartbeatSender) attachDeclaration(hb *agentpb.Heartbeat) bool {
	if h.declare == nil || h.advertised == nil {
		return false
	}
	set, host, hash := h.declare(context.Background())
	hb.CapabilityHash = hash
	if hash == "" {
		return false
	}
	if last, _ := h.advertised.Load().(string); last == hash {
		return false
	}
	hb.Capabilities = capwire.ToProto(set)
	hb.Host = capwire.HostToProto(host)
	return true
}

func (h *HeartbeatSender) notice() *preemptionNotice {
	if h.preempted == nil {
		return nil
	}
	return h.preempted()
}
