// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	agentpb "github.com/takenking9879/zenthros-oss/operator/internal/grpc/gen"
	"github.com/takenking9879/zenthros-oss/operator/pkg/capability"
	"github.com/takenking9879/zenthros-oss/operator/pkg/capability/capwire"
	"github.com/takenking9879/zenthros-oss/operator/pkg/liveness"
	"github.com/takenking9879/zenthros-oss/operator/pkg/metrics"
	"github.com/takenking9879/zenthros-oss/operator/pkg/runtimegate"
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

// send heartbeats with the machine's declaration when it changed. The
// declaration is read before the turn to send: a VM's runs systemctl, and a
// heartbeat that frees a slot (sendCount) must not wait behind it.
func (h *HeartbeatSender) send() {
	declaration := h.readDeclaration()
	h.beat(&declaration)
}

// sendCount heartbeats the job count alone, at once: a job started or ended.
// The hub sends a machine's next job as it hears the last one's end, which
// this heartbeat follows on the stream; a read of the machine between the
// two found it full and kept that for a second (BACKEND_BUSY). It reads no
// declaration: a VM's took long enough for that read on each task of a map
// (lab, 2026-11-18). The next periodic heartbeat declares.
func (h *HeartbeatSender) sendCount() {
	h.beat(nil)
}

// declaration is what the executor declared, and its hash.
type declaration struct {
	set  capability.Set
	host capability.Host
	hash string
}

func (h *HeartbeatSender) readDeclaration() declaration {
	if h.declare == nil || h.advertised == nil {
		return declaration{}
	}
	set, host, hash := h.declare(context.Background())
	return declaration{set: set, host: host, hash: hash}
}

// beat sends one heartbeat with the count when its turn comes, and with
// declared when it is not nil and differs from what the gateway holds.
func (h *HeartbeatSender) beat(declared *declaration) {
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
	changed := declared != nil && h.attachDeclaration(msg.Heartbeat, *declared)
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

// attachDeclaration puts declared on hb when it differs from the advertised
// one, and reports whether it did.
func (h *HeartbeatSender) attachDeclaration(hb *agentpb.Heartbeat, declared declaration) bool {
	hb.CapabilityHash = declared.hash
	if declared.hash == "" {
		return false
	}
	if last, _ := h.advertised.Load().(string); last == declared.hash {
		return false
	}
	hb.Capabilities = capwire.ToProto(declared.set)
	hb.Host = capwire.HostToProto(declared.host)
	return true
}

func (h *HeartbeatSender) notice() *preemptionNotice {
	if h.preempted == nil {
		return nil
	}
	return h.preempted()
}
