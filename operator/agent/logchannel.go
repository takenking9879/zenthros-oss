// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/zenthros/operator/internal/grpc/errorclass"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// serveLogChannel opens a second ClientConn to ControllerAddr and runs
// LogChannel until ctx is cancelled. A dead log follow does not drop jobs:
// this loop is independent of Register. Do not reuse the Register ClientConn.
//
// Nothing is open while nobody reads the machine's logs: the loop waits for
// the gateway's ask on the control stream, opens the channel, and waits
// again once it ends. The gateway closes a channel nobody has read for a
// while, and asks again while a reader waits, so a channel that failed to
// open is retried at the gateway's pace, not the agent's.
func (a *Agent) serveLogChannel(ctx context.Context, tlsCfg *tls.Config) {
	retry := logChannelRetry{max: a.cfg.reconnectBackoff()}
	// again: the ask still stands, the channel only landed on a replica
	// that does not hold the session.
	again := false
	for {
		if ctx.Err() != nil {
			return
		}
		epoch := a.sessionEpoch.Load()
		if epoch == 0 || a.logAcceptedEpoch.Load() != epoch {
			// Not registered, or the gateway has not accepted this session
			// yet: an ask can only be the previous one's.
			select {
			case <-ctx.Done():
				return
			case <-a.logSessionChanged:
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		if !again {
			if !a.awaitLogChannelAsk(ctx) {
				continue
			}
			retry = logChannelRetry{max: a.cfg.reconnectBackoff()}
		}
		opened := time.Now()
		open := a.serveLogChannelOnce
		if a.logChannelOnce != nil {
			open = a.logChannelOnce
		}
		err := open(ctx, tlsCfg)
		misrouted := errorclass.IsNotThisReplica(err)
		backoff := retry.next(time.Since(opened), misrouted)
		again = misrouted && !retry.exhausted()
		if err != nil && ctx.Err() == nil {
			a.logger.Error(err, "log channel ended", "backoff", backoff)
		}
		if !again {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// logSessionAccepted records the session the gateway just accepted: its asks
// for the LogChannel count from now. Asks belong to a session: one left
// from the previous is dropped.
func (a *Agent) logSessionAccepted(epoch int64) {
	select {
	case <-a.logAsked:
	default:
	}
	a.logAcceptedEpoch.Store(epoch)
	select {
	case a.logSessionChanged <- struct{}{}:
	default:
	}
}

// askLogChannel is the gateway's ask for the LogChannel. An ask while one is
// pending adds nothing.
func (a *Agent) askLogChannel() {
	select {
	case a.logAsked <- struct{}{}:
	default:
	}
}

// awaitLogChannelAsk waits for the gateway's ask. False when ctx ended or
// the session changed first: the caller looks at the new session's mode.
func (a *Agent) awaitLogChannelAsk(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-a.logSessionChanged:
		return false
	case <-a.logAsked:
		return true
	}
}

// logChannelRetry spaces LogChannel reconnects. The first come quickly: the
// channel is a second TCP connection, and behind a Service with several
// Gateway replicas it can land on one that does not hold the agent's
// Register session and is refused at once. Until it lands on the right one,
// nobody can read the agent's logs, and a container starts jobs within a
// second of joining. Such a miss is a fresh draw on the next connection, so
// it is retried at the first interval without backing off, up to
// logChannelMisses in a row: over a slow link (a userspace tailnet node)
// each attempt costs a handshake, and a doubling backoff left the channel
// closed for seconds after a job started. Other failures back off to max; a
// channel that stayed open starts the schedule over.
type logChannelRetry struct {
	max    time.Duration
	wait   time.Duration
	misses int
}

const (
	logChannelFirstRetry = 100 * time.Millisecond
	// logChannelStable is how long a channel stays open to count as healthy.
	logChannelStable = 10 * time.Second
	// logChannelMisses bounds the quick retries of wrong-replica refusals:
	// with two replicas, 20 misses in a row is a one-in-a-million draw, and
	// past it something else is wrong.
	logChannelMisses = 20
)

// exhausted: the quick retries of wrong-replica refusals ran out.
func (r *logChannelRetry) exhausted() bool { return r.misses >= logChannelMisses }

func (r *logChannelRetry) next(lasted time.Duration, notThisReplica bool) time.Duration {
	if lasted >= logChannelStable {
		r.wait, r.misses = 0, 0
	}
	if notThisReplica && r.misses < logChannelMisses {
		r.misses++
		return min(logChannelFirstRetry, r.max)
	}
	if r.wait == 0 {
		r.wait = logChannelFirstRetry
	} else {
		r.wait *= 2
	}
	r.wait = min(r.wait, r.max)
	return r.wait
}

func (a *Agent) serveLogChannelOnce(ctx context.Context, tlsCfg *tls.Config) error {
	epoch := a.sessionEpoch.Load()
	if epoch == 0 {
		return nil
	}
	// Second TCP to the same Gateway :9443. Sharing the Register ClientConn
	// would put logs on the same HTTP/2 connection and HOL-block Heartbeat.
	conn, err := grpc.NewClient(a.gatewayTarget(), a.gatewayDialOpts(tlsCfg)...)
	if err != nil {
		return fmt.Errorf("dial log channel %s: %w", a.cfg.ControllerAddr, err)
	}
	defer conn.Close()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go a.cancelLogChannelOnEpochChange(streamCtx, cancel, epoch)

	stream, err := agentpb.NewAgentClient(conn).LogChannel(streamCtx)
	if err != nil {
		return fmt.Errorf("open log channel: %w", err)
	}
	if err := stream.Send(&agentpb.LogChannelFromAgent{
		BackendId:    a.cfg.BackendID,
		SessionEpoch: epoch,
	}); err != nil {
		return fmt.Errorf("send log channel hello: %w", err)
	}
	// The channel answers every ask up to here: one that arrived while it
	// was opening must not open another once this one closes.
	select {
	case <-a.logAsked:
	default:
	}
	a.logger.Info("log channel open", "backendID", a.cfg.BackendID, "sessionEpoch", epoch)
	return a.serveLogPulls(streamCtx, stream)
}

func (a *Agent) cancelLogChannelOnEpochChange(ctx context.Context, cancel context.CancelFunc, epoch int64) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if a.sessionEpoch.Load() != epoch {
				cancel()
				return
			}
		}
	}
}

func (a *Agent) serveLogPulls(ctx context.Context, stream agentpb.Agent_LogChannelClient) error {
	logs := NewLogServer(a.executor, a.tracker, a.logger)
	logs.maxLogBytes = a.cfg.maxLogBytes()
	logs.leftoverHint = leftoverHintFor(a.cfg.BackendType)
	logs.hourly = a.hourlyLogs
	client := &logChannelClient{stream: stream}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("recv log channel: %w", err)
		}
		if msg.GetCancel() {
			client.cancelPull(msg.GetRequestId())
			continue
		}
		pull := msg.GetPull()
		if pull == nil || msg.GetRequestId() == "" {
			continue
		}
		pullCtx, cancel := context.WithCancel(ctx)
		client.storePull(msg.GetRequestId(), cancel)
		go a.handleLogPull(pullCtx, client, logs, msg.GetRequestId(), pull)
	}
}

func (a *Agent) handleLogPull(ctx context.Context, client *logChannelClient, logs *LogServer, requestID string, req *agentpb.StreamLogsRequest) {
	defer client.cancelPull(requestID)
	sender := newChannelLogSender(ctx, client, requestID, a.logBatchWindow())
	err := logs.streamLogs(req, sender)
	// The lines waiting leave before the pull says how it ended.
	if flushErr := sender.Flush(); err == nil {
		err = flushErr
	}
	if err != nil && ctx.Err() == nil {
		_ = client.send(&agentpb.LogChannelFromAgent{RequestId: requestID, Error: err.Error()})
	}
	_ = client.send(&agentpb.LogChannelFromAgent{RequestId: requestID, Eof: true})
}

type logChannelClient struct {
	stream agentpb.Agent_LogChannelClient
	sendMu sync.Mutex
	pulls  sync.Map
}

func (c *logChannelClient) send(msg *agentpb.LogChannelFromAgent) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream.Send(msg)
}

func (c *logChannelClient) storePull(requestID string, cancel context.CancelFunc) {
	c.pulls.Store(requestID, cancel)
}

func (c *logChannelClient) cancelPull(requestID string) {
	if v, ok := c.pulls.LoadAndDelete(requestID); ok {
		v.(context.CancelFunc)()
	}
}
