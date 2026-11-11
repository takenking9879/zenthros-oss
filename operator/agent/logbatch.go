// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"sync"
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// Log lines travel to the Gateway in batches (scale lab, 2026-10-11).
//
// Each line was its own message: with 900 readers of two lines a second the
// Gateways spent all their CPU on one read, one write and one TLS record per
// line. A reader gets the same bytes if the lines of a burst arrive in one
// chunk: a chunk's data is raw log bytes, newlines included, and every
// reader writes it as it comes.
//
// A line that follows a quiet moment is sent at once, so the first line of a
// log and a job that prints now and then are not delayed. Lines that come
// within the window of the last message wait for the window to end, or for
// logBatchBytes of them, whichever is first. Lines of the other stream
// (stderr after stdout) and the truncation notice are never joined to the
// ones waiting; what is waiting goes first, in order.
//
// The window is the installation's: the Gateway names it to every agent it
// accepts (RegisterResponse.log_batch_window_ms). An agent given its own
// (Config.LogBatchWindow) keeps it: the scale lab runs the same readers with
// several, to see what each costs a reader in delay. With 900 readers of jobs
// that print a line every half second, a window of 0.1 s saved the Gateways
// nothing and one of 1 s halved their CPU (2026-10-12), which is why a
// second is the default: a window only saves when it is longer than the gap
// between a job's lines.
const (
	defaultLogBatchWindow = time.Second
	logBatchBytes         = 16 << 10
)

// channelLogSender sends one pull's chunks over the machine's LogChannel.
type channelLogSender struct {
	client    *logChannelClient
	requestID string
	ctx       context.Context
	// window is the agent's; zero sends every line at once.
	window time.Duration

	mu sync.Mutex
	// pending are the lines waiting for the window to end; timer ends it.
	pending *agentpb.LogChunk
	timer   *time.Timer
	// sentAt is when the last message left; err the first failed send.
	sentAt time.Time
	err    error
}

func newChannelLogSender(ctx context.Context, client *logChannelClient, requestID string, window time.Duration) *channelLogSender {
	return &channelLogSender{client: client, requestID: requestID, ctx: ctx, window: window}
}

func (s *channelLogSender) Send(chunk *agentpb.LogChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.pending != nil && (chunk.GetTruncated() || chunk.GetStream() != s.pending.GetStream()) {
		if err := s.flushLocked(); err != nil {
			return err
		}
	}
	if chunk.GetTruncated() {
		return s.sendLocked(chunk)
	}
	if s.pending == nil {
		quiet := time.Since(s.sentAt)
		if quiet >= s.window {
			return s.sendLocked(chunk)
		}
		s.pending = &agentpb.LogChunk{
			Stream:        chunk.GetStream(),
			Data:          append(make([]byte, 0, 4*len(chunk.GetData())), chunk.GetData()...),
			TimestampUnix: chunk.GetTimestampUnix(),
		}
		s.timer = time.AfterFunc(s.window-quiet, s.windowEnded)
	} else {
		s.pending.Data = append(s.pending.Data, chunk.GetData()...)
	}
	if len(s.pending.Data) >= logBatchBytes {
		return s.flushLocked()
	}
	return nil
}

// Flush sends the lines waiting. The pull calls it before it reports its
// error or its end: nothing is said after a stream's last line.
func (s *channelLogSender) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *channelLogSender) windowEnded() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.flushLocked()
}

func (s *channelLogSender) flushLocked() error {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	if s.pending == nil {
		return s.err
	}
	chunk := s.pending
	s.pending = nil
	return s.sendLocked(chunk)
}

func (s *channelLogSender) sendLocked(chunk *agentpb.LogChunk) error {
	if s.err != nil {
		return s.err
	}
	s.err = s.client.send(&agentpb.LogChannelFromAgent{RequestId: s.requestID, Chunk: chunk})
	s.sentAt = time.Now()
	return s.err
}

func (s *channelLogSender) Context() context.Context {
	return s.ctx
}
