// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"slices"
	"sync"
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
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
// logBatchBytes of them, whichever is first; a chunk that is that much
// alone (a read of a busy log) leaves at once, as it came. A chunk joined to
// the waiting ones gives the batch its cursor. Lines of the other stream (stderr after
// stdout), a chunk after a gap or one that says what its read left out, and
// a line that would take the batch past backend.MaxLogChunk are never joined
// to the ones waiting; what is waiting goes first, in order.
//
// A batch leaves packed (logpack.go), and the machine's hourly budget counts
// what leaves: the packed bytes. Past it a batch is dropped, and the next one
// sent says what was skipped, in bytes of log; a read never ends for it. What
// was skipped at a read's end is said by a chunk of no bytes, with the place
// after it.
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
	// hourly is the machine's budget, which every read shares; skipped is
	// what this read dropped past it and has not said yet, and after the
	// place of the last chunk dropped.
	hourly  *hourlyBudget
	skipped int64
	after   *agentpb.LogChunk

	mu sync.Mutex
	// pending are the lines waiting for the window to end; timer ends it.
	pending *agentpb.LogChunk
	timer   *time.Timer
	// sentAt is when the last message left; err the first failed send.
	sentAt time.Time
	err    error
}

func newChannelLogSender(ctx context.Context, client *logChannelClient, requestID string, window time.Duration, hourly *hourlyBudget) *channelLogSender {
	return &channelLogSender{client: client, requestID: requestID, ctx: ctx, window: window, hourly: hourly}
}

// Send sends chunk, or keeps it for the window: the chunk is the sender's
// from then on.
func (s *channelLogSender) Send(chunk *agentpb.LogChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	// -1 is a gap or a history no one can count: said too.
	alone := chunk.GetGap() != 0 || chunk.GetOmitted() != 0
	if s.pending != nil && (alone || chunk.GetStream() != s.pending.GetStream() ||
		len(s.pending.Data)+len(chunk.GetData()) > backend.MaxLogChunk) {
		if err := s.flushLocked(); err != nil {
			return err
		}
	}
	if s.pending == nil {
		// A chunk that fills a batch alone (a read of a busy log) has
		// nothing to wait for: joining it to nothing only copied it.
		quiet := time.Since(s.sentAt)
		if quiet >= s.window || len(chunk.GetData()) >= logBatchBytes {
			return s.sendLocked(chunk)
		}
		// The batch is the chunk. Its bytes may share an array with a
		// chunk that follows (the pieces of a long line): clipped, the
		// first append copies them rather than writing over that one.
		chunk.Data = slices.Clip(chunk.GetData())
		s.pending = chunk
		s.timer = time.AfterFunc(s.window-quiet, s.windowEnded)
	} else {
		s.pending.Data = append(s.pending.Data, chunk.GetData()...)
		// A piece of a long line has no place of its own: the batch keeps
		// the last one it was given.
		if cursor := chunk.GetCursor(); cursor != "" {
			s.pending.Cursor = cursor
		}
	}
	if len(s.pending.Data) >= logBatchBytes {
		return s.flushLocked()
	}
	return nil
}

// Flush sends the lines waiting, and says what the budget skipped since the
// last chunk sent. The pull calls it before it reports its error or its end:
// nothing is said after a stream's last line.
func (s *channelLogSender) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushLocked(); err != nil || s.skipped == 0 {
		return err
	}
	return s.sendLocked(s.after)
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
	packLog(chunk)
	if !s.hourly.takeAll(len(chunk.GetData())) {
		s.skip(chunk)
		return nil
	}
	if s.skipped != 0 {
		if s.skipped < 0 || chunk.GetGap() < 0 {
			chunk.Gap = backend.Uncounted
		} else {
			chunk.Gap += s.skipped
		}
		chunk.GapReason = agentpb.GapReason_GAP_BUDGET
		s.skipped, s.after = 0, nil
	}
	s.err = s.client.send(&agentpb.LogChannelFromAgent{RequestId: s.requestID, Chunk: chunk})
	s.sentAt = time.Now()
	return s.err
}

// skip drops chunk past the budget: its bytes of log and what it said never
// came are said by the next chunk sent, or a part no one can count when one
// of those was.
func (s *channelLogSender) skip(chunk *agentpb.LogChunk) {
	cursor := chunk.GetCursor()
	if cursor == "" && s.after != nil {
		cursor = s.after.Cursor
	}
	s.after = &agentpb.LogChunk{Stream: chunk.GetStream(), Cursor: cursor, TimestampUnix: chunk.GetTimestampUnix()}
	if s.skipped < 0 || chunk.GetGap() < 0 {
		s.skipped = backend.Uncounted
		return
	}
	raw := int64(len(chunk.GetData()))
	if chunk.GetEncoding() == agentpb.LogEncoding_LOG_ZSTD {
		raw = chunk.GetRawLen()
	}
	s.skipped += chunk.GetGap() + raw
}

func (s *channelLogSender) Context() context.Context {
	return s.ctx
}
