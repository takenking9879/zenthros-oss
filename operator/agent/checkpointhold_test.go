// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/reportspool"
)

// controllerStream answers each checkpoint commit sent on it with answer's
// ack, the way the gateway does: on the stream, after the send.
type controllerStream struct {
	recordingStream
	waiter *checkpointWaiter
	mu     sync.Mutex
	answer func(*agentpb.CheckpointCommit) *agentpb.CheckpointAck
}

func (s *controllerStream) Send(msg *agentpb.RegisterRequest) error {
	_ = s.recordingStream.Send(msg)
	s.mu.Lock()
	answer := s.answer
	s.mu.Unlock()
	commit := msg.GetCheckpointCommit()
	ack := answer(commit)
	ack.JobId, ack.Phase, ack.Seq = commit.GetJobId(), commit.GetPhase(), commit.GetSeq()
	go s.waiter.HandleAck(ack)
	return nil
}

func (s *controllerStream) controller(answer func(*agentpb.CheckpointCommit) *agentpb.CheckpointAck) {
	s.mu.Lock()
	s.answer = answer
	s.mu.Unlock()
}

// sentCommits is each commit sent, as job/phase/seq.
func (s *controllerStream) sentCommits() []string {
	s.recordingStream.mu.Lock()
	defer s.recordingStream.mu.Unlock()
	var out []string
	for _, msg := range s.sent {
		if c := msg.GetCheckpointCommit(); c != nil {
			out = append(out, checkpointKey(c.GetJobId(), c.GetPhase(), c.GetSeq()))
		}
	}
	return out
}

func away(*agentpb.CheckpointCommit) *agentpb.CheckpointAck {
	return &agentpb.CheckpointAck{ErrorCode: executionreport.CodeStoreUnavailable, Error: "hub unavailable"}
}

func deciding(*agentpb.CheckpointCommit) *agentpb.CheckpointAck {
	return &agentpb.CheckpointAck{Accepted: true}
}

func newHeldWaiter(t *testing.T) (*checkpointWaiter, *reportspool.Spool, *controllerStream) {
	t.Helper()
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(t.TempDir(), "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spool.Close() })
	waiter := newCheckpointWaiter(spool, logr.Discard())
	stream := &controllerStream{waiter: waiter, answer: away}
	return waiter, spool, stream
}

func commitOf(job, phase string, seq int64, complete bool) *agentpb.CheckpointCommit {
	return &agentpb.CheckpointCommit{JobId: job, Generation: 1, Phase: phase, Seq: seq, Uri: "s3://b/" + phase, Sha256: "aa", Complete: complete}
}

// A job's COMPLETE while the controller is away (no stream, or the gateway
// answering STORE_UNAVAILABLE) is held and acked to the job, which exits 0:
// the agent sends it again until the controller decides it. Refused to the
// job, it failed every job that ended during a long controller outage
// (scale lab: 1,451 of 3,000 Failed CHECKPOINT_INCOMPLETE).
func TestACommitTheControllerCannotDecideIsHeldAndSentAgain(t *testing.T) {
	waiter, spool, stream := newHeldWaiter(t)
	ctx := context.Background()

	for _, seq := range []int64{1, 2} { // no stream, then a gateway without its hub
		if seq == 2 {
			waiter.setSender(newStreamSender(stream))
		}
		ack, err := waiter.Commit(ctx, commitOf("job-1", "train", seq, seq == 2))
		if err != nil || !ack.GetAccepted() {
			t.Fatalf("commit %d with the controller away = %+v, %v; want it held and acked", seq, ack, err)
		}
	}
	if holds, err := spool.HoldsCheckpoints(ctx, "job-1"); err != nil || !holds {
		t.Fatalf("held = %v, %v", holds, err)
	}

	waiter.resend(ctx) // still away: kept
	stream.controller(deciding)
	waiter.resend(ctx)
	if holds, _ := spool.HoldsCheckpoints(ctx, "job-1"); holds {
		t.Fatal("the commit the controller accepted is still held")
	}
	// Seq 2 went behind the held seq 1 and took its place: the controller
	// takes a phase's newest seq, so seq 1 never had to go.
	sent := stream.sentCommits()
	if len(sent) != 2 || sent[0] != "job-1/train/2" || sent[1] != "job-1/train/2" {
		t.Fatalf("sent %v; want train seq 2 alone, sent while away and again when decided", sent)
	}
	if last := stream.sent[1].GetCheckpointCommit(); !last.GetComplete() || last.GetGeneration() != 1 {
		t.Fatalf("resent %+v; want the commit as the job made it", last)
	}
}

// A job with commits held goes behind them, so the controller takes its
// commits in their order (an older seq after a newer one is refused); a
// commit the controller refuses is no longer held.
func TestACommitOfAJobWithCommitsHeldGoesBehindThem(t *testing.T) {
	waiter, spool, stream := newHeldWaiter(t)
	waiter.setSender(newStreamSender(stream))
	ctx := context.Background()

	if _, err := waiter.Commit(ctx, commitOf("job-1", "prep", 1, true)); err != nil {
		t.Fatal(err)
	}
	stream.controller(func(c *agentpb.CheckpointCommit) *agentpb.CheckpointAck {
		if c.GetPhase() == "prep" {
			return &agentpb.CheckpointAck{ErrorCode: executionreport.CodeCheckpointFrozen, Error: "cursor frozen"}
		}
		return &agentpb.CheckpointAck{Accepted: true}
	})
	ack, err := waiter.Commit(ctx, commitOf("job-1", "train", 1, false))
	if err != nil || !ack.GetAccepted() {
		t.Fatalf("commit behind a held one = %+v, %v", ack, err)
	}
	if sent := stream.sentCommits(); len(sent) != 1 {
		t.Fatalf("sent %v; want train held behind prep, not sent before it", sent)
	}
	waiter.resend(ctx)
	if sent := stream.sentCommits(); len(sent) != 3 || sent[1] != "job-1/prep/1" || sent[2] != "job-1/train/1" {
		t.Fatalf("sent %v; want prep, then train", sent)
	}
	if held, _ := spool.HeldCheckpoints(ctx); len(held) != 0 {
		t.Fatalf("held %+v; want the refused and the accepted commit both decided", held)
	}
}

// A job whose commit the controller still cannot decide (its Workspace's
// store away) keeps its own later commits behind it, and only them: another
// job's held commits go on. The resend stopped at the first undecided one,
// and every job behind it waited for that one job's store.
func TestAStuckCommitHoldsOnlyItsOwnJob(t *testing.T) {
	waiter, spool, stream := newHeldWaiter(t)
	ctx := context.Background()
	for _, c := range []*agentpb.CheckpointCommit{commitOf("job-1", "prep", 1, true), commitOf("job-1", "train", 1, true), commitOf("job-2", "train", 1, true)} {
		if _, err := waiter.Commit(ctx, c); err != nil { // no stream: held
			t.Fatal(err)
		}
	}
	waiter.setSender(newStreamSender(stream))
	stream.controller(func(c *agentpb.CheckpointCommit) *agentpb.CheckpointAck {
		if c.GetJobId() == "job-1" {
			return away(c)
		}
		return deciding(c)
	})
	waiter.resend(ctx)
	if sent := stream.sentCommits(); len(sent) != 2 || sent[0] != "job-1/prep/1" || sent[1] != "job-2/train/1" {
		t.Fatalf("sent %v; want job-1's first commit, then job-2's, and job-1's train behind its prep", sent)
	}
	if holds, _ := spool.HoldsCheckpoints(ctx, "job-2"); holds {
		t.Fatal("job-2's commit waited behind job-1's")
	}
	if held, _ := spool.HeldCheckpoints(ctx); len(held) != 2 {
		t.Fatalf("held %+v; want job-1's two commits", held)
	}
}
