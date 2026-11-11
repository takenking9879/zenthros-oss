// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/reportspool"
)

const checkpointAckTimeout = 10 * time.Second

// checkpointResendEvery is how often the commits the controller has not
// decided go again when nothing else wakes them: well inside the 2
// heartbeats a Succeeded the controller saw first waits for its COMPLETE.
const checkpointResendEvery = 5 * time.Second

// checkpointWaiter takes a job's checkpoint commits to the controller. One
// the controller could not decide is held in the spool and sent again by Run.
type checkpointWaiter struct {
	mu      sync.Mutex
	sender  *streamSender
	pending map[string]chan *agentpb.CheckpointAck

	spool  *reportspool.Spool
	logger Logger
	wake   chan struct{}
}

func newCheckpointWaiter(spool *reportspool.Spool, logger Logger) *checkpointWaiter {
	return &checkpointWaiter{
		pending: map[string]chan *agentpb.CheckpointAck{},
		spool:   spool, logger: logger.WithName("checkpoint"), wake: make(chan struct{}, 1),
	}
}

func checkpointKey(jobID, phase string, seq int64) string {
	return fmt.Sprintf("%s/%s/%d", jobID, phase, seq)
}

func (w *checkpointWaiter) setSender(sender *streamSender) {
	w.mu.Lock()
	w.sender = sender
	pending := w.pending
	w.pending = map[string]chan *agentpb.CheckpointAck{}
	w.mu.Unlock()
	if sender == nil {
		for _, ch := range pending {
			close(ch)
		}
		return
	}
	w.Wake() // the held commits go on the new stream
}

func (w *checkpointWaiter) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Commit is a job's checkpoint commit, decided by the controller. One it
// could not decide (no stream, no answer, STORE_UNAVAILABLE: the controller
// away) is held and acked to the job, and Run sends it until the controller
// decides it: refused to the job, a long controller outage failed every job
// that ended during it, its COMPLETE never acked (CHECKPOINT_INCOMPLETE). A
// job with commits held goes behind them, so the controller takes its
// commits in their order. The controller decides by the lease and the
// generation the commit carries, whenever it arrives. Only a commit that
// could not be held is undecided to the job (503).
func (w *checkpointWaiter) Commit(ctx context.Context, commit *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error) {
	behind, err := w.spool.HoldsCheckpoints(ctx, commit.GetJobId())
	if err != nil {
		return nil, err
	}
	if !behind {
		if ack, err := w.send(ctx, commit); decided(ack, err) {
			return ack, nil
		}
	}
	raw, err := proto.Marshal(commit)
	if err != nil {
		return nil, err
	}
	if err := w.spool.HoldCheckpoint(ctx, reportspool.HeldCheckpoint{
		JobID: commit.GetJobId(), Phase: commit.GetPhase(), Seq: commit.GetSeq(), Commit: raw,
	}); err != nil {
		return nil, err
	}
	w.logger.Info("held a checkpoint commit until the controller decides it",
		"job_id", commit.GetJobId(), "phase", commit.GetPhase(), "seq", commit.GetSeq(), "complete", commit.GetComplete())
	w.Wake()
	return &agentpb.CheckpointAck{JobId: commit.GetJobId(), Phase: commit.GetPhase(), Seq: commit.GetSeq(), Accepted: true}, nil
}

// holds reports a job with commits held. A spool that cannot be read holds
// nothing back: Commit and Run cannot read it either.
func (w *checkpointWaiter) holds(ctx context.Context, jobID string) bool {
	held, err := w.spool.HoldsCheckpoints(ctx, jobID)
	if err != nil {
		w.logger.Error(err, "failed to read whether a job's checkpoint commits are held", "job_id", jobID)
	}
	return held
}

// decided reports an answer of the controller: an accept or a refusal.
func decided(ack *agentpb.CheckpointAck, err error) bool {
	return err == nil && ack.GetErrorCode() != executionreport.CodeStoreUnavailable
}

// Run sends the held commits again on each wake and every
// checkpointResendEvery, until ctx ends.
func (w *checkpointWaiter) Run(ctx context.Context) {
	for {
		w.resend(ctx)
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-time.After(checkpointResendEvery):
		}
	}
}

// resend sends the held commits in the order they were held, and stops at
// one the controller still cannot decide. A refusal is logged and forgotten:
// the job already went on from its ack.
func (w *checkpointWaiter) resend(ctx context.Context) {
	held, err := w.spool.HeldCheckpoints(ctx)
	if err != nil {
		w.logger.Error(err, "failed to list the held checkpoint commits")
		return
	}
	// A job whose commit is still undecided keeps its later ones behind it;
	// the other jobs' go on. One job's store away (its Workspace's) held
	// every job's commits behind it.
	stuck := map[string]bool{}
	for _, h := range held {
		if stuck[h.JobID] {
			continue
		}
		var commit agentpb.CheckpointCommit
		if err := proto.Unmarshal(h.Commit, &commit); err != nil {
			w.logger.Error(err, "dropped a held checkpoint commit that does not read", "job_id", h.JobID, "phase", h.Phase, "seq", h.Seq)
		} else {
			ack, err := w.send(ctx, &commit)
			if !decided(ack, err) {
				stuck[h.JobID] = true
				continue
			}
			if !ack.GetAccepted() {
				w.logger.Info("the controller refused a held checkpoint commit", "job_id", h.JobID, "phase", h.Phase, "seq", h.Seq,
					"code", ack.GetErrorCode(), "reason", ack.GetError())
			}
		}
		if err := w.spool.ReleaseCheckpoint(ctx, h.JobID, h.Phase, h.Seq); err != nil {
			w.logger.Error(err, "failed to forget a decided checkpoint commit", "job_id", h.JobID)
			return
		}
	}
}

// send sends commit on the stream and waits for the controller's ack.
func (w *checkpointWaiter) send(ctx context.Context, commit *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error) {
	w.mu.Lock()
	sender := w.sender
	if sender == nil {
		w.mu.Unlock()
		return nil, fmt.Errorf("control stream is down")
	}
	key := checkpointKey(commit.GetJobId(), commit.GetPhase(), commit.GetSeq())
	ch := make(chan *agentpb.CheckpointAck, 1)
	w.pending[key] = ch
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		delete(w.pending, key)
		w.mu.Unlock()
	}()

	if err := sender.Send(&agentpb.RegisterRequest{CheckpointCommit: commit}); err != nil {
		return nil, err
	}
	timer := time.NewTimer(checkpointAckTimeout)
	defer timer.Stop()
	select {
	case ack, ok := <-ch:
		if !ok || ack == nil {
			return nil, fmt.Errorf("control stream is down")
		}
		return ack, nil
	case <-timer.C:
		return nil, fmt.Errorf("checkpoint ack timed out")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *checkpointWaiter) HandleAck(ack *agentpb.CheckpointAck) {
	if ack == nil {
		return
	}
	key := checkpointKey(ack.GetJobId(), ack.GetPhase(), ack.GetSeq())
	w.mu.Lock()
	ch, ok := w.pending[key]
	w.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- ack:
	default:
	}
}
