// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/reportspool"
)

func TestCheckpointWaiterDeliversAck(t *testing.T) {
	waiter, _, _ := newHeldWaiter(t)
	stream := &recordingStream{}
	waiter.setSender(newStreamSender(stream))
	done := make(chan *agentpb.CheckpointAck, 1)
	go func() {
		ack, err := waiter.Commit(context.Background(), &agentpb.CheckpointCommit{
			JobId: "job-1", Phase: "train", Seq: 1, Uri: "s3://b/x", Sha256: "aa",
		})
		if err != nil {
			t.Errorf("commit: %v", err)
			close(done)
			return
		}
		done <- ack
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stream.mu.Lock()
		n := len(stream.sent)
		stream.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	waiter.HandleAck(&agentpb.CheckpointAck{JobId: "job-1", Phase: "train", Seq: 1, Accepted: true})
	select {
	case ack := <-done:
		if ack == nil || !ack.GetAccepted() {
			t.Fatalf("ack = %+v", ack)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ack")
	}
}

func TestCheckpointHTTPRequiresLiveAck(t *testing.T) {
	dir := t.TempDir()
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(dir, "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	token, err := spool.IssueToken(context.Background(), "job-1")
	if err != nil {
		t.Fatal(err)
	}
	server := newReportServerWithCheckpoint(spool, logr.Discard(), nil, func(ctx context.Context, commit *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error) {
		return &agentpb.CheckpointAck{JobId: commit.GetJobId(), Phase: commit.GetPhase(), Seq: commit.GetSeq(), Accepted: true}, nil
	})
	body, _ := json.Marshal(map[string]any{
		"job_id": "job-1", "generation": 1, "phase": "train", "seq": 1,
		"uri": "s3://b/jobs/job-1/gen/1/train/1", "sha256": "abc",
	})
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/checkpoints", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := &captureResponse{}
	server.handleCheckpoint(rec, req)
	if rec.status != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.status, rec.buf.String())
	}
}

func TestCheckpointHTTPDownStreamIsUnavailable(t *testing.T) {
	dir := t.TempDir()
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(dir, "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	token, _ := spool.IssueToken(context.Background(), "job-1")
	server := newReportServer(spool, logr.Discard(), nil)
	body, _ := json.Marshal(map[string]any{
		"job_id": "job-1", "phase": "train", "seq": 1, "uri": "s3://b/x", "sha256": "abc",
	})
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/checkpoints", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := &captureResponse{}
	server.handleCheckpoint(rec, req)
	if rec.status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when agent cannot ACK", rec.status)
	}
}

// A commit the controller could not decide (STORE_UNAVAILABLE) reaches the
// job as 503: it may have been kept, and a job told 409 commits other bytes
// at the same seq, refused against the kept one (scale lab, 4 jobs Failed).
// The controller's refusal stays 409.
func TestCheckpointHTTPNoDecisionIsUnavailable(t *testing.T) {
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(t.TempDir(), "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	token, _ := spool.IssueToken(context.Background(), "job-1")
	for _, c := range []struct {
		ack  *agentpb.CheckpointAck
		want int
	}{
		{&agentpb.CheckpointAck{ErrorCode: "STORE_UNAVAILABLE", Error: "write checkpoint progress: context canceled"}, http.StatusServiceUnavailable},
		{&agentpb.CheckpointAck{Error: "conflicting commit at acked seq"}, http.StatusConflict},
	} {
		server := newReportServerWithCheckpoint(spool, logr.Discard(), nil, func(ctx context.Context, commit *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error) {
			ack := c.ack
			ack.JobId, ack.Phase, ack.Seq = commit.GetJobId(), commit.GetPhase(), commit.GetSeq()
			return ack, nil
		})
		body, _ := json.Marshal(map[string]any{"job_id": "job-1", "generation": 1, "phase": "train", "seq": 2, "uri": "s3://b/x", "sha256": "abc"})
		req, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/checkpoints", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := &captureResponse{}
		server.handleCheckpoint(rec, req)
		if rec.status != c.want {
			t.Fatalf("ack %q/%q: status %d, want %d (body %s)", c.ack.GetErrorCode(), c.ack.GetError(), rec.status, c.want, rec.buf.String())
		}
	}
}
