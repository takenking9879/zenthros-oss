// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/executionreport"
)

// childSpawnTimeout bounds how long a job waits for its children to exist:
// the gateway gives the controller a minute, and the answer needs to travel
// back.
const childSpawnTimeout = 75 * time.Second

// childWaiter forwards a job's spawns to the controller and hands each
// answer back to the job that asked. It decides nothing.
type childWaiter struct {
	mu      sync.Mutex
	sender  *streamSender
	pending map[string]chan *agentpb.ChildSpawnAck
}

func newChildWaiter() *childWaiter {
	return &childWaiter{pending: map[string]chan *agentpb.ChildSpawnAck{}}
}

func (w *childWaiter) setSender(sender *streamSender) {
	w.mu.Lock()
	w.sender = sender
	pending := w.pending
	w.pending = map[string]chan *agentpb.ChildSpawnAck{}
	w.mu.Unlock()
	if sender == nil {
		for _, ch := range pending {
			close(ch)
		}
	}
}

// Spawn sends req and waits for the controller's answer.
func (w *childWaiter) Spawn(ctx context.Context, req *agentpb.ChildSpawnRequest) (*agentpb.ChildSpawnAck, error) {
	w.mu.Lock()
	sender := w.sender
	if sender == nil {
		w.mu.Unlock()
		return nil, fmt.Errorf("control stream is down")
	}
	if req.GetRequestId() == "" {
		var b [8]byte
		_, _ = rand.Read(b[:])
		req.RequestId = hex.EncodeToString(b[:])
	}
	ch := make(chan *agentpb.ChildSpawnAck, 1)
	w.pending[req.GetRequestId()] = ch
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.pending, req.GetRequestId())
		w.mu.Unlock()
	}()

	if err := sender.Send(&agentpb.RegisterRequest{ChildSpawn: req}); err != nil {
		return nil, err
	}
	timer := time.NewTimer(childSpawnTimeout)
	defer timer.Stop()
	select {
	case ack, ok := <-ch:
		if !ok || ack == nil {
			return nil, fmt.Errorf("control stream is down")
		}
		return ack, nil
	case <-timer.C:
		return nil, fmt.Errorf("no answer from the controller in %s", childSpawnTimeout)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *childWaiter) HandleAck(ack *agentpb.ChildSpawnAck) {
	if ack == nil {
		return
	}
	w.mu.Lock()
	ch, ok := w.pending[ack.GetRequestId()]
	w.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- ack:
	default:
	}
}

// SpawnChildren forwards a job's spawn (or list) to the controller over the
// control stream and returns its answer. The caller checked the job's token.
func (a *Agent) SpawnChildren(ctx context.Context, req *agentpb.ChildSpawnRequest) (*agentpb.ChildSpawnAck, error) {
	if a.spawnRefused != "" {
		return &agentpb.ChildSpawnAck{RequestId: req.GetRequestId(), ErrorCode: executionreport.CodeRejected, Error: a.spawnRefused}, nil
	}
	return a.children.Spawn(ctx, req)
}
