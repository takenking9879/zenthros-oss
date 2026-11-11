// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"sync"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// statusLaneWidth bounds the Status queries answered at once.
const statusLaneWidth = 4

// orderedLaneWidth bounds the Submit, Cancel and Prepare commands running at
// once, across jobs.
const orderedLaneWidth = 4

// orderedLaneDepth is how many Submit/Cancel/Prepare commands may wait before
// the receive loop waits too.
const orderedLaneDepth = 64

// prepareKey orders Prepare commands among themselves: they carry no job.
const prepareKey = "prepare"

// commandLanes answers controller commands off the receive loop:
//
//   - Submit, Cancel and Prepare change the backend. Those of one job run one
//     at a time, in the order they arrived: a Cancel never overtakes its own
//     job's Submit. Those of different jobs run beside each other, at most
//     orderedLaneWidth at once, so a Submit pulling an image for minutes does
//     not hold another job's Cancel (F12).
//   - Status only reads (the status watcher already calls the executor beside
//     them) and runs on its own bounded lane, so a long Submit does not leave
//     every Status unanswered until the gateway gives up on it.
//   - ACKs are handled inline by the receive loop; they only update local
//     bookkeeping.
type commandLanes struct {
	agent  *Agent
	send   func(*agentpb.RegisterRequest) error
	status chan struct{}
	errs   chan error
	wg     sync.WaitGroup

	// queued bounds the ordered commands admitted and not yet answered;
	// running bounds those executing.
	queued  chan struct{}
	running chan struct{}
	mu      sync.Mutex
	// jobs holds, per job with a command executing, the ones waiting behind
	// it in arrival order.
	jobs map[string][]*agentpb.RegisterResponse
}

func (a *Agent) startLanes(_ context.Context, send func(*agentpb.RegisterRequest) error) *commandLanes {
	return &commandLanes{
		agent:   a,
		send:    send,
		status:  make(chan struct{}, statusLaneWidth),
		errs:    make(chan error, 1),
		queued:  make(chan struct{}, orderedLaneDepth),
		running: make(chan struct{}, orderedLaneWidth),
		jobs:    map[string][]*agentpb.RegisterResponse{},
	}
}

// orderedKey is the job a backend-changing command acts on.
func orderedKey(cmd *agentpb.RegisterResponse) string {
	switch {
	case cmd.GetSubmit() != nil:
		return "job:" + cmd.GetSubmit().GetJob().GetId()
	case cmd.GetCancel() != nil:
		if id := cmd.GetCancel().GetId(); id != "" {
			return "job:" + id
		}
		return "handle:" + cmd.GetCancel().GetHandleName()
	default:
		return prepareKey
	}
}

// route hands cmd to its lane. It blocks only when that lane is full.
func (l *commandLanes) route(ctx context.Context, cmd *agentpb.RegisterResponse) {
	switch {
	case cmd.GetSubmit() != nil, cmd.GetCancel() != nil, cmd.GetPrepare() != nil:
		select {
		case l.queued <- struct{}{}:
		case <-ctx.Done():
			return
		}
		key := orderedKey(cmd)
		l.mu.Lock()
		if waiting, busy := l.jobs[key]; busy {
			l.jobs[key] = append(waiting, cmd)
			l.mu.Unlock()
			return
		}
		l.jobs[key] = nil
		l.mu.Unlock()
		l.wg.Add(1)
		go l.drain(ctx, key, cmd)
	case cmd.GetStatus() != nil:
		select {
		case l.status <- struct{}{}:
		case <-ctx.Done():
			return
		}
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			defer func() { <-l.status }()
			l.answer(ctx, cmd)
		}()
	default:
		l.answer(ctx, cmd)
	}
}

// drain answers cmd, then each command that queued behind it for the same
// job, in order.
func (l *commandLanes) drain(ctx context.Context, key string, cmd *agentpb.RegisterResponse) {
	defer l.wg.Done()
	for {
		select {
		case l.running <- struct{}{}:
			l.answer(ctx, cmd)
			<-l.running
		case <-ctx.Done():
		}
		<-l.queued
		l.mu.Lock()
		waiting := l.jobs[key]
		if len(waiting) == 0 || ctx.Err() != nil {
			for range waiting {
				<-l.queued
			}
			delete(l.jobs, key)
			l.mu.Unlock()
			return
		}
		cmd, l.jobs[key] = waiting[0], waiting[1:]
		l.mu.Unlock()
	}
}

func (l *commandLanes) answer(ctx context.Context, cmd *agentpb.RegisterResponse) {
	resp := l.agent.Dispatch(ctx, cmd)
	if resp == nil {
		return
	}
	if err := l.send(resp); err != nil {
		select {
		case l.errs <- fmt.Errorf("send response: %w", err):
		default:
		}
	}
}

// failed carries the first send error; the stream is then unusable.
func (l *commandLanes) failed() <-chan error { return l.errs }

// wait returns once every lane goroutine has stopped. The caller cancels
// their context first.
func (l *commandLanes) wait() { l.wg.Wait() }
