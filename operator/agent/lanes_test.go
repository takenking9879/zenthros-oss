// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/jobspec"
)

// laneStream delivers its commands in order, then blocks; it records
// every response.
type laneStream struct {
	cmds chan *agentpb.RegisterResponse
	mu   sync.Mutex
	sent []*agentpb.RegisterRequest
}

func (s *laneStream) Recv() (*agentpb.RegisterResponse, error) { return <-s.cmds, nil }

func (s *laneStream) Send(msg *agentpb.RegisterRequest) error {
	if msg.GetSubmitResponse() == nil && msg.GetCancelResponse() == nil && msg.GetStatusResponse() == nil {
		return nil // heartbeats and status pushes share the stream
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, msg)
	return nil
}

func (s *laneStream) responses() []*agentpb.RegisterRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentpb.RegisterRequest(nil), s.sent...)
}

// A Submit that takes minutes (an image pull) must not hold the Status
// queries behind it; its own job's Cancel still waits for it.
func TestStatusAnsweredWhileSubmitRuns(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	job, err := envelope.Encode(jobspec.JobSpec{ID: "uid-slow", Kind: v1.WorkloadKindJob, Entrypoint: "echo hi"})
	if err != nil {
		t.Fatal(err)
	}
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, blockingExecutor{started: started, release: release}, logr.Discard())
	stream := &laneStream{cmds: make(chan *agentpb.RegisterResponse, 8)}
	stream.cmds <- &agentpb.RegisterResponse{Submit: &agentpb.SubmitRequest{RequestId: "submit", Job: job}}
	stream.cmds <- &agentpb.RegisterResponse{Cancel: &agentpb.CancelRequest{RequestId: "cancel", Id: "uid-slow", HandleName: "handle"}}
	stream.cmds <- &agentpb.RegisterResponse{Status: &agentpb.StatusRequest{RequestId: "status", HandleName: "other"}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.ServeStream(ctx, stream) }()

	<-started
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := stream.responses()
		if len(got) == 1 && got[0].GetStatusResponse().GetRequestId() == "status" {
			break
		}
		if len(got) > 1 || time.Now().After(deadline) {
			t.Fatalf("while Submit runs, responses = %v; want only the Status", got)
		}
		time.Sleep(time.Millisecond)
	}

	close(release)
	deadline = time.Now().Add(2 * time.Second)
	for len(stream.responses()) < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("responses = %v", stream.responses())
		}
		time.Sleep(time.Millisecond)
	}
	got := stream.responses()
	if got[1].GetSubmitResponse().GetRequestId() != "submit" || got[2].GetCancelResponse().GetRequestId() != "cancel" {
		t.Fatalf("ordered lane answered out of order: %v", got)
	}
	cancel()
	<-done
}

// countingExecutor blocks every Submit until released and records how many
// ran at once.
type countingExecutor struct {
	blockingExecutor
	mu      sync.Mutex
	now     int
	most    int
	entered chan string
}

func (e *countingExecutor) Submit(ctx context.Context, spec jobspec.JobSpec, _ map[string]string) (string, error) {
	e.mu.Lock()
	e.now++
	if e.now > e.most {
		e.most = e.now
	}
	e.mu.Unlock()
	e.entered <- spec.ID
	defer func() {
		e.mu.Lock()
		e.now--
		e.mu.Unlock()
	}()
	select {
	case <-e.release:
		return "handle-" + spec.ID, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// enter waits for the next Submit to start, failing the test instead of
// hanging when none does.
func (e *countingExecutor) enter(t *testing.T) string {
	t.Helper()
	select {
	case id := <-e.entered:
		return id
	case <-time.After(2 * time.Second):
		t.Fatal("no Submit started")
		return ""
	}
}

func submitCmd(t *testing.T, requestID, jobID string) *agentpb.RegisterResponse {
	t.Helper()
	job, err := envelope.Encode(jobspec.JobSpec{ID: jobID, Kind: v1.WorkloadKindJob, Entrypoint: "echo hi"})
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.RegisterResponse{Submit: &agentpb.SubmitRequest{RequestId: requestID, Job: job}}
}

func waitResponse(t *testing.T, stream *laneStream, requestID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, r := range stream.responses() {
			if r.GetCancelResponse().GetRequestId() == requestID || r.GetSubmitResponse().GetRequestId() == requestID {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no response to %s; got %v", requestID, stream.responses())
		}
		time.Sleep(time.Millisecond)
	}
}

// Another job's Cancel does not wait behind a Submit pulling an image for
// minutes (F12): commands are ordered per job, not across the backend. The
// slow job's own Cancel still waits, and is answered after its Submit.
func TestOtherJobsCancelAnsweredWhileSubmitRuns(t *testing.T) {
	exec := &countingExecutor{blockingExecutor: blockingExecutor{release: make(chan struct{})}, entered: make(chan string, 8)}
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, exec, logr.Discard())
	stream := &laneStream{cmds: make(chan *agentpb.RegisterResponse, 8)}
	stream.cmds <- submitCmd(t, "submit-slow", "uid-slow")
	stream.cmds <- &agentpb.RegisterResponse{Cancel: &agentpb.CancelRequest{RequestId: "cancel-slow", Id: "uid-slow", HandleName: "handle-uid-slow"}}
	stream.cmds <- &agentpb.RegisterResponse{Cancel: &agentpb.CancelRequest{RequestId: "cancel-other", Id: "uid-other", HandleName: "handle-uid-other"}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.ServeStream(ctx, stream) }()

	exec.enter(t)
	waitResponse(t, stream, "cancel-other")
	for _, r := range stream.responses() {
		if r.GetCancelResponse().GetRequestId() == "cancel-slow" {
			t.Fatal("the slow job's Cancel was answered before its Submit")
		}
	}
	close(exec.release)
	waitResponse(t, stream, "cancel-slow")
	var order []string
	for _, r := range stream.responses() {
		if id := r.GetSubmitResponse().GetRequestId() + r.GetCancelResponse().GetRequestId(); id != "cancel-other" {
			order = append(order, id)
		}
	}
	if len(order) != 2 || order[0] != "submit-slow" || order[1] != "cancel-slow" {
		t.Fatalf("the slow job's commands answered as %v, want its Submit then its Cancel", order)
	}
	cancel()
	<-done
}

// Different jobs' Submits run beside each other, but never more than
// orderedLaneWidth at once: a burst of Submits does not pull every image at
// once.
func TestOrderedLaneBoundsConcurrency(t *testing.T) {
	exec := &countingExecutor{blockingExecutor: blockingExecutor{release: make(chan struct{})}, entered: make(chan string, 16)}
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, exec, logr.Discard())
	stream := &laneStream{cmds: make(chan *agentpb.RegisterResponse, 16)}
	for i := 0; i < 2*orderedLaneWidth; i++ {
		stream.cmds <- submitCmd(t, fmt.Sprintf("submit-%d", i), fmt.Sprintf("uid-%d", i))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.ServeStream(ctx, stream) }()

	for i := 0; i < orderedLaneWidth; i++ {
		exec.enter(t)
	}
	select {
	case id := <-exec.entered:
		t.Fatalf("Submit %s started beyond the lane width %d", id, orderedLaneWidth)
	case <-time.After(100 * time.Millisecond):
	}
	close(exec.release)
	for i := orderedLaneWidth; i < 2*orderedLaneWidth; i++ {
		exec.enter(t)
	}
	for i := 0; i < 2*orderedLaneWidth; i++ {
		waitResponse(t, stream, fmt.Sprintf("submit-%d", i))
	}
	exec.mu.Lock()
	most := exec.most
	exec.mu.Unlock()
	if most != orderedLaneWidth {
		t.Fatalf("at most %d Submits ran at once, want %d", most, orderedLaneWidth)
	}
	cancel()
	<-done
}

// resolvingExecutor reports a digest for whatever it pulled.
type resolvingExecutor struct{ blockingExecutor }

func (resolvingExecutor) Submit(context.Context, jobspec.JobSpec, map[string]string) (string, error) {
	return "handle", nil
}

func (resolvingExecutor) ResolvedImage(_ context.Context, spec jobspec.JobSpec) string {
	return spec.Image + "@sha256:ab"
}

// Submit answers with the digest its pull resolved the image to (F11), so
// the controller can pin it; a job without an image asks nothing of the
// executor.
func TestSubmitAnswersResolvedImage(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, resolvingExecutor{}, logr.Discard())
	for _, tc := range []struct{ image, want string }{{"repo/train", "repo/train@sha256:ab"}, {"", ""}} {
		job, err := envelope.Encode(jobspec.JobSpec{ID: "uid-img", Kind: v1.WorkloadKindJob, Entrypoint: "echo hi", Image: tc.image})
		if err != nil {
			t.Fatal(err)
		}
		resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "s", Job: job})
		if resp.GetError() != "" || resp.GetResolvedImage() != tc.want {
			t.Errorf("image %q: resolved %q (error %q), want %q", tc.image, resp.GetResolvedImage(), resp.GetError(), tc.want)
		}
	}
}
