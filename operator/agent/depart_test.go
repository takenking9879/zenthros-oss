// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// closingStream is a control stream the agent can close: once it does, the
// gateway's side ends (Recv returns io.EOF) when answer is set.
type closingStream struct {
	answer bool
	mu     sync.Mutex
	closed bool
	sends  int
	eof    chan struct{}
}

func newClosingStream(answer bool) *closingStream {
	return &closingStream{answer: answer, eof: make(chan struct{})}
}

func (s *closingStream) Send(*agentpb.RegisterRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.sends++
	}
	return nil
}

func (s *closingStream) Recv() (*agentpb.RegisterResponse, error) {
	<-s.eof
	return nil, io.EOF
}

func (s *closingStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && s.answer {
		close(s.eof)
	}
	s.closed = true
	return nil
}

func (s *closingStream) state() (closed bool, sendsAfter int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed, s.sends
}

// A stopped agent says goodbye: it closes its side of the stream before
// ServeStream returns, so its gateway reads io.EOF (a machine that left on
// purpose) and not a broken connection, and it sends nothing after.
func TestStoppedAgentClosesItsStreamFirst(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, blockingExecutor{}, logr.Discard())
	stream := newClosingStream(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.ServeStream(ctx, stream) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(departWait + time.Second):
		t.Fatal("ServeStream did not return after its goodbye")
	}
	if closed, after := stream.state(); !closed || after != 0 {
		t.Fatalf("closed %v, %d sends after the goodbye; want closed and none", closed, after)
	}
}

// A gateway that never ends the stream does not hold a stopping agent past
// departWait.
func TestGoodbyeWaitsOnlyAMoment(t *testing.T) {
	restore := departWait
	departWait = 200 * time.Millisecond
	t.Cleanup(func() { departWait = restore })
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, blockingExecutor{}, logr.Discard())
	stream := newClosingStream(false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.ServeStream(ctx, stream) }()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	cancel()
	select {
	case <-done:
		if took := time.Since(start); took < departWait/2 {
			t.Fatalf("returned after %s: the goodbye did not wait for the gateway", took)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a silent gateway held the stopping agent")
	}
}
