// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"sync"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// streamSender serializes Send calls on a controlStream. gRPC streams are not
// safe for concurrent sends; the StatusWatcher, heartbeat, and command-response
// paths all share the same stream.
type streamSender struct {
	mu     sync.Mutex
	stream controlStream
	closed bool
}

// errStreamClosed is a Send after the agent closed its side (closeSend).
var errStreamClosed = errors.New("control stream closed by the agent")

func newStreamSender(stream controlStream) *streamSender {
	return &streamSender{stream: stream}
}

func (s *streamSender) Send(msg *agentpb.RegisterRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStreamClosed
	}
	return s.stream.Send(msg)
}

// closeSend ends the agent's side of the stream, between two Sends: gRPC
// allows no CloseSend while a Send is in progress. Sends after it fail.
// False when the stream cannot be closed from this side (a test fake).
func (s *streamSender) closeSend() bool {
	closer, ok := s.stream.(interface{ CloseSend() error })
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return closer.CloseSend() == nil
}
