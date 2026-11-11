// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/internal/grpc/errorclass"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// channelOpens counts the LogChannels the agent's loop opens, and answers
// each with the next scripted result; a channel past the script stays open
// until release.
type channelOpens struct {
	mu      sync.Mutex
	n       int
	script  []error
	release chan struct{}
}

func (c *channelOpens) open(ctx context.Context, _ *tls.Config) error {
	c.mu.Lock()
	c.n++
	var result error
	scripted := len(c.script) > 0
	if scripted {
		result, c.script = c.script[0], c.script[1:]
	}
	c.mu.Unlock()
	if scripted {
		return result
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.release:
		return nil
	}
}

func (c *channelOpens) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func (c *channelOpens) waitFor(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for c.count() < want {
		if time.Now().After(deadline) {
			t.Fatalf("the loop opened %d log channels, want %d", c.count(), want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// stays fails if the loop opens another channel within a moment.
func (c *channelOpens) stays(t *testing.T, want int, why string) {
	t.Helper()
	time.Sleep(150 * time.Millisecond)
	if got := c.count(); got != want {
		t.Fatalf("%s: the loop opened %d log channels, want %d", why, got, want)
	}
}

func logChannelAgent(t *testing.T, opens *channelOpens) *Agent {
	t.Helper()
	a := NewWithExecutor(Config{BackendID: "vm-1", BackendType: "vm", ReconnectBackoff: 20 * time.Millisecond}, SystemdExecutor{}, logr.Discard())
	a.logChannelOnce = opens.open
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go a.serveLogChannel(ctx, nil)
	return a
}

func session(a *Agent, epoch int64) {
	a.sessionEpoch.Store(epoch)
	a.logSessionAccepted(epoch)
}

// A machine nobody reads holds no log channel: the agent opens one when the
// gateway asks, and once it ends waits for the next ask. It never
// reconnects by itself.
func TestOnDemandLogChannelOpensOnlyWhenAsked(t *testing.T) {
	opens := &channelOpens{release: make(chan struct{})}
	a := logChannelAgent(t, opens)
	session(a, 1)
	opens.stays(t, 0, "nobody asked")

	a.askLogChannel()
	opens.waitFor(t, 1)
	opens.stays(t, 1, "one ask")

	opens.release <- struct{}{} // the gateway closed it: nobody read for a while
	opens.stays(t, 1, "the channel ended and nobody asked again")

	a.askLogChannel()
	opens.waitFor(t, 2)
}

// The ask arrives on the control stream, between the commands.
func TestOpenLogChannelOnTheControlStreamIsAnAsk(t *testing.T) {
	opens := &channelOpens{release: make(chan struct{})}
	a := logChannelAgent(t, opens)
	session(a, 1)

	stream := newRecordingStream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.ServeStream(ctx, stream) }()
	opens.stays(t, 0, "a stream with no ask")
	stream.recv <- &agentpb.RegisterResponse{OpenLogChannel: &agentpb.OpenLogChannel{}}
	opens.waitFor(t, 1)
}

// A channel that lands on a replica without the session is dialed again at
// once: the ask still stands. Any other failure waits for the gateway to ask
// again, which it does while a reader waits.
func TestOnDemandLogChannelRetriesAMisrouteNotAFailure(t *testing.T) {
	misrouted := errorclass.NotThisReplica("vm-1")
	opens := &channelOpens{release: make(chan struct{}), script: []error{misrouted, misrouted, errors.New("dial: connection refused")}}
	a := logChannelAgent(t, opens)
	session(a, 1)

	a.askLogChannel()
	opens.waitFor(t, 3)
	opens.stays(t, 3, "a failed dial with no new ask")

	a.askLogChannel()
	opens.waitFor(t, 4)
}

// An ask belongs to its session: one left from the previous session is not
// carried into the next.
func TestLogChannelAskBelongsToItsSession(t *testing.T) {
	opens := &channelOpens{release: make(chan struct{})}
	a := logChannelAgent(t, opens)
	session(a, 1)
	a.askLogChannel()
	opens.waitFor(t, 1)

	a.askLogChannel() // waits while the channel is open
	session(a, 2)
	opens.release <- struct{}{}
	opens.stays(t, 1, "the ask belonged to the previous session")
}

// Until the gateway has accepted the session an ask can only be the previous
// session's, and opens nothing: the channel would carry an epoch the gateway
// does not hold yet.
func TestLogChannelWaitsForTheGatewaysAcceptance(t *testing.T) {
	opens := &channelOpens{release: make(chan struct{})}
	a := logChannelAgent(t, opens)
	a.sessionEpoch.Store(7) // advertised, not accepted yet
	a.askLogChannel()
	opens.stays(t, 0, "advertised, not accepted")
	a.logSessionAccepted(7)
	opens.stays(t, 0, "accepted: the ask from before it is dropped")
	a.askLogChannel()
	opens.waitFor(t, 1)
}

// Register records the session the gateway accepted: the asks that follow
// are its own.
func TestRegisterRecordsTheAcceptedSession(t *testing.T) {
	{
		a := NewWithExecutor(Config{BackendID: "vm-1", BackendType: "vm", Network: NetworkTailscaleUserspace}, SystemdExecutor{}, logr.Discard())
		stream := newRecordingStream()
		stream.recv <- &agentpb.RegisterResponse{Accepted: true}
		if err := a.Register(context.Background(), stream); err != nil {
			t.Fatal(err)
		}
		if a.logAcceptedEpoch.Load() == 0 || a.logAcceptedEpoch.Load() != a.sessionEpoch.Load() {
			t.Fatal("the accepted session was not recorded")
		}
	}
}
