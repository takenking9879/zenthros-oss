// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/zenthros/operator/internal/grpc/errorclass"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// pullGateway sends one StreamLogs pull over LogChannel and records chunks.
type pullGateway struct {
	agentpb.UnimplementedAgentServer
	got  []string
	done chan struct{}
	pull *agentpb.StreamLogsRequest
}

func (g *pullGateway) LogChannel(stream agentpb.Agent_LogChannelServer) error {
	pull := g.pull
	if pull == nil {
		pull = &agentpb.StreamLogsRequest{HandleName: "handle-1"}
	}
	if err := stream.Send(&agentpb.LogChannelFromGateway{
		RequestId: "req-1",
		Pull:      pull,
	}); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if chunk := msg.GetChunk(); chunk != nil {
			g.got = append(g.got, string(chunk.GetData()))
		}
		if msg.GetEof() {
			close(g.done)
			return nil
		}
	}
}

func TestLogChannelOpensAgainstBufconn(t *testing.T) {
	exec := &scriptedLogExecutor{chunks: [][]byte{[]byte("hello\n")}}
	a := NewWithExecutor(Config{BackendID: "vm-1", BackendType: "vm", MaxLogBytes: 64}, exec, logr.Discard())

	gw := &pullGateway{done: make(chan struct{})}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	agentpb.RegisterAgentServer(gs, gw)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	stream, err := agentpb.NewAgentClient(conn).LogChannel(ctx)
	if err != nil {
		t.Fatalf("LogChannel: %v", err)
	}
	go func() { _ = a.serveLogPulls(ctx, stream) }()

	select {
	case <-gw.done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for log pull on bufconn")
	}
	if len(gw.got) == 0 || gw.got[0] != "hello\n" {
		t.Fatalf("chunks = %q, want hello\\n", gw.got)
	}
}

// A LogChannel refused by a Gateway replica that does not hold the agent's
// session is retried within a fraction of a second, not after the full
// reconnect backoff: the container e2e read no logs from a job that ran in
// the agent's first five seconds. Failures back off to the maximum, and a
// channel that stayed open starts over.
func TestLogChannelRetriesQuicklyThenBacksOff(t *testing.T) {
	r := logChannelRetry{max: 5 * time.Second}
	var got []time.Duration
	for i := 0; i < 8; i++ {
		got = append(got, r.next(0, false))
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
		1600 * time.Millisecond, 3200 * time.Millisecond, 5 * time.Second, 5 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("retries %v, want %v", got, want)
		}
	}
	if after := r.next(time.Minute, false); after != logChannelFirstRetry {
		t.Fatalf("after a healthy channel the retry is %v, want %v", after, logChannelFirstRetry)
	}
	if small := (&logChannelRetry{max: 50 * time.Millisecond}).next(0, false); small != 50*time.Millisecond {
		t.Fatalf("a backoff below the first retry is not honoured: %v", small)
	}
}

// A refusal by a replica without the agent's session is a routing miss: the
// next connection is a fresh draw, so it is retried at the first interval
// every time, not after a doubling backoff. Over the RunPod tailnet five
// misses in a row grew the wait to seconds and `zctl logs -f` found no
// channel for a job that had just started. The quick retries are bounded,
// and a channel that stayed open restores them.
func TestLogChannelRetriesAMisroutedChannelAtOnce(t *testing.T) {
	r := logChannelRetry{max: 5 * time.Second}
	for i := 0; i < logChannelMisses; i++ {
		if got := r.next(time.Second, true); got != logChannelFirstRetry {
			t.Fatalf("miss %d retried after %v, want %v", i, got, logChannelFirstRetry)
		}
	}
	if got := r.next(time.Second, true); got != logChannelFirstRetry {
		t.Fatalf("past the quick retries the first backoff is %v, want %v", got, logChannelFirstRetry)
	}
	if got := r.next(time.Second, true); got != 2*logChannelFirstRetry {
		t.Fatalf("past the quick retries misses back off: got %v", got)
	}
	r.next(time.Minute, false)
	if got := r.next(time.Second, true); got != logChannelFirstRetry {
		t.Fatalf("after a healthy channel a miss is retried after %v", got)
	}
}

// The agent recognizes the Gateway's own refusal, and nothing else, as a
// routing miss: errorclass marks it, and both sides use it.
func TestNotThisReplicaIsRecognized(t *testing.T) {
	if !errorclass.IsNotThisReplica(fmt.Errorf("recv log channel: %w", errorclass.NotThisReplica("ct-1"))) {
		t.Fatal("the Gateway's wrong-replica refusal is not recognized through a wrap")
	}
	for _, err := range []error{
		nil,
		errors.New("no connected agent for backend \"ct-1\""),
		// The same words from anyone else: the refusal is known by its
		// detail, never its message.
		status.Error(codes.Unavailable, "no connected agent for backend \"ct-1\""),
		status.Error(codes.Unavailable, "connection refused"),
		status.Error(codes.FailedPrecondition, "log channel epoch 1 does not match live 2"),
	} {
		if errorclass.IsNotThisReplica(err) {
			t.Fatalf("%v read as a wrong-replica refusal", err)
		}
	}
}
