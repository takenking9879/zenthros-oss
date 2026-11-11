// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/metrics"
)

// LogServer streams job logs from the executor over the Agent-initiated
// LogChannel to the Gateway, on behalf of `zctl logs`. Nothing dials the agent:
// the channel is a second connection the agent opens, so log data does not
// share the command stream (architecture §28).
type LogServer struct {
	executor     Executor
	tracker      *jobTracker
	logger       logr.Logger
	maxLogBytes  int
	leftoverHint string
	hourly       *hourlyBudget
}

// NewLogServer builds a LogServer that streams logs from the given executor.
func NewLogServer(executor Executor, tracker *jobTracker, logger logr.Logger) *LogServer {
	return &LogServer{
		executor: executor,
		tracker:  tracker,
		logger:   logger.WithName("agent-log-server"),
	}
}

// logChunkStream is the Send+Context surface of the Agent-initiated
// LogChannel pull path.
type logChunkStream interface {
	Send(*agentpb.LogChunk) error
	Context() context.Context
}

func (s *LogServer) streamLogs(req *agentpb.StreamLogsRequest, stream logChunkStream) error {
	metrics.AgentLogStreamOpened()
	defer metrics.AgentLogStreamClosed()

	handle := req.GetHandleName()
	if handle == "" {
		return status.Error(codes.InvalidArgument, "handle_name is required")
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	ch, err := s.executor.Logs(ctx, handle, req.GetFollow(), int(req.GetTailLines()))
	if err != nil {
		metrics.RecordAgentExecutorError(metrics.OpLog)
		return fault.GRPC(fault.Wrap(fault.Unknown, fmt.Sprintf("open logs for %s", handle), err))
	}

	budget := s.maxLogBytes
	if budget <= 0 {
		budget = defaultMaxLogBytes
	}
	// Filter first (X10), then the per-request byte cap (X3). A new RPC is a
	// new filter window; leftover stays on the pod/unit.
	filter := lineFilterFor(req)
	sender := &cappedLogSender{server: s, stream: stream, budget: budget}
	var partial string
	for chunk := range ch {
		for _, line := range filter.FeedAll(takeLines(chunk.Data, &partial)) {
			out := chunk
			out.Data = []byte(line)
			hit, err := sender.send(out)
			if err != nil {
				return err
			}
			if hit {
				cancel()
				for range ch {
				}
				return nil
			}
		}
	}
	if partial != "" {
		hit, err := sender.sendAll(filter.Feed(partial))
		if err != nil || hit {
			return err
		}
	}
	_, err = sender.sendAll(filter.Flush())
	return err
}

type cappedLogSender struct {
	server *LogServer
	stream logChunkStream
	budget int
	sent   int
}

func (c *cappedLogSender) sendAll(lines []string) (bool, error) {
	for _, line := range lines {
		hit, err := c.send(backend.LogChunk{Stream: "stdout", Data: []byte(line)})
		if err != nil || hit {
			return hit, err
		}
	}
	return false, nil
}

func (c *cappedLogSender) send(chunk backend.LogChunk) (bool, error) {
	n := len(chunk.Data)
	remainReq := c.budget - c.sent
	if remainReq <= 0 {
		return true, c.stream.Send(c.server.truncatedChunk(c.budget))
	}
	allow := n
	hitRequest := false
	if remainReq < allow {
		allow = remainReq
		hitRequest = true
	}
	hitHourly := false
	if c.server.hourly != nil {
		taken := c.server.hourly.take(allow)
		if taken < allow {
			hitHourly = true
		}
		allow = taken
	}
	if allow < n {
		if allow > 0 {
			chunk.Data = chunk.Data[:allow]
			if err := c.stream.Send(toProtoLogChunk(chunk)); err != nil {
				return false, err
			}
			c.sent += allow
		}
		if hitHourly && !hitRequest {
			return true, c.stream.Send(c.server.hourlyTruncatedChunk())
		}
		return true, c.stream.Send(c.server.truncatedChunk(c.budget))
	}
	if err := c.stream.Send(toProtoLogChunk(chunk)); err != nil {
		return false, err
	}
	c.sent += n
	return false, nil
}

func leftoverHintFor(backendType string) string {
	switch backendType {
	case "k8s":
		return "pod (kubectl logs)"
	case "vm":
		return "unit (journalctl)"
	default:
		return "pod (kubectl logs) or unit (journalctl)"
	}
}

func (s *LogServer) truncatedChunk(capBytes int) *agentpb.LogChunk {
	hint := s.leftoverHint
	if hint == "" {
		hint = leftoverHintFor("")
	}
	return &agentpb.LogChunk{
		Stream:    "stderr",
		Data:      []byte(fmt.Sprintf("\noutput truncated at %d bytes; leftover is on the %s\n", capBytes, hint)),
		Truncated: true,
	}
}

func (s *LogServer) hourlyTruncatedChunk() *agentpb.LogChunk {
	hint := s.leftoverHint
	if hint == "" {
		hint = leftoverHintFor("")
	}
	limit := defaultHourlyLogBytes
	if s.hourly != nil && s.hourly.limit > 0 {
		limit = s.hourly.limit
	}
	return &agentpb.LogChunk{
		Stream:    "stderr",
		Data:      []byte(fmt.Sprintf("\noutput truncated at hourly log budget %d bytes; leftover is on the %s\n", limit, hint)),
		Truncated: true,
	}
}

func toProtoLogChunk(chunk backend.LogChunk) *agentpb.LogChunk {
	return &agentpb.LogChunk{
		Stream:        chunk.Stream,
		Data:          chunk.Data,
		TimestampUnix: chunk.Timestamp.Unix(),
	}
}
