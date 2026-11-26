// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "github.com/takenking9879/zenthros-oss/operator/internal/grpc/gen"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
	"github.com/takenking9879/zenthros-oss/operator/pkg/fault"
	"github.com/takenking9879/zenthros-oss/operator/pkg/metrics"
)

// LogServer streams job logs from the executor over the Agent-initiated
// LogChannel to the Gateway, on behalf of `zctl logs`. Nothing dials the agent:
// the channel is a second connection the agent opens, so log data does not
// share the command stream (architecture §28).
//
// A read has no cap of its own: it follows for as long as the job writes
// (P9). The machine's hourly budget bounds what all its reads send
// (channelLogSender); past it a read goes on from where the log is when the
// budget comes back, and says what it skipped.
type LogServer struct {
	executor Executor
	tracker  *jobTracker
	logger   logr.Logger
	readers  *logReaders
	// history is the history of a read that names none; 0 is
	// backend.DefaultHistoryBytes.
	history int64
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
	release, err := s.readers.take(handle)
	if err != nil {
		return err
	}
	defer release()

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	history := req.GetHistoryBytes()
	if history == 0 {
		history = s.history
	}
	ch, err := s.executor.Logs(ctx, handle, backend.LogRequest{
		Follow:       req.GetFollow(),
		TailLines:    int(req.GetTailLines()),
		HistoryBytes: history,
		After:        req.GetAfter(),
	})
	if errors.Is(err, backend.ErrNotFound) {
		// The machine no longer keeps it: NotFound, in the reason's words
		// alone (27).
		return fault.GRPC(fault.Wrap(fault.Absent, "", err))
	}
	if err != nil {
		metrics.RecordAgentExecutorError(metrics.OpLog)
		return fault.GRPC(fault.Wrap(fault.Unknown, fmt.Sprintf("open logs for %s", handle), err))
	}
	if !req.GetSummarize() {
		for chunk := range ch {
			if err := stream.Send(backend.ProtoLogChunk(chunk)); err != nil {
				return err
			}
		}
		return nil
	}
	// A summary is of lines: it has no cursor to go on from.
	filter := NewIntelligentLogFilter()
	var partial string
	first := true
	for chunk := range ch {
		for _, line := range filter.FeedAll(takeLines(chunk.Data, &partial)) {
			out := &agentpb.LogChunk{Stream: chunk.Stream, Data: []byte(line), TimestampUnix: chunk.Timestamp.Unix()}
			if first {
				out.Omitted, first = chunk.Omitted, false
			}
			if err := stream.Send(out); err != nil {
				return err
			}
		}
	}
	var lines []string
	if partial != "" {
		lines = filter.Feed(partial)
	}
	lines = append(lines, filter.Flush()...)
	for _, line := range lines {
		if err := stream.Send(&agentpb.LogChunk{Stream: "stdout", Data: []byte(line)}); err != nil {
			return err
		}
	}
	return nil
}

// Readers of one machine's logs at once: each one opens the job's file and
// an inotify instance (the kernel's default is 128 a user), and costs ~2
// mCPU while its job writes (W0). One job's readers take a share.
const (
	maxLogReaders       = 16
	maxLogReadersPerJob = 8
)

// logReaders counts the machine's reads, for every log channel session.
type logReaders struct {
	mu    sync.Mutex
	total int
	byJob map[string]int
}

func newLogReaders() *logReaders { return &logReaders{byJob: map[string]int{}} }

// take admits one more read of handle's log, or says why not.
func (r *logReaders) take(handle string) (release func(), err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.total >= maxLogReaders {
		return nil, status.Errorf(codes.ResourceExhausted, "this machine serves %d log reads at once, all in use", maxLogReaders)
	}
	if r.byJob[handle] >= maxLogReadersPerJob {
		return nil, status.Errorf(codes.ResourceExhausted, "this job's log is read %d times at once, the most a job may", maxLogReadersPerJob)
	}
	r.total++
	r.byJob[handle]++
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.total--
		if r.byJob[handle]--; r.byJob[handle] == 0 {
			delete(r.byJob, handle)
		}
	}, nil
}
