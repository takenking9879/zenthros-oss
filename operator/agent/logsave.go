// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/takenking9879/zenthros-oss/operator/agent/logpersist"
	v1 "github.com/takenking9879/zenthros-oss/operator/api/v1"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
	"github.com/takenking9879/zenthros-oss/operator/pkg/checkpoint"
	"github.com/takenking9879/zenthros-oss/operator/pkg/fault"
	"github.com/takenking9879/zenthros-oss/operator/pkg/jobspec"
	"github.com/takenking9879/zenthros-oss/operator/pkg/objectput"
)

// A job with spec.workload.template.logs has its whole log saved to the
// user's bucket by the agent (agent/logpersist), from its Submit to its end,
// across agent restarts: the queue is under Config.LogQueueDir.

// logDrain bounds what a stopping agent spends sending the queue: its unit
// gives it 30 s (TimeoutStopSec) before SIGKILL.
const logDrain = 25 * time.Second

// LogCredentialer is an executor that resolves a job's log credentials on
// the machine, as it resolves the job's own secrets, for the agent alone:
// the job's environment never gets them.
type LogCredentialer interface {
	LogCredentials(ctx context.Context, spec jobspec.JobSpec) (map[string]string, error)
}

// newLogSaver is the agent's saver of logs, nil when it keeps no queue (an
// agent in a test).
func newLogSaver(cfg Config, executor Executor, logger Logger) *logpersist.Saver {
	if cfg.LogQueueDir == "" {
		return nil
	}
	saver, err := logpersist.New(logpersist.Config{
		Dir:    cfg.LogQueueDir,
		Source: logSource{executor},
		Open: func(ctx context.Context, job logpersist.Job) (logpersist.Store, error) {
			return openLogStore(ctx, executor, job)
		},
		Logger: logger.WithName("log-saver"),
	})
	if err != nil {
		logger.Error(err, "jobs' logs cannot be saved to their buckets here")
		return nil
	}
	return saver
}

// logSource is the executor as a saved log's reader.
type logSource struct{ executor Executor }

func (s logSource) Logs(ctx context.Context, handle string, req backend.LogRequest) (<-chan backend.LogChunk, error) {
	return s.executor.Logs(ctx, handle, req)
}

// Ended is the executor's word on the job: its end, or the workload gone.
func (s logSource) Ended(ctx context.Context, handle string) bool {
	state, err := s.executor.Status(ctx, handle)
	if err != nil {
		return errors.Is(err, backend.ErrNotFound) || fault.Of(err) == fault.Absent
	}
	return state.Phase.Terminal()
}

// openLogStore is the job's bucket, with the credentials its executor
// resolves here, or the machine's own.
func openLogStore(ctx context.Context, executor Executor, job logpersist.Job) (logpersist.Store, error) {
	var creds map[string]string
	if len(job.Logs.Credentials) > 0 {
		resolver, ok := executor.(LogCredentialer)
		if !ok {
			return nil, fault.Wrap(fault.Refused, "credentials", errors.New("this machine resolves no credentials for a log's bucket"))
		}
		var err error
		creds, err = resolver.LogCredentials(ctx, jobspec.JobSpec{ID: job.ID, Namespace: job.Namespace, SecretSources: job.Logs.Credentials})
		if err != nil {
			if _, ok := backend.SecretSourceReason(err); ok {
				// A reference that does not resolve here never will.
				return nil, fault.Wrap(fault.Refused, "credentials", err)
			}
			return nil, err
		}
	}
	return objectput.Open(ctx, objectput.Config{Destination: job.Logs.Destination, Endpoint: job.Logs.Endpoint,
		Region: job.Logs.Region, CAData: []byte(job.Logs.CAData), Credentials: creds})
}

// saveLog starts saving the log of the job just submitted under handle.
func (a *Agent) saveLog(spec jobspec.JobSpec, handle string) {
	if spec.Logs == nil || a.logs == nil {
		return
	}
	generation, _ := strconv.ParseInt(spec.Env[checkpoint.EnvGeneration], 10, 64)
	job := logpersist.Job{ID: spec.ID, Namespace: spec.Namespace, Name: spec.Name, UID: spec.OwnerUID,
		Generation: generation, Attempt: spec.Attempt, Handle: handle, Logs: *spec.Logs}
	if err := a.logs.Start(job); err != nil {
		a.logger.Error(err, "the job's log cannot be saved to its bucket", "job_id", spec.ID, "handle", handle)
	}
}

// withLogProblem adds to state why the job's log is not saved whole.
func (a *Agent) withLogProblem(handle string, state backend.JobState) backend.JobState {
	if a.logs == nil {
		return state
	}
	if reason := a.logs.Problem(handle); reason != nil {
		state.Reasons = append(append([]v1.Reason(nil), state.Reasons...), *reason)
	}
	return state
}

// decorate adds to the executor's state what the agent knows beyond it: a
// Service's endpoints, whether the job's log is saved whole, and an end
// held while its outputs are saved (withOutputs).
func (a *Agent) decorate(ctx context.Context, handle string, state backend.JobState) backend.JobState {
	return a.withLogProblem(handle, a.withOutputs(handle, a.withEndpoints(ctx, handle, state)))
}

// drainLogs sends what waits in the queue as the agent stops.
func (a *Agent) drainLogs() {
	if a.logs == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), logDrain)
	defer cancel()
	a.logs.Drain(ctx)
}

// reasonsKey is the codes of state's reasons, empty without any: a reason
// that comes or goes is pushed even when the phase stays.
func reasonsKey(state backend.JobState) string {
	if len(state.Reasons) == 0 {
		return ""
	}
	key := "|reasons="
	for _, r := range state.Reasons {
		key += r.Code + ";"
	}
	return key
}
