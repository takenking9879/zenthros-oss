// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/zenthros/operator/pkg/backend"
)

// Logs reads the job's log file (stdout and stderr together, as the process
// wrote them) as req asks: with Follow, until the job ends and its last bytes
// are sent, or ctx ends. A job this session did not start ended before it.
func (e *Executor) Logs(ctx context.Context, handle string, req backend.LogRequest) (<-chan backend.LogChunk, error) {
	f, err := e.logs.File(filepath.Join(e.jobDir(handle), logFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, backend.Classified(backend.ErrNotFound, err)
	}
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	j := e.jobs[handle]
	e.mu.Unlock()
	if j == nil {
		return f.Read(ctx, req, endedBefore)
	}
	return f.Read(ctx, req, j.done)
}

// endedBefore is the end of a job this session did not start.
var endedBefore = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// logTail is the last n lines of the job's log, for a failure message.
func (e *Executor) logTail(handle string, n int) string {
	f, err := e.logs.File(filepath.Join(e.jobDir(handle), logFile))
	if err != nil {
		return ""
	}
	return f.Tail(n)
}
