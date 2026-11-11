// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zenthros/operator/pkg/backend"
)

const (
	logPollInterval = 250 * time.Millisecond
	logChunkBytes   = 32 << 10
)

// Logs streams the job's output file (stdout and stderr together, as the
// process wrote them). tailLines limits the start to the last lines (-1 is
// all). With follow, the stream stays open until the job ends and its last
// bytes are sent, or ctx ends.
func (e *Executor) Logs(ctx context.Context, handle string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	path := filepath.Join(e.jobDir(handle), logFile)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, backend.Classified(backend.ErrNotFound, err)
		}
		return nil, err
	}
	if tailLines >= 0 {
		if _, err := f.Seek(tailOffset(f, tailLines), io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	e.mu.Lock()
	j := e.jobs[handle]
	e.mu.Unlock()
	out := make(chan backend.LogChunk, 16)
	go func() {
		defer close(out)
		defer f.Close()
		buf := make([]byte, logChunkBytes)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				chunk := backend.LogChunk{Stream: "stdout", Data: bytes.Clone(buf[:n]), Timestamp: time.Now()}
				select {
				case out <- chunk:
				case <-ctx.Done():
					return
				}
				continue
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return
			}
			if !follow || j == nil || finished(j) {
				// A finished job wrote everything before it finished: one
				// more read above already returned EOF after its last bytes.
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-j.done:
			case <-time.After(logPollInterval):
			}
		}
	}()
	return out, nil
}

func finished(j *job) bool {
	select {
	case <-j.done:
		return true
	default:
		return false
	}
}

// tailOffset is where the last n lines of f begin.
func tailOffset(f *os.File, n int) int64 {
	info, err := f.Stat()
	if err != nil {
		return 0
	}
	size := info.Size()
	if n == 0 {
		return size
	}
	const block = 4096
	var (
		pos   = size
		lines = 0
		buf   = make([]byte, block)
	)
	// A trailing newline ends the last line; it does not start another.
	skipLast := true
	for pos > 0 {
		readSize := int64(block)
		if pos < readSize {
			readSize = pos
		}
		pos -= readSize
		if _, err := f.ReadAt(buf[:readSize], pos); err != nil && !errors.Is(err, io.EOF) {
			return 0
		}
		for i := readSize - 1; i >= 0; i-- {
			if buf[i] != '\n' {
				skipLast = false
				continue
			}
			if skipLast {
				skipLast = false
				continue
			}
			lines++
			if lines == n {
				return pos + i + 1
			}
		}
	}
	return 0
}

// tail is the last n lines of path, joined by " | ", for a failure message.
func tail(path string, n int) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	if _, err := f.Seek(tailOffset(f, n), io.SeekStart); err != nil {
		return ""
	}
	raw, err := io.ReadAll(io.LimitReader(f, 4096))
	if err != nil {
		return ""
	}
	var lines []string
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, " | ")
}

// copyCapped copies the job's output into its log until max bytes, then
// says the log was cut and drops the rest, still draining the pipe so the
// job never blocks on a full one.
func copyCapped(dst io.Writer, src io.Reader, max int64) {
	written, _ := io.Copy(dst, io.LimitReader(src, max))
	if written < max {
		return
	}
	var probe [1]byte
	if n, _ := io.ReadFull(src, probe[:]); n == 0 {
		return // exactly max, and nothing after it
	}
	fmt.Fprintf(dst, "\n[zenthros: log truncated at %d bytes; later output dropped]\n", max)
	_, _ = io.Copy(io.Discard, src)
}
