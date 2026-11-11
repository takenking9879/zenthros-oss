// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/zenthros/operator/pkg/backend"
)

const (
	// followPollInterval is how often the follow streams' TerminalWatch checks
	// whether their units reached a terminal state.
	followPollInterval = 500 * time.Millisecond

	// followCloseGrace is the grace period after detecting termination before the
	// journalctl follow is cancelled, letting it flush the final lines (e.g. the
	// systemd "Deactivated" message).
	followCloseGrace = 500 * time.Millisecond
)

// LogStreamer streams journalctl output for a unit. It adapts the systemd journal
// to the backend.LogChunk channel interface the Executor.Logs contract requires.
type LogStreamer struct {
	cfg Config
	// watch tells a follow stream its unit ended so it can close. Nil
	// disables termination detection (the stream then follows until ctx ends).
	watch *TerminalWatch
}

// NewLogStreamer builds a LogStreamer for the given scope/privilege configuration.
// runner is used to detect unit termination on follow streams; pass nil to disable.
// Streams that share a host share a watch: NewWatchedLogStreamer.
func NewLogStreamer(cfg Config, runner Runner) *LogStreamer {
	if runner == nil {
		return &LogStreamer{cfg: cfg}
	}
	return NewWatchedLogStreamer(cfg, NewTerminalWatch(cfg, runner))
}

// NewWatchedLogStreamer builds a LogStreamer whose follow streams learn their
// units ended from watch (Manager.TerminalWatch).
func NewWatchedLogStreamer(cfg Config, watch *TerminalWatch) *LogStreamer {
	return &LogStreamer{cfg: cfg, watch: watch}
}

// Stream returns a channel of log chunks for the unit. If follow is true, the
// channel stays open until the unit terminates or ctx is cancelled. tailLines
// limits initial output (-1 = all).
func (s *LogStreamer) Stream(ctx context.Context, unit string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	args := s.buildArgs(unit, follow, tailLines)
	cmdCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(cmdCtx, "journalctl", args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("journalctl stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start journalctl: %w", err)
	}

	ch := make(chan backend.LogChunk, 64)
	go func() {
		defer close(ch)
		// journalctl is stopped before it is waited for: a reader that
		// stopped early left a following journalctl running, and Wait held
		// the stream open.
		defer func() {
			cancel()
			_ = cmd.Wait()
		}()
		s.pipeToChannel(ctx, stdout, ch)
	}()

	if follow {
		go s.followUntilTerminal(cmdCtx, cancel, unit)
	}
	return ch, nil
}

// followUntilTerminal cancels the journalctl follow once the unit reaches a
// terminal Zenthros phase (Succeeded/Failed). journalctl -f follows the journal
// indefinitely and has no "stop when the unit stops" option, so without this a
// follow stream would never close on job completion.
func (s *LogStreamer) followUntilTerminal(ctx context.Context, cancel context.CancelFunc, unit string) {
	if s.watch == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-s.watch.Ended(ctx, unit):
	}
	select {
	case <-time.After(followCloseGrace):
	case <-ctx.Done():
	}
	cancel()
}

func (s *LogStreamer) buildArgs(unit string, follow bool, tailLines int) []string {
	args := []string{"-u", unit, "--output=cat", "--no-pager"}
	if s.cfg.Scope == ScopeUser {
		args = append(args, "--user")
	}
	if follow {
		args = append(args, "-f")
	}
	if tailLines >= 0 {
		args = append(args, "-n", fmt.Sprintf("%d", tailLines))
	}
	return args
}

func (s *LogStreamer) pipeToChannel(ctx context.Context, r io.Reader, ch chan<- backend.LogChunk) {
	scanner := backend.LogLines(r)
	for scanner.Scan() {
		line := scanner.Text()
		stream := "stdout"
		if strings.HasPrefix(line, "<stderr>") {
			stream = "stderr"
			line = strings.TrimPrefix(line, "<stderr>")
		}
		chunk := backend.LogChunk{
			Stream:    stream,
			Data:      []byte(line + "\n"),
			Timestamp: time.Now(),
		}
		select {
		case ch <- chunk:
		case <-ctx.Done():
			return
		}
	}
}
