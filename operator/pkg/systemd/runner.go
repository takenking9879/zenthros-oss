// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// Runner executes shell commands on the host that owns systemd. The agent uses a
// local in-process runner; tests inject a fake. (The SSH-direct M1 path keeps its
// own remote transport and does not use this interface.)
type Runner interface {
	// Run executes a shell command and returns its stdout. A non-zero exit is an
	// error carrying the exit code and stderr.
	Run(ctx context.Context, cmd string) (string, error)
}

// InputRunner is a Runner that can also feed a command's standard input.
// Data that can be large goes that way: a command line is one argument to
// bash, and Linux refuses any argument over 128 KiB (MAX_ARG_STRLEN).
type InputRunner interface {
	Runner
	RunInput(ctx context.Context, cmd string, stdin io.Reader) (string, error)
}

// ExitError reports a command that ran but exited non-zero.
type ExitError struct {
	// Command is the command line that was run.
	Command string
	// Stderr is the captured standard error.
	Stderr string
	// Code is the process exit code.
	Code int
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("command %q exited with code %d: %s", e.Command, e.Code, strings.TrimSpace(e.Stderr))
}
