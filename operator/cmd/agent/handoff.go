// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/zenthros/operator/pkg/containerboot"
)

// The secrets a container's agent is launched with must leave its
// environment. The environ a process was exec'd with stays readable in
// /proc/<pid>/environ by every process of its user, the container's jobs
// included, whatever os.Unsetenv does later. PID 1
// re-execs itself without them and hands them to the agent proper over a
// pipe.

// envHandoffFD names the descriptor the secrets are read from.
const envHandoffFD = "ZENTHROS_HANDOFF_FD"

// handedOff is what leaves the environment: the one-time join token, the
// tailnet auth key and the presigned artifact URLs.
var handedOff = []string{"ZENTHROS_JOIN_TOKEN", EnvTailscaleAuthKey, containerboot.EnvArtifactURLs}

// maxHandoff keeps the payload inside a pipe's buffer, written before
// anything reads it.
const maxHandoff = 60 << 10

// splitHandoff returns environ without the handed-off variables (and
// without a previous handoff's descriptor), and those variables.
func splitHandoff(environ []string) (clean, secrets []string) {
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		switch {
		case name == envHandoffFD:
		case slices.Contains(handedOff, name):
			secrets = append(secrets, kv)
		default:
			clean = append(clean, kv)
		}
	}
	return clean, secrets
}

// handoffPipe returns the read end of a pipe that holds secrets.
func handoffPipe(secrets []string) (*os.File, error) {
	payload := strings.Join(secrets, "\x00")
	if len(payload) > maxHandoff {
		return nil, fmt.Errorf("%d bytes of secrets do not fit a pipe", len(payload))
	}
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	_, err = w.WriteString(payload)
	if closeErr := w.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// receiveHandoff reads the secrets the process was handed, and closes the
// descriptor. Without one it returns nothing.
func receiveHandoff() ([]string, error) {
	raw := os.Getenv(envHandoffFD)
	if raw == "" {
		return nil, nil
	}
	_ = os.Unsetenv(envHandoffFD)
	fd, err := strconv.Atoi(raw)
	if err != nil || fd < 3 {
		return nil, fmt.Errorf("%s=%q is not a descriptor", envHandoffFD, raw)
	}
	f := os.NewFile(uintptr(fd), "handoff")
	defer f.Close()
	payload, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read the handed-off secrets: %w", err)
	}
	if len(payload) == 0 {
		return nil, nil
	}
	return strings.Split(string(payload), "\x00"), nil
}

// scrubInit makes PID 1's environ clean and returns it with the secrets it
// took out. The first start re-execs itself (the PID stays 1) with them in
// a pipe; the second reads them back. If the re-exec fails the agent still
// runs, its environ as it was.
func scrubInit(argv []string) (clean, secrets []string, err error) {
	secrets, err = receiveHandoff()
	clean, found := splitHandoff(os.Environ())
	if len(found) == 0 {
		return clean, secrets, err
	}
	r, err := handoffPipe(found)
	if err != nil {
		return clean, found, err
	}
	// The descriptor must outlive the exec.
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, r.Fd(), syscall.F_SETFD, 0); errno != 0 {
		r.Close()
		return clean, found, errno
	}
	err = syscall.Exec(argv[0], argv, append(clean, envHandoffFD+"="+strconv.Itoa(int(r.Fd()))))
	r.Close()
	return clean, found, errors.Join(errors.New("re-exec without the secrets"), err)
}

// applyHandoff sets the handed-off secrets in this process's environment,
// which /proc/<pid>/environ does not show.
func applyHandoff() error {
	secrets, err := receiveHandoff()
	for _, kv := range secrets {
		if name, value, ok := strings.Cut(kv, "="); ok {
			if setErr := os.Setenv(name, value); setErr != nil && err == nil {
				err = setErr
			}
		}
	}
	return err
}
