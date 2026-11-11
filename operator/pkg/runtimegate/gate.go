// SPDX-License-Identifier: Apache-2.0

// Package runtimegate is the agent's "ready for work" gate: while the guest
// runtime is still being prepared (an installer on a machine, a Python
// environment in a container) the agent advertises max_jobs=-1 and refuses
// Submit as busy. Two marker files carry it, so an installer that is not the
// agent can drive it.
package runtimegate

import (
	"fmt"
	"os"
	"path/filepath"
)

// Env names that move the marker files; a container keeps them in its state dir.
const (
	EnvReadyFile   = "ZENTHROS_RUNTIME_READY_FILE"
	EnvPendingFile = "ZENTHROS_RUNTIME_PENDING_FILE"
)

const (
	defaultReadyFile   = "/var/lib/zenthros/runtime-ready"
	defaultPendingFile = "/var/lib/zenthros/runtime-pending"
)

// ReadyFile is where the ready marker lives.
func ReadyFile() string { return envOr(EnvReadyFile, defaultReadyFile) }

// PendingFile is where the pending marker lives.
func PendingFile() string { return envOr(EnvPendingFile, defaultPendingFile) }

// Ready reports whether the guest runtime is ready: the ready marker exists,
// or no pending marker does (nothing is being prepared).
func Ready() bool {
	if _, err := os.Stat(ReadyFile()); err == nil {
		return true
	}
	if _, err := os.Stat(PendingFile()); err == nil {
		return false
	}
	return true
}

// maxFailure bounds what a failure says: it travels in every heartbeat.
const maxFailure = 1024

// failureFile holds why the preparation failed, beside the marker that keeps
// the gate closed.
func failureFile() string { return PendingFile() + ".failed" }

// Failure is why the guest runtime could not be prepared, or "".
func Failure() string {
	said, err := os.ReadFile(failureFile())
	if err != nil {
		return ""
	}
	return string(said)
}

// MarkFailed says why the preparation failed. The gate stays closed: the
// machine takes no work, and its heartbeat says why.
func MarkFailed(why string) error {
	if len(why) > maxFailure {
		why = why[:maxFailure]
	}
	return os.WriteFile(failureFile(), []byte(why), 0o644)
}

// MarkPending closes the gate: work is refused until MarkReady.
func MarkPending() error {
	_ = os.Remove(ReadyFile())
	_ = os.Remove(failureFile())
	return touch(PendingFile())
}

// MarkReady opens the gate. The ready marker is written before the pending
// one is removed, so no reader sees neither and a pending one together.
func MarkReady() error {
	if err := touch(ReadyFile()); err != nil {
		return err
	}
	if err := os.Remove(PendingFile()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("runtimegate: clear pending marker: %w", err)
	}
	return nil
}

func touch(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("runtimegate: %w", err)
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		return fmt.Errorf("runtimegate: %w", err)
	}
	return nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
