// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"errors"
	"fmt"
)

// Sentinel errors returned across the ExecutionBackend contract.
var (
	// ErrSpecMismatch indicates the submitted spec differs from the live resource.
	// The adapter rejects rather than silently replacing; the operator decides.
	ErrSpecMismatch = errors.New("spec mismatch")

	// ErrUnsupportedKind indicates the workload kind is not handled by the backend.
	ErrUnsupportedKind = errors.New("unsupported workload kind")

	// ErrUnsupportedBackend indicates a valid workload cannot run on this backend type.
	ErrUnsupportedBackend = errors.New("unsupported backend")

	// ErrBackendUnavailable indicates the registry says the target backend has
	// no agent connected (or nobody could tell, with observe.ErrUnknown).
	ErrBackendUnavailable = errors.New("backend unavailable")

	// ErrDestinationUnavailable indicates the backend's agent is connected
	// and the destination behind it answered that it cannot take the call
	// now (a remote cluster's 429, 5xx or timeout). Transient: the job waits
	// within the submit budget and never fails fast on it.
	ErrDestinationUnavailable = errors.New("destination unavailable")

	// ErrQuotaExceeded indicates the submission was rejected because a resource
	// quota (CPU, memory, GPU, or object count) would be exceeded.
	ErrQuotaExceeded = errors.New("quota exceeded")

	// ErrAdmissionRejected indicates a validating/mutating admission webhook
	// rejected the resource creation.
	ErrAdmissionRejected = errors.New("admission rejected")

	// ErrImagePull indicates the workload image could not be pulled (missing,
	// auth failure, or registry unreachable).
	ErrImagePull = errors.New("image pull failed")

	// ErrSecretSource indicates a permanent, redacted secret reference failure.
	ErrSecretSource = errors.New("secret source error")

	// ErrRejected indicates the backend received the submission and refused it
	// for a reason with no sentinel of its own (for example, the agent could not
	// materialize the job). It is permanent. An error that never reached the
	// backend (timeout, transport, reconnect) is not ErrRejected.
	ErrRejected = errors.New("rejected by backend")

	// ErrBackendBusy indicates the target backend is healthy but at capacity
	// (its agent reports active jobs at or above its advertised maximum). The
	// submission is refused transiently; the caller requeues rather than failing.
	ErrBackendBusy = errors.New("backend busy")

	// ErrRateLimited indicates the submission was throttled by a controller-side
	// concurrency limit. Transient; the caller requeues.
	ErrRateLimited = errors.New("rate limited")

	// ErrQueueFull indicates the controller's submission queue is at depth and
	// cannot accept another waiting submission. Transient; the caller requeues.
	ErrQueueFull = errors.New("queue full")

	// ErrForbidden indicates the destination refused the call for lack of
	// permission (for example the agent's RBAC). Permanent: retrying with the
	// same identity cannot succeed.
	ErrForbidden = errors.New("forbidden")

	// ErrInvalid indicates the destination rejected the workload as invalid
	// (schema validation, a validating admission policy). Permanent.
	ErrInvalid = errors.New("invalid")

	// ErrNotFound indicates the workload or an object it references does not
	// exist on the destination. Permanent on Submit; on Status it means the
	// workload is gone and may be submitted again.
	ErrNotFound = errors.New("not found")
)

// classified keeps the destination's error text exactly and still answers
// errors.Is for the sentinel that drives retry policy. Zenthros is an
// intermediary: the message a user reads is the one the destination wrote.
type classified struct {
	class error
	cause error
}

func (e *classified) Error() string   { return e.cause.Error() }
func (e *classified) Unwrap() []error { return []error{e.class, e.cause} }

// Classified marks cause with class without changing cause's text. A nil
// cause returns nil.
func Classified(class, cause error) error {
	if cause == nil {
		return nil
	}
	return &classified{class: class, cause: cause}
}

// SecretSourceError carries a stable reason code without secret material.
type SecretSourceError struct {
	Code   string
	Detail string
}

func (e *SecretSourceError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

func (e *SecretSourceError) Unwrap() error {
	return ErrSecretSource
}

// NewSecretSourceError creates a redacted permanent submission error.
func NewSecretSourceError(code, detail string) *SecretSourceError {
	return &SecretSourceError{Code: code, Detail: detail}
}

// SecretSourceReason extracts the stable reason code from a local typed error.
func SecretSourceReason(err error) (string, bool) {
	var secretErr *SecretSourceError
	if !errors.As(err, &secretErr) {
		return "", false
	}
	return secretErr.Code, true
}

// SpecMismatchError carries the field that diverged, for actionable status reasons.
type SpecMismatchError struct {
	Field    string
	Expected string
	Actual   string
}

func (e *SpecMismatchError) Error() string {
	return fmt.Sprintf("%s: field %q expected %q, got %q", ErrSpecMismatch, e.Field, e.Expected, e.Actual)
}

func (e *SpecMismatchError) Unwrap() error {
	return ErrSpecMismatch
}

// NewSpecMismatchError builds a SpecMismatchError for the given field.
func NewSpecMismatchError(field, expected, actual string) *SpecMismatchError {
	return &SpecMismatchError{Field: field, Expected: expected, Actual: actual}
}
