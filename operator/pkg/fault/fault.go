// SPDX-License-Identifier: Apache-2.0

// Package fault is the one table of what an error means for the caller: retry
// now, retry later, read back, give up, or ask a human. Every component used
// to classify errors with its own switch (backend.Classify, the wire
// classes, the provider classes, a gRPC code picked per handler), and a
// transient failure read as permanent in one of them failed jobs that a
// retry would have run.
//
// Of is the table. GRPC, HTTPStatus and Retryable are the only ways out of
// it; FromHTTP and FromSQL feed it from the two transports that carry no Go
// error type. A domain package declares its sentinels' kinds once, with
// Register, in its own init.
package fault

import (
	"context"
	"errors"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Kind is what an error means for whoever must decide the next step.
type Kind uint8

const (
	// Unknown: the outcome is not known (a timeout after the request was
	// sent, an error nobody classified). Read back before acting again;
	// retry within the transient budget.
	Unknown Kind = iota
	// Transient: nothing happened, and a later attempt may succeed (429,
	// 503, a refused connection, a busy backend).
	Transient
	// Conflict: an optimistic lock was lost. Read again and retry now.
	Conflict
	// Exists: the object is already there. Read it and check whose it is
	// (CreateOwned).
	Exists
	// Absent: the thing does not exist, as an authoritative read says. A
	// cache's NotFound is not one: confirm it with a live read (an API
	// reader, the provider's own describe) before acting on it.
	Absent
	// Refused: permanent. Unsupported, forbidden by policy, or a state the
	// request does not fit; retrying the same request can never succeed.
	Refused
	// Unauthorized: a credential or an identity was refused. A human acts.
	Unauthorized
	// Canceled: the caller's own context ended. It says nothing about the
	// callee.
	Canceled
	// Invalid: the request itself is malformed (a bad argument, a CSR that
	// does not parse). Permanent, like Refused, and the caller's to fix:
	// InvalidArgument, HTTP 400.
	Invalid
)

var kindNames = [...]string{"unknown", "transient", "conflict", "exists", "absent", "refused", "unauthorized", "canceled", "invalid"}

func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "unknown"
}

// Error is an error with its kind. It keeps the cause: Unwrap returns it, so
// errors.Is and errors.As still see every sentinel underneath.
type Error struct {
	Kind Kind
	// Op names what was being done, for the message; it may be empty.
	Op  string
	Err error
}

func (e *Error) Error() string {
	if e.Op == "" {
		return e.Err.Error()
	}
	return e.Op + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// Wrap gives err the kind k. A nil err stays nil.
func Wrap(k Kind, op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: k, Op: op, Err: err}
}

var (
	registryMu sync.RWMutex
	registry   []registered
)

type registered struct {
	sentinel error
	kind     Kind
}

// Register declares the kind of a domain sentinel. Call it from the
// sentinel's own package, at init. The first sentinel registered that an
// error wraps decides its kind.
func Register(sentinel error, k Kind) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = append(registry, registered{sentinel: sentinel, kind: k})
}

// Sentinels returns the registered sentinels, in registration order, for
// tests that check a family is fully mapped.
func Sentinels() map[error]Kind {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make(map[error]Kind, len(registry))
	for _, r := range registry {
		out[r.sentinel] = r.kind
	}
	return out
}

// Of is the table: the kind of err. In order: an explicit *Error, the
// caller giving up, a registered domain sentinel, a deadline, a Kubernetes
// API status, a gRPC status, and Unknown for the rest. A sentinel goes before
// the transports and the deadline because it is a decision someone made
// about this very error: a wire error carries both its sentinel and a
// generic Internal code, and a provider read its client timed out is
// Retryable with the deadline as its cause.
func Of(err error) Kind {
	if err == nil {
		return Unknown
	}
	if e := (*Error)(nil); errors.As(err, &e) {
		return e.Kind
	}
	if errors.Is(err, context.Canceled) {
		return Canceled
	}
	if k, ok := ofSentinel(err); ok {
		return k
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Unknown
	}
	if k, ok := ofAPIStatus(err); ok {
		return k
	}
	if k, ok := ofGRPC(err); ok {
		return k
	}
	return Unknown
}

func ofSentinel(err error) (Kind, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	for _, r := range registry {
		if errors.Is(err, r.sentinel) {
			return r.kind, true
		}
	}
	return Unknown, false
}

func ofAPIStatus(err error) (Kind, bool) {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return Unknown, false
	}
	switch {
	case apierrors.IsConflict(err):
		return Conflict, true
	case apierrors.IsAlreadyExists(err):
		return Exists, true
	case apierrors.IsNotFound(err), apierrors.IsGone(err):
		return Absent, true
	case apierrors.IsUnauthorized(err), apierrors.IsForbidden(err):
		return Unauthorized, true
	case apierrors.IsInvalid(err), apierrors.IsBadRequest(err):
		return Invalid, true
	case apierrors.IsMethodNotSupported(err), apierrors.IsNotAcceptable(err), apierrors.IsUnsupportedMediaType(err),
		apierrors.IsRequestEntityTooLargeError(err):
		return Refused, true
	case apierrors.IsTooManyRequests(err), apierrors.IsServiceUnavailable(err), apierrors.IsInternalError(err),
		apierrors.IsUnexpectedServerError(err):
		return Transient, true
	case apierrors.IsTimeout(err), apierrors.IsServerTimeout(err):
		// The server may have done it.
		return Unknown, true
	}
	return Unknown, true
}

// Retryable reports whether trying again may succeed: Unknown, Transient
// and Conflict. Unknown retries only after reading back what happened.
func Retryable(err error) bool {
	switch Of(err) {
	case Unknown, Transient, Conflict:
		return true
	}
	return false
}
