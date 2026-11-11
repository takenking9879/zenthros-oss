// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"errors"

	"github.com/zenthros/operator/pkg/fault"
)

// ErrorClass partitions submission errors by retry semantics. It drives
// the reconciler's retry policy: transient errors back off and retry, permanent
// errors fail the job fast.
type ErrorClass int

const (
	// ClassTransient errors are worth retrying with backoff: the system is momentarily
	// unable to accept the job (busy, throttled, queue full, disconnected) but a later
	// attempt can succeed.
	ClassTransient ErrorClass = iota

	// ClassPermanent errors fail fast: retrying the same submission can never succeed
	// (invalid spec, unsupported kind, bad image, exhausted quota, admission rejected).
	ClassPermanent
)

// String renders the class for logging.
func (c ErrorClass) String() string {
	if c == ClassPermanent {
		return "permanent"
	}
	return "transient"
}

// Classify reports whether a Submit error is transient (retry with backoff) or
// permanent (fail fast): permanent when it carries a sentinel of this package
// that the kinds below (init) call a refusal, the terminal reasons plus
// ErrRejected, an explicit refusal by the backend that received the job.
// Errors carrying no such sentinel are treated as transient: a generic network or
// timeout failure should be retried, not surfaced as a terminal job failure.
func Classify(err error) ErrorClass {
	for _, sentinel := range sentinels {
		if !errors.Is(err, sentinel) {
			continue
		}
		if kind := fault.Of(sentinel); kind == fault.Refused || kind == fault.Unauthorized {
			return ClassPermanent
		}
	}
	return ClassTransient
}

// sentinels are every error this package defines for the retry policy.
var sentinels = []error{
	ErrSpecMismatch, ErrUnsupportedKind, ErrUnsupportedBackend,
	ErrBackendUnavailable, ErrDestinationUnavailable, ErrQuotaExceeded, ErrAdmissionRejected, ErrImagePull,
	ErrSecretSource, ErrRejected, ErrBackendBusy, ErrRateLimited, ErrQueueFull,
	ErrForbidden, ErrInvalid, ErrNotFound,
}

// HasSentinel reports whether err already carries one of this package's
// sentinels, so a later classifier does not override a more specific class.
func HasSentinel(err error) bool {
	for _, sentinel := range sentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// The kinds fault.Of gives this package's sentinels. A refusal the
// destination itself made (ErrNotFound, ErrInvalid) is Refused: retrying the
// same submission cannot succeed.
func init() {
	for _, refused := range []error{ErrSpecMismatch, ErrUnsupportedKind, ErrUnsupportedBackend,
		ErrSecretSource, ErrQuotaExceeded, ErrAdmissionRejected, ErrImagePull, ErrInvalid, ErrNotFound, ErrRejected} {
		fault.Register(refused, fault.Refused)
	}
	fault.Register(ErrForbidden, fault.Unauthorized)
	for _, transient := range []error{ErrBackendUnavailable, ErrDestinationUnavailable, ErrBackendBusy, ErrRateLimited, ErrQueueFull} {
		fault.Register(transient, fault.Transient)
	}
}
