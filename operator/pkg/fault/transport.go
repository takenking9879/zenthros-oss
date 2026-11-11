// SPDX-License-Identifier: Apache-2.0

package fault

import (
	"errors"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type grpcStatus interface{ GRPCStatus() *status.Status }

// Forbidden reports a refusal of a caller who is known but may not do it (a
// 403, gRPC PermissionDenied), apart from one whose credential is missing or
// was refused (a 401, Unauthenticated): Of says Unauthorized of both, but
// only the second is fixed by signing in again.
func Forbidden(err error) bool {
	if apierrors.IsForbidden(err) {
		return true
	}
	var carrier grpcStatus
	return errors.As(err, &carrier) && carrier.GRPCStatus().Code() == codes.PermissionDenied
}

func ofGRPC(err error) (Kind, bool) {
	var carrier grpcStatus
	if !errors.As(err, &carrier) {
		return Unknown, false
	}
	switch carrier.GRPCStatus().Code() {
	case codes.OK:
		return Unknown, false
	case codes.Unavailable, codes.ResourceExhausted:
		return Transient, true
	case codes.Aborted:
		return Conflict, true
	case codes.AlreadyExists:
		return Exists, true
	case codes.NotFound:
		return Absent, true
	case codes.InvalidArgument, codes.OutOfRange:
		return Invalid, true
	case codes.FailedPrecondition, codes.Unimplemented:
		return Refused, true
	case codes.PermissionDenied, codes.Unauthenticated:
		return Unauthorized, true
	case codes.Canceled:
		return Canceled, true
	default: // DeadlineExceeded, Internal, Unknown, DataLoss
		return Unknown, true
	}
}

// GRPC is the only way a server turns an error into a gRPC status: the code
// says what the caller may do, and the message is the error's. A nil err
// stays nil; an error that already is a status keeps its code.
func GRPC(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown && !wrapsKind(err) {
		return err
	}
	return status.Error(grpcCode(Of(err)), err.Error())
}

// wrapsKind reports whether err carries an explicit kind or a registered
// sentinel, which decide over a status code underneath.
func wrapsKind(err error) bool {
	if e := (*Error)(nil); errors.As(err, &e) {
		return true
	}
	_, ok := ofSentinel(err)
	return ok
}

func grpcCode(k Kind) codes.Code {
	switch k {
	case Conflict:
		return codes.Aborted
	case Exists:
		return codes.AlreadyExists
	case Absent:
		return codes.NotFound
	case Refused:
		return codes.FailedPrecondition
	case Invalid:
		return codes.InvalidArgument
	case Unauthorized:
		return codes.PermissionDenied
	case Canceled:
		return codes.Canceled
	default: // Unknown, Transient: the caller retries.
		return codes.Unavailable
	}
}

// HTTPStatus is the only way a handler picks a status code from an error.
// Anything nobody classified is 503: the client retries, and never reads a
// store outage as its own mistake.
func HTTPStatus(err error) int {
	switch Of(err) {
	case Invalid:
		return http.StatusBadRequest
	case Refused:
		return http.StatusUnprocessableEntity
	case Unauthorized:
		return http.StatusForbidden
	case Absent:
		return http.StatusNotFound
	case Conflict, Exists:
		return http.StatusConflict
	case Canceled:
		return 499 // client closed request
	default:
		return http.StatusServiceUnavailable
	}
}

// FromHTTP classifies a response a third party sent (a cloud API, a
// registry): 404 is Absent; 401 and 403 Unauthorized; 409 Exists for a
// create and Conflict otherwise; 408, 429 and 5xx Transient, but Unknown for
// a create, which the server may have carried out; 400 and 422 Invalid;
// other 4xx Refused. err is
// the cause kept for the message (the body, the status line).
func FromHTTP(code int, create bool, err error) error {
	if err == nil {
		err = errors.New(strings.ToLower(http.StatusText(code)))
	}
	return &Error{Kind: httpKind(code, create), Err: err}
}

// TransientHTTP reports an HTTP error answer that a read, or a call that may
// be sent again, asks again: 408, 429 and any 5xx (FromHTTP). The one table
// for every client of an HTTP server.
func TransientHTTP(code int) bool {
	return code >= 400 && httpKind(code, false) == Transient
}

func httpKind(code int, create bool) Kind {
	switch {
	case code == http.StatusNotFound:
		return Absent
	case code == http.StatusUnauthorized, code == http.StatusForbidden:
		return Unauthorized
	case code == http.StatusConflict:
		if create {
			return Exists
		}
		return Conflict
	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests, code >= 500:
		if create && code != http.StatusTooManyRequests && code != http.StatusServiceUnavailable {
			return Unknown
		}
		return Transient
	case code == http.StatusBadRequest, code == http.StatusUnprocessableEntity:
		return Invalid
	case code >= 400:
		return Refused
	}
	return Unknown
}

// FromSQL classifies a database error by its SQLSTATE class: connection
// (08), transaction rollback (40), resources (53) and operator intervention
// (57) are Transient; data (22) and integrity (23) are Refused, a fault of
// the request, not of the pool. An error without a SQLSTATE is Unknown.
func FromSQL(err error) error {
	if err == nil {
		return nil
	}
	var coded interface{ SQLState() string }
	if !errors.As(err, &coded) || len(coded.SQLState()) < 2 {
		return &Error{Kind: Unknown, Err: err}
	}
	switch coded.SQLState()[:2] {
	case "08", "40", "53", "57":
		return &Error{Kind: Transient, Err: err}
	case "22", "23":
		return &Error{Kind: Refused, Err: err}
	}
	return &Error{Kind: Unknown, Err: err}
}
