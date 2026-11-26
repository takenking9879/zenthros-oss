// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Detached is the text of a Gateway's end of a session whose machine lost
// its access (a retire: zenthros.delete, zenthros detach), and of the
// refusal a revoked machine gets when it registers again. People read it;
// the agent reads the refusal's reason (DetachedError), stops its jobs and
// exits for good (agent.ErrDetached): the machine is no longer the hub's,
// and nothing it runs can report.
const Detached = "this machine was detached from the hub"

// refusalDomain is the domain of the ErrorInfo a Gateway's refusal carries.
const refusalDomain = "zenthros.io"

const (
	reasonDetached    = "DETACHED"
	reasonGatewayFull = "GATEWAY_FULL"
)

// DetachedError is the refusal that ends a machine's place on the hub:
// PermissionDenied, with the Detached text and the DETACHED reason.
func DetachedError(cause string) error {
	return refusal(codes.PermissionDenied, Detached+": "+cause, reasonDetached)
}

// RefusedDetached reports whether err ends the machine's place on the hub.
// Only the reason says so: the agent stops every job and exits on it, and
// read from the text, any error that quoted it did that (c4-E7).
func RefusedDetached(err error) bool {
	return refusedFor(err, reasonDetached)
}

func refusal(code codes.Code, message, reason string) error {
	refused, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{Domain: refusalDomain, Reason: reason})
	if err != nil {
		// WithDetails fails only for codes.OK.
		panic(err)
	}
	return refused.Err()
}

func refusedFor(err error, reason string) bool {
	refused, ok := status.FromError(err)
	if !ok {
		return false
	}
	for _, detail := range refused.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetDomain() == refusalDomain && info.GetReason() == reason {
			return true
		}
	}
	return false
}
