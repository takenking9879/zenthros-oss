// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"fmt"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
)

// The agent sends a secret refusal's reason in its own field, so the
// controller never reads it out of the text (c4-E7).
func TestASecretRefusalCarriesItsReason(t *testing.T) {
	refused := fmt.Errorf("resolve secrets: %w", backend.NewSecretSourceError(v1.ReasonSecretAccessDenied, "role cannot read workload/token"))
	resp := submitFailure("req-1", refused)
	if resp.GetErrorClass() != agentpb.ErrorClass_INVALID_SECRET_SOURCE || resp.GetErrorReason() != v1.ReasonSecretAccessDenied {
		t.Fatalf("class %s reason %q, want INVALID_SECRET_SOURCE %s", resp.GetErrorClass(), resp.GetErrorReason(), v1.ReasonSecretAccessDenied)
	}
	if other := submitFailure("req-2", fmt.Errorf("start unit: %s: quoted", v1.ReasonSecretNotFound)); other.GetErrorReason() != "" {
		t.Fatalf("an error that only quotes a reason sends reason %q", other.GetErrorReason())
	}
}

// An error the agent cannot classify goes UNSPECIFIED, which the controller
// retries within the submit budget (U2). Sent as REJECTED, a dbus timeout
// starting the unit failed the job for good (c4-A2).
func TestAnUnclassifiedSubmitErrorIsSentRetryable(t *testing.T) {
	resp := submitFailure("req-1", errors.New("start unit: dbus: connection timed out"))
	if resp.GetErrorClass() != agentpb.ErrorClass_UNSPECIFIED {
		t.Fatalf("class %s, want UNSPECIFIED", resp.GetErrorClass())
	}
}
