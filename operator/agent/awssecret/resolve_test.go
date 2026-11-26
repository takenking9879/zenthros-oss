// SPDX-License-Identifier: Apache-2.0

package awssecret

import (
	"net/http"
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// An AWS answer is asked again when its status is a moment's (408, 429,
// 5xx) or its code is throttling: as every caller of an HTTP answer decides
// (fault.TransientHTTP). A 409 is the answer, not a moment's: asked again it
// is the same (R4 ronda 5, M3).
func TestAnAWSAnswerIsAskedAgainOnlyWhenItIsAMoments(t *testing.T) {
	answer := func(status int, code string) error {
		return &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      &smithy.GenericAPIError{Code: code},
		}
	}
	for _, tc := range []struct {
		status int
		code   string
		again  bool
	}{
		{http.StatusServiceUnavailable, "ServiceUnavailable", true},
		{http.StatusRequestTimeout, "RequestTimeout", true},
		{http.StatusBadRequest, "ThrottlingException", true},
		{http.StatusConflict, "ResourceConflictException", false},
		{http.StatusNotFound, "ResourceNotFoundException", false},
		{http.StatusBadRequest, "AccessDeniedException", false},
	} {
		if got := isRetryableAWSError(answer(tc.status, tc.code)); got != tc.again {
			t.Errorf("%d %s: asked again = %t, want %t", tc.status, tc.code, got, tc.again)
		}
	}
}
