// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"errors"
	"fmt"
	"testing"
)

func TestSpecMismatchErrorUnwraps(t *testing.T) {
	err := NewSpecMismatchError("image", "ray:2.52.0", "ray:2.51.0")

	if !errors.Is(err, ErrSpecMismatch) {
		t.Fatalf("expected errors.Is(err, ErrSpecMismatch) to be true, got %v", err)
	}
	if err.Field != "image" {
		t.Fatalf("expected Field=image, got %q", err.Field)
	}
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{
		ErrSpecMismatch,
		ErrUnsupportedKind,
		ErrUnsupportedBackend,
		ErrBackendUnavailable,
		ErrQuotaExceeded,
		ErrAdmissionRejected,
		ErrImagePull,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j && errors.Is(a, b) {
				t.Errorf("sentinel %d (%v) should not match sentinel %d (%v)", i, a, j, b)
			}
		}
	}
}

func TestWrappedSentinelErrors(t *testing.T) {
	cases := []struct {
		name    string
		wrapped error
		target  error
	}{
		{"quota wrapped", fmt.Errorf("create RayJob: %w", ErrQuotaExceeded), ErrQuotaExceeded},
		{"admission wrapped", fmt.Errorf("webhook: %w", ErrAdmissionRejected), ErrAdmissionRejected},
		{"image pull wrapped", fmt.Errorf("pull ray:latest: %w", ErrImagePull), ErrImagePull},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.wrapped, tc.target) {
				t.Errorf("errors.Is(%v, %v) = false, want true", tc.wrapped, tc.target)
			}
		})
	}
}
