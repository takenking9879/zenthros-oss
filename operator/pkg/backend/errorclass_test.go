// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want ErrorClass
	}{
		{"spec mismatch", ErrSpecMismatch, ClassPermanent},
		{"spec mismatch wrapped", fmt.Errorf("submit: %w", ErrSpecMismatch), ClassPermanent},
		{"unsupported kind", ErrUnsupportedKind, ClassPermanent},
		{"unsupported backend", ErrUnsupportedBackend, ClassPermanent},
		{"quota exceeded", ErrQuotaExceeded, ClassPermanent},
		{"admission rejected", ErrAdmissionRejected, ClassPermanent},
		{"image pull", ErrImagePull, ClassPermanent},
		{"backend busy", ErrBackendBusy, ClassTransient},
		{"rate limited", ErrRateLimited, ClassTransient},
		{"queue full", ErrQueueFull, ClassTransient},
		{"backend unavailable", ErrBackendUnavailable, ClassTransient},
		{"unknown network error", errors.New("connection reset by peer"), ClassTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Errorf("Classify = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestErrorClassString(t *testing.T) {
	if got := ClassTransient.String(); got != "transient" {
		t.Errorf("ClassTransient.String() = %q, want transient", got)
	}
	if got := ClassPermanent.String(); got != "permanent" {
		t.Errorf("ClassPermanent.String() = %q, want permanent", got)
	}
}
