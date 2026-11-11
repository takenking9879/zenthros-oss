// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

// Scheme is a scheme with each of add's types registered, as a fake client
// is built on.
func Scheme(t testing.TB, add ...func(*runtime.Scheme) error) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, register := range add {
		if err := register(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}
