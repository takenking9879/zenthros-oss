// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// RequireEnvtest skips t unless its envtest suite was asked for by name:
// ZENTHROS_ENVTEST lists it (make test-integration sets "integration", make
// test-load "load") and KUBEBUILDER_ASSETS is set. Exporting
// KUBEBUILDER_ASSETS alone no longer makes go test ./... boot an apiserver
// per test.
func RequireEnvtest(t testing.TB, suite string) {
	t.Helper()
	if !slices.Contains(strings.Fields(strings.ReplaceAll(os.Getenv("ZENTHROS_ENVTEST"), ",", " ")), suite) ||
		os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skipf("set ZENTHROS_ENVTEST=%s and KUBEBUILDER_ASSETS to run the envtest %s tests (make test-%s)", suite, suite, suite)
	}
}
