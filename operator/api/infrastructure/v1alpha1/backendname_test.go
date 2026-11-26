// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// ValidBackendName is spec.name's rule as the CRD states it: code that
// checks a name before writing it (zenthros-api, a token) refuses what the
// apiserver would.
func TestValidBackendNameIsTheCRDsPattern(t *testing.T) {
	raw, err := os.ReadFile("backendaccess_types.go")
	if err != nil {
		t.Fatal(err)
	}
	marker := regexp.MustCompile("(?s)Pattern=`([^`]+)`\\n\\t// \\+optional\\n\\tName string").FindStringSubmatch(string(raw))
	if marker == nil || marker[1] != backendNameRE.String() {
		t.Fatalf("CRD pattern %v, ValidBackendName's %s", marker, backendNameRE)
	}
	for name, want := range map[string]bool{
		"mi-pc": true, "a": true, "gpu-1": true, strings.Repeat("a", 63): true,
		"": false, "Mi-PC": false, "-a": false, "a-": false, "a_b": false, strings.Repeat("a", 64): false,
	} {
		if ValidBackendName(name) != want {
			t.Errorf("ValidBackendName(%q) = %v", name, !want)
		}
	}
}
