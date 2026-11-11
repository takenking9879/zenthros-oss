// SPDX-License-Identifier: Apache-2.0

package backend

import "testing"

// TestTraitsTable pins what each backend type implies. container is stated
// trait by trait, not copied from vm: it has no host runtime class.
func TestTraitsTable(t *testing.T) {
	cases := map[string]Traits{
		TypeK8s:       {Cluster: true},
		TypeVM:        {ExclusiveByDefault: true, ManagedNeedsPoolLease: true},
		TypeContainer: {ExclusiveByDefault: true, ManagedNeedsPoolLease: true},
		"":            {ManagedNeedsPoolLease: true},
		"unknown":     {ManagedNeedsPoolLease: true},
	}
	for backendType, want := range cases {
		if got := TraitsOf(backendType); got != want {
			t.Errorf("TraitsOf(%q) = %+v, want %+v", backendType, got, want)
		}
	}
}
