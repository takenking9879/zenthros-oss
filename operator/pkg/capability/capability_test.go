// SPDX-License-Identifier: Apache-2.0

package capability

import "testing"

// A host's cache matches an image however either side spells it: a
// Prewarm often names busybox:1.36, a job docker.io/library/busybox:1.36.
func TestHostWarmNormalizesBothSides(t *testing.T) {
	h := Host{CachedImages: []string{"busybox:1.36", "ghcr.io/org/train:1", "org/tool:2"}}
	for image, want := range map[string]bool{
		"docker.io/library/busybox:1.36": true,
		"busybox:1.36":                   true,
		"ghcr.io/org/train:1":            true,
		"docker.io/org/tool:2":           true,
		"busybox:1.37":                   false,
		"":                               false,
	} {
		if got := h.Warm(image); got != want {
			t.Fatalf("Warm(%q) = %v, want %v", image, got, want)
		}
	}
}
