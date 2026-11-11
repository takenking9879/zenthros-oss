// SPDX-License-Identifier: Apache-2.0

package capability

import "testing"

// A job's later attempts carry the image its first attempt resolved, pinned
// by digest (F11). A container that runs that repository by tag takes the
// pinned reference; a digest of another repository, or another digest of a
// container pinned itself, is still another image.
// The same digest spelled with or without the tag is the same image: the
// local E2E's containers know theirs, and refused it.
func TestRunsImageAcceptsADigestOfItsRepository(t *testing.T) {
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const other = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	for _, tc := range []struct {
		host, job string
		want      bool
	}{
		{"python:3.12", "docker.io/library/python@" + digest, true},
		{"ghcr.io/acme/train:v1", "ghcr.io/acme/train@" + digest, true},
		{"localhost:5000/acme/train:v1", "localhost:5000/acme/train@" + digest, true},
		{"ghcr.io/acme/train", "ghcr.io/acme/train@" + digest, true},
		{"ghcr.io/acme/train:v1", "ghcr.io/acme/train:v1@" + digest, true},
		{"ghcr.io/acme/train:v1", "ghcr.io/acme/other@" + digest, false},
		{"ghcr.io/acme/train@" + digest, "ghcr.io/acme/train@" + digest, true},
		{"ghcr.io/acme/train@" + digest, "ghcr.io/acme/train@" + other, false},
		// A container that knows its digest (the local E2E's run
		// repo:tag@digest) takes that digest however the job spells it,
		// and no other.
		{"localhost:5000/acme/train:v1@" + digest, "localhost:5000/acme/train@" + digest, true},
		{"localhost:5000/acme/train:v1@" + digest, "localhost:5000/acme/train:v1@" + digest, true},
		{"localhost:5000/acme/train:v1@" + digest, "localhost:5000/acme/train@" + other, false},
		{"localhost:5000/acme/train:v1@" + digest, "localhost:5000/acme/other@" + digest, false},
		{"ghcr.io/acme/train:v1", "ghcr.io/acme/train:v2", false},
		{"localhost:5000/acme/train:v1", "localhost:5000/acme/train:v2", false},
	} {
		if got := (Host{ContainerImage: tc.host}).RunsImage(tc.job); got != tc.want {
			t.Errorf("container %s runs %s = %v, want %v", tc.host, tc.job, got, tc.want)
		}
	}
}
