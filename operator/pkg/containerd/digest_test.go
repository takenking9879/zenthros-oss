// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"context"
	"strings"
	"testing"
)

type listingRunner struct {
	listing string
	cmd     string
}

func (r *listingRunner) Run(_ context.Context, cmd string) (string, error) {
	r.cmd = cmd
	return r.listing, nil
}

// The digest a tag resolved to is read from containerd's own table, for the
// image asked (F11): its repository at that digest is what later runs pull.
func TestImageDigestPinsTheListedDigest(t *testing.T) {
	r := &listingRunner{listing: "REF                                TYPE                                                      DIGEST                                                                  SIZE     PLATFORMS   LABELS\n" +
		"docker.io/library/python:3.13-slim application/vnd.oci.image.index.v1+json                  sha256:4f6b0b1a2c3d4e5f60718293a4b5c6d7e8f9011223344556677889900aabbccdd 41.2 MiB linux/amd64 -\n"}
	got, err := ImageDigest(context.Background(), r, "sudo ", "python:3.13-slim")
	if err != nil {
		t.Fatal(err)
	}
	if want := "docker.io/library/python@sha256:4f6b0b1a2c3d4e5f60718293a4b5c6d7e8f9011223344556677889900aabbccdd"; got != want {
		t.Fatalf("ImageDigest = %q, want %q", got, want)
	}
	if !strings.Contains(r.cmd, "'name==docker.io/library/python:3.13-slim'") {
		t.Fatalf("ctr asked for %q, not the image by name", r.cmd)
	}

	r.listing = "REF TYPE DIGEST SIZE PLATFORMS LABELS\n"
	if got, _ := ImageDigest(context.Background(), r, "", "python:3.13-slim"); got != "" {
		t.Fatalf("an image containerd does not list pinned as %q", got)
	}
}

func TestPinnedRefKeepsTheRepository(t *testing.T) {
	for _, tc := range []struct{ ref, want string }{
		{"docker.io/library/python:3.13-slim", "docker.io/library/python@sha256:ab"},
		{"localhost:5000/team/train:v2", "localhost:5000/team/train@sha256:ab"},
		{"localhost:5000/team/train", "localhost:5000/team/train@sha256:ab"},
		{"repo/train@sha256:old", "repo/train@sha256:ab"},
	} {
		if got := PinnedRef(tc.ref, "sha256:ab"); got != tc.want {
			t.Errorf("PinnedRef(%q) = %q, want %q", tc.ref, got, tc.want)
		}
	}
	if got, _ := ImageDigest(context.Background(), &listingRunner{}, "", "repo/train@sha256:given"); got != "docker.io/repo/train@sha256:given" {
		t.Errorf("a digest reference resolved to %q, want itself, normalized", got)
	}
}
