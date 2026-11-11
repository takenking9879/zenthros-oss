// SPDX-License-Identifier: Apache-2.0

package jobsource

import (
	"archive/tar"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Each link is inside the work directory when read alone, but s points at
// the directory and s/t at its parent: t/x is written next to the work
// directory, not in it. The extraction refuses it and writes nothing
// outside.
func TestInlineSymlinkChainCannotEscape(t *testing.T) {
	parent := t.TempDir()
	work := filepath.Join(parent, "work")
	if err := os.Mkdir(work, 0o755); err != nil {
		t.Fatal(err)
	}
	encoded := tarball(t,
		entry{name: "s", link: ".", typ: tar.TypeSymlink},
		entry{name: "s/t", link: "..", typ: tar.TypeSymlink},
		entry{name: "t/x", body: "escaped"},
	)
	err := ExtractInline(strings.NewReader(encoded), work)
	if _, statErr := os.Stat(filepath.Join(parent, "x")); statErr == nil {
		t.Fatalf("t/x was written outside the work directory (extract error: %v)", err)
	}
	if err == nil {
		t.Fatal("a symlink chain out of the work directory was extracted without error")
	}
}
