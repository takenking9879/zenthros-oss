// SPDX-License-Identifier: Apache-2.0

package containerboot

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The agent an earlier start downloaded sits in the state dir, which the
// container's jobs can write: a restart verifies it against the pin again
// before it runs, and a replaced one never runs.
func TestBootReverifiesTheCachedAgent(t *testing.T) {
	im := newImage(t, "curl")
	if out, err := im.boot(); err != nil || im.agentRan() != "downloaded" {
		t.Fatalf("first start: %v\n%s", err, out)
	}
	evil := []byte("#!/bin/sh\necho evil > " + im.ran + "\n")
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	_, _ = w.Write(evil)
	_ = w.Close()
	cached := filepath.Join(im.state, "bin")
	entries, err := os.ReadDir(cached)
	if err != nil || len(entries) == 0 {
		t.Fatalf("nothing cached in %s: %v", cached, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		body := evil
		if strings.HasSuffix(e.Name(), ".gz") {
			body = gz.Bytes()
		}
		if err := os.WriteFile(filepath.Join(cached, e.Name()), body, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	_ = os.Remove(im.ran)

	out, err := im.boot()
	if im.agentRan() == "evil" {
		t.Fatalf("a replaced cached agent ran on restart\n%s", out)
	}
	if err != nil || im.agentRan() != "downloaded" {
		t.Fatalf("restart: %v ran %q\n%s", err, im.agentRan(), out)
	}
}
