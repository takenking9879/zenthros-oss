// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"strings"
	"testing"
)

// The local runner feeds stdin to bash: 300 KiB arrive whole, where the same
// bytes as a command-line argument exceed Linux's 128 KiB per argument.
func TestLocalRunnerFeedsLargeStdin(t *testing.T) {
	out, err := LocalRunner{}.RunInput(context.Background(), "wc -c", strings.NewReader(strings.Repeat("x", 300<<10)))
	if err != nil {
		t.Fatalf("RunInput: %v", err)
	}
	if got := strings.TrimSpace(out); got != "307200" {
		t.Fatalf("bash read %s bytes, want 307200", got)
	}
}
