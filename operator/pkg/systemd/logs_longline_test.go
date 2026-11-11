// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// journalctl's output past a line longer than the scanner's buffer reaches
// the reader, and the stream ends with journalctl (the log
// stopped there, silently).
func TestAJournalLineLongerThanTheBufferDoesNotEndTheLog(t *testing.T) {
	dir := t.TempDir()
	script := "#!/bin/sh\nhead -c 300000 /dev/zero | tr '\\0' x\necho\necho after\n"
	if err := os.WriteFile(filepath.Join(dir, "journalctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch, err := NewLogStreamer(Config{}, nil).Stream(ctx, "zenthros-x.service", false, -1)
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	for chunk := range ch {
		out.Write(chunk.Data)
	}
	if ctx.Err() != nil {
		t.Fatal("the stream did not end with journalctl")
	}
	if !strings.HasSuffix(out.String(), "after\n") || strings.Count(out.String(), "x") != 300000 {
		t.Fatalf("got %d bytes ending %q", out.Len(), out.String()[max(0, out.Len()-20):])
	}
}
