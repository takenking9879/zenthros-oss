// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func applyFilter(in string) string {
	filter := NewIntelligentLogFilter()
	var b strings.Builder
	for _, line := range strings.SplitAfter(in, "\n") {
		if line == "" {
			continue
		}
		for _, out := range filter.Feed(line) {
			b.WriteString(out)
			if !strings.HasSuffix(out, "\n") && strings.HasSuffix(line, "\n") {
				b.WriteByte('\n')
			}
		}
	}
	for _, out := range filter.Flush() {
		b.WriteString(out)
		if !strings.HasSuffix(out, "\n") {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func TestIntelligentLogFilterFixtures(t *testing.T) {
	cases := []string{
		"keep_resources",
		"keep_phase",
		"keep_error",
		"keep_metrics",
		"collapse_epoch",
		"drop_heartbeat",
		"drop_progress",
		"drop_gpu_prom",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			inBytes, err := os.ReadFile(filepath.Join("testdata", "x10", name+".in"))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			wantBytes, err := os.ReadFile(filepath.Join("testdata", "x10", name+".want"))
			if err != nil {
				t.Fatalf("read want: %v", err)
			}
			got := applyFilter(string(inBytes))
			if got != string(wantBytes) {
				t.Errorf("filter mismatch for %s\n got:\n%s\nwant:\n%s", name, got, wantBytes)
			}
		})
	}
}

func TestIntelligentLogFilterNewWindow(t *testing.T) {
	in := "[synth-train] Epoch 0 loss=1.0\n[synth-train] Epoch 1 loss=0.9\n[synth-train] Epoch 2 loss=0.8\n"
	first := applyFilter(in)
	second := applyFilter(in)
	if first != second {
		t.Fatalf("new filter window must be independent\n first:\n%s\nsecond:\n%s", first, second)
	}
	if !strings.Contains(first, "Epoch 0") || !strings.Contains(first, "Epoch 2") {
		t.Fatalf("window must keep first+last epoch, got %q", first)
	}
	if strings.Contains(first, "Epoch 1") {
		t.Fatalf("window must collapse middle epoch, got %q", first)
	}
}
