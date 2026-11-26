// SPDX-License-Identifier: Apache-2.0

package shellword

import (
	"os/exec"
	"testing"
)

// A quoted word reaches the command as the value, whatever it holds.
func TestQuoteIsTheValueToTheShell(t *testing.T) {
	for _, value := range []string{"", "plain", "it's", "''", `a"b`, "$HOME `id` \\n", "x\ny"} {
		out, err := exec.Command("sh", "-c", "printf %s "+Quote(value)).Output()
		if err != nil || string(out) != value {
			t.Fatalf("Quote(%q) = %q through sh (%v)", value, out, err)
		}
	}
}
