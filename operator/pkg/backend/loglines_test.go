// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"strings"
	"testing"
)

// A line longer than MaxLogLine comes in pieces, and the log goes on after
// it: the scanner stopped there, and every reader dropped the rest.
func TestALongLogLineComesInPiecesAndTheLogGoesOn(t *testing.T) {
	long := strings.Repeat("x", 2*MaxLogLine+100)
	scanner := LogLines(strings.NewReader("before\n" + long + "\nafter\n"))
	var got []int
	var last string
	for scanner.Scan() {
		got = append(got, len(scanner.Text()))
		last = scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	want := []int{len("before"), MaxLogLine, MaxLogLine, 100, len("after")}
	if len(got) != len(want) || last != "after" {
		t.Fatalf("pieces %v, last %q; want %v", got, last, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pieces %v, want %v", got, want)
		}
	}
}
