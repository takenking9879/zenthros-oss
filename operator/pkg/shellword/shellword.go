// SPDX-License-Identifier: Apache-2.0

// Package shellword is the one way a value becomes a literal POSIX shell
// word. Five copies of it lived in the packages that write scripts and unit
// lines; one of them escaped differently (19#10).
package shellword

import "strings"

// Quote is value as one single-quoted shell word: an embedded single quote
// closes the literal, adds an escaped quote and reopens it.
func Quote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
