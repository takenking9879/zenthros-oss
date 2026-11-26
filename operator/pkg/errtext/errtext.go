// SPDX-License-Identifier: Apache-2.0

// Package errtext bounds error text on its way to a user without rewriting
// it. Zenthros relays what a destination (Kubernetes, an operator, the agent)
// said; the only edit allowed is cutting an oversized message, and the cut
// keeps the head, where the cause is, and a short tail.
package errtext

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// MaxMessage bounds ZenthrosJob.status.message.
	MaxMessage = 32 << 10
	// MaxDetail bounds one status reason's detail or condition message.
	MaxDetail = 8 << 10
	// MaxWire bounds an error string on the control channel.
	MaxWire = 64 << 10

	tail = 1 << 10
)

// Bound returns s unchanged when it fits in max bytes. Otherwise it keeps as
// much of the head as fits, a marker with the number of bytes cut, and the
// last KiB, all on UTF-8 boundaries. Bytes that are not UTF-8 become U+FFFD
// first, the one other edit: protobuf refuses them in a string, and a stray
// one near the start cut the head back to it, the cause with it, in time
// quadratic in the length.
func Bound(s string, max int) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	if len(s) <= max {
		return s
	}
	keepTail := tail
	if keepTail > max/4 {
		keepTail = max / 4
	}
	marker := func(cut int) string { return fmt.Sprintf("…[truncated %d bytes]…", cut) }
	head := max - keepTail - len(marker(len(s)))
	if head < 0 {
		head = 0
	}
	h := runePrefix(s, head)
	t := runeSuffix(s, keepTail)
	return h + marker(len(s)-len(h)-len(t)) + t
}

// runePrefix is the longest prefix of valid s of at most n bytes that ends on
// a rune boundary.
func runePrefix(s string, n int) string {
	for n > 0 && n < len(s) && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// runeSuffix is the longest suffix of valid s of at most n bytes that starts
// on a rune boundary.
func runeSuffix(s string, n int) string {
	start := len(s) - n
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[start:]
}
