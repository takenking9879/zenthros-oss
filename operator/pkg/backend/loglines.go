// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"bufio"
	"io"
)

// MaxLogLine is the longest piece of a log line one LogChunk carries.
const MaxLogLine = 256 << 10

// LogLines scans r line by line, as a log is read. A line longer than
// MaxLogLine comes in pieces of at most that size: bufio.Scanner stopped at
// such a line with ErrTooLong, and every reader ignored the error, so the
// rest of the log was silently lost.
func LogLines(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), MaxLogLine)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		advance, token, err := bufio.ScanLines(data, atEOF)
		if advance > 0 || token != nil || err != nil || len(data) < MaxLogLine {
			return advance, token, err
		}
		return MaxLogLine, data[:MaxLogLine], nil
	})
	return scanner
}
