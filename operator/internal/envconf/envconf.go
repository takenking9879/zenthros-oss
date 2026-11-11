// SPDX-License-Identifier: Apache-2.0

// Package envconf reads the settings the binaries take from their
// environment. A malformed value ends the process at startup: a
// misconfigured bound is caught there, not silently ignored.
package envconf

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Or is name's value, or fallback when it is unset or empty.
func Or(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// Int is name's integer value, or fallback when it is unset.
func Int(name string, fallback int) int {
	return parse(name, fallback, "integer", strconv.Atoi)
}

// Bool is name's boolean value, or fallback when it is unset.
func Bool(name string, fallback bool) bool {
	return parse(name, fallback, "bool", strconv.ParseBool)
}

// Duration is name's duration value ("60s"), or fallback when it is unset.
func Duration(name string, fallback time.Duration) time.Duration {
	return parse(name, fallback, "duration", time.ParseDuration)
}

func parse[T any](name string, fallback T, kind string, from func(string) (T, error)) T {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := from(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid %s for %s=%q: %v\n", kind, name, value, err)
		os.Exit(1)
	}
	return parsed
}
