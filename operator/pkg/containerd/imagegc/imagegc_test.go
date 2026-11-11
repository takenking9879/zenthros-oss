// SPDX-License-Identifier: Apache-2.0

package imagegc

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The two settings: a week and 40% unless set; an age in days or as a
// duration, a space as a share of the disk or a size; a value it cannot
// read names its variable.
func TestPolicyReadsItsTwoSettings(t *testing.T) {
	p, err := ParsePolicy("", "")
	if err != nil || p.MaxAge != 7*24*time.Hour || p.Fraction != 0.4 || p.Bytes != 0 || p.Limit(1000) != 400 {
		t.Fatalf("defaults: %+v %v", p, err)
	}
	for raw, want := range map[string]time.Duration{"1s": time.Second, "36h": 36 * time.Hour, "7d": 7 * 24 * time.Hour, "1.5d": 36 * time.Hour} {
		if p, err := ParsePolicy(raw, ""); err != nil || p.MaxAge != want {
			t.Errorf("age %q: %v %v, want %v", raw, p.MaxAge, err, want)
		}
	}
	for raw, want := range map[string]int64{"30GB": 30e9, "500MiB": 500 << 20, "2 TB": 2e12, "10gib": 10 << 30} {
		if p, err := ParsePolicy("", raw); err != nil || p.Bytes != want || p.Limit(1) != want {
			t.Errorf("space %q: %d %v, want %d", raw, p.Bytes, err, want)
		}
	}
	for _, c := range [][2]string{{"0d", ""}, {"week", ""}, {"-1h", ""}, {"", "0%"}, {"", "120%"}, {"", "lots"}, {"", "30XB"}} {
		_, err := ParsePolicy(c[0], c[1])
		if err == nil || !strings.Contains(err.Error(), "ZENTHROS_IMAGE_MAX_") {
			t.Errorf("%q %q: %v, want a refusal naming its variable", c[0], c[1], err)
		}
	}
	if (Policy{MaxAge: time.Second}).Grace() != time.Second || (Policy{MaxAge: DefaultMaxAge}).Grace() != time.Hour {
		t.Error("the grace is an hour, or the age when it is shorter")
	}
}

// The ledger outlives the agent: what was noted is there after a restart.
// An image it does not know starts as used now, one no longer on the
// machine is dropped, and a removed one is forgotten.
func TestTheLedgerKeepsWhenEachImageWasUsed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "images.json")
	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	l, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Note("a", t0); err != nil {
		t.Fatal(err)
	}
	if err := l.Note("gone", t0); err != nil {
		t.Fatal(err)
	}
	again, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	now := t0.Add(48 * time.Hour)
	present, err := again.Present([]string{"a", "unknown"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]time.Time{"a": t0, "unknown": now}; !reflect.DeepEqual(present, want) {
		t.Fatalf("after a restart: %v, want %v", present, want)
	}
	if err := again.Forget("a"); err != nil {
		t.Fatal(err)
	}
	last, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if present, _ := last.Present([]string{"unknown"}, now.Add(time.Hour)); !present["unknown"].Equal(now) || len(present) != 1 {
		t.Fatalf("the ledger kept %v; want only unknown, first seen at %v", present, now)
	}
}

// What may go: never an image a container uses, never one used within the
// grace, the least recently used first; stale are those of them unused for
// the age.
func TestOnlyUnusedImagesMayGoTheLeastRecentlyUsedFirst(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	p := Policy{MaxAge: 7 * 24 * time.Hour, Fraction: 0.4}
	present := map[string]time.Time{
		"in-use-old": now.Add(-30 * 24 * time.Hour),
		"old":        now.Add(-8 * 24 * time.Hour),
		"older":      now.Add(-9 * 24 * time.Hour),
		"recent":     now.Add(-2 * time.Hour),
		"just-named": now.Add(-time.Minute),
	}
	inUse := map[string]bool{"in-use-old": true}
	if got, want := Candidates(present, inUse, now, p), []string{"older", "old", "recent"}; !reflect.DeepEqual(got, want) {
		t.Errorf("candidates %v, want %v", got, want)
	}
	if got, want := Stale(present, inUse, now, p), []string{"older", "old"}; !reflect.DeepEqual(got, want) {
		t.Errorf("stale %v, want %v", got, want)
	}
}
