// SPDX-License-Identifier: Apache-2.0

// Package imagegc decides which of a machine's job images to remove: those
// no job used for a while, then, while the images take more of the disk
// than they may, the least recently used. An image in use is never removed:
// one a container uses, or one a job holds from its name until its unit
// ends (agent.ImageGC reads both at each sweep); nor one named within the
// grace, a Prewarm's among them. The agent runs the sweep (agent.ImageGC);
// this package holds the decision and the record of when each image was
// last used, kept across restarts.
package imagegc

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Defaults: a week unused, and 40% of the disk.
const (
	DefaultMaxAge   = 7 * 24 * time.Hour
	DefaultMaxSpace = "40%"
	// MaxGrace bounds the time an image a job just named is kept whatever
	// the disk: its pull and its container's start take less.
	MaxGrace = time.Hour
)

// Policy is how long an unused image stays, and how much of the disk the
// images may take: Bytes, or Fraction of the disk when Bytes is 0.
type Policy struct {
	MaxAge   time.Duration
	Bytes    int64
	Fraction float64
}

// Grace is how long an image a job named is kept whatever the disk: an
// hour, or MaxAge when that is shorter.
func (p Policy) Grace() time.Duration {
	return min(MaxGrace, p.MaxAge)
}

// Limit is the most bytes the images may take on a disk of disk bytes.
func (p Policy) Limit(disk int64) int64 {
	if p.Bytes > 0 {
		return p.Bytes
	}
	return int64(math.Floor(float64(disk) * p.Fraction))
}

// ParsePolicy reads ZENTHROS_IMAGE_MAX_AGE (a duration: 7d, 36h, 90m) and
// ZENTHROS_IMAGE_MAX_SPACE (a share of the disk, 40%, or a size, 30GB,
// 500MiB); empty is the default.
func ParsePolicy(maxAge, maxSpace string) (Policy, error) {
	p := Policy{MaxAge: DefaultMaxAge}
	if maxAge != "" {
		age, err := parseAge(maxAge)
		if err != nil {
			return Policy{}, fmt.Errorf("ZENTHROS_IMAGE_MAX_AGE %q: %w", maxAge, err)
		}
		p.MaxAge = age
	}
	if maxSpace == "" {
		maxSpace = DefaultMaxSpace
	}
	if share, ok := strings.CutSuffix(maxSpace, "%"); ok {
		n, err := strconv.ParseFloat(share, 64)
		if err != nil || n <= 0 || n > 100 {
			return Policy{}, fmt.Errorf("ZENTHROS_IMAGE_MAX_SPACE %q: a share of the disk is more than 0%% and at most 100%%", maxSpace)
		}
		p.Fraction = n / 100
		return p, nil
	}
	n, err := parseSize(maxSpace)
	if err != nil {
		return Policy{}, fmt.Errorf("ZENTHROS_IMAGE_MAX_SPACE %q: %w", maxSpace, err)
	}
	p.Bytes = n
	return p, nil
}

func parseAge(raw string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(raw, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil || n <= 0 {
			return 0, errors.New("days are a positive number (7d)")
		}
		return time.Duration(n * float64(24*time.Hour)), nil
	}
	age, err := time.ParseDuration(raw)
	if err != nil || age <= 0 {
		return 0, errors.New("a positive duration (7d, 36h, 90m)")
	}
	return age, nil
}

var sizeUnits = map[string]float64{
	"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
	"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "TIB": 1 << 40,
}

func parseSize(raw string) (int64, error) {
	upper := strings.ToUpper(strings.TrimSpace(raw))
	digits := strings.TrimRight(upper, "BKMGTI")
	unit := strings.TrimSpace(upper[len(digits):])
	scale, ok := sizeUnits[unit]
	n, err := strconv.ParseFloat(strings.TrimSpace(digits), 64)
	if !ok || err != nil || n <= 0 {
		return 0, errors.New("a share of the disk (40%) or a size (30GB, 500MiB)")
	}
	return int64(n * scale), nil
}

// Ledger is when each job image on the machine was last used (a job named
// it, or a Prewarm pulled it), kept in a file across the agent's restarts.
type Ledger struct {
	path string
	mu   sync.Mutex
	used map[string]time.Time
}

type ledgerFile struct {
	Images map[string]time.Time `json:"images"`
}

// New is an empty ledger kept at path.
func New(path string) *Ledger {
	return &Ledger{path: path, used: map[string]time.Time{}}
}

// Load reads the ledger at path; none there is an empty one.
func Load(path string) (*Ledger, error) {
	l := New(path)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	var file ledgerFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for ref, at := range file.Images {
		l.used[ref] = at
	}
	return l, nil
}

// Note records ref as used at now.
func (l *Ledger) Note(ref string, now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.used[ref] = now
	return l.save()
}

// Present is the images on the machine, each with when it was last used:
// one the ledger does not know starts as used now (never removed at first
// sight), and one no longer there is dropped from the ledger.
func (l *Ledger) Present(refs []string, now time.Time) (map[string]time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	present := make(map[string]time.Time, len(refs))
	for _, ref := range refs {
		at, ok := l.used[ref]
		if !ok {
			at = now
		}
		present[ref] = at
	}
	l.used = present
	out := make(map[string]time.Time, len(present))
	for ref, at := range present {
		out[ref] = at
	}
	return out, l.save()
}

// LastUsed is when ref was last used, as the ledger says now.
func (l *Ledger) LastUsed(ref string) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used[ref]
}

// Forget drops a removed image.
func (l *Ledger) Forget(ref string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.used, ref)
	return l.save()
}

func (l *Ledger) save() error {
	raw, err := json.Marshal(ledgerFile{Images: l.used})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, l.path)
}

// Candidates are the images that may be removed, least recently used
// first: none a container uses, none used within the grace.
func Candidates(present map[string]time.Time, inUse map[string]bool, now time.Time, p Policy) []string {
	var out []string
	for ref, at := range present {
		if !inUse[ref] && now.Sub(at) >= p.Grace() {
			out = append(out, ref)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if present[out[a]].Equal(present[out[b]]) {
			return out[a] < out[b]
		}
		return present[out[a]].Before(present[out[b]])
	})
	return out
}

// Stale are the candidates no job used for MaxAge, oldest first.
func Stale(present map[string]time.Time, inUse map[string]bool, now time.Time, p Policy) []string {
	var out []string
	for _, ref := range Candidates(present, inUse, now, p) {
		if now.Sub(present[ref]) >= p.MaxAge {
			out = append(out, ref)
		}
	}
	return out
}
