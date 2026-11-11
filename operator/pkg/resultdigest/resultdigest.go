// SPDX-License-Identifier: Apache-2.0

// Package resultdigest keeps the SHA-256s zenthros.run runners report for
// the results they write, on the job's status, and pins them for the jobs
// that read those results on a machine.
package resultdigest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
)

const (
	// Report is the payload key the runner publishes {uri, sha256} under.
	Report = "zenthros.result"
	// EnvUpstreamResults is the job's status.upstreamResults as JSON, for
	// the runner: upstream job name -> Pin of its results.
	EnvUpstreamResults = "ZENTHROS_UPSTREAM_RESULTS"
	// Max bounds status.results: a map has at most 256 items.
	Max = 1024
	// PerURI bounds the digests kept for one result: each attempt that
	// rewrote it reported its own.
	PerURI = 3
	// MaxURI bounds one URI.
	MaxURI = 2048
)

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// FromPayload is the result digest in a report payload, if it carries a
// well-formed one.
func FromPayload(payload []byte) (v1.ResultDigest, bool) {
	var body map[string]json.RawMessage
	if json.Unmarshal(payload, &body) != nil || body[Report] == nil {
		return v1.ResultDigest{}, false
	}
	var d struct {
		URI    string `json:"uri"`
		SHA256 string `json:"sha256"`
	}
	if json.Unmarshal(body[Report], &d) != nil || d.URI == "" || len(d.URI) > MaxURI || !sha256Hex.MatchString(d.SHA256) {
		return v1.ResultDigest{}, false
	}
	return v1.ResultDigest{URI: d.URI, SHA256: d.SHA256}, true
}

// Add appends d unless the list holds it already, then drops the oldest
// digests past PerURI for its URI and past Max overall.
func Add(list []v1.ResultDigest, d v1.ResultDigest) ([]v1.ResultDigest, bool) {
	for _, have := range list {
		if have == d {
			return list, false
		}
	}
	list = append(list, d)
	same := 0
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].URI != d.URI {
			continue
		}
		if same++; same > PerURI {
			list = append(list[:i], list[i+1:]...)
		}
	}
	if len(list) > Max {
		list = list[len(list)-Max:]
	}
	return list, true
}

// Vouches reports whether the list holds a digest for uri.
func Vouches(list []v1.ResultDigest, uri string) bool {
	for _, have := range list {
		if have.URI == uri {
			return true
		}
	}
	return false
}

// URIs counts the distinct results the list vouches for.
func URIs(list []v1.ResultDigest) int {
	seen := map[string]bool{}
	for _, d := range list {
		seen[d.URI] = true
	}
	return len(seen)
}

// Pin is one digest for all of a job's results: SHA-256 over the latest
// digest of each URI, sorted by URI, as "uri sha256\n" lines. The runner
// computes the same over what it read. "" for no results.
func Pin(list []v1.ResultDigest) string {
	if len(list) == 0 {
		return ""
	}
	latest := map[string]string{}
	for _, d := range list {
		latest[d.URI] = d.SHA256
	}
	uris := make([]string, 0, len(latest))
	for uri := range latest {
		uris = append(uris, uri)
	}
	sort.Strings(uris)
	var b strings.Builder
	for _, uri := range uris {
		b.WriteString(uri + " " + latest[uri] + "\n")
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// EnvValue is EnvUpstreamResults for a job's status.upstreamResults; "" for
// none.
func EnvValue(pins map[string]string) string {
	if len(pins) == 0 {
		return ""
	}
	raw, _ := json.Marshal(pins)
	return string(raw)
}
