// SPDX-License-Identifier: Apache-2.0

package resultdigest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
)

func sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func TestFromPayloadReadsOnlyAWellFormedResult(t *testing.T) {
	good := fmt.Sprintf(`{"zenthros.result":{"uri":"s3://b/run/result.pkl","sha256":%q}}`, sum("a"))
	if d, ok := FromPayload([]byte(good)); !ok || d.URI != "s3://b/run/result.pkl" || d.SHA256 != sum("a") {
		t.Fatalf("FromPayload(%s) = %+v, %v", good, d, ok)
	}
	for _, bad := range []string{
		`{"loss":0.1}`,
		`{"zenthros.result":"s3://b/x"}`,
		`{"zenthros.result":{"uri":"","sha256":"` + sum("a") + `"}}`,
		`{"zenthros.result":{"uri":"s3://b/x","sha256":"ABC"}}`,
		`{"zenthros.result":{"uri":"s3://b/x","sha256":"` + strings.ToUpper(sum("a")) + `"}}`,
		`{"zenthros.result":{"uri":"` + strings.Repeat("x", MaxURI+1) + `","sha256":"` + sum("a") + `"}}`,
		`not json`,
	} {
		if d, ok := FromPayload([]byte(bad)); ok {
			t.Fatalf("FromPayload(%.80s) = %+v, want refused", bad, d)
		}
	}
}

// Every attempt's digest is the job's word, but the status is bounded: a
// URI keeps its last PerURI digests and the list its last Max.
func TestAddKeepsEachDigestOnceWithinBounds(t *testing.T) {
	var list []v1.ResultDigest
	var changed bool
	a := v1.ResultDigest{URI: "u", SHA256: sum("1")}
	if list, changed = Add(list, a); !changed || len(list) != 1 {
		t.Fatalf("first add: %v %v", list, changed)
	}
	if list, changed = Add(list, a); changed || len(list) != 1 {
		t.Fatalf("repeat add changed the list: %v", list)
	}
	for i := 2; i <= PerURI+2; i++ {
		list, _ = Add(list, v1.ResultDigest{URI: "u", SHA256: sum(fmt.Sprint(i))})
	}
	if len(list) != PerURI || list[len(list)-1].SHA256 != sum(fmt.Sprint(PerURI+2)) {
		t.Fatalf("per-URI bound: %d entries, last %v", len(list), list[len(list)-1])
	}
	list = nil
	for i := 0; i < Max+5; i++ {
		list, _ = Add(list, v1.ResultDigest{URI: fmt.Sprintf("u%d", i), SHA256: sum("x")})
	}
	if len(list) != Max || list[0].URI != "u5" {
		t.Fatalf("bound: %d entries, first %s", len(list), list[0].URI)
	}
}

// Pin is what the SDK's runner computes over what it read: the latest
// digest of each URI, sorted by URI, one "uri sha256\n" line each.
func TestPinIsTheLatestDigestPerURISorted(t *testing.T) {
	list := []v1.ResultDigest{
		{URI: "s3://b/r/results/1.pkl", SHA256: sum("old")},
		{URI: "s3://b/r/results/0.pkl", SHA256: sum("zero")},
		{URI: "s3://b/r/results/1.pkl", SHA256: sum("new")},
	}
	want := sha256.Sum256([]byte("s3://b/r/results/0.pkl " + sum("zero") + "\n" + "s3://b/r/results/1.pkl " + sum("new") + "\n"))
	if got := Pin(list); got != hex.EncodeToString(want[:]) {
		t.Fatalf("Pin = %s, want %s", got, hex.EncodeToString(want[:]))
	}
	if Pin(nil) != "" {
		t.Fatal("Pin of nothing is not empty")
	}
	if n := URIs(list); n != 2 {
		t.Fatalf("URIs = %d, want 2", n)
	}
}

// The SDK's zenthros.Artifact reports in this form (sdk/python/tests/test_cache.py
// reads the same file): what it says besides the URI and digest does not
// change what the controller keeps.
func TestTheSDKsArtifactIsAResultDigest(t *testing.T) {
	sample, err := os.ReadFile("testdata/artifact.json")
	if err != nil {
		t.Fatal(err)
	}
	got, ok := FromPayload(sample)
	if !ok {
		t.Fatalf("an Artifact is not read as a result digest: %s", sample)
	}
	want := v1.ResultDigest{URI: "gs://b/churn-train-7c1d/result.pkl",
		SHA256: "9f2c6d0a5b3e4c7d8e1f0a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d"}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
