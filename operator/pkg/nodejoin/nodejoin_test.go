// SPDX-License-Identifier: Apache-2.0

package nodejoin

import (
	"strings"
	"testing"
	"time"
)

func testCredential() Credential {
	return Credential{
		Server: "https://10.0.0.1:6443", TokenID: "abcdef", TokenSecret: "0123456789abcdef",
		CACert:    []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"),
		ExpiresAt: time.Date(2026, 10, 8, 12, 15, 0, 0, time.UTC),
	}
}

// Only the machine that asked, for the node it asked for, opens the
// credential; the hub carrying it reads nothing.
func TestSealOpensOnlyForTheMachineAndItsNode(t *testing.T) {
	key, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	const node = "zenthros://pool/slot-0/uid-1"
	sealed, err := Seal(key.PublicKey().Bytes(), node, testCredential())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(sealed), testCredential().TokenSecret) || strings.Contains(string(sealed), "10.0.0.1") {
		t.Fatal("the sealed credential shows its contents")
	}
	got, err := Open(key, node, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if got.TokenSecret != testCredential().TokenSecret || got.Server != testCredential().Server || !got.ExpiresAt.Equal(testCredential().ExpiresAt) {
		t.Fatalf("opened %+v", got)
	}

	other, _ := NewKey()
	if _, err := Open(other, node, sealed); err == nil {
		t.Error("another machine's key opened the credential")
	}
	if _, err := Open(key, "zenthros://pool/slot-1/uid-2", sealed); err == nil {
		t.Error("the credential opened for another node")
	}
	tampered := append([]byte{}, sealed...)
	tampered[len(tampered)-1] ^= 1
	if _, err := Open(key, node, tampered); err == nil {
		t.Error("a tampered credential opened")
	}
	if _, err := Seal([]byte("short"), node, testCredential()); err == nil {
		t.Error("sealed to a malformed key")
	}
}

func TestOpenRejectsACredentialNobodyCanJoinWith(t *testing.T) {
	key, _ := NewKey()
	bad := testCredential()
	bad.Server = "http://10.0.0.1:6443"
	sealed, err := Seal(key.PublicKey().Bytes(), "n", bad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(key, "n", sealed); err == nil {
		t.Error("opened a credential for a plain-http server")
	}
}

// The token is k3s' own full form (k3s token create): the agent pins the
// server's CA by its hash before sending the secret.
func TestK3sTokenIsTheFullFormPinningTheCA(t *testing.T) {
	credential := testCredential()
	credential.CACert = []byte("ca")
	// sha256("ca"), as `echo -n ca | sha256sum`.
	const want = "K106959097001d10501ac7d54c0bdb8db61420f658f2922cc26e46d536119a31126::abcdef.0123456789abcdef"
	if got := K3sToken(credential); got != want {
		t.Fatalf("token %q, want %q", got, want)
	}
}

func TestReleasesPickTheClustersMinorNeverNewer(t *testing.T) {
	pin := func(v, sha string) string {
		return `{"version":"` + v + `","url":"https://example/` + v + `","sha256":"` + strings.Repeat(sha, 64) + `"}`
	}
	// The chart lists pins in any order.
	for _, order := range [][]string{
		{pin("v1.33.5+k3s1", "a"), pin("v1.34.0+k3s1", "b"), pin("v1.34.2+k3s1", "c")},
		{pin("v1.34.2+k3s1", "c"), pin("v1.33.5+k3s1", "a"), pin("v1.34.0+k3s1", "b")},
	} {
		releases, err := ParseReleases("k3s", "["+strings.Join(order, ",")+"]")
		if err != nil {
			t.Fatal(err)
		}
		pickTheClustersMinor(t, releases)
	}
	for name, raw := range map[string]string{
		"a version pinned twice": "[" + pin("v1.34.1+k3s1", "a") + "," + pin("v1.34.1+k3s2", "b") + "]",
		"no sha":                 `[{"version":"v1.34.1+k3s1","url":"https://a","sha256":""}]`,
		"no url":                 `[{"version":"v1.34.1+k3s1","url":"","sha256":"` + strings.Repeat("a", 64) + `"}]`,
		"no version":             "[" + pin("", "a") + "]",
	} {
		if _, err := ParseReleases("k3s", raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if empty, err := ParseReleases("kubelet", ""); err != nil || len(empty.Pins) != 0 {
		t.Errorf("no pins: %+v %v", empty, err)
	} else if _, err := empty.For("v1.34.0"); err == nil || !strings.Contains(err.Error(), "no kubelet pinned") {
		t.Errorf("no pins For: %v", err)
	}
}

// One pin for the CNI plugins; none is none, a broken one an error.
func TestParseReleaseIsOnePin(t *testing.T) {
	for _, raw := range []string{"", "{}", "null"} {
		if got, err := ParseRelease("cni-plugins", raw); got != nil || err != nil {
			t.Errorf("%q: %+v %v, want none", raw, got, err)
		}
	}
	got, err := ParseRelease("cni-plugins", `{"version":"v1.8.0","url":"https://example/cni.tgz","sha256":"`+strings.Repeat("d", 64)+`"}`)
	if err != nil || got.Version != "v1.8.0" {
		t.Fatalf("%+v %v", got, err)
	}
	for _, raw := range []string{`{"url":"https://x","sha256":"` + strings.Repeat("d", 64) + `"}`, `{"version":"v1","url":"ftp://x","sha256":"` + strings.Repeat("d", 64) + `"}`, `{"version":"v1","url":"https://x","sha256":"zz"}`} {
		if _, err := ParseRelease("cni-plugins", raw); err == nil {
			t.Errorf("%s accepted", raw)
		}
	}
}

func pickTheClustersMinor(t *testing.T, releases Releases) {
	t.Helper()
	for _, c := range []struct{ server, want, wantErr string }{
		// The newest pin of the server's minor not newer than the server.
		{"v1.34.2+k3s1", "v1.34.2+k3s1", ""},
		{"v1.34.5", "v1.34.2+k3s1", ""},
		{"v1.34.1+k3s1", "v1.34.0+k3s1", ""},
		{"v1.34.0", "v1.34.0+k3s1", ""},
		{"v1.33.9-eks-1234", "v1.33.5+k3s1", ""},
		{"v1.33.4+k3s1", "", "pinned k3s v1.33.5+k3s1 is newer"},
		{"v1.32.4", "", "no k3s pinned for Kubernetes 1.32"},
		{"garbage", "", "cluster version"},
	} {
		got, err := releases.For(c.server)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%s: err %v, want %q", c.server, err, c.wantErr)
			}
			continue
		}
		if err != nil || got.Version != c.want {
			t.Errorf("%s: %+v %v, want %s", c.server, got, err, c.want)
		}
	}
}
