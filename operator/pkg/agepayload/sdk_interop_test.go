// SPDX-License-Identifier: Apache-2.0

package agepayload

import (
	"io"
	"os"
	"strings"
	"testing"

	"filippo.io/age"
	"filippo.io/age/armor"
)

// The Python SDK seals Secret.encrypted itself (sdk/python/zenthros/seal.py),
// so the value never leaves the caller's process. testdata/sdk-sealed.age is
// what it writes with fixed randomness (sdk/python/tests/test_seal.py pins
// that, byte for byte); here age opens it, as the agent does
// (agent/agepayload), with the test-only identity it was sealed to, past a
// stanza that is another key's.
func TestTheSDKsSealOpensWithAge(t *testing.T) {
	raw, err := os.ReadFile("testdata/sdk-identity.txt")
	if err != nil {
		t.Fatal(err)
	}
	identities, err := age.ParseIdentities(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := os.ReadFile("testdata/sdk-sealed.age")
	if err != nil {
		t.Fatal(err)
	}
	if !LooksLikeCiphertext(string(sealed)) {
		t.Fatalf("not armored: %s", sealed)
	}
	reader, err := age.Decrypt(armor.NewReader(strings.NewReader(string(sealed))), identities...)
	if err != nil {
		t.Fatalf("age cannot open what the SDK sealed: %v", err)
	}
	plain, err := io.ReadAll(reader)
	if err != nil || string(plain) != "sealed by the zenthros SDK" {
		t.Fatalf("opened %q, %v", plain, err)
	}
	other, _ := age.GenerateX25519Identity()
	if _, err := age.Decrypt(armor.NewReader(strings.NewReader(string(sealed))), other); err == nil {
		t.Fatal("a key it was not sealed to opened it")
	}
}
