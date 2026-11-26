// SPDX-License-Identifier: Apache-2.0

package agepayload

import (
	"strings"
	"testing"

	"filippo.io/age"
)

func TestEncryptRejectsEmptyAndOversized(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	recipient := identity.Recipient().String()
	if _, err := Encrypt(nil, []string{recipient}); err == nil {
		t.Fatal("empty plaintext should fail")
	}
	if _, err := Encrypt(make([]byte, MaxPlaintextBytes+1), []string{recipient}); err == nil {
		t.Fatal("oversized plaintext should fail")
	}
	if _, err := Encrypt([]byte("ok"), nil); err == nil {
		t.Fatal("missing recipient should fail")
	}
}

func TestEncryptProducesArmoredCiphertextWithoutPlaintext(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	const plaintext = "never-in-ciphertext-body"
	armored, err := Encrypt([]byte(plaintext), []string{identity.Recipient().String()})
	if err != nil {
		t.Fatal(err)
	}
	if !LooksLikeCiphertext(armored) {
		t.Fatalf("missing armor: %s", armored)
	}
	if strings.Contains(armored, plaintext) {
		t.Fatal("armored ciphertext contained plaintext")
	}
}

func TestParseRecipientsRejectsInvalid(t *testing.T) {
	if _, err := ParseRecipients([]string{"not-an-age-key"}); err == nil {
		t.Fatal("invalid recipient should fail")
	}
	tooMany := make([]string, MaxRecipients+1)
	for i := range tooMany {
		identity, err := age.GenerateX25519Identity()
		if err != nil {
			t.Fatal(err)
		}
		tooMany[i] = identity.Recipient().String()
	}
	if _, err := ParseRecipients(tooMany); err == nil {
		t.Fatal("too many recipients should fail")
	}
}
