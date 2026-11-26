// SPDX-License-Identifier: Apache-2.0

// Package agepayload encrypts and validates age ciphertext for SecretSource.
// It never decrypts: decryption stays in the destination agent.
package agepayload

import (
	"bytes"
	"fmt"
	"strings"

	"filippo.io/age"
	"filippo.io/age/armor"
)

const (
	MaxPlaintextBytes  = 32 * 1024
	MaxCiphertextBytes = 64 * 1024
	MaxRecipients      = 8
	ArmorBegin         = "-----BEGIN AGE ENCRYPTED FILE-----"
	ArmorEnd           = "-----END AGE ENCRYPTED FILE-----"
)

// Encrypt seals plaintext to the given age1 recipients and returns ASCII armor.
func Encrypt(plaintext []byte, recipients []string) (string, error) {
	if len(plaintext) == 0 {
		return "", fmt.Errorf("plaintext is empty")
	}
	if len(plaintext) > MaxPlaintextBytes {
		return "", fmt.Errorf("plaintext exceeds %d bytes", MaxPlaintextBytes)
	}
	parsed, err := ParseRecipients(recipients)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	armorWriter := armor.NewWriter(&buf)
	writer, err := age.Encrypt(armorWriter, parsed...)
	if err != nil {
		return "", fmt.Errorf("age encrypt: %w", err)
	}
	if _, err := writer.Write(plaintext); err != nil {
		return "", fmt.Errorf("age encrypt write: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("age encrypt close: %w", err)
	}
	if err := armorWriter.Close(); err != nil {
		return "", fmt.Errorf("age armor close: %w", err)
	}
	armored := buf.String()
	if len(armored) > MaxCiphertextBytes {
		return "", fmt.Errorf("ciphertext exceeds %d bytes", MaxCiphertextBytes)
	}
	return armored, nil
}

// ParseRecipients validates age1 X25519 recipient strings.
func ParseRecipients(recipients []string) ([]age.Recipient, error) {
	if len(recipients) == 0 {
		return nil, fmt.Errorf("at least one recipient is required")
	}
	if len(recipients) > MaxRecipients {
		return nil, fmt.Errorf("recipient count exceeds %d", MaxRecipients)
	}
	parsed := make([]age.Recipient, 0, len(recipients))
	seen := make(map[string]struct{}, len(recipients))
	for _, recipient := range recipients {
		if _, dup := seen[recipient]; dup {
			continue
		}
		seen[recipient] = struct{}{}
		item, err := age.ParseX25519Recipient(recipient)
		if err != nil {
			return nil, fmt.Errorf("recipient %q is invalid", recipient)
		}
		parsed = append(parsed, item)
	}
	return parsed, nil
}

// KeyID returns a short public fingerprint for diagnostics. It is not secret.
func KeyID(recipient string) string {
	trimmed := strings.TrimPrefix(recipient, "age1")
	if len(trimmed) > 12 {
		return trimmed[:12]
	}
	return trimmed
}

// LooksLikeCiphertext reports whether value has age armor markers.
func LooksLikeCiphertext(value string) bool {
	trimmed := strings.TrimSpace(value)
	return strings.HasPrefix(trimmed, ArmorBegin) && strings.Contains(trimmed, ArmorEnd)
}
