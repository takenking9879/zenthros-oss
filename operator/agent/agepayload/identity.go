// SPDX-License-Identifier: Apache-2.0

package agepayload

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"filippo.io/age"
	"filippo.io/age/armor"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend"
)

const (
	identityFile         = "identity"
	previousIdentityFile = "identity.previous"
	identityDirMode      = 0o700
	identityFileMode     = 0o600
)

// Bundle is a destination-only identity set. Private keys never leave this host.
type Bundle struct {
	current  *age.X25519Identity
	previous *age.X25519Identity
}

// Generate creates a new X25519 identity.
func Generate() (Bundle, error) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		return Bundle{}, fmt.Errorf("generate age identity: %w", err)
	}
	return Bundle{current: identity}, nil
}

// Recipients returns advertised public keys, current first then previous.
func (b Bundle) Recipients() []string {
	out := make([]string, 0, 2)
	if b.current != nil {
		out = append(out, b.current.Recipient().String())
	}
	if b.previous != nil {
		out = append(out, b.previous.Recipient().String())
	}
	return out
}

// Decrypt opens ASCII-armored age ciphertext with local identities.
func (b Bundle) Decrypt(ciphertext string) ([]byte, error) {
	identities := b.identities()
	if len(identities) == 0 {
		return nil, backend.NewSecretSourceError(v1.ReasonUnsupportedSecretSource, "destination agent has no age identity")
	}
	reader := armor.NewReader(strings.NewReader(ciphertext))
	decrypted, err := age.Decrypt(reader, identities...)
	if err != nil {
		if isWrongRecipient(err) {
			return nil, backend.NewSecretSourceError(v1.ReasonSecretAccessDenied, "ciphertext is not sealed to this destination")
		}
		return nil, backend.NewSecretSourceError(v1.ReasonSecretResolutionFailed, "destination agent could not decrypt the encrypted payload")
	}
	plaintext, err := io.ReadAll(decrypted)
	if err != nil {
		return nil, backend.NewSecretSourceError(v1.ReasonSecretResolutionFailed, "destination agent could not decrypt the encrypted payload")
	}
	if len(plaintext) > agepayload.MaxPlaintextBytes {
		return nil, backend.NewSecretSourceError(v1.ReasonInvalidSecretSource, "decrypted payload exceeds size limit")
	}
	return plaintext, nil
}

func (b Bundle) identities() []age.Identity {
	out := make([]age.Identity, 0, 2)
	if b.current != nil {
		out = append(out, b.current)
	}
	if b.previous != nil {
		out = append(out, b.previous)
	}
	return out
}

// isWrongRecipient reports ciphertext sealed to none of the local
// identities: age says so with its own type.
func isWrongRecipient(err error) bool {
	var noMatch *age.NoIdentityMatchError
	return errors.As(err, &noMatch)
}

// LoadDir loads identities from a destination directory, generating current if missing.
func LoadDir(dir string) (Bundle, error) {
	if err := os.MkdirAll(dir, identityDirMode); err != nil {
		return Bundle{}, fmt.Errorf("create age identity directory: %w", err)
	}
	currentPath := filepath.Join(dir, identityFile)
	current, err := readIdentityFile(currentPath)
	if err != nil {
		return Bundle{}, err
	}
	if current == nil {
		generated, err := Generate()
		if err != nil {
			return Bundle{}, err
		}
		if err := writeIdentityFile(currentPath, generated.current); err != nil {
			return Bundle{}, err
		}
		current = generated.current
	}
	previous, err := readIdentityFile(filepath.Join(dir, previousIdentityFile))
	if err != nil {
		return Bundle{}, err
	}
	return Bundle{current: current, previous: previous}, nil
}

// RotateDir moves current to previous and generates a new current identity.
func RotateDir(dir string) (Bundle, error) {
	bundle, err := LoadDir(dir)
	if err != nil {
		return Bundle{}, err
	}
	generated, err := Generate()
	if err != nil {
		return Bundle{}, err
	}
	if err := writeIdentityFile(filepath.Join(dir, previousIdentityFile), bundle.current); err != nil {
		return Bundle{}, err
	}
	if err := writeIdentityFile(filepath.Join(dir, identityFile), generated.current); err != nil {
		return Bundle{}, err
	}
	return Bundle{current: generated.current, previous: bundle.current}, nil
}

func readIdentityFile(path string) (*age.X25519Identity, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read age identity: %w", err)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	identity, err := parseIdentity(string(raw))
	if err != nil {
		return nil, err
	}
	return identity, nil
}

func writeIdentityFile(path string, identity *age.X25519Identity) error {
	if identity == nil {
		return fmt.Errorf("age identity is required")
	}
	if err := os.WriteFile(path, []byte(identity.String()+"\n"), identityFileMode); err != nil {
		return fmt.Errorf("write age identity: %w", err)
	}
	return os.Chmod(path, identityFileMode)
}

func parseIdentity(raw string) (*age.X25519Identity, error) {
	identities, err := age.ParseIdentities(strings.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse age identity: %w", err)
	}
	if len(identities) == 0 {
		return nil, fmt.Errorf("age identity file is empty")
	}
	typed, ok := identities[0].(*age.X25519Identity)
	if !ok {
		return nil, fmt.Errorf("age identity is not X25519")
	}
	return typed, nil
}

func parseIdentityBytes(raw []byte) (*age.X25519Identity, error) {
	return parseIdentity(string(raw))
}
