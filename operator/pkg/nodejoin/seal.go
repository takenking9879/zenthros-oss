// SPDX-License-Identifier: Apache-2.0

// Package nodejoin is the node credential a joined pool's machine receives
// from its cluster (docs/design/K8S_NODE_SUPPLY.md): the cluster's agent
// issues it and seals it to a key only the machine holds, so the hub that
// carries it between them cannot read it.
package nodejoin

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// sealInfo binds the key derivation to this use and version.
const sealInfo = "zenthros node join credential v1"

// Credential is what a machine needs to join its cluster.
type Credential struct {
	// Server is the cluster's API URL as its nodes reach it.
	Server string `json:"server"`
	// TokenID and TokenSecret are a Kubernetes bootstrap token for this one
	// node.
	TokenID     string `json:"tokenID"`
	TokenSecret string `json:"tokenSecret"`
	// CACert is the cluster's CA bundle (PEM): the node trusts the API it
	// joins only through it.
	CACert []byte `json:"caCert"`
	// ExpiresAt is when the bootstrap token stops working.
	ExpiresAt time.Time `json:"expiresAt"`
}

// Validate reports a credential a machine cannot join with.
func (c Credential) Validate() error {
	switch {
	case len(c.Server) < len("https://x") || c.Server[:8] != "https://":
		return fmt.Errorf("credential server %q is not an https URL", c.Server)
	case !validTokenPart(c.TokenID, 6) || !validTokenPart(c.TokenSecret, 16):
		return errors.New("credential has no well-formed bootstrap token")
	case len(c.CACert) == 0:
		return errors.New("credential has no cluster CA")
	}
	return nil
}

// validTokenPart: a bootstrap token's id is [a-z0-9]{6}, its secret [a-z0-9]{16}.
func validTokenPart(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// NewKey is a machine's key for one join request.
func NewKey() (*ecdh.PrivateKey, error) {
	return ecdh.X25519().GenerateKey(rand.Reader)
}

// Seal encrypts credential to the machine's public key, for the node named by
// providerID only: an ephemeral X25519 key agreement, HKDF-SHA256 and
// AES-256-GCM with the providerID as additional data. The result is the
// ephemeral public key, the nonce and the ciphertext.
func Seal(machineKey []byte, providerID string, credential Credential) ([]byte, error) {
	recipient, err := ecdh.X25519().NewPublicKey(machineKey)
	if err != nil {
		return nil, fmt.Errorf("machine key: %w", err)
	}
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	shared, err := ephemeral.ECDH(recipient)
	if err != nil {
		return nil, err
	}
	aead, err := sealAEAD(shared, ephemeral.PublicKey().Bytes(), machineKey)
	if err != nil {
		return nil, err
	}
	plaintext, err := json.Marshal(credential)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := append(ephemeral.PublicKey().Bytes(), nonce...)
	return aead.Seal(out, nonce, plaintext, []byte(providerID)), nil
}

// Open decrypts what Seal produced for the machine's key and providerID.
func Open(key *ecdh.PrivateKey, providerID string, sealed []byte) (Credential, error) {
	const keySize = 32
	if len(sealed) < keySize {
		return Credential{}, errors.New("sealed credential is too short")
	}
	ephemeral, err := ecdh.X25519().NewPublicKey(sealed[:keySize])
	if err != nil {
		return Credential{}, err
	}
	shared, err := key.ECDH(ephemeral)
	if err != nil {
		return Credential{}, err
	}
	aead, err := sealAEAD(shared, sealed[:keySize], key.PublicKey().Bytes())
	if err != nil {
		return Credential{}, err
	}
	rest := sealed[keySize:]
	if len(rest) < aead.NonceSize() {
		return Credential{}, errors.New("sealed credential is too short")
	}
	plaintext, err := aead.Open(nil, rest[:aead.NonceSize()], rest[aead.NonceSize():], []byte(providerID))
	if err != nil {
		return Credential{}, errors.New("sealed credential does not open with this key for this node")
	}
	var credential Credential
	if err := json.Unmarshal(plaintext, &credential); err != nil {
		return Credential{}, err
	}
	return credential, credential.Validate()
}

// sealAEAD derives the AES-256-GCM key from the agreement and both public keys.
func sealAEAD(shared, ephemeralKey, machineKey []byte) (cipher.AEAD, error) {
	salt := append(append([]byte{}, ephemeralKey...), machineKey...)
	key, err := hkdf.Key(sha256.New, shared, salt, sealInfo, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
