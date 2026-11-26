// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"regexp"
	"strings"
)

// CAPin returns the discovery pin "sha256:<hex>" of a CA certificate, computed over
// its SubjectPublicKeyInfo — the same scheme as kubeadm's
// --discovery-token-ca-cert-hash. The agent compares it against the CA the controller
// presents during the Enroll handshake to authenticate it on first contact.
func CAPin(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// JoinTokenPrefix heads a short join token, zt1_<CA pin hex>_<token>: the
// pin travels with the bearer token, as in k3s's K10 tokens, so a join
// needs only the hub's address and this. The pin still authenticates the
// hub on first contact; only how it is carried changed.
const JoinTokenPrefix = "zt1_"

// JoinToken is the short join token of a bootstrap token and the CA pin
// "sha256:<hex>" its join verifies.
func JoinToken(caPIN, token string) string {
	return JoinTokenPrefix + strings.TrimPrefix(strings.ToLower(caPIN), "sha256:") + "_" + token
}

// SplitJoinToken is the CA pin and the bootstrap token a short join token
// carries; ok is false for anything else, a bare token included.
func SplitJoinToken(s string) (caPIN, token string, ok bool) {
	rest, short := strings.CutPrefix(strings.TrimSpace(s), JoinTokenPrefix)
	if !short {
		return "", "", false
	}
	pin, token, found := strings.Cut(rest, "_")
	if !found || len(pin) != hex.EncodedLen(sha256.Size) || !tokenRE.MatchString(token) {
		return "", "", false
	}
	if _, err := hex.DecodeString(pin); err != nil || strings.ToLower(pin) != pin {
		return "", "", false
	}
	return "sha256:" + pin, token, true
}

// CAPinFromPEM computes the discovery pin "sha256:<hex>" of a PEM CA certificate. The
// agent compares this against the CA the controller presents during the Enroll
// handshake to authenticate it on first contact (kubeadm --discovery-token-ca-cert-hash).
func CAPinFromPEM(caPEM []byte) (string, error) {
	block, _ := pem.Decode(caPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", fmt.Errorf("bootstrap: no CERTIFICATE PEM in CA bundle")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("bootstrap: parse CA: %w", err)
	}
	return CAPin(cert), nil
}

// tokenRE is the kubeadm token shape: <6 alnum>.<16 alnum>. The first group is the
// token id (used in the secret name); the whole string is the secret.
var tokenRE = regexp.MustCompile(`^([a-z0-9]{6})\.([a-z0-9]{16})$`)

// TokenID validates the token format and returns its 6-char id (the secret suffix).
func TokenID(token string) (string, error) {
	m := tokenRE.FindStringSubmatch(token)
	if m == nil {
		return "", fmt.Errorf("bootstrap: invalid token format (want <6 alnum>.<16 alnum>)")
	}
	return m[1], nil
}
