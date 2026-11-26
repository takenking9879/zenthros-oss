// SPDX-License-Identifier: Apache-2.0

package tlsconfig

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writePair(t *testing.T, dir, cn string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func leafCN(t *testing.T, pair *keyPair) string {
	t.Helper()
	cert, err := pair.current()
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf.Subject.CommonName
}

func write(t *testing.T, path string, data []byte, mod time.Time) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}
}

// A renewed certificate on disk is served on the next handshake; a half
// replaced pair (new key, old certificate) keeps the old one in service.
func TestKeyPairReloadsRenewedFilesAndKeepsOldOnMismatch(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	oldCert, oldKey := writePair(t, dir, "old")
	start := time.Now().Add(-time.Minute)
	write(t, certFile, oldCert, start)
	write(t, keyFile, oldKey, start)
	pair, err := newKeyPair(certFile, keyFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cn := leafCN(t, pair); cn != "old" {
		t.Fatalf("cn = %q", cn)
	}

	newCert, newKey := writePair(t, dir, "new")
	write(t, keyFile, newKey, start.Add(time.Second))
	if cn := leafCN(t, pair); cn != "old" {
		t.Fatalf("mismatched pair served %q, want the old certificate", cn)
	}
	write(t, certFile, newCert, start.Add(2*time.Second))
	if cn := leafCN(t, pair); cn != "new" {
		t.Fatalf("renewed pair served %q, want new", cn)
	}
}
