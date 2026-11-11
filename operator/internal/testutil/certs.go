// SPDX-License-Identifier: Apache-2.0

// Package testutil holds what tests across packages build the same way.
package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA is a self-signed certificate authority for tests, valid for an hour
// either side of now. KeyPEM is its key in SEC 1 ("EC PRIVATE KEY").
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
}

// NewCA is a CA whose subject is name.
func NewCA(t testing.TB, name string) *CA {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	cert, certPEM := create(t, tmpl, tmpl, key, key)
	return &CA{Cert: cert, Key: key, CertPEM: certPEM, KeyPEM: keyPEM(t, key)}
}

// CAPEM is a fresh CA's certificate, PEM-encoded.
func CAPEM(t testing.TB) []byte {
	t.Helper()
	return NewCA(t, "test-ca").CertPEM
}

// Pool trusts ca alone.
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	return pool
}

// Leaf is the certificate Issue signs: its subject, the names it answers
// to, and what it is used for.
type Leaf struct {
	CommonName   string
	Organization []string
	DNSNames     []string
	IPAddresses  []net.IP
	ExtKeyUsage  []x509.ExtKeyUsage
	// NotAfter is when it expires; zero is an hour from now.
	NotAfter time.Time
}

// Issued is a leaf certificate a CA signed, with its key. KeyPEM is in
// SEC 1 ("EC PRIVATE KEY").
type Issued struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
}

// Issue signs leaf with ca, valid for the two hours up to its NotAfter: by
// default an hour either side of now.
func (ca *CA) Issue(t testing.TB, leaf Leaf) Issued {
	t.Helper()
	notAfter := leaf.NotAfter
	if notAfter.IsZero() {
		notAfter = time.Now().Add(time.Hour)
	}
	key := newKey(t)
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: leaf.CommonName, Organization: leaf.Organization},
		NotBefore:    notAfter.Add(-2 * time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  leaf.ExtKeyUsage,
		DNSNames:     leaf.DNSNames,
		IPAddresses:  leaf.IPAddresses,
	}
	cert, certPEM := create(t, tmpl, ca.Cert, key, ca.Key)
	return Issued{Cert: cert, Key: key, CertPEM: certPEM, KeyPEM: keyPEM(t, key)}
}

// TLS is the leaf as a tls.Certificate.
func (i Issued) TLS() tls.Certificate {
	return tls.Certificate{Certificate: [][]byte{i.Cert.Raw}, PrivateKey: i.Key, Leaf: i.Cert}
}

// Write puts the leaf in dir/name.crt and dir/name.key and returns their
// paths.
func (i Issued) Write(t testing.TB, dir, name string) (certFile, keyFile string) {
	t.Helper()
	certFile, keyFile = filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	if err := os.WriteFile(certFile, i.CertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, i.KeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func create(t testing.TB, tmpl, parent *x509.Certificate, key, signer *ecdsa.PrivateKey) (*x509.Certificate, []byte) {
	t.Helper()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func keyPEM(t testing.TB, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}
