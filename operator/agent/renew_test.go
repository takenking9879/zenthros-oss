// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

type renewCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newRenewCA(t *testing.T) *renewCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"}, IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(365 * 24 * time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &renewCA{cert: cert, key: key}
}

func (ca *renewCA) sign(t *testing.T, cn string, pub any, validFor time.Duration) []byte {
	t.Helper()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:   pkix.Name{CommonName: cn, Organization: []string{"zenthros-agents"}},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(validFor)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writeAgentPair(t *testing.T, ca *renewCA, validFor time.Duration) *certRenewer {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	r := &certRenewer{certFile: filepath.Join(dir, "agent.crt"), keyFile: filepath.Join(dir, "agent.key"),
		now: time.Now, logger: logr.Discard()}
	if err := os.WriteFile(r.keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.certFile, ca.sign(t, "vm-1", &key.PublicKey, validFor), 0o644); err != nil {
		t.Fatal(err)
	}
	return r
}

// dueAgentPair is a seven-day pair two days into its life: due.
func dueAgentPair(t *testing.T, ca *renewCA) *certRenewer {
	t.Helper()
	r := writeAgentPair(t, ca, 7*24*time.Hour)
	r.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	return r
}

// The agent renews by its certificate's own life, whatever the installation
// signs: once a seventh of it is spent, a day of the default seven. The
// certificate in hand then always has most of its life left, and a machine
// switched off for a long weekend comes back with one that still works.
// (With a fixed "ten days left", a seven-day certificate was due the moment
// it was signed.)
func TestCertRenewerRenewsByTheCertificatesOwnLife(t *testing.T) {
	day := 24 * time.Hour
	for _, tc := range []struct {
		validFor, age time.Duration
		due           bool
	}{
		{7 * day, 0, false},
		{7 * day, 12 * time.Hour, false},
		{7 * day, 25 * time.Hour, true},
		{7 * day, 6 * day, true},
		{30 * day, 2 * day, false},
		{30 * day, 5 * day, true},
		// The shortest an installation may sign. A certificate starts an
		// hour before it is signed, which is most of a shorter one's share.
		{12 * time.Hour, 30 * time.Minute, false},
		{12 * time.Hour, time.Hour, true},
	} {
		ca := newRenewCA(t)
		r := writeAgentPair(t, ca, tc.validFor)
		r.now = func() time.Time { return time.Now().Add(tc.age) }
		renewed, err := r.renewIfDue(context.Background(), func(_ context.Context, csrDER []byte) ([]byte, error) {
			csr, err := x509.ParseCertificateRequest(csrDER)
			if err != nil {
				return nil, err
			}
			return ca.sign(t, "vm-1", csr.PublicKey, tc.validFor), nil
		})
		if err != nil || renewed != tc.due {
			t.Errorf("a %v certificate, %v after it was signed: renewed=%v err=%v, want %v", tc.validFor, tc.age, renewed, err, tc.due)
		}
	}
}

// The agent looks at its certificate often enough to see it due well before
// it expires, however short the installation makes it.
func TestCertCheckFollowsAShortCertificate(t *testing.T) {
	day := 24 * time.Hour
	for validFor, want := range map[time.Duration]time.Duration{
		30 * day:       12 * time.Hour,
		7 * day:        12 * time.Hour,
		2 * day:        (2*day + time.Hour) / 14,
		12 * time.Hour: 13 * time.Hour / 14,
	} {
		r := writeAgentPair(t, newRenewCA(t), validFor)
		if got := r.checkEvery(certCheckInterval); (got - want).Abs() > time.Second {
			t.Errorf("a %v certificate is looked at every %v, want %v", validFor, got, want)
		}
	}
}

func TestCertRenewerWaitsUntilDue(t *testing.T) {
	r := writeAgentPair(t, newRenewCA(t), 30*24*time.Hour)
	renewed, err := r.renewIfDue(context.Background(), func(context.Context, []byte) ([]byte, error) {
		t.Fatal("renewed a certificate with 30 days left")
		return nil, nil
	})
	if err != nil || renewed {
		t.Fatalf("renewed=%v err=%v", renewed, err)
	}
}

// A due certificate is replaced by a pair that loads (new key matches the new
// certificate), and the CSR asks for the same identity.
func TestCertRenewerReplacesADuePair(t *testing.T) {
	ca := newRenewCA(t)
	r := dueAgentPair(t, ca)
	oldKey, _ := os.ReadFile(r.keyFile)
	renewed, err := r.renewIfDue(context.Background(), func(_ context.Context, csrDER []byte) ([]byte, error) {
		csr, err := x509.ParseCertificateRequest(csrDER)
		if err != nil {
			return nil, err
		}
		if csr.Subject.CommonName != "vm-1" {
			return nil, errors.New("csr for " + csr.Subject.CommonName)
		}
		return ca.sign(t, "vm-1", csr.PublicKey, 30*24*time.Hour), nil
	})
	if err != nil || !renewed {
		t.Fatalf("renewed=%v err=%v", renewed, err)
	}
	newKey, _ := os.ReadFile(r.keyFile)
	if bytes.Equal(oldKey, newKey) {
		t.Fatal("key was not replaced")
	}
	if _, err := tls.LoadX509KeyPair(r.certFile, r.keyFile); err != nil {
		t.Fatalf("renewed pair does not load: %v", err)
	}
	if info, _ := os.Stat(r.keyFile); info.Mode().Perm() != 0o600 {
		t.Fatalf("key mode = %v", info.Mode().Perm())
	}
}

// A failed or wrong answer leaves the working pair untouched.
func TestCertRenewerKeepsPairOnBadAnswer(t *testing.T) {
	ca := newRenewCA(t)
	r := dueAgentPair(t, ca)
	oldCert, _ := os.ReadFile(r.certFile)
	oldKey, _ := os.ReadFile(r.keyFile)
	for name, answer := range map[string]RenewFunc{
		"error": func(context.Context, []byte) ([]byte, error) { return nil, errors.New("hub down") },
		"other identity": func(_ context.Context, csrDER []byte) ([]byte, error) {
			csr, _ := x509.ParseCertificateRequest(csrDER)
			return ca.sign(t, "vm-2", csr.PublicKey, time.Hour), nil
		},
	} {
		if renewed, err := r.renewIfDue(context.Background(), answer); err == nil || renewed {
			t.Fatalf("%s: renewed=%v err=%v", name, renewed, err)
		}
		gotCert, _ := os.ReadFile(r.certFile)
		gotKey, _ := os.ReadFile(r.keyFile)
		if !bytes.Equal(gotCert, oldCert) || !bytes.Equal(gotKey, oldKey) {
			t.Fatalf("%s: pair changed after a failed renewal", name)
		}
	}
}

// A Kubernetes agent's files are its Secret, mounted read-only: the renewal
// goes to SaveCertificate whole, and the files stay as they are until the
// kubelet updates them. Writing them failed ("read-only file system") on
// every try, and the agent was cut off when its certificate expired (E2E
// 2026-10-28: the k8s agent seven days after its enrollment).
func TestARenewalWhoseFilesAreReadOnlyGoesWhereTheyComeFrom(t *testing.T) {
	ca := newRenewCA(t)
	r := dueAgentPair(t, ca)
	oldCert, _ := os.ReadFile(r.certFile)
	dir := filepath.Dir(r.certFile)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	var savedKey, savedCert []byte
	r.save = func(_ context.Context, keyPEM, certPEM []byte) error {
		savedKey, savedCert = keyPEM, certPEM
		return nil
	}
	renewed, err := r.renewIfDue(context.Background(), func(_ context.Context, csrDER []byte) ([]byte, error) {
		csr, err := x509.ParseCertificateRequest(csrDER)
		if err != nil {
			return nil, err
		}
		return ca.sign(t, "vm-1", csr.PublicKey, 7*24*time.Hour), nil
	})
	if err != nil || !renewed {
		t.Fatalf("renewed=%v err=%v", renewed, err)
	}
	if _, err := tls.X509KeyPair(savedCert, savedKey); err != nil {
		t.Fatalf("the pair saved does not load: %v", err)
	}
	if got, _ := os.ReadFile(r.certFile); !bytes.Equal(got, oldCert) {
		t.Fatal("the renewal wrote the certificate file")
	}
}
