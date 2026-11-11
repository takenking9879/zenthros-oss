// SPDX-License-Identifier: Apache-2.0

package metrics

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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func selfSigned(t *testing.T, cn string, serial int64, notAfter time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn},
		NotBefore: notAfter.Add(-time.Hour), NotAfter: notAfter, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// Every CA of a two-CA bundle (a rotation) and the server certificate are
// exported with their expiry; a missing file exports nothing.
func TestCertificateExpiryExportsEveryCAInTheBundle(t *testing.T) {
	dir := t.TempDir()
	oldCA, newCA := time.Unix(1_900_000_000, 0), time.Unix(2_100_000_000, 0)
	bundle := append(selfSigned(t, "zenthros-ca", 0x1a, oldCA), selfSigned(t, "zenthros-ca", 0x2b, newCA)...)
	caFile, certFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "server.crt")
	if err := os.WriteFile(caFile, bundle, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, selfSigned(t, "zenthros-controller", 3, time.Unix(1_800_000_000, 0)), 0o644); err != nil {
		t.Fatal(err)
	}
	want := `
# HELP zenthros_ca_certificate_expiry_timestamp_seconds NotAfter of each CA in the mounted CA bundle, as a Unix timestamp. Every mTLS link fails when the CA it chains to expires.
# TYPE zenthros_ca_certificate_expiry_timestamp_seconds gauge
zenthros_ca_certificate_expiry_timestamp_seconds{serial="1a",subject="zenthros-ca"} 1.9e+09
zenthros_ca_certificate_expiry_timestamp_seconds{serial="2b",subject="zenthros-ca"} 2.1e+09
# HELP zenthros_server_certificate_expiry_timestamp_seconds NotAfter of the mounted server certificate, as a Unix timestamp. Nothing renews it.
# TYPE zenthros_server_certificate_expiry_timestamp_seconds gauge
zenthros_server_certificate_expiry_timestamp_seconds 1.8e+09
`
	if err := testutil.CollectAndCompare(CertificateExpiry(caFile, certFile), strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
	if got := EarliestExpiry(ReadCertificates(caFile)); !got.Equal(oldCA) {
		t.Errorf("earliest CA expiry = %s, want %s", got, oldCA)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(CertificateExpiry(filepath.Join(dir, "missing"), ""))
	if n, err := testutil.GatherAndCount(registry); err != nil || n != 0 {
		t.Errorf("missing files exported %d series (%v)", n, err)
	}
}
