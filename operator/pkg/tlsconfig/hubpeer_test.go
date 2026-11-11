// SPDX-License-Identifier: Apache-2.0

package tlsconfig

import (
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zenthros/operator/internal/testutil"
)

// signedBy returns a DER certificate for org/cn signed by ca.
func signedBy(t *testing.T, ca *testutil.CA, org, cn string) []byte {
	t.Helper()
	return ca.Issue(t, testutil.Leaf{CommonName: cn, Organization: []string{org},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}).Cert.Raw
}

// The gateway dials the Hub verifying only the chain: the Hub's certificate
// names no address the gateway dials. An agent's certificate chains to the
// same CA, so the Hub is also told by its organization, which only the
// control plane's certificates carry.
func TestHubPeerMustBeTheControlPlane(t *testing.T) {
	dir := t.TempDir()
	ca := testutil.NewCA(t, "zenthros-ca")
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, ca.CertPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM := writePair(t, dir, "gateway")
	certFile, keyFile := filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key")
	write(t, certFile, certPEM, time.Now())
	write(t, keyFile, keyPEM, time.Now())

	cfg, err := ClientConfig{CertFile: certFile, KeyFile: keyFile, CAFile: caFile, VerifyChainOnly: true,
		ServerOrganization: ControlPlaneOrganization}.Config()
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.VerifyPeerCertificate([][]byte{signedBy(t, ca, "zenthros-agents", "vm-1")}, nil); err == nil {
		t.Fatal("an agent's certificate was taken for the Hub")
	}
	if err := cfg.VerifyPeerCertificate([][]byte{signedBy(t, ca, ControlPlaneOrganization, "zenthros-controller")}, nil); err != nil {
		t.Fatalf("the controller's certificate was refused: %v", err)
	}
}
