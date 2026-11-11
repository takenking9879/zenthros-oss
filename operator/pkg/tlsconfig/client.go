// SPDX-License-Identifier: Apache-2.0

package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"slices"
)

// ClientConfig holds the agent's TLS client material.
type ClientConfig struct {
	// CertFile is the PEM client certificate presented to the controller.
	CertFile string
	// KeyFile is the PEM private key for the client certificate.
	KeyFile string
	// CAFile is the PEM CA bundle used to verify the controller's server certificate.
	CAFile string
	// ServerName overrides the expected server certificate name (dev/testing only).
	ServerName string
	// InsecureSkipVerify disables server certificate verification (dev only).
	InsecureSkipVerify bool
	// VerifyChainOnly verifies the server certificate chains to the CA but skips
	// hostname verification. Used by the controller when dialing an agent's log
	// server: agent certs are identified by their backend-id CN and carry no hostname
	// SANs, so hostname verification would always fail.
	VerifyChainOnly bool
	// ServerOrganization, with VerifyChainOnly, is the organization the
	// server's certificate must carry. An agent's certificate chains to the
	// same CA: the gateway's Hub hop names ControlPlaneOrganization so an
	// agent cannot pose as the Hub.
	ServerOrganization string
}

// ControlPlaneOrganization is the organization of the controller's and the
// gateway's certificates; an agent's is zenthros-agents.
const ControlPlaneOrganization = "Zenthros"

// Validate fails fast on missing paths so a misconfigured agent is surfaced at
// startup, not at first dial.
func (c ClientConfig) Validate() error {
	for name, path := range map[string]string{"cert": c.CertFile, "key": c.KeyFile, "ca": c.CAFile} {
		if path == "" {
			return fmt.Errorf("tls client: %s file is required", name)
		}
	}
	return nil
}

// Config builds the client *tls.Config: it presents the client certificate and
// verifies the controller's certificate against the configured CA (mutual TLS).
func (c ClientConfig) Config() (*tls.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	pair, err := newKeyPair(c.CertFile, c.KeyFile, nil)
	if err != nil {
		return nil, fmt.Errorf("tls client: %w", err)
	}
	pool, err := loadCAPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		// Read per handshake so a renewed certificate is used on the next dial.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return pair.current()
		},
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
	if c.VerifyChainOnly {
		// Agent certs carry no hostname SANs (they are keyed by backend-id CN), so
		// hostname verification cannot succeed. Security comes from the private CA
		// (only Zenthros agents hold a signed cert) plus Tailscale MagicDNS, which
		// already addresses a specific backend by name — verify the chain, skip the
		// hostname.
		cfg.InsecureSkipVerify = true
		cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("tls client: server presented no certificate")
			}
			leaf, parseErr := x509.ParseCertificate(rawCerts[0])
			if parseErr != nil {
				return fmt.Errorf("tls client: parse server certificate: %w", parseErr)
			}
			if _, verifyErr := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); verifyErr != nil {
				return verifyErr
			}
			if c.ServerOrganization != "" && !slices.Contains(leaf.Subject.Organization, c.ServerOrganization) {
				return fmt.Errorf("tls client: server certificate %q is of %v, not %s", leaf.Subject.CommonName, leaf.Subject.Organization, c.ServerOrganization)
			}
			return nil
		}
		return cfg, nil
	}
	cfg.ServerName = c.ServerName
	cfg.InsecureSkipVerify = c.InsecureSkipVerify
	return cfg, nil
}
