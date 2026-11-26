// SPDX-License-Identifier: Apache-2.0

// Package tlsconfig builds the mTLS configurations for the Zenthros control
// channel. The Gateway serves TLS and asks for a client certificate, which
// every handler but Enroll requires; the agent presents its client
// certificate and verifies the server name zenthros-controller. All material is PEM
// files resolved once at startup (architecture §7, §17).
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// ServerConfig holds the controller's TLS server material.
type ServerConfig struct {
	// CertFile is the PEM server certificate (full chain).
	CertFile string
	// KeyFile is the PEM private key for the server certificate.
	KeyFile string
	// CAFile is the PEM CA bundle used to verify agent client certificates.
	CAFile string
}

// Validate fails fast on missing paths so a misconfigured server is surfaced at
// startup, not at first connection.
func (c ServerConfig) Validate() error {
	for name, path := range map[string]string{"cert": c.CertFile, "key": c.KeyFile, "ca": c.CAFile} {
		if path == "" {
			return fmt.Errorf("tls server: %s file is required", name)
		}
	}
	return nil
}

// Config builds the server *tls.Config. It presents the server certificate together
// with the CA (full chain) so a bootstrapping agent — which does not yet hold the CA —
// receives it during the handshake and can pin it (Enroll, architecture §20). A client
// certificate is OPTIONAL at the TLS layer: the Enroll RPC is called without one
// (bootstrap-token auth); every other RPC enforces a verified client certificate in
// its handler (see grpcserver).
func (c ServerConfig) Config() (*tls.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	caCerts, err := loadCABundle(c.CAFile)
	if err != nil {
		return nil, err
	}
	pair, err := newKeyPair(c.CertFile, c.KeyFile, func(cert *tls.Certificate) {
		for _, ca := range caCerts {
			cert.Certificate = append(cert.Certificate, ca.Raw)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("tls server: %w", err)
	}
	pool := x509.NewCertPool()
	for _, ca := range caCerts {
		pool.AddCert(ca)
	}
	return &tls.Config{
		// Read per handshake so a renewed certificate is served without a restart.
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return pair.current()
		},
		ClientCAs:  pool,
		ClientAuth: tls.VerifyClientCertIfGiven,
		MinVersion: tls.VersionTLS12,
	}, nil
}

// loadCAPool reads a PEM CA bundle into a certificate pool.
func loadCAPool(caFile string) (*x509.CertPool, error) {
	certs, err := loadCABundle(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	for _, c := range certs {
		pool.AddCert(c)
	}
	return pool, nil
}

// loadCABundle parses every CERTIFICATE block in a PEM bundle, in order.
func loadCABundle(caFile string) ([]*x509.Certificate, error) {
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tls: read CA %s: %w", caFile, err)
	}
	var certs []*x509.Certificate
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("tls: parse CA %s: %w", caFile, err)
		}
		certs = append(certs, c)
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("tls: no valid certificates in CA %s", caFile)
	}
	return certs, nil
}
