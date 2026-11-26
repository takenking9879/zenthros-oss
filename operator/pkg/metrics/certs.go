// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	caCertExpiryDesc = prometheus.NewDesc("zenthros_ca_certificate_expiry_timestamp_seconds",
		"NotAfter of each CA in the mounted CA bundle, as a Unix timestamp. Every mTLS link fails when the CA it chains to expires.",
		[]string{"subject", "serial"}, nil)
	serverCertExpiryDesc = prometheus.NewDesc("zenthros_server_certificate_expiry_timestamp_seconds",
		"NotAfter of the mounted server certificate, as a Unix timestamp. Nothing renews it.",
		nil, nil)
)

// certificateExpiry reads the mounted CA bundle and server certificate at
// each scrape: the server pair is re-read per handshake, so a re-issued one
// shows at once, and a file that does not parse exports nothing.
type certificateExpiry struct {
	caFile, certFile string
}

// CertificateExpiry exports when the CA bundle at caFile and the server
// certificate at certFile expire (nothing watched
// the CA, which once lapsed with its leaves a year after install).
func CertificateExpiry(caFile, certFile string) prometheus.Collector {
	return certificateExpiry{caFile: caFile, certFile: certFile}
}

func (c certificateExpiry) Describe(ch chan<- *prometheus.Desc) {
	ch <- caCertExpiryDesc
	ch <- serverCertExpiryDesc
}

func (c certificateExpiry) Collect(ch chan<- prometheus.Metric) {
	for _, ca := range ReadCertificates(c.caFile) {
		ch <- prometheus.MustNewConstMetric(caCertExpiryDesc, prometheus.GaugeValue,
			float64(ca.NotAfter.Unix()), ca.Subject.CommonName, ca.SerialNumber.Text(16))
	}
	if certs := ReadCertificates(c.certFile); len(certs) > 0 {
		ch <- prometheus.MustNewConstMetric(serverCertExpiryDesc, prometheus.GaugeValue, float64(certs[0].NotAfter.Unix()))
	}
}

// ReadCertificates parses every certificate in a PEM file, in order; nil
// when the file is unset, unreadable or holds none.
func ReadCertificates(path string) []*x509.Certificate {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var certs []*x509.Certificate
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			return certs
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
			certs = append(certs, cert)
		}
	}
}

// EarliestExpiry is the first NotAfter among certs, zero for none.
func EarliestExpiry(certs []*x509.Certificate) time.Time {
	var first time.Time
	for _, cert := range certs {
		if first.IsZero() || cert.NotAfter.Before(first) {
			first = cert.NotAfter
		}
	}
	return first
}
