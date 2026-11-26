// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
)

const (
	// certRenewAfterShare is when the agent asks for a new certificate: once
	// one part in this many of the certificate's own life is spent, a day of
	// the seven the controller signs by default
	// (the hub's enrollment). The certificate in hand then has
	// most of its life left, and a machine switched off for a few days comes
	// back with one that still works. It reads the life from the certificate:
	// the installation may sign any other.
	certRenewAfterShare = 7
	// certCheckInterval is how often a connected agent looks at its expiry,
	// and certChecksPerLife how many times at least in a certificate's life.
	certCheckInterval = 12 * time.Hour
	certChecksPerLife = 2 * certRenewAfterShare
)

// RenewFunc sends a CSR over the authenticated control connection and returns
// the signed certificate. The Gateway reads the identity from the certificate
// in use, not from the CSR.
type RenewFunc func(ctx context.Context, csrDER []byte) (certPEM []byte, err error)

// certRenewer replaces the agent certificate before it expires. The TLS
// configuration reads the files on every handshake, so the next connection
// presents the new certificate without a restart.
type certRenewer struct {
	certFile, keyFile string
	// save keeps the new pair where the files come from, in place of
	// writing them (Config.SaveCertificate).
	save   func(ctx context.Context, keyPEM, certPEM []byte) error
	now    func() time.Time
	logger logr.Logger
}

func (a *Agent) certRenewer() *certRenewer {
	return &certRenewer{
		certFile: a.cfg.TLS.CertFile, keyFile: a.cfg.TLS.KeyFile,
		save: a.cfg.SaveCertificate, now: time.Now, logger: a.logger,
	}
}

// due reports whether cert has spent its share of its life
// (certRenewAfterShare).
func (r *certRenewer) due(cert *x509.Certificate) bool {
	return r.now().Sub(cert.NotBefore) >= cert.NotAfter.Sub(cert.NotBefore)/certRenewAfterShare
}

// checkEvery is how long to wait for the next look at the certificate:
// interval, or less for a certificate too short for that.
func (r *certRenewer) checkEvery(interval time.Duration) time.Duration {
	current, err := readCertificate(r.certFile)
	if err != nil {
		return interval
	}
	return max(min(interval, current.NotAfter.Sub(current.NotBefore)/certChecksPerLife), time.Minute)
}

// renewIfDue renews a certificate that is due. It reports whether it
// renewed.
func (r *certRenewer) renewIfDue(ctx context.Context, renew RenewFunc) (bool, error) {
	current, err := readCertificate(r.certFile)
	if err != nil {
		return false, err
	}
	if !r.due(current) {
		return false, nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return false, fmt.Errorf("generate agent key: %w", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: current.Subject.CommonName, Organization: current.Subject.Organization},
	}, key)
	if err != nil {
		return false, fmt.Errorf("create CSR: %w", err)
	}
	certPEM, err := renew(ctx, csrDER)
	if err != nil {
		return false, fmt.Errorf("renew certificate: %w", err)
	}
	issued, err := parseCertificatePEM(certPEM)
	if err != nil {
		return false, fmt.Errorf("renewed certificate: %w", err)
	}
	if issued.Subject.CommonName != current.Subject.CommonName {
		return false, fmt.Errorf("renewed certificate is for %q, not %q", issued.Subject.CommonName, current.Subject.CommonName)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return false, fmt.Errorf("encode agent key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if r.save != nil {
		err = r.save(ctx, keyPEM, certPEM)
	} else {
		err = r.commit(keyPEM, certPEM)
	}
	if err != nil {
		return false, err
	}
	r.logger.Info("agent certificate renewed", "backendID", issued.Subject.CommonName, "notAfter", issued.NotAfter)
	return true, nil
}

// keepRenewed checks at connect and then every interval (checkEvery) until
// ctx ends.
func (r *certRenewer) keepRenewed(ctx context.Context, renew RenewFunc, interval time.Duration) {
	for {
		if _, err := r.renewIfDue(ctx, renew); err != nil && ctx.Err() == nil {
			r.logger.Error(err, "certificate renewal failed; retrying later")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.checkEvery(interval)):
		}
	}
}

func readCertificate(path string) (*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read agent certificate: %w", err)
	}
	return parseCertificatePEM(data)
}

func parseCertificatePEM(data []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("no PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

// writeAtomic replaces path through a rename in the same directory, so a
// reader sees the old file or the new one, never half of it.
func writeAtomic(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := renameFile(tmp.Name(), path); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// renameFile is os.Rename; tests stop it at a step, as a crash would.
var renameFile = os.Rename

// commit replaces the pair. Two renames are not one: a process that dies
// between them leaves a key and a certificate that do not match, and the
// agent cannot connect again. So both new files are
// written beside the old ones first, the certificate last: once it is there
// the new pair is complete, and recoverPending finishes the replacement at
// the next start.
func (r *certRenewer) commit(keyPEM, certPEM []byte) error {
	keyNext, certNext := r.keyFile+pendingSuffix, r.certFile+pendingSuffix
	if err := writeAtomic(keyNext, keyPEM, 0o600); err != nil {
		return err
	}
	if err := writeAtomic(certNext, certPEM, 0o644); err != nil {
		return err
	}
	// Until the certificate follows the key the pair does not match, and the
	// TLS loader keeps serving the old one.
	if err := renameFile(keyNext, r.keyFile); err != nil {
		return fmt.Errorf("replace %s: %w", r.keyFile, err)
	}
	if err := renameFile(certNext, r.certFile); err != nil {
		return fmt.Errorf("replace %s: %w", r.certFile, err)
	}
	return nil
}

// pendingSuffix names a new key or certificate written but not in place yet.
const pendingSuffix = ".next"

// recoverPending finishes or drops a replacement a dead process left: both
// new files, or the certificate alone after the key moved, are the new pair
// and go in place; a key alone is a renewal that never got its certificate.
// It runs before the agent first reads the pair.
func (r *certRenewer) recoverPending() error {
	keyNext, certNext := r.keyFile+pendingSuffix, r.certFile+pendingSuffix
	haveKey, haveCert := exists(keyNext), exists(certNext)
	switch {
	case haveKey && haveCert:
		if _, err := tls.LoadX509KeyPair(certNext, keyNext); err != nil {
			return errors.Join(os.Remove(keyNext), os.Remove(certNext))
		}
		if err := renameFile(keyNext, r.keyFile); err != nil {
			return err
		}
		return renameFile(certNext, r.certFile)
	case haveCert:
		if _, err := tls.LoadX509KeyPair(certNext, r.keyFile); err != nil {
			return os.Remove(certNext)
		}
		return renameFile(certNext, r.certFile)
	case haveKey:
		return os.Remove(keyNext)
	}
	return nil
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
