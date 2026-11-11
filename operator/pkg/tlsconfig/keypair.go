// SPDX-License-Identifier: Apache-2.0

package tlsconfig

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"
)

// keyPair serves a certificate from disk and reloads it when either file
// changes, so a renewed certificate takes effect on the next handshake
// without a restart. A pair that fails to load (for example the key already
// replaced and the certificate not yet) keeps the previous one in service.
type keyPair struct {
	certFile, keyFile string
	// extend adds to a freshly loaded certificate (the server appends its CA).
	extend func(*tls.Certificate)

	mu      sync.Mutex
	cert    *tls.Certificate
	certMod time.Time
	keyMod  time.Time
}

func newKeyPair(certFile, keyFile string, extend func(*tls.Certificate)) (*keyPair, error) {
	k := &keyPair{certFile: certFile, keyFile: keyFile, extend: extend}
	if _, err := k.current(); err != nil {
		return nil, err
	}
	return k, nil
}

func (k *keyPair) current() (*tls.Certificate, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	certMod, keyMod := modTime(k.certFile), modTime(k.keyFile)
	if k.cert != nil && certMod.Equal(k.certMod) && keyMod.Equal(k.keyMod) {
		return k.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(k.certFile, k.keyFile)
	if err != nil {
		if k.cert != nil {
			return k.cert, nil
		}
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	if k.extend != nil {
		k.extend(&cert)
	}
	k.cert, k.certMod, k.keyMod = &cert, certMod, keyMod
	return k.cert, nil
}

func modTime(path string) time.Time {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return info.ModTime()
}
