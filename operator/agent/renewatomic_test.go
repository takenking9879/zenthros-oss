// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// The process can die between any two renames of a renewal. Whatever step it
// died at, the agent's next start finds a key and a certificate that load
// together: the old pair, or the new one. Both new
// files are written before either replaces its old one.
func TestRenewWritesKeyAndCertAtomically(t *testing.T) {
	for crashAt := 1; crashAt <= 4; crashAt++ {
		ca := newRenewCA(t)
		r := dueAgentPair(t, ca)
		renames := 0
		renameFile = func(from, to string) error {
			renames++
			if renames == crashAt {
				return errors.New("crash")
			}
			return os.Rename(from, to)
		}
		_, err := r.renewIfDue(context.Background(), func(_ context.Context, csrDER []byte) ([]byte, error) {
			csr, err := x509.ParseCertificateRequest(csrDER)
			if err != nil {
				return nil, err
			}
			return ca.sign(t, "vm-1", csr.PublicKey, 30*24*time.Hour), nil
		})
		renameFile = os.Rename
		if err == nil && crashAt <= renames {
			t.Fatalf("crash at rename %d: renewal reported success", crashAt)
		}
		// The next start.
		if err := r.recoverPending(); err != nil {
			t.Fatalf("crash at rename %d: recover: %v", crashAt, err)
		}
		if _, err := tls.LoadX509KeyPair(r.certFile, r.keyFile); err != nil {
			t.Fatalf("crash at rename %d: the pair on disk does not load: %v", crashAt, err)
		}
		entries, _ := os.ReadDir(strings.TrimSuffix(r.keyFile, "/agent.key"))
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".") || strings.HasSuffix(e.Name(), pendingSuffix) {
				t.Fatalf("crash at rename %d: left %s behind", crashAt, e.Name())
			}
		}
	}
	t.Cleanup(func() { renameFile = os.Rename })
}
