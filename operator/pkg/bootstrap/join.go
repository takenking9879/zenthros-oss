// SPDX-License-Identifier: Apache-2.0

// Package bootstrap is how a machine joins a Zenthros hub, kubeadm-style
// (architecture §20, option 2): a one-time token, pinned to the hub's CA
// (JoinToken), redeemed by the Enroll RPC with a CSR the hub signs; the agent
// then joins the control channel with that mTLS certificate. The hub's side
// (the tokens it keeps, the Enroll service) is pkg/enrollment.
package bootstrap

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// Credential is the mTLS material an agent obtains from a successful Enroll.
type Credential struct {
	ClientCertPEM []byte
	ClientKeyPEM  []byte
	CACertPEM     []byte
}

// JoinParams configures an enrollment.
type JoinParams struct {
	ControllerAddr    string // host:port of the controller control channel, or a list of them (Addresses)
	TLSServerName     string // SNI / :authority expected on the server cert (e.g. zenthros-controller)
	Token             string // one-time bootstrap token
	CACertHash        string // discovery pin "sha256:<hex>" of the controller CA
	CACertPEM         []byte // optional CA PEM; when set, TLS verifies against it (and the pin)
	BackendID         string
	BackendType       string
	IdentityDocument  []byte
	IdentitySignature []byte
	// Dial, when set, opens the connection to the controller (a userspace
	// tailnet node's dialer); the address reaches it unresolved, so it may
	// be a name only that network knows. Nil dials the host network.
	Dial func(ctx context.Context, addr string) (net.Conn, error)
}

// DialerTarget is addr as a gRPC target for a custom dialer: passthrough,
// so gRPC hands the dialer the address as written instead of resolving it
// with the host's DNS first (a MagicDNS name resolves only on the tailnet).
func DialerTarget(addr string) string {
	return "passthrough:///" + addr
}

// Join enrolls a new agent against the controller (kubeadm-style discovery + CSR,
// architecture §20): it generates a keypair, authenticates the controller on first
// contact by pinning the CA it presents (caCertHash), submits a CSR over the Enroll
// RPC and returns the signed credential. The caller persists it with Write.
func Join(ctx context.Context, p JoinParams) (*Credential, error) {
	if pin, token, ok := SplitJoinToken(p.Token); ok {
		// A short join token: the pin it carries is the one to verify, and
		// a pin given beside it must be the same.
		if p.CACertHash != "" && !strings.EqualFold(p.CACertHash, pin) {
			return nil, fmt.Errorf("the token's CA pin %s and --ca-cert-hash %s differ", pin, p.CACertHash)
		}
		p.Token, p.CACertHash = token, pin
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate agent key: %w", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: p.BackendID, Organization: []string{"zenthros-agents"}},
	}, key)
	if err != nil {
		return nil, fmt.Errorf("create CSR: %w", err)
	}

	pin, err := parseCAPin(p.CACertHash)
	if err != nil {
		return nil, err
	}
	// The agent has no client certificate yet and does not yet trust any CA, so it
	// disables standard verification and authenticates the controller manually by
	// pinning the CA certificate it presents (TOFU, like kubeadm discovery).
	// Pin with crypto/tls directly. Do not attach VerifyPeerCertificate to the
	// tls.Config given to grpc-go: ClientHandshake overwrites ServerName from the
	// target host, and that combination has been observed to run the default x509
	// verifier (ECDSA unknown-authority) against this private CA. A raw tls.Dial
	// with skip-verify + pin succeeds on the same path.
	// The name TLS checks and gRPC's authority: the certificate's, else the
	// first address's host, never the list.
	serverName := p.TLSServerName
	if serverName == "" {
		serverName = HostOf(p.ControllerAddr)
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	}
	if len(p.CACertPEM) > 0 {
		filePin, pinErr := CAPinFromPEM(p.CACertPEM)
		if pinErr != nil {
			return nil, fmt.Errorf("parse --ca-cert: %w", pinErr)
		}
		if filePin != p.CACertHash {
			return nil, fmt.Errorf("--ca-cert does not match --ca-cert-hash")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(p.CACertPEM) {
			return nil, fmt.Errorf("--ca-cert: no certificates")
		}
		tlsCfg.RootCAs = pool
	} else {
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // pin applied below
		tlsCfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyCAPin(rawCerts, pin)
		}
	}

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))}
	if p.Dial != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(p.Dial))
	}
	dialOpts = append(dialOpts, DialOptions(p.ControllerAddr, serverName)...)
	conn, err := grpc.NewClient(DialTarget(p.ControllerAddr, p.Dial != nil), dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial controller %s: %w", p.ControllerAddr, err)
	}
	defer conn.Close()

	resp, err := agentpb.NewAgentClient(conn).Enroll(ctx, &agentpb.EnrollRequest{
		Token:             p.Token,
		Csr:               csrDER,
		BackendId:         p.BackendID,
		BackendType:       p.BackendType,
		IdentityDocument:  p.IdentityDocument,
		IdentitySignature: p.IdentitySignature,
	})
	if err != nil {
		return nil, fmt.Errorf("enroll: %w", err)
	}

	// ca.crt is trusted from now on: it must be the CA the pin names, or an
	// answer from anyone but the real hub would leave its own CA behind.
	// The pins compare as digests: the handshake takes --ca-cert-hash in any
	// spelling.
	respPin, err := CAPinFromPEM(resp.GetCaCert())
	if err != nil {
		return nil, fmt.Errorf("enroll response CA: %w", err)
	}
	got, gotErr := parseCAPin(respPin)
	want, wantErr := parseCAPin(p.CACertHash)
	if gotErr != nil || wantErr != nil || got != want {
		return nil, fmt.Errorf("enroll response CA does not match --ca-cert-hash (possible MITM)")
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal agent key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return &Credential{
		ClientCertPEM: resp.GetClientCert(),
		ClientKeyPEM:  keyPEM,
		CACertPEM:     resp.GetCaCert(),
	}, nil
}

// BackendID is the backend the hub enrolled the certificate as, its
// common name. A short join names none: the token's own is the one.
func (c *Credential) BackendID() (string, error) {
	return CertCommonName(c.ClientCertPEM)
}

// CertCommonName is the common name of a PEM certificate.
func CertCommonName(certPEM []byte) (string, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return "", errors.New("no PEM certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse certificate: %w", err)
	}
	if cert.Subject.CommonName == "" {
		return "", errors.New("the certificate names no backend")
	}
	return cert.Subject.CommonName, nil
}

// Write persists the credential as agent.crt / agent.key / ca.crt in dir (the key is
// written 0600).
func (c *Credential) Write(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	// Each file is written whole beside its name and renamed over it, the
	// key last: a crash or a full disk left a truncated agent.key beside the
	// others, which a re-join with --skip-if-existing took for credentials.
	// Without the key the set is not complete,
	// and a re-join enrolls again.
	for _, f := range []struct {
		name string
		data []byte
		perm os.FileMode
	}{
		{"ca.crt", c.CACertPEM, 0o644},
		{"agent.crt", c.ClientCertPEM, 0o644},
		{"agent.key", c.ClientKeyPEM, 0o600},
	} {
		if err := writeWhole(filepath.Join(dir, f.name), f.data, f.perm); err != nil {
			return fmt.Errorf("write %s: %w", f.name, err)
		}
	}
	return nil
}

// writeWhole writes data to path through a synced temporary file renamed
// over it: path holds the old bytes or all the new ones.
func writeWhole(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

// verifyCAPin authenticates the controller on first contact: one of the
// presented certificates is a CA whose SPKI hashes to pin, and the leaf
// chains to that CA. The CA certificate is public (the gateway sends it in
// every handshake), so finding it in the chain proves nothing by itself; the
// handshake only proves the leaf's key.
func verifyCAPin(rawCerts [][]byte, pin [sha256.Size]byte) error {
	if len(rawCerts) == 0 {
		return fmt.Errorf("controller presented no certificate")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("controller certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	roots := x509.NewCertPool()
	pinned := false
	for _, cert := range certs {
		if cert.IsCA && sha256.Sum256(cert.RawSubjectPublicKeyInfo) == pin {
			roots.AddCert(cert)
			pinned = true
		}
	}
	if !pinned {
		return fmt.Errorf("controller CA does not match --ca-cert-hash (possible MITM)")
	}
	intermediates := x509.NewCertPool()
	for _, cert := range certs[1:] {
		intermediates.AddCert(cert)
	}
	if _, err := certs[0].Verify(x509.VerifyOptions{
		Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return fmt.Errorf("controller certificate is not signed by the pinned CA (possible MITM): %w", err)
	}
	return nil
}

func parseCAPin(s string) ([sha256.Size]byte, error) {
	var out [sha256.Size]byte
	b, err := hex.DecodeString(strings.TrimPrefix(s, "sha256:"))
	if err != nil || len(b) != sha256.Size {
		return out, fmt.Errorf("invalid --ca-cert-hash %q (want sha256:<64 hex>)", s)
	}
	copy(out[:], b)
	return out, nil
}

// NetworkTailscaleUserspace is how a Container pool's agent is on a
// tailscale installation's tailnet: a node inside the agent (the agent's
// ZENTHROS_NETWORK; not an installation setting).
const NetworkTailscaleUserspace = "tailscale-userspace"
