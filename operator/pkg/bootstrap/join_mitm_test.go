// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// enrollServer answers Enroll with caPEM and records the token it was sent.
type enrollServer struct {
	agentpb.UnimplementedAgentServer
	tokens chan string
	caPEM  []byte
}

func (s *enrollServer) Enroll(_ context.Context, req *agentpb.EnrollRequest) (*agentpb.EnrollResponse, error) {
	s.tokens <- req.GetToken()
	return &agentpb.EnrollResponse{ClientCert: []byte("cert"), CaCert: s.caPEM}, nil
}

func testCert(t *testing.T, cn string, isCA bool, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: isCA, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	if parent == nil {
		parent, parentKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func certPEM(c *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})
}

// serveEnroll starts a TLS gRPC server presenting chain (leaf first) with
// key, and answering Enroll with caPEM.
func serveEnroll(t *testing.T, chain []*x509.Certificate, key *ecdsa.PrivateKey, caPEM []byte) (string, *enrollServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var raw [][]byte
	for _, c := range chain {
		raw = append(raw, c.Raw)
	}
	srv := &enrollServer{tokens: make(chan string, 1), caPEM: caPEM}
	g := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{{Certificate: raw, PrivateKey: key}}})))
	agentpb.RegisterAgentServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String(), srv
}

func joinWith(t *testing.T, addr, pin string, caPEM []byte) (*Credential, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return Join(ctx, JoinParams{ControllerAddr: addr, Token: "abcdef.0123456789abcdef", CACertHash: pin, CACertPEM: caPEM, BackendID: "vm-1"})
}

// The Zenthros CA certificate is public: the gateway appends it to every
// handshake. A man in the middle that copies it after a leaf of its own
// passed a pin that only looked for the CA in the chain, and received the
// one-time token. The leaf must chain to the pinned CA.
func TestJoinRejectsForeignLeafBehindPinnedCA(t *testing.T) {
	realCA, _ := testCert(t, "zenthros-ca", true, nil, nil)
	pin := CAPin(realCA)
	attackerLeaf, attackerKey := testCert(t, "zenthros-controller", false, nil, nil)
	addr, srv := serveEnroll(t, []*x509.Certificate{attackerLeaf, realCA}, attackerKey, certPEM(realCA))

	if _, err := joinWith(t, addr, pin, nil); err == nil {
		t.Fatal("Join accepted a leaf the pinned CA did not sign")
	}
	select {
	case tok := <-srv.tokens:
		t.Fatalf("the impostor received the bootstrap token %q", tok)
	default:
	}
}

// The CA the response carries becomes ca.crt, which the agent trusts from
// then on. It must be the pinned CA, with or without --ca-cert, or a
// compromised enroll answer leaves its own CA behind.
func TestJoinRejectsResponseCAOtherThanPin(t *testing.T) {
	realCA, caKey := testCert(t, "zenthros-ca", true, nil, nil)
	leaf, leafKey := testCert(t, "zenthros-controller", false, realCA, caKey)
	otherCA, _ := testCert(t, "attacker-ca", true, nil, nil)
	pin := CAPin(realCA)

	for _, withFile := range []bool{false, true} {
		var file []byte
		if withFile {
			file = certPEM(realCA)
		}
		addr, _ := serveEnroll(t, []*x509.Certificate{leaf, realCA}, leafKey, certPEM(otherCA))
		_, err := joinWith(t, addr, pin, file)
		if err == nil || !strings.Contains(err.Error(), "ca-cert-hash") {
			t.Errorf("withFile=%v: Join kept a response CA other than the pin: %v", withFile, err)
		}

		addr, _ = serveEnroll(t, []*x509.Certificate{leaf, realCA}, leafKey, certPEM(realCA))
		cred, err := joinWith(t, addr, pin, file)
		if err != nil {
			t.Fatalf("withFile=%v: Join refused the real gateway: %v", withFile, err)
		}
		if string(cred.CACertPEM) != string(certPEM(realCA)) {
			t.Errorf("withFile=%v: ca.crt is not the pinned CA", withFile)
		}
	}
}

// The handshake accepts the pin however it is spelled (upper-case hex, no
// sha256: prefix); the check on the response CA compared the strings, and
// refused the real gateway's CA as a MITM.
func TestJoinAcceptsThePinHoweverSpelled(t *testing.T) {
	realCA, caKey := testCert(t, "zenthros-ca", true, nil, nil)
	leaf, leafKey := testCert(t, "zenthros-controller", false, realCA, caKey)
	pin := CAPin(realCA)
	for _, spelled := range []string{pin, strings.ToUpper(pin[len("sha256:"):]), "sha256:" + strings.ToUpper(pin[len("sha256:"):]), pin[len("sha256:"):]} {
		addr, _ := serveEnroll(t, []*x509.Certificate{leaf, realCA}, leafKey, certPEM(realCA))
		if _, err := joinWith(t, addr, spelled, nil); err != nil {
			t.Errorf("pin %q: Join refused the real gateway: %v", spelled, err)
		}
	}
}
