// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zenthros/operator/internal/testutil"
)

func TestVerifyCAPinAcceptsPresentedCA(t *testing.T) {
	ca := testutil.NewCA(t, "zenthros-ca")
	caCert, caDER := ca.Cert, ca.Cert.Raw
	leafDER := ca.Issue(t, testutil.Leaf{CommonName: "zenthros-controller", DNSNames: []string{"zenthros-controller"}}).Cert.Raw
	pin := sha256.Sum256(caCert.RawSubjectPublicKeyInfo)
	if err := verifyCAPin([][]byte{leafDER, caDER}, pin); err != nil {
		t.Fatalf("verifyCAPin: %v", err)
	}
	wrong := sha256.Sum256([]byte("not-the-ca"))
	if err := verifyCAPin([][]byte{leafDER, caDER}, wrong); err == nil {
		t.Fatal("verifyCAPin accepted a mismatched pin")
	}
}

// A list with no certificate's name is checked as its first host, as one
// address is: the whole list went as the TLS name, which no certificate and
// no passthrough by name (Traefik's HostSNI) matches (code review 2026-11-08).
func TestJoinNamesTheHubByItsFirstHost(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	ca := testutil.NewCA(t, "zenthros-ca")
	leaf := ca.Issue(t, testutil.Leaf{CommonName: "zenthros-controller", DNSNames: []string{"zenthros-controller"}})
	names := make(chan string, 4)
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			server := tls.Server(conn, &tls.Config{GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
				names <- hello.ServerName
				return &tls.Config{Certificates: []tls.Certificate{leaf.TLS()}}, nil
			}})
			_ = server.Handshake()
			_ = conn.Close()
		}
	}()
	port := lis.Addr().(*net.TCPAddr).Port
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	pin := sha256.Sum256(ca.Cert.RawSubjectPublicKeyInfo)
	_, _ = Join(ctx, JoinParams{
		ControllerAddr: "localhost:" + strconv.Itoa(port) + ",localhost:1",
		Token:          "aaaaaa.bbbbbbbbbbbbbbbb",
		CACertHash:     "sha256:" + hex.EncodeToString(pin[:]),
		BackendID:      "x",
	})
	select {
	case name := <-names:
		if name != "localhost" || strings.Contains(name, ",") {
			t.Fatalf("the TLS name a list sent = %q, want its first host", name)
		}
	case <-ctx.Done():
		t.Fatal("no handshake reached the hub")
	}
}

func TestJoinRejectsCACertPEMThatDoesNotMatchPin(t *testing.T) {
	_, err := Join(t.Context(), JoinParams{
		ControllerAddr: "127.0.0.1:1",
		TLSServerName:  "zenthros-controller",
		Token:          "aaaaaa.bbbbbbbbbbbbbbbb",
		CACertHash:     "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		CACertPEM:      []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"),
		BackendID:      "x",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}
