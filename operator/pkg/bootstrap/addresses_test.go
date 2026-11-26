// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"crypto/tls"
	"google.golang.org/grpc/credentials"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestAddressesAreTheListInOrder(t *testing.T) {
	for list, want := range map[string][]string{
		"hub:9443":                 {"hub:9443"},
		"hub:9443,hub:443":         {"hub:9443", "hub:443"},
		" hub:9443 , hub:443 , ":   {"hub:9443", "hub:443"},
		"[2001:db8::1]:9443,h:443": {"[2001:db8::1]:9443", "h:443"},
	} {
		got := Addresses(list)
		if len(got) != len(want) {
			t.Fatalf("%q: %v", list, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%q: %v", list, got)
			}
		}
	}
	if HostOf("hub.example:9443,hub.example:443") != "hub.example" || HostOf("[2001:db8::1]:9443") != "2001:db8::1" {
		t.Fatal("HostOf is not the first address's host")
	}
	if DialTarget("hub:9443", false) != "hub:9443" || DialTarget("hub:9443", true) != "passthrough:///hub:9443" {
		t.Fatal("one address is not dialed as it was")
	}
	if DialTarget("hub:9443,hub:443", false) != "zenthros-hub:///hub:9443,hub:443" || DialOptions("hub:9443", "") != nil || len(DialOptions("a:1,a:2", "")) != 1 ||
		DialOptions("a:1,a:2", "zenthros-controller") != nil {
		t.Fatal("a list is not dialed through the list resolver with its options")
	}
}

// serve starts a gRPC server whose health service answers SERVING for name
// only, so a call names which server took it.
func serve(t *testing.T, lis net.Listener, name string) *grpc.Server {
	t.Helper()
	srv := grpc.NewServer()
	h := health.NewServer()
	h.SetServingStatus(name, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, h)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return srv
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return lis
}

func reaches(t *testing.T, conn *grpc.ClientConn, name string, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: name}, grpc.WaitForReady(true))
		cancel()
		if err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func dial(t *testing.T, list string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(DialTarget(list, false), append(DialOptions(list, ""),
		grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// The first address that takes the connection is the one used, in the
// list's order: the Gateway's port when it answers, the passthrough when it
// does not. When the connection breaks, the first is tried again.
func TestAListReachesTheHubOnTheFirstAddressThatAnswers(t *testing.T) {
	first, second := listen(t), listen(t)
	firstAddr := first.Addr().String()
	_ = first.Close() // refused: a port the network closes
	second2 := serve(t, second, "second")
	conn := dial(t, firstAddr+","+second.Addr().String())
	if !reaches(t, conn, "second", 10*time.Second) {
		t.Fatal("the second address was not used while the first refused")
	}

	again, err := net.Listen("tcp", firstAddr)
	if err != nil {
		t.Skipf("the first port was taken meanwhile: %v", err)
	}
	serve(t, again, "first")
	second2.Stop()
	if !reaches(t, conn, "first", 20*time.Second) {
		t.Fatal("after the second address went away, the first was not tried again")
	}
}

// Both answering, the first is used: the Gateway's own port, not Traefik's.
func TestAListPrefersItsFirstAddress(t *testing.T) {
	first, second := listen(t), listen(t)
	serve(t, first, "first")
	serve(t, second, "second")
	if !reaches(t, dial(t, first.Addr().String()+","+second.Addr().String()), "first", 10*time.Second) {
		t.Fatal("both addresses answer and the first was not used")
	}
}

// A first address that takes the TCP connection and never answers (a
// firewall that drops what follows, a hung proxy) gives way to the next soon,
// not after gRPC's 20 s connect timeout.
func TestASilentFirstAddressGivesWaySoon(t *testing.T) {
	silent := listen(t)
	t.Cleanup(func() { _ = silent.Close() })
	go func() {
		for {
			c, err := silent.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	answering := listen(t)
	serve(t, answering, "second")
	started := time.Now()
	if !reaches(t, dial(t, silent.Addr().String()+","+answering.Addr().String()), "second", 8*time.Second) {
		t.Fatal("the second address was not used after the first stayed silent")
	}
	if took := time.Since(started); took > 3*time.Second {
		t.Fatalf("the silent first address held the connection %s", took)
	}
}

// A list dialed with TLS that names the hub's certificate (join.tlsServerName,
// zenthros-controller) is a client gRPC builds: the name the TLS checks is
// the authority, and no other is set beside it. The agent's first dial on
// the real hub with two addresses failed here (window B, 2026-11-08: "authority
// from transport creds and dial option don't match").
func TestAListDialsWithTheCertificatesName(t *testing.T) {
	list := "hub.example:9443,hub.example:443"
	creds := grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{ServerName: "zenthros-controller"}))
	conn, err := grpc.NewClient(DialTarget(list, false), append([]grpc.DialOption{creds}, DialOptions(list, "zenthros-controller")...)...)
	if err != nil {
		t.Fatalf("a list with the certificate's name: %v", err)
	}
	defer conn.Close()
	if conn.CanonicalTarget() == "" {
		t.Fatal("no target")
	}
}
