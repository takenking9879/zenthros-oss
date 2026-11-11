// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	externalgrpcpb "github.com/zenthros/operator/internal/grpc/gen/externalgrpc"
	"github.com/zenthros/operator/internal/testutil"
)

// fakeGateway answers every CloudProvider call with an empty response (or
// fail) and records which ones reached it.
type fakeGateway struct {
	mu     sync.Mutex
	called []string
	fail   error
}

func (g *fakeGateway) handle(_ any, stream grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(stream)
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		return err
	}
	g.mu.Lock()
	g.called = append(g.called, method[strings.LastIndex(method, "/")+1:])
	g.mu.Unlock()
	if g.fail != nil {
		return g.fail
	}
	return stream.SendMsg(&emptypb.Empty{})
}

func dialFakeGateway(t *testing.T, g *fakeGateway) externalgrpcpb.CloudProviderClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnknownServiceHandler(g.handle))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///gateway", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return externalgrpcpb.NewCloudProviderClient(conn)
}

// cloudProviderMethods are the RPCs Cluster Autoscaler may call.
func cloudProviderMethods() []reflect.Method {
	iface := reflect.TypeOf((*externalgrpcpb.CloudProviderServer)(nil)).Elem()
	var methods []reflect.Method
	for i := 0; i < iface.NumMethod(); i++ {
		if m := iface.Method(i); m.IsExported() {
			methods = append(methods, m)
		}
	}
	return methods
}

// Every call Cluster Autoscaler makes reaches the gateway: one the proxy
// does not forward would answer Unimplemented, and CA would read that as
// the cloud provider's answer.
func TestCAProxyForwardsEveryCall(t *testing.T) {
	g := &fakeGateway{}
	proxy := reflect.ValueOf(CAProxy{Hub: dialFakeGateway(t, g)})
	var want []string
	for _, m := range cloudProviderMethods() {
		want = append(want, m.Name)
		req := reflect.New(m.Type.In(1).Elem())
		out := proxy.MethodByName(m.Name).Call([]reflect.Value{reflect.ValueOf(context.Background()), req})
		if err, _ := out[1].Interface().(error); err != nil {
			t.Errorf("%s: %v", m.Name, err)
		}
	}
	sort.Strings(want)
	sort.Strings(g.called)
	if len(want) < 15 || strings.Join(g.called, ",") != strings.Join(want, ",") {
		t.Fatalf("reached the gateway: %v\nwant: %v", g.called, want)
	}
}

// The gateway's refusals reach CA as they are: IncreaseSize's
// FailedPrecondition and ResourceExhausted drive its backoff.
func TestCAProxyPassesTheGatewaysErrors(t *testing.T) {
	g := &fakeGateway{fail: status.Error(codes.FailedPrecondition, "the cluster cannot take nodes now")}
	proxy := CAProxy{Hub: dialFakeGateway(t, g)}
	_, err := proxy.NodeGroupIncreaseSize(context.Background(), &externalgrpcpb.NodeGroupIncreaseSizeRequest{Id: "pool-a", Delta: 1})
	if s := status.Convert(err); s.Code() != codes.FailedPrecondition || s.Message() != "the cluster cannot take nodes now" {
		t.Fatalf("err = %v", err)
	}
}

// issue writes a leaf of ca to dir/<name>.crt and .key.
func issue(t *testing.T, ca *testutil.CA, dir, name string, usage x509.ExtKeyUsage, dns ...string) (string, string) {
	t.Helper()
	return ca.Issue(t, testutil.Leaf{CommonName: name, DNSNames: dns, ExtKeyUsage: []x509.ExtKeyUsage{usage}}).Write(t, dir, name)
}

// Only Cluster Autoscaler, with the client certificate the chart gave it,
// gets through: no certificate, or one of another CA, fails the handshake
// and never reaches the gateway.
func TestCAProxyTakesOnlyItsClusterAutoscaler(t *testing.T) {
	dir := t.TempDir()
	local, other := testutil.NewCA(t, "zenthros-ca-proxy"), testutil.NewCA(t, "someone else")
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, local.CertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	serverCert, serverKey := issue(t, local, dir, "server", x509.ExtKeyUsageServerAuth, "zenthros-agent-ca-proxy.zenthros-system.svc")
	tlsCfg, err := CAProxyTLS(serverCert, serverKey, caFile)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	g := &fakeGateway{}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- ServeCAProxy(ctx, addr, tlsCfg, dialFakeGateway(t, g), logr.Discard()) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("ServeCAProxy: %v", err)
		}
	})

	roots := x509.NewCertPool()
	roots.AddCert(local.Cert)
	call := func(client *tls.Certificate) error {
		cfg := &tls.Config{RootCAs: roots, ServerName: "zenthros-agent-ca-proxy.zenthros-system.svc", MinVersion: tls.VersionTLS12}
		if client != nil {
			cfg.Certificates = []tls.Certificate{*client}
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
		if err != nil {
			return err
		}
		defer conn.Close()
		callCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_, err = externalgrpcpb.NewCloudProviderClient(conn).NodeGroups(callCtx, &externalgrpcpb.NodeGroupsRequest{}, grpc.WaitForReady(false))
		return err
	}
	load := func(certFile, keyFile string) *tls.Certificate {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatal(err)
		}
		return &pair
	}
	var lastErr error
	for range 50 { // the listener starts asynchronously
		if lastErr = call(load(issue(t, local, dir, "cluster-autoscaler", x509.ExtKeyUsageClientAuth))); lastErr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("Cluster Autoscaler refused: %v", lastErr)
	}
	if err := call(nil); err == nil {
		t.Error("a caller without a certificate got through")
	}
	if err := call(load(issue(t, other, dir, "intruder", x509.ExtKeyUsageClientAuth))); err == nil {
		t.Error("a certificate of another CA got through")
	}
	if len(g.called) != 1 {
		t.Fatalf("the gateway saw %v, want only Cluster Autoscaler's call", g.called)
	}
}
