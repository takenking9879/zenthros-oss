// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/nodejoin"
)

const testProviderID = "zenthros://pool-a/slot-0/uid-1"

// fakeJoinHub is the gateway's NodeJoin: not ready on the first ask, then the
// credential sealed to the key the machine asked with.
type fakeJoinHub struct {
	agentpb.UnimplementedNodeJoinServer
	credential nodejoin.Credential
	plan       *agentpb.NodeJoinCredentialResponse
	refuse     error

	mu      sync.Mutex
	asks    int
	keys    map[string]bool
	reports []*agentpb.NodeJoinReport
}

func (h *fakeJoinHub) Credential(_ context.Context, req *agentpb.NodeJoinCredentialRequest) (*agentpb.NodeJoinCredentialResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.refuse != nil {
		return nil, h.refuse
	}
	h.asks++
	if h.keys == nil {
		h.keys = map[string]bool{}
	}
	h.keys[string(req.GetPublicKey())] = true
	if h.asks == 1 {
		return &agentpb.NodeJoinCredentialResponse{RetryAfterSeconds: 0}, nil
	}
	sealed, err := nodejoin.Seal(req.GetPublicKey(), testProviderID, h.credential)
	if err != nil {
		return nil, err
	}
	resp := proto.Clone(h.plan).(*agentpb.NodeJoinCredentialResponse)
	resp.Ready, resp.Sealed = true, sealed
	return resp, nil
}

func (h *fakeJoinHub) Report(_ context.Context, req *agentpb.NodeJoinReport) (*agentpb.NodeJoinReportAck, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reports = append(h.reports, req)
	return &agentpb.NodeJoinReportAck{}, nil
}

type joinNodeFixture struct {
	hub         *fakeJoinHub
	params      JoinNodeParams
	systemctl   [][]string
	execs       [][]string
	k3s         []byte
	downloads   map[string][]byte
	downloadURL string
	fsType      int64
}

// newJoinNodeFixture is a machine whose gateway hands it a credential for
// the cluster API served by api, and whose k3s download serves k3s.
func newJoinNodeFixture(t *testing.T, api *httptest.Server) *joinNodeFixture {
	t.Helper()
	f := &joinNodeFixture{k3s: []byte("#!/bin/sh\necho k3s\n")}
	sum := sha256.Sum256(f.k3s)
	f.downloads = map[string][]byte{"/k3s": f.k3s}
	download := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, ok := f.downloads[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(download.Close)
	f.downloads["/k3s"] = f.k3s
	f.hub = &fakeJoinHub{
		credential: nodejoin.Credential{
			Server: api.URL, TokenID: "abcdef", TokenSecret: "0123456789abcdef",
			CACert:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw}),
			ExpiresAt: time.Now().Add(15 * time.Minute),
		},
		plan: &agentpb.NodeJoinCredentialResponse{
			Method: "k3s", NodeName: "slot-0-abcdef12", ProviderId: testProviderID,
			Labels:     map[string]string{"zone": "a b", "infrastructure.zenthros.io/vmpool": "pool-a"},
			Taints:     []string{"infrastructure.zenthros.io/vmpool=pool-a:NoSchedule", "gpu=true:NoSchedule"},
			K3SVersion: "v1.34.1+k3s1", K3SUrl: download.URL + "/k3s", K3SSha256: hex.EncodeToString(sum[:]),
			KubeReservedCpu: "70m", KubeReservedMemory: "1Gi", EvictionHardMemory: "100Mi",
		},
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	agentpb.RegisterNodeJoinServer(server, f.hub)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	f.params = JoinNodeParams{
		Root: t.TempDir(), Wait: 10 * time.Second, PreflightRetry: time.Millisecond,
		Dial: func(context.Context) (*grpc.ClientConn, error) {
			return grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		},
		Systemctl: func(_ context.Context, args ...string) error {
			f.systemctl = append(f.systemctl, args)
			return nil
		},
		Exec: func(_ context.Context, name string, args ...string) error {
			f.execs = append(f.execs, append([]string{name}, args...))
			return nil
		},
		LookPath:     func(name string) (string, error) { return "/usr/local/bin/" + name, nil },
		FSType:       func(string) (int64, error) { return f.fsType, nil },
		RuntimeReady: func() bool { return true },
	}
	f.downloadURL = download.URL
	return f
}

func (f *joinNodeFixture) file(t *testing.T, path string) (string, os.FileMode) {
	t.Helper()
	info, err := os.Stat(filepath.Join(f.params.Root, path))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(f.params.Root, path))
	if err != nil {
		t.Fatal(err)
	}
	return string(data), info.Mode().Perm()
}

// The machine waits for its credential, checks the cluster's API, installs
// the pinned k3s and starts an agent that registers as the slot's node: the
// providerID the cluster autoscaler maps back, the labels and taints the
// template node has, the token only in its 0600 file.
func TestJoinNodeStartsTheK3sAgentWithTheSealedCredential(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	defer api.Close()
	f := newJoinNodeFixture(t, api)
	if err := JoinNode(t.Context(), f.params, logr.Discard()); err != nil {
		t.Fatalf("JoinNode: %v", err)
	}
	if f.hub.asks < 2 || len(f.hub.keys) != 1 {
		t.Fatalf("asked %d times with %d keys, want one key asked again until ready", f.hub.asks, len(f.hub.keys))
	}

	token, mode := f.file(t, k3sJoinTokenFile)
	if mode != 0o600 || strings.TrimSpace(token) != nodejoin.K3sToken(f.hub.credential) {
		t.Fatalf("token file %o %q, want 0600 with the k3s token", mode, token)
	}
	raw, mode := f.file(t, k3sConfigDropIn)
	var config map[string]any
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		t.Fatal(err)
	}
	if mode != 0o600 || config["server"] != api.URL || config["node-name"] != "slot-0-abcdef12" || config["token-file"] != k3sJoinTokenFile {
		t.Fatalf("k3s config %o %v", mode, config)
	}
	// The plan's reservations (nodeshape, as the template holds back); the
	// memory threshold joins k3s' own disk ones.
	if args, _ := json.Marshal(config["kubelet-arg"]); string(args) != `["provider-id=`+testProviderID+`",`+
		`"kube-reserved=cpu=70m,memory=1Gi","eviction-hard=memory.available\u003c100Mi,imagefs.available\u003c5%,nodefs.available\u003c5%"]` {
		t.Fatalf("kubelet-arg %s", args)
	}
	if labels := config["node-label"].([]any); len(labels) != 2 || labels[0] != "infrastructure.zenthros.io/vmpool=pool-a" || labels[1] != "zone=a b" {
		t.Fatalf("node-label %v", labels)
	}
	if taints := config["node-taint"].([]any); len(taints) != 2 || taints[0] != "infrastructure.zenthros.io/vmpool=pool-a:NoSchedule" {
		t.Fatalf("node-taint %v, want the pool taint first", taints)
	}
	if strings.Contains(raw, f.hub.credential.TokenSecret) {
		t.Fatal("the token is in the config, not only in its file")
	}
	if k3s, mode := f.file(t, k3sBinary); k3s != string(f.k3s) || mode != 0o755 {
		t.Fatalf("k3s binary %o %q", mode, k3s)
	}
	if unit, _ := f.file(t, k3sAgentUnitFile); !strings.Contains(unit, "ExecStart="+k3sBinary+" agent") {
		t.Fatalf("unit %s", unit)
	}
	if len(f.systemctl) != 2 || f.systemctl[0][0] != "daemon-reload" || strings.Join(f.systemctl[1], " ") != "enable --now --no-block "+k3sAgentUnitName {
		t.Fatalf("systemctl %v", f.systemctl)
	}
}

// A machine that cannot reach the cluster's API says so (the slot fails
// with JoinUnreachable for the autoscaler) and writes nothing.
func TestJoinNodeReportsAnUnreachableCluster(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	f := newJoinNodeFixture(t, api)
	api.Close()
	err := JoinNode(t.Context(), f.params, logr.Discard())
	if err == nil || !strings.Contains(err.Error(), infrav1alpha1.NodeJoinReasonUnreachable) {
		t.Fatalf("JoinNode = %v, want %s", err, infrav1alpha1.NodeJoinReasonUnreachable)
	}
	f.assertReportedNothingWritten(t, infrav1alpha1.NodeJoinReasonUnreachable)
}

// An API whose certificate does not chain to the cluster CA in the
// credential is not the cluster: the token is never sent there.
func TestJoinNodeRefusesAnAPIOutsideTheClusterCA(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	defer api.Close()
	f := newJoinNodeFixture(t, api)
	// A CA nobody's server presents: every httptest server presents the
	// same built-in certificate.
	f.hub.credential.CACert = testutil.NewCA(t, "another cluster CA").CertPEM
	err := JoinNode(t.Context(), f.params, logr.Discard())
	if err == nil || !strings.Contains(err.Error(), infrav1alpha1.NodeJoinReasonRejected) {
		t.Fatalf("JoinNode = %v, want %s", err, infrav1alpha1.NodeJoinReasonRejected)
	}
	f.assertReportedNothingWritten(t, infrav1alpha1.NodeJoinReasonRejected)
}

// A k3s download that does not match its pin is never installed or run.
// Only the pin says the k3s is of the cluster's minor: a binary off it, or a
// plan with no pin, never runs (not even one already on the machine); nor
// does a plan whose reservations are not quantities.
func TestJoinNodeRefusesAK3sOffItsPin(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	defer api.Close()
	for name, unpin := range map[string]func(*agentpb.NodeJoinCredentialResponse){
		"wrong digest": func(plan *agentpb.NodeJoinCredentialResponse) { plan.K3SSha256 = strings.Repeat("0", 64) },
		"no url":       func(plan *agentpb.NodeJoinCredentialResponse) { plan.K3SUrl = "" },
		"no digest":    func(plan *agentpb.NodeJoinCredentialResponse) { plan.K3SSha256 = "" },
		"bad reserve":  func(plan *agentpb.NodeJoinCredentialResponse) { plan.KubeReservedMemory = "1Gi,cpu=9" },
	} {
		f := newJoinNodeFixture(t, api)
		unpin(f.hub.plan)
		if name != "wrong digest" {
			// A k3s the image happened to ship.
			if err := os.MkdirAll(filepath.Dir(filepath.Join(f.params.Root, k3sBinary)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.params.Root, k3sBinary), []byte("k3s"), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		err := JoinNode(t.Context(), f.params, logr.Discard())
		if err == nil || (name == "wrong digest" && !errors.Is(err, errDigest)) {
			t.Fatalf("%s: JoinNode = %v, want a refusal", name, err)
		}
		if _, err := os.Stat(filepath.Join(f.params.Root, k3sJoinTokenFile)); err == nil {
			t.Errorf("%s: join token written", name)
		}
		if len(f.systemctl) != 0 {
			t.Errorf("%s: systemctl ran: %v", name, f.systemctl)
		}
	}
}

func TestJoinNodeStopsWhenRefused(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	defer api.Close()
	f := newJoinNodeFixture(t, api)
	f.hub.refuse = status.Error(codes.PermissionDenied, "not a machine of a joined pool")
	start := time.Now()
	if err := JoinNode(t.Context(), f.params, logr.Discard()); err == nil || status.Code(errors.Unwrap(err)) != codes.PermissionDenied {
		t.Fatalf("JoinNode = %v, want the refusal", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("a refusal was retried")
	}
}

func (f *joinNodeFixture) assertReportedNothingWritten(t *testing.T, reason string) {
	t.Helper()
	if len(f.hub.reports) != 1 || f.hub.reports[0].GetReason() != reason {
		t.Fatalf("reports %v, want one %s", f.hub.reports, reason)
	}
	for _, path := range []string{k3sJoinTokenFile, k3sConfigDropIn, k3sBinary} {
		if _, err := os.Stat(filepath.Join(f.params.Root, path)); err == nil {
			t.Errorf("%s written", path)
		}
	}
	if len(f.systemctl) != 0 {
		t.Errorf("systemctl ran: %v", f.systemctl)
	}
}
