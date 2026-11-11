// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zenthros/operator/pkg/bootstrap"
)

const shortPin = "sha256:0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"

// certFor is a PEM certificate whose common name is backendID, as the hub
// signs an agent's.
func certFor(t *testing.T, backendID string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: backendID},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// shortJoin is a short join against a listening address, with the enroll,
// the unit and the agent's readiness replaced; steps records their order.
type shortJoin struct {
	p        JoinParams
	steps    []string
	enrolled bootstrap.JoinParams
	deployed JoinParams
	out      bytes.Buffer
	mu       sync.Mutex
}

func (j *shortJoin) step(s string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.steps = append(j.steps, s)
}

func newShortJoin(t *testing.T, readyAfter int32) *shortJoin {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	j := &shortJoin{}
	j.p = JoinParams{ControllerAddr: l.Addr().String(), Token: bootstrap.JoinToken(shortPin, "abcdef.0123456789abcdef"),
		DeployMode: DeploySystemd, StateDir: t.TempDir(), Wait: 5 * time.Second, Out: &j.out}

	var asked atomic.Int32
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if asked.Add(1) > readyAfter {
			j.step("connected")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(ready.Close)

	restoreEnroll, restoreDeploy, restoreReady, restoreRetry := enroll, deployUnit, agentReady, enrollRetryFor
	t.Cleanup(func() {
		enroll, deployUnit, agentReady, enrollRetryFor = restoreEnroll, restoreDeploy, restoreReady, restoreRetry
	})
	// The person's errors are said at the first answer; asking again has
	// its own test.
	enrollRetryFor = 0
	agentReady = ready.URL + "/readyz"
	enroll = func(_ context.Context, p bootstrap.JoinParams) (*bootstrap.Credential, error) {
		j.step("enroll")
		j.enrolled = p
		return &bootstrap.Credential{ClientCertPEM: certFor(t, "vm-1a2b3c4d"), ClientKeyPEM: []byte("key"), CACertPEM: []byte("ca")}, nil
	}
	deployUnit = func(p JoinParams) error {
		if _, err := os.Stat(filepath.Join(p.StateDir, "agent.key")); err != nil {
			t.Errorf("the unit started before the credentials were written: %v", err)
		}
		j.step("deploy")
		j.deployed = p
		return nil
	}
	return j
}

// The short join a person pastes (U-05): the hub's address and a zt1_
// token. The token's pin is what the hub is verified with, the backend is
// the one the hub's certificate names, and the join ends 0 only once the
// agent is connected, in this order.
func TestShortJoinEnrollsTheTokensBackendThenWaitsUntilConnected(t *testing.T) {
	j := newShortJoin(t, 2)
	if err := Join(context.Background(), j.p, logr.Discard()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if got := strings.Join(j.steps, " → "); got != "enroll → deploy → connected" {
		t.Fatalf("steps %s, want enroll → deploy → connected", got)
	}
	if j.enrolled.Token != "abcdef.0123456789abcdef" || j.enrolled.CACertHash != shortPin || j.enrolled.BackendID != "" {
		t.Fatalf("enrolled with token %q pin %q backend %q: want the token's bearer and pin, and no backend named",
			j.enrolled.Token, j.enrolled.CACertHash, j.enrolled.BackendID)
	}
	if j.deployed.BackendID != "vm-1a2b3c4d" || j.deployed.Network != NetworkDirect {
		t.Fatalf("the unit runs backend %q on network %q, want the certificate's vm-1a2b3c4d, direct", j.deployed.BackendID, j.deployed.Network)
	}
	// And how its jobs run: never as root, writing only their own.
	want := "Joining " + j.p.ControllerAddr + "…\n✓ joined as vm-1a2b3c4d\n✓ connected\n" + jobsRunAs(nil) + "\n"
	if j.out.String() != want {
		t.Fatalf("the person reads %q, want %q", j.out.String(), want)
	}
}

// An agent that never connects is a failed join, within its wait, with
// where to look: it neither hangs nor ends 0 (E-08).
func TestShortJoinFailsWhenTheAgentNeverConnects(t *testing.T) {
	j := newShortJoin(t, 1<<30)
	j.p.Wait = 1500 * time.Millisecond
	start := time.Now()
	err := Join(context.Background(), j.p, logr.Discard())
	if err == nil || !strings.Contains(err.Error(), "journalctl -u zenthros-agent") || !strings.Contains(err.Error(), "vm-1a2b3c4d") {
		t.Fatalf("Join = %v, want the agent named as not connected, and where to look", err)
	}
	// The agent never connects (1<<30 polls): a join that ignored its wait
	// would not end. The bound is on that hang, not on speed: under -race
	// with every package running the join took 8 s (2026-10-21).
	if took := time.Since(start); took > 30*time.Second {
		t.Fatalf("the join took %s for a 1.5s wait", took)
	}
	if strings.Contains(j.out.String(), "✓ connected") {
		t.Fatalf("the person read connected: %q", j.out.String())
	}
}

// Run again on a machine already joined, the short join adopts the backend
// its certificate names, and still waits (E-08: reinstalling keeps the
// identity).
func TestShortJoinAgainKeepsTheMachinesIdentity(t *testing.T) {
	j := newShortJoin(t, 0)
	cred := &bootstrap.Credential{ClientCertPEM: certFor(t, "vm-1a2b3c4d"), ClientKeyPEM: []byte("key"), CACertPEM: []byte("ca")}
	if err := cred.Write(j.p.StateDir); err != nil {
		t.Fatal(err)
	}
	j.p.SkipIfExisting = true
	if err := Join(context.Background(), j.p, logr.Discard()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if got := strings.Join(j.steps, " → "); got != "deploy → connected" || j.deployed.BackendID != "vm-1a2b3c4d" {
		t.Fatalf("steps %s for backend %q, want deploy → connected for vm-1a2b3c4d and no enroll", got, j.deployed.BackendID)
	}
}

// A join with the long flags and no wait is as before: a machine's own
// bootstrap runs steps after it, and a slow agent must not fail them.
func TestALongJoinDoesNotWait(t *testing.T) {
	j := newShortJoin(t, 1<<30)
	j.p.Token, j.p.CACertHash, j.p.BackendID, j.p.Wait, j.p.Network = "abcdef.0123456789abcdef", shortPin, "vm-given", 0, NetworkDirect
	if err := Join(context.Background(), j.p, logr.Discard()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if got := strings.Join(j.steps, " → "); got != "enroll → deploy" || j.deployed.BackendID != "vm-given" {
		t.Fatalf("steps %s for %q, want enroll → deploy for vm-given", got, j.deployed.BackendID)
	}
}

func TestTheNetworkFollowsFromTheAddress(t *testing.T) {
	for addr, want := range map[string]string{
		"zenthros-hub.tail1234.ts.net:9443": NetworkTailscale,
		"api.zenthros.app:9443":             NetworkDirect,
		"203.0.113.7:9443":                  NetworkDirect,
		"zenthros-hub.tail1234.ts.net:9443,zenthros-hub.tail1234.ts.net:443": NetworkTailscale,
	} {
		if got := NetworkFor(addr); got != want {
			t.Errorf("NetworkFor(%q) = %s, want %s", addr, got, want)
		}
	}
	for addr, want := range map[string]string{
		"hub.zenthros.dev": "hub.zenthros.dev:9443", "203.0.113.7": "203.0.113.7:9443", "hub:443": "hub:443", "::1": "[::1]:9443",
		"hub.zenthros.dev,hub.zenthros.dev:443": "hub.zenthros.dev:9443,hub.zenthros.dev:443",
	} {
		if got := WithDefaultPort(addr); got != want {
			t.Errorf("WithDefaultPort(%q) = %s, want %s", addr, got, want)
		}
	}
}

// A short token's pin and one given beside it must agree.
func TestShortJoinRefusesTwoPins(t *testing.T) {
	j := newShortJoin(t, 0)
	j.p.CACertHash = "sha256:" + strings.Repeat("ab", 32)
	if err := Join(context.Background(), j.p, logr.Discard()); err == nil || !strings.Contains(err.Error(), "differ") || len(j.steps) != 0 {
		t.Fatalf("Join = %v after %v, want the pins refused before anything", err, j.steps)
	}
}

// Pasted again on a machine this hub enrolled, with no flag, the short join
// is that machine: its token is spent, and it must not be asked for (E-08).
// Credentials another hub signed are enrolled over.
func TestShortJoinPastedAgainIsTheMachineOfThisHubOnly(t *testing.T) {
	for _, c := range []struct {
		name      string
		sameHub   bool
		wantSteps string
	}{
		{"this hub", true, "deploy → connected"},
		{"another hub", false, "enroll → deploy → connected"},
	} {
		t.Run(c.name, func(t *testing.T) {
			j := newShortJoin(t, 0)
			ca := certFor(t, "hub-ca")
			pin, err := bootstrap.CAPinFromPEM(ca)
			if err != nil {
				t.Fatal(err)
			}
			if !c.sameHub {
				ca = certFor(t, "another-hub-ca")
			}
			cred := &bootstrap.Credential{ClientCertPEM: certFor(t, "vm-1a2b3c4d"), ClientKeyPEM: []byte("key"), CACertPEM: ca}
			if err := cred.Write(j.p.StateDir); err != nil {
				t.Fatal(err)
			}
			// Enrolled with this line (TestANewLineOfTheSameHubIsANewMachine
			// has another).
			if err := writeEnrolledToken(j.p.StateDir, "abcdef.0123456789abcdef"); err != nil {
				t.Fatal(err)
			}
			j.p.Token = bootstrap.JoinToken(pin, "abcdef.0123456789abcdef")
			if err := Join(context.Background(), j.p, logr.Discard()); err != nil {
				t.Fatalf("Join: %v", err)
			}
			if got := strings.Join(j.steps, " → "); got != c.wantSteps || j.deployed.BackendID != "vm-1a2b3c4d" {
				t.Fatalf("steps %s for backend %q, want %s for vm-1a2b3c4d", got, j.deployed.BackendID, c.wantSteps)
			}
		})
	}
}

// The identity a machine keeps is the line's: the same line pasted again is
// that machine (E-08), a new line of the same hub is a new machine, enrolled
// over the old one. Found in window B: a VM detached (its agent stops for
// good, its credentials stay) and attached again with a new line kept the
// detached machine's identity, and its agent stopped at once, refused.
func TestANewLineOfTheSameHubIsANewMachine(t *testing.T) {
	j := newShortJoin(t, 0)
	ca := certFor(t, "hub-ca")
	pin, err := bootstrap.CAPinFromPEM(ca)
	if err != nil {
		t.Fatal(err)
	}
	machines := 0
	enroll = func(_ context.Context, p bootstrap.JoinParams) (*bootstrap.Credential, error) {
		j.step("enroll")
		machines++
		return &bootstrap.Credential{ClientCertPEM: certFor(t, fmt.Sprintf("vm-%08d", machines)), ClientKeyPEM: []byte("key"), CACertPEM: ca}, nil
	}
	join := func(token string) (steps, backend string) {
		t.Helper()
		j.steps, j.p.BackendID = nil, ""
		j.p.Token = bootstrap.JoinToken(pin, token)
		if err := Join(context.Background(), j.p, logr.Discard()); err != nil {
			t.Fatalf("Join %s: %v", token, err)
		}
		return strings.Join(j.steps, " → "), j.deployed.BackendID
	}
	if steps, backend := join("abcdef.0123456789abcdef"); steps != "enroll → deploy → connected" || backend != "vm-00000001" {
		t.Fatalf("the first line: %s as %s", steps, backend)
	}
	if steps, backend := join("abcdef.0123456789abcdef"); steps != "deploy → connected" || backend != "vm-00000001" {
		t.Fatalf("the same line again: %s as %s, want deploy → connected as vm-00000001 (its token is spent)", steps, backend)
	}
	if steps, backend := join("ghijkl.0123456789abcdef"); steps != "enroll → deploy → connected" || backend != "vm-00000002" {
		t.Fatalf("a new line: %s as %s, want enroll → deploy → connected as the new machine vm-00000002", steps, backend)
	}
}

// Found by the attach E2E: on a VM that is a privileged pod (systemd as
// PID 1, a service account mounted), zenadm deployed the agent through the
// cluster's API, which the machine has no credentials for. Where systemd
// runs, the agent is a unit; a pod without it (an enrollment Job) uses the API.
func TestDeployModeIsSystemdWhereSystemdRuns(t *testing.T) {
	for _, c := range []struct {
		name  string
		paths []string
		want  string
	}{
		{"a VM", []string{"run/systemd/system"}, DeploySystemd},
		{"a VM that is a pod", []string{"run/systemd/system", "var/run/secrets/kubernetes.io/serviceaccount"}, DeploySystemd},
		{"an enrollment Job", []string{"var/run/secrets/kubernetes.io/serviceaccount"}, DeployK8s},
		{"neither", nil, DeploySystemd},
	} {
		root := t.TempDir()
		for _, p := range c.paths {
			if err := os.MkdirAll(filepath.Join(root, p), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		if got := DetectDeployMode(root); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

// What a person reads when the join they pasted fails at the hub (E-08): a
// token it refuses needs a new attach; a hub it cannot reach is the
// network, and the same line works once it can.
func TestShortJoinFailuresSayWhatToDo(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want []string
	}{
		"a used token": {status.Error(codes.Unauthenticated, "invalid bootstrap token"),
			[]string{"the hub refused this join (invalid bootstrap token)", "Run zenthros attach again for a new one"}},
		"a token another machine used": {status.Error(codes.AlreadyExists, "bootstrap token already consumed by another machine"),
			[]string{"the hub refused this join (bootstrap token already consumed by another machine)", "Run zenthros attach again for a new one"}},
		"an expired token": {status.Error(codes.Unauthenticated, "bootstrap token expired"),
			[]string{"bootstrap token expired", "Run zenthros attach again"}},
		"a revoked token": {status.Error(codes.PermissionDenied, "bootstrap token revoked"),
			[]string{"Run zenthros attach again"}},
		"a hub that does not answer": {status.Error(codes.Unavailable, "connection refused"),
			[]string{"cannot reach the hub at", "then paste the join again; the token is not spent"}},
		"a hub that never answers": {status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			[]string{"cannot reach the hub at", "the token is not spent"}},
	} {
		t.Run(name, func(t *testing.T) {
			j := newShortJoin(t, 0)
			enroll = func(_ context.Context, _ bootstrap.JoinParams) (*bootstrap.Credential, error) {
				return nil, fmt.Errorf("enroll: %w", tc.err)
			}
			err := Join(context.Background(), j.p, logr.Discard())
			for _, want := range tc.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("Join = %v, want it to say %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "rpc error") {
				t.Fatalf("Join = %v: the gRPC wrapping reaches the person", err)
			}
		})
	}
}

// The Enroll has a deadline, and a pasted join against an address that does
// not listen says it is joining at once and gives up on waiting soon: it
// neither hangs nor stays silent (E-08).
func TestShortJoinAgainstAHubThatIsDownEndsSoon(t *testing.T) {
	j := newShortJoin(t, 0)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	j.p.ControllerAddr = l.Addr().String()
	_ = l.Close()
	restoreWait := shortJoinControllerWait
	shortJoinControllerWait = 300 * time.Millisecond
	t.Cleanup(func() { shortJoinControllerWait = restoreWait })
	var deadline time.Duration
	enroll = func(ctx context.Context, _ bootstrap.JoinParams) (*bootstrap.Credential, error) {
		if d, ok := ctx.Deadline(); ok {
			deadline = time.Until(d)
		}
		return nil, status.Error(codes.Unavailable, "connection refused")
	}
	start := time.Now()
	err = Join(context.Background(), j.p, logr.Discard())
	if err == nil || !strings.Contains(err.Error(), "cannot reach the hub at "+j.p.ControllerAddr) {
		t.Fatalf("Join = %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("a join against a hub that is down took %s", took)
	}
	if deadline <= 0 || deadline > enrollTimeout {
		t.Fatalf("the Enroll's deadline is %s away, want within %s", deadline, enrollTimeout)
	}
	if j.out.String() != "Joining "+j.p.ControllerAddr+"…\n" {
		t.Fatalf("the person reads %q, want the Joining line first", j.out.String())
	}
}

// A hub that answers "try again" (a check it could not make, its rate limit)
// is asked again, and the join goes on; one that refuses the token is not
// (c4-B1).
func TestAJoinAsksAgainWhenTheHubSaysSo(t *testing.T) {
	j := newShortJoin(t, 0)
	enrollRetryFor = 10 * time.Second
	answers := []error{
		status.Error(codes.Unavailable, "verify the machine's identity: etcd leader changed"),
		status.Error(codes.ResourceExhausted, "enroll rate limit exceeded"),
	}
	calls := 0
	enroll = func(_ context.Context, _ bootstrap.JoinParams) (*bootstrap.Credential, error) {
		calls++
		if calls <= len(answers) {
			return nil, answers[calls-1]
		}
		return &bootstrap.Credential{ClientCertPEM: certFor(t, "vm-1a2b3c4d"), ClientKeyPEM: []byte("key"), CACertPEM: []byte("ca")}, nil
	}
	if err := Join(context.Background(), j.p, logr.Discard()); err != nil || calls != 3 {
		t.Fatalf("Join = %v after %d Enrolls, want it joined at the third", err, calls)
	}

	refused := newShortJoin(t, 0)
	enrollRetryFor = 10 * time.Second
	calls = 0
	enroll = func(_ context.Context, _ bootstrap.JoinParams) (*bootstrap.Credential, error) {
		calls++
		return nil, status.Error(codes.Unauthenticated, "bootstrap token expired")
	}
	if err := Join(context.Background(), refused.p, logr.Discard()); err == nil || calls != 1 {
		t.Fatalf("Join = %v after %d Enrolls, want the refusal at the first", err, calls)
	}
}

// The hub's address is a list (the Gateway's port, then Traefik's
// passthrough on 443): a network that closes the first enrolls at the second,
// the unit keeps the whole list for the agent's own reconnections, and a hub
// that answers on neither is said with both.
func TestShortJoinEnrollsAtTheFirstAddressThatAnswers(t *testing.T) {
	j := newShortJoin(t, 0)
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	open := j.p.ControllerAddr
	j.p.ControllerAddr = closed.Addr().String() + "," + open
	if err := Join(context.Background(), j.p, logr.Discard()); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if j.enrolled.ControllerAddr != open {
		t.Fatalf("enrolled at %q, want the address that answers, %q", j.enrolled.ControllerAddr, open)
	}
	if j.deployed.ControllerAddr != j.p.ControllerAddr {
		t.Fatalf("the unit dials %q, want the whole list %q", j.deployed.ControllerAddr, j.p.ControllerAddr)
	}
	if unit := renderAgentUnit(j.deployed, "/usr/local/bin/zenthros-agent"); !strings.Contains(unit, "Environment=ZENTHROS_CONTROLLER_ADDR="+j.p.ControllerAddr+"\n") ||
		!strings.Contains(unit, "Environment=ZENTHROS_TLS_SERVER_NAME=127.0.0.1\n") {
		t.Fatalf("the agent's unit does not carry the list and its first host:\n%s", unit)
	}

	err = unreachable("hub.example:9443,hub.example:443")
	if err == nil || !strings.Contains(err.Error(), "cannot reach the hub at hub.example:9443 or hub.example:443") ||
		!strings.Contains(err.Error(), "(port 9443 or 443)") {
		t.Fatalf("unreachable = %v, want both addresses and ports", err)
	}
}
