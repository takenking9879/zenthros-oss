// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/bootstrap"
)

// freeAddr is an address nothing listens on, until the caller listens.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitJoinParams(addr string, t *testing.T) JoinParams {
	return JoinParams{
		ControllerAddr: addr, Token: "abcdef.0123456789abcdef", CACertHash: "sha256:00",
		BackendID: "vm-1", DeployMode: DeploySystemd, Network: NetworkDirect, StateDir: t.TempDir(),
	}
}

var errStopAfterEnroll = errors.New("enrolled; the test stops here")

// A join run while the controller rolls out (only the new leader serves the
// hub) waits for the address to listen, and only then sends its one-time
// token: before, it failed at once with "connection refused".
func TestJoinWaitsForTheControllerBeforeItEnrolls(t *testing.T) {
	addr := freeAddr(t)
	var listening, enrolledEarly atomic.Bool
	enrolled := make(chan struct{}, 1)
	restore := enroll
	enroll = func(_ context.Context, p bootstrap.JoinParams) (*bootstrap.Credential, error) {
		enrolledEarly.Store(!listening.Load())
		enrolled <- struct{}{}
		return nil, errStopAfterEnroll
	}
	t.Cleanup(func() { enroll = restore })

	go func() {
		time.Sleep(1500 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("listen %s: %v", addr, err)
			return
		}
		listening.Store(true)
		t.Cleanup(func() { _ = l.Close() })
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	err := Join(context.Background(), waitJoinParams(addr, t), logr.Discard())
	if !errors.Is(err, errStopAfterEnroll) {
		t.Fatalf("join = %v, want the enroll's error", err)
	}
	<-enrolled
	if enrolledEarly.Load() {
		t.Fatal("the token went out while the controller refused connections")
	}
}

// The wait is bounded: a controller that never listens still gets the
// Enroll, whose error is what the user reads.
func TestJoinEnrollsAnywayOnceTheWaitPasses(t *testing.T) {
	addr := freeAddr(t)
	restoreWait, restoreEnroll := controllerWait, enroll
	controllerWait = time.Second
	called := false
	enroll = func(context.Context, bootstrap.JoinParams) (*bootstrap.Credential, error) {
		called = true
		return nil, errStopAfterEnroll
	}
	t.Cleanup(func() { controllerWait, enroll = restoreWait, restoreEnroll })

	start := time.Now()
	err := Join(context.Background(), waitJoinParams(addr, t), logr.Discard())
	if !called || !errors.Is(err, errStopAfterEnroll) {
		t.Fatalf("join = %v (enroll called %v), want the enroll's error", err, called)
	}
	if took := time.Since(start); took < time.Second || took > 10*time.Second {
		t.Fatalf("join took %s, want about the 1s wait", took)
	}
}

// The token is spent only once the credentials can be written: a join that
// could not make its state directory used up a one-time token and wrote
// nothing.
func TestJoinSpendsNoTokenWhereItCannotWrite(t *testing.T) {
	restoreCmd, restoreEnroll := stateDirCmd, enroll
	t.Cleanup(func() { stateDirCmd, enroll = restoreCmd, restoreEnroll })
	stateDirCmd = func(context.Context, string, ...string) error { return errors.New("sudo: a password is required") }
	called := false
	enroll = func(context.Context, bootstrap.JoinParams) (*bootstrap.Credential, error) {
		called = true
		return nil, errStopAfterEnroll
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	p := waitJoinParams(l.Addr().String(), t)
	p.StateDir = filepath.Join(t.TempDir(), "missing", "zenthros")
	if err := Join(context.Background(), p, logr.Discard()); err == nil || called {
		t.Fatalf("join = %v, enrolled %v: the token went out with nowhere to write", err, called)
	}
}

// The credentials are written whole: a write cut short leaves the files as
// they were, never a truncated key beside the others.
func TestCredentialsAreWrittenWhole(t *testing.T) {
	dir := t.TempDir()
	cred := &bootstrap.Credential{CACertPEM: []byte("ca"), ClientCertPEM: []byte("crt"), ClientKeyPEM: []byte("key")}
	if err := cred.Write(dir); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"ca.crt": "ca", "agent.crt": "crt", "agent.key": "key"} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s = %q, %v", name, got, err)
		}
	}
	if info, err := os.Stat(filepath.Join(dir, "agent.key")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("agent.key mode %v, %v", info.Mode(), err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 3 {
		t.Fatalf("left %d files, want the three: %v", len(entries), entries)
	}
}
