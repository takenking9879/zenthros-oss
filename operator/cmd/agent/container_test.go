// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zenthros/operator/agent"
	"github.com/zenthros/operator/pkg/bootstrap"
	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/liveness"
	"github.com/zenthros/operator/pkg/pyenv"
	"github.com/zenthros/operator/pkg/runtimegate"
)

// The test binary doubles as the init under test: with this variable set it
// runs runInit over the command in its arguments and exits as it does.
func TestMain(m *testing.M) {
	if os.Getenv("ZENTHROS_TEST_RUN_INIT") == "1" {
		os.Exit(runInit(os.Args[len(os.Args)-3:], os.Environ(), nil))
	}
	// main's attach branch, and a stand-in for the agent proper it runs.
	if os.Getenv("ZENTHROS_TEST_ATTACH") != "" {
		if len(os.Args) > 1 && os.Args[1] == "attach" {
			os.Exit(runAttach(os.Args))
		}
		os.Exit(attachedTestAgent())
	}
	switch os.Getenv("ZENTHROS_TEST_HANDOFF") {
	case "init":
		// main's PID 1 branch, then the agent proper as its child.
		env, secrets, err := scrubInit(os.Args)
		if err != nil {
			fmt.Println("INIT_ERROR", err)
			os.Exit(3)
		}
		fmt.Println("INIT_ENVIRON", environHolds(handoffMarker))
		for i, kv := range env {
			if strings.HasPrefix(kv, "ZENTHROS_TEST_HANDOFF=") {
				env[i] = "ZENTHROS_TEST_HANDOFF=child"
			}
		}
		os.Exit(runInit(os.Args, env, secrets))
	case "child":
		if err := applyHandoff(); err != nil {
			fmt.Println("CHILD_ERROR", err)
			os.Exit(3)
		}
		fmt.Println("CHILD_TOKEN", os.Getenv("ZENTHROS_JOIN_TOKEN"))
		fmt.Println("CHILD_AUTHKEY", os.Getenv(EnvTailscaleAuthKey))
		fmt.Println("CHILD_ENVIRON", environHolds(handoffMarker))
		os.Exit(0)
	}
	os.Exit(m.Run())
}

const handoffMarker = "e2e-secret-4f1c"

// environHolds reports whether this process's /proc environ, the one any
// process of its user can read, holds marker.
func environHolds(marker string) string {
	raw, err := os.ReadFile("/proc/self/environ")
	if err != nil {
		return "unreadable: " + err.Error()
	}
	if strings.Contains(string(raw), marker) {
		return "leaks"
	}
	return "clean"
}

// Everything a container agent writes is under its state directory: the
// root of the image may be read-only, and nothing else is promised.
func TestContainerModeWritesOnlyUnderItsStateDir(t *testing.T) {
	state := t.TempDir()
	t.Setenv("ZENTHROS_STATE_DIR", state)
	t.Setenv(runtimegate.EnvReadyFile, "")
	t.Setenv(runtimegate.EnvPendingFile, "")
	cfg := agent.Config{ControllerAddr: "gw.example:9443"}
	applyContainerDefaults(&cfg)
	paths := []string{
		cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.CAFile,
		defaultReportListen(agentModeContainer), defaultPreemptDir(agentModeContainer),
		defaultReportSpool(agentModeContainer), os.Getenv(runtimegate.EnvReadyFile), os.Getenv(runtimegate.EnvPendingFile),
		strings.TrimPrefix(defaultReportEndpoint(agentModeContainer), "unix://"),
	}
	for _, path := range paths {
		if !strings.HasPrefix(path, state+"/") {
			t.Errorf("%q is outside the state directory %s", path, state)
		}
	}
	if !cfg.SkipMetadataNotices {
		t.Error("a container must not poll cloud metadata for notices")
	}
	if cfg.TLS.ServerName != "gw.example" {
		t.Errorf("server name = %q, want the controller host", cfg.TLS.ServerName)
	}
	if got := envNetwork(agentModeContainer); got != agent.NetworkDirect {
		t.Errorf("container network default = %q, want direct", got)
	}
}

// The agent never knows a provider's name: the provider tells it which of
// its variables holds the container's id.
func TestContainerIdentityFromTheNamedVariable(t *testing.T) {
	t.Setenv("ZENTHROS_CONTAINER_ID", "")
	t.Setenv("ZENTHROS_CONTAINER_ID_ENV", "SOME_PROVIDER_POD_ID")
	t.Setenv("SOME_PROVIDER_POD_ID", "pod-123")
	if got := containerIdentity(); got != "pod-123" {
		t.Fatalf("identity = %q", got)
	}
	t.Setenv("ZENTHROS_CONTAINER_ID", "explicit")
	if got := containerIdentity(); got != "explicit" {
		t.Fatalf("identity = %q, want the explicit one", got)
	}
}

// A container with its certificate (state that survived a restart)
// reconnects; one without it and without a token cannot come back, and
// says why instead of retrying forever.
func TestEnrollOnlyWithoutACertificate(t *testing.T) {
	state := t.TempDir()
	t.Setenv("ZENTHROS_STATE_DIR", state)
	t.Setenv("ZENTHROS_JOIN_TOKEN", "")
	cfg := agent.Config{ControllerAddr: "127.0.0.1:1", BackendID: "c-1", BackendType: "container"}
	applyContainerDefaults(&cfg)
	err := enrollIfNeeded(context.Background(), cfg, logr.Discard())
	if err == nil || !strings.Contains(err.Error(), "ZENTHROS_JOIN_TOKEN") {
		t.Fatalf("no cert, no token: %v", err)
	}
	for _, f := range []string{cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.CAFile} {
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := enrollIfNeeded(context.Background(), cfg, logr.Discard()); err != nil {
		t.Fatalf("with a certificate the agent must not enroll again: %v", err)
	}
}

// A refusal is final; a controller that does not answer yet is retried.
func TestEnrollRefusalIsFinal(t *testing.T) {
	for code, final := range map[codes.Code]bool{
		codes.Unauthenticated: true, codes.AlreadyExists: true, codes.PermissionDenied: true,
		codes.Unavailable: false, codes.DeadlineExceeded: false,
		// A check the controller could not make, and its rate limit, are
		// asked again (c4-B1); a token the hub does not know is final, as
		// fault.Retryable says for every client.
		codes.Internal: false, codes.ResourceExhausted: false, codes.NotFound: true,
	} {
		if got := enrollRefused(wrap(status.Error(code, "x"))); got != final {
			t.Errorf("%s: refused = %v, want %v", code, got, final)
		}
	}
}

func wrap(err error) error { return &wrapped{err} }

type wrapped struct{ err error }

func (w *wrapped) Error() string { return "enroll: " + w.err.Error() }
func (w *wrapped) Unwrap() error { return w.err }

// SIGTERM is the container's notice. The jobs hear it first (interrupt),
// with the grace as its deadline; the agent stops once they have ended and
// their reports had a moment, not at the end of the grace.
func TestSIGTERMNotifiesThenStopsWhenJobsEnd(t *testing.T) {
	sigs := make(chan os.Signal, 2)
	var got atomic.Pointer[cloudmeta.Notice]
	var running atomic.Bool
	running.Store(true)
	ctx := containerSignalContext(sigs, func(n cloudmeta.Notice) { got.Store(&n) },
		func() bool { return !running.Load() }, time.Minute, logr.Discard())
	sigs <- syscall.SIGTERM
	time.Sleep(300 * time.Millisecond)
	notice := got.Load()
	if notice == nil || notice.Source != liveness.NoticeContainerSIGTERM || time.Until(notice.Deadline) < 50*time.Second {
		t.Fatalf("notice = %+v", notice)
	}
	if ctx.Err() != nil {
		t.Fatal("stopped while a job was still running")
	}
	running.Store(false)
	select {
	case <-ctx.Done():
	case <-time.After(reportFlush + 2*time.Second):
		t.Fatal("did not stop after the jobs ended")
	}
}

func TestSecondSignalStopsAtOnce(t *testing.T) {
	sigs := make(chan os.Signal, 2)
	ctx := containerSignalContext(sigs, func(cloudmeta.Notice) {}, func() bool { return false }, time.Minute, logr.Discard())
	sigs <- syscall.SIGTERM
	time.Sleep(100 * time.Millisecond)
	sigs <- syscall.SIGINT
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a second signal did not stop the agent")
	}
}

func TestSIGINTStopsWithoutANotice(t *testing.T) {
	sigs := make(chan os.Signal, 1)
	var notified atomic.Bool
	ctx := containerSignalContext(sigs, func(cloudmeta.Notice) { notified.Store(true) }, func() bool { return false }, time.Minute, logr.Discard())
	sigs <- syscall.SIGINT
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("SIGINT did not stop the agent")
	}
	if notified.Load() {
		t.Fatal("SIGINT is not an interruption notice")
	}
}

// As the container's init, the agent reaps what is orphaned to it while it
// runs, and exits with its child's status.
func TestInitReapsOrphansAndExitsAsTheAgent(t *testing.T) {
	// The child leaves an orphan that ends at once, then lives on a while.
	cmd := exec.Command(os.Args[0], "-test.run=^$", "sh", "-c", "(sleep 0.1 &); sleep 1; exit 3")
	cmd.Env = append(os.Environ(), "ZENTHROS_TEST_RUN_INIT=1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
	if zombies := zombieChildren(t, cmd.Process.Pid); len(zombies) > 0 {
		t.Errorf("init left zombies %v", zombies)
	}
	err := cmd.Wait()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 3 {
		t.Fatalf("init exited %v, want the child's code 3", err)
	}
}

func zombieChildren(t *testing.T, parent int) []int {
	t.Helper()
	stats, _ := filepath.Glob("/proc/[0-9]*/stat")
	var out []int
	for _, path := range stats {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// pid (comm) state ppid ...; comm may hold spaces, so cut after ")".
		rest := string(raw[strings.LastIndexByte(string(raw), ')')+2:])
		fields := strings.Fields(rest)
		if len(fields) < 2 || fields[0] != "Z" {
			continue
		}
		if ppid, _ := strconv.Atoi(fields[1]); ppid == parent {
			pid, _ := strconv.Atoi(strings.Fields(string(raw))[0])
			out = append(out, pid)
		}
	}
	return out
}

// A container completing its Python is connected but takes no work: the gate
// is closed before prepareToolchains returns (so before the agent's first
// heartbeat), the jobs' env is published before the gate opens (so the first
// job sees the venv), and a failed install never opens it.
func TestToolchainGateOrder(t *testing.T) {
	const raw = `[{"name":"python","packages":["requests"]}]`
	var order []string
	release := make(chan error)
	holder := &toolchainEnv{}
	steps := toolchainSteps{
		markPending: func() error { order = append(order, "pending"); return nil },
		markReady: func() error {
			order = append(order, "ready with venv="+holder.extraEnv()["VIRTUAL_ENV"])
			return nil
		},
		markFailed: func(err error) error { order = append(order, "failed: "+err.Error()); return nil },
		prepare: func(ctx context.Context, tc pyenv.Toolchain) (pyenv.Env, error) {
			if err := <-release; err != nil {
				return pyenv.Env{}, err
			}
			return pyenv.Env{PathPrefix: []string{"/s/venv/bin"}, Vars: map[string]string{"VIRTUAL_ENV": "/s/venv"}}, nil
		},
	}
	done, err := prepareToolchains(context.Background(), raw, steps, holder, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 1 || order[0] != "pending" {
		t.Fatalf("the gate was not closed before prepareToolchains returned: %v", order)
	}
	if holder.pathPrefix() != nil {
		t.Fatal("the jobs' env was published before the install finished")
	}
	release <- nil
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if want := []string{"pending", "ready with venv=/s/venv"}; strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order %v, want %v", order, want)
	}

	order = nil
	failed := &toolchainEnv{}
	done, err = prepareToolchains(context.Background(), raw, steps, failed, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	release <- errors.New("No solution found when resolving dependencies")
	if err := <-done; err == nil || !strings.Contains(err.Error(), "No solution found") {
		t.Fatalf("done = %v, want uv's error", err)
	}
	if strings.Join(order, ",") != "pending,failed: No solution found when resolving dependencies" || failed.pathPrefix() != nil {
		t.Fatalf("a failed install opened the gate or published an env: %v %v", order, failed.pathPrefix())
	}
}

// No toolchain, no gate: the container takes work at once. A toolchain a
// container cannot add is refused before anything starts.
func TestToolchainsNoneOrRefused(t *testing.T) {
	steps := toolchainSteps{
		markPending: func() error { t.Fatal("the gate closed without a toolchain"); return nil },
	}
	if done, err := prepareToolchains(context.Background(), "", steps, &toolchainEnv{}, logr.Discard()); done != nil || err != nil {
		t.Fatalf("no toolchain: %v %v", done, err)
	}
	_, err := prepareToolchains(context.Background(), `[{"name":"java","version":"25"}]`, steps, &toolchainEnv{}, logr.Discard())
	if err == nil || !strings.Contains(err.Error(), `toolchain "java" cannot be added to a container`) {
		t.Fatalf("java: %v", err)
	}
}

// A container joined with the short join was told no backend: it is the
// one its certificate names, on its first start and after every restart.
func TestAShortJoinedContainerIsTheBackendItsCertificateNames(t *testing.T) {
	state := t.TempDir()
	t.Setenv("ZENTHROS_STATE_DIR", state)
	t.Setenv("ZENTHROS_AGENT_MODE", agentModeContainer)
	t.Setenv("ZENTHROS_JOIN_TOKEN", "")
	cfg := agent.Config{ControllerAddr: "127.0.0.1:1", BackendType: "container"}
	applyContainerDefaults(&cfg)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ct-1a2b3c4d"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cred := &bootstrap.Credential{ClientCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		ClientKeyPEM: []byte("key"), CACertPEM: []byte("ca")}
	if err := cred.Write(state); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareToConnect(context.Background(), &cfg, nil, logr.Discard()); err != nil {
		t.Fatalf("prepareToConnect: %v", err)
	}
	if cfg.BackendID != "ct-1a2b3c4d" {
		t.Fatalf("the agent runs as %q, want the certificate's ct-1a2b3c4d", cfg.BackendID)
	}
	if err := cfg.ValidateForRun(); err != nil {
		t.Fatalf("a short-joined container cannot run: %v", err)
	}
}

// Connected, an agent whose runtime failed stays: its heartbeat says why, and
// the machine it runs on ends by its boot's timeout or by whoever waited for it.
// Only with no hub to tell (local) does it stop, to say it in its exit.
func TestAFailedRuntimeStopsTheAgentOnlyWithNoHubToTell(t *testing.T) {
	failure := errors.New("No solution found")
	var cause error
	stop := func(err error) { cause = err }
	afterPrepare(failure, false, stop, logr.Discard())
	if cause != nil {
		t.Fatalf("a connected agent stopped: %v", cause)
	}
	afterPrepare(nil, true, stop, logr.Discard())
	if cause != nil {
		t.Fatalf("a runtime that is ready stopped the agent: %v", cause)
	}
	afterPrepare(failure, true, stop, logr.Discard())
	if cause == nil || !strings.Contains(cause.Error(), "No solution found") {
		t.Fatalf("local: stopped with %v, want the failure", cause)
	}
}
