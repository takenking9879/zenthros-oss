// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// RequireEnvtest skips t unless its envtest suite was asked for by name:
// ZENTHROS_ENVTEST lists it (make test-integration sets "integration", make
// test-load "load") and KUBEBUILDER_ASSETS is set. Exporting
// KUBEBUILDER_ASSETS alone no longer makes go test ./... boot an apiserver
// per test.
func RequireEnvtest(t testing.TB, suite string) {
	t.Helper()
	if !slices.Contains(strings.Fields(strings.ReplaceAll(os.Getenv("ZENTHROS_ENVTEST"), ",", " ")), suite) ||
		os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skipf("set ZENTHROS_ENVTEST=%s and KUBEBUILDER_ASSETS to run the envtest %s tests (make test-%s)", suite, suite, suite)
	}
}

// StartEnvtest starts env, from the binaries in KUBEBUILDER_ASSETS, and
// stops it with t. Its etcd and kube-apiserver end with the test binary
// however it ends: envtest runs them in process groups of their own, which
// no Ctrl-C reaches, and a kill -9 or go test's -timeout runs no cleanup (an
// apiserver and its etcd outlived one by two days, 2026-11-10). setpriv
// gives each the signal KILL when its parent dies; the parent is the OS
// thread that started it, so that thread stays locked to a goroutine that
// ends only after they stop.
func StartEnvtest(t testing.TB, env *envtest.Environment) *rest.Config {
	t.Helper()
	prepare(t, env)

	type started struct {
		cfg *rest.Config
		err error
	}
	out := make(chan started)
	release := make(chan struct{})
	go func() {
		// Never unlocked: the thread ends with this goroutine, and what it
		// started with it.
		runtime.LockOSThread()
		cfg, err := env.Start()
		out <- started{cfg, err}
		<-release
	}()
	s := <-out
	if s.err != nil {
		// What started of it ends with its thread.
		close(release)
		t.Fatalf("start envtest: %v", s.err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stop envtest: %v", err)
		}
		// Stop returns before the control plane stops when an earlier step
		// fails: stopping it again keeps no apiserver or etcd behind.
		if err := env.ControlPlane.Stop(); err != nil {
			t.Logf("stop the envtest control plane: %v", err)
		}
		close(release)
	})
	return s.cfg
}

// crdWait is how long env.Start waits for its CRDs to be served. Envtest's
// 10 s is less than eight apiservers starting at once take on a 4-CPU runner
// (make test-integration's -parallel 8): three tests failed on GitHub with
// "waiting for CRDs to appear as API resources: context deadline exceeded",
// 2026-11-17.
const crdWait = 2 * time.Minute

// prepare makes env's etcd and kube-apiserver end with the test binary
// (setpriv) and gives its CRDs crdWait to be served.
func prepare(t testing.TB, env *envtest.Environment) {
	t.Helper()
	setpriv, err := exec.LookPath("setpriv")
	if err != nil {
		t.Fatalf("envtest needs setpriv (util-linux) to end with the tests: %v", err)
	}
	dir := t.TempDir()
	wrap := func(name string) string {
		path := filepath.Join(dir, name)
		script := "#!/bin/sh\nexec " + shellQuote(setpriv) + " --pdeathsig KILL -- " +
			shellQuote(filepath.Join(os.Getenv("KUBEBUILDER_ASSETS"), name)) + " \"$@\"\n"
		if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if env.ControlPlane.Etcd == nil {
		env.ControlPlane.Etcd = &envtest.Etcd{}
	}
	env.ControlPlane.Etcd.Path = wrap("etcd")
	env.ControlPlane.GetAPIServer().Path = wrap("kube-apiserver")
	env.CRDInstallOptions.MaxTime = crdWait
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
