// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"os"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Envtest's own 10 s for the CRDs to be served failed three integration
// tests on a 4-CPU GitHub runner with -parallel 8 (2026-11-17): every
// envtest waits as long as crdWait, and its control plane ends with the
// test binary.
func TestEnvtestWaitsForItsCRDsAndEndsWithTheTests(t *testing.T) {
	t.Setenv("KUBEBUILDER_ASSETS", "/assets")
	env := &envtest.Environment{}
	prepare(t, env)

	if got := env.CRDInstallOptions.MaxTime; got < time.Minute {
		t.Fatalf("CRD wait %v: envtest's 10 s is too short beside seven other apiservers", got)
	}
	for name, path := range map[string]string{
		"etcd":           env.ControlPlane.Etcd.Path,
		"kube-apiserver": env.ControlPlane.GetAPIServer().Path,
	} {
		script, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s's wrapper: %v", name, err)
		}
		if !strings.Contains(string(script), "--pdeathsig KILL -- '/assets/"+name+"'") {
			t.Fatalf("%s does not end with the test binary:\n%s", name, script)
		}
	}
}
