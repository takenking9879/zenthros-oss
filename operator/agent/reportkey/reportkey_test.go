// SPDX-License-Identifier: Apache-2.0

package reportkey

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/zenthros/operator/internal/testutil"
)

// The key is created once, private to root, and read back unchanged: every
// later start of the agent derives the same tokens.
func TestLoadFileCreatesOnceAndKeeps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zenthros", "report.key")
	first, err := LoadFile(path)
	if err != nil || len(first) != Size {
		t.Fatalf("first load: %d bytes, %v", len(first), err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %v, %v", info.Mode().Perm(), err)
	}
	again, err := LoadFile(path)
	if err != nil || !bytes.Equal(first, again) {
		t.Fatalf("second load changed the key: %v", err)
	}
	if err := os.WriteFile(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path); err == nil {
		t.Fatal("a truncated key was accepted")
	}
}

// On Kubernetes the key lives in a Secret: created on first start, the same
// on the next, so a restarted agent pod with an emptyDir spool keeps
// accepting a running job's reports.
func TestLoadSecretCreatesOnceAndKeeps(t *testing.T) {
	scheme := testutil.Scheme(t, corev1.AddToScheme)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()
	first, err := LoadSecret(ctx, c, "zenthros-system", "")
	if err != nil || len(first) != Size {
		t.Fatalf("first load: %v", err)
	}
	again, err := LoadSecret(ctx, c, "zenthros-system", "")
	if err != nil || !bytes.Equal(first, again) {
		t.Fatalf("second load changed the key: %v", err)
	}
	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: "zenthros-system", Name: DefaultSecretName}, &secret); err != nil {
		t.Fatal(err)
	}
}
