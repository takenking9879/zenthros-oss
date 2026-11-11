// SPDX-License-Identifier: Apache-2.0

package secretfile

import (
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
)

func TestApplyWrites0600AndCleanupRemovesPlaintext(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "run", "secret")
	bindings := []Binding{
		{
			Ref: v1.SecretSourceRef{
				Type:            v1.SecretSourceTypeEncryptedPayload,
				Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T15_SECRET"},
			},
			Value: "destination-only",
		},
		{
			Ref: v1.SecretSourceRef{
				Type:            v1.SecretSourceTypeEncryptedPayload,
				Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationFile, Path: filePath},
			},
			Value: "destination-only",
		},
	}
	result, err := Apply(root, "job-1", bindings)
	if err != nil {
		t.Fatal(err)
	}
	envFile := filepath.Join(root, "job-1", secretFileDir, "T15_SECRET")
	if len(result.Mounts) != 1 || result.Mounts[0].Target != filePath {
		t.Fatalf("mounts = %+v", result.Mounts)
	}
	stored := result.Mounts[0].Source
	for _, path := range []string{envFile, stored} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != FileMode {
			t.Fatalf("%s mode = %o", path, info.Mode().Perm())
		}
	}
	if result.WrapperPath == "" {
		t.Fatal("env materialization must write a wrapper")
	}
	if err := CleanupRoot(root, "job-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(envFile); !os.IsNotExist(err) {
		t.Fatal("env file remained after cleanup")
	}
	if _, err := os.Stat(stored); !os.IsNotExist(err) {
		t.Fatal("materialized file remained after cleanup")
	}
}

func TestDefaultRootIsWritable(t *testing.T) {
	root := DefaultRoot()
	if !canUseRoot(filepath.Dir(root), root) {
		t.Fatalf("DefaultRoot %q is not writable", root)
	}
}

// A secret root, and every directory between it and the shared one it is
// made in, is the agent's and writable by no one else: a local user who made
// /dev/shm/zenthros first could rename secrets/ and plant a job's wrapper.
func TestASecretRootSomeoneElseCanWriteIsRefused(t *testing.T) {
	base := t.TempDir()
	if !canUseRoot(base, filepath.Join(base, "zenthros", "secrets")) {
		t.Fatal("a root the agent made is refused")
	}

	open := t.TempDir()
	if err := os.Mkdir(filepath.Join(open, "zenthros"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(open, "zenthros"), 0o777); err != nil {
		t.Fatal(err)
	}
	if canUseRoot(open, filepath.Join(open, "zenthros", "secrets")) {
		t.Fatal("a root under a world-writable parent is used")
	}

	linked := t.TempDir()
	elsewhere := t.TempDir()
	if err := os.Symlink(elsewhere, filepath.Join(linked, "zenthros")); err != nil {
		t.Fatal(err)
	}
	if canUseRoot(linked, filepath.Join(linked, "zenthros", "secrets")) {
		t.Fatal("a root through a symlink is used")
	}

	// Apply refuses a root others may write and leaves nothing in it.
	shared := filepath.Join(t.TempDir(), "zenthros-secrets")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	binding := Binding{Ref: v1.SecretSourceRef{Type: v1.SecretSourceTypeEncryptedPayload,
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T_SECRET"}}, Value: "v"}
	if _, err := Apply(shared, "job-1", []Binding{binding}); err == nil {
		t.Fatal("Apply wrote into a world-writable root")
	}
	if entries, _ := os.ReadDir(shared); len(entries) != 0 {
		t.Fatalf("Apply left %d entries in the refused root", len(entries))
	}
}
