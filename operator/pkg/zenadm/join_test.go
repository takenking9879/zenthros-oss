// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zenthros/operator/internal/testutil"
)

func TestDirUsable(t *testing.T) {
	t.Run("existing writable dir", func(t *testing.T) {
		dir := t.TempDir()
		if !dirUsable(dir) {
			t.Fatalf("dirUsable(%q) = false, want true", dir)
		}
	})

	t.Run("nonexistent dir", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "does-not-exist")
		if dirUsable(dir) {
			t.Fatalf("dirUsable(%q) = true, want false", dir)
		}
	})

	t.Run("path is a file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if dirUsable(file) {
			t.Fatalf("dirUsable(%q) = true, want false for a regular file", file)
		}
	})

	t.Run("probe leaves no file behind", func(t *testing.T) {
		dir := t.TempDir()
		if !dirUsable(dir) {
			t.Fatalf("dirUsable(%q) = false, want true", dir)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("probe left %d entries in %q, want 0", len(entries), dir)
		}
	})
}

func TestCredentialsExist(t *testing.T) {
	dir := t.TempDir()
	if credentialsExist(dir) {
		t.Fatal("credentialsExist = true for an empty state directory")
	}
	ca := testutil.NewCA(t, "zenthros-ca")
	write := func(cert []byte) {
		for name, body := range map[string][]byte{"agent.crt": cert, "agent.key": []byte("key"), "ca.crt": []byte("ca")} {
			if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(ca.Issue(t, testutil.Leaf{CommonName: "vm-1"}).CertPEM)
	if !credentialsExist(dir) {
		t.Fatal("credentialsExist = false after all credential files were written")
	}
	// A machine switched off longer than its certificate's life cannot
	// renew it: a join enrolls over it instead of reusing it.
	write(ca.Issue(t, testutil.Leaf{CommonName: "vm-1", NotAfter: time.Now().Add(-time.Minute)}).CertPEM)
	if credentialsExist(dir) {
		t.Fatal("credentialsExist = true for an expired certificate")
	}
}
