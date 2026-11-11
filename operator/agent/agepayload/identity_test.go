// SPDX-License-Identifier: Apache-2.0

package agepayload

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/secretsource"
)

const plaintext = "t15-must-never-leave-destination"

func TestDecryptWrongRecipientAndCorruptCiphertext(t *testing.T) {
	destination, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	other, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := agepayload.Encrypt([]byte(plaintext), other.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	_, err = destination.Decrypt(sealed)
	code, ok := backend.SecretSourceReason(err)
	if !ok || code != v1.ReasonSecretAccessDenied {
		t.Fatalf("wrong recipient error = %v", err)
	}
	if strings.Contains(err.Error(), plaintext) {
		t.Fatal("wrong-recipient error leaked plaintext")
	}

	good, err := agepayload.Encrypt([]byte(plaintext), destination.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	corrupted := strings.Replace(good, "AGE", "XXX", 1)
	_, err = destination.Decrypt(corrupted)
	code, ok = backend.SecretSourceReason(err)
	if !ok || code != v1.ReasonSecretResolutionFailed {
		t.Fatalf("corrupt ciphertext error = %v", err)
	}
}

func TestDecryptRoundTripAndRotationOverlap(t *testing.T) {
	dir := t.TempDir()
	original, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	sealedOld, err := agepayload.Encrypt([]byte(plaintext), original.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := RotateDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotated.Recipients()) != 2 {
		t.Fatalf("overlap recipients = %d, want 2", len(rotated.Recipients()))
	}
	got, err := rotated.Decrypt(sealedOld)
	if err != nil {
		t.Fatalf("overlap decrypt: %v", err)
	}
	if string(got) != plaintext {
		t.Fatalf("overlap plaintext = %q", got)
	}
	sealedBoth, err := agepayload.Encrypt([]byte(plaintext), rotated.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rotated.Decrypt(sealedBoth); err != nil {
		t.Fatalf("current decrypt: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, previousIdentityFile)); err != nil {
		t.Fatal(err)
	}
	currentOnly, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = currentOnly.Decrypt(sealedOld)
	code, ok := backend.SecretSourceReason(err)
	if !ok || code != v1.ReasonSecretAccessDenied {
		t.Fatalf("retired recipient error = %v", err)
	}
}

func TestBindingsMaterializeEnvAndCleanup(t *testing.T) {
	bundle, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := agepayload.Encrypt([]byte(plaintext), bundle.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	ref := v1.SecretSourceRef{
		Type: v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{
			Ciphertext: sealed,
			Recipients: bundle.Recipients(),
			KeyID:      agepayload.KeyID(bundle.Recipients()[0]),
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T15_SECRET"},
	}
	spec := jobspec.JobSpec{ID: "job-1", SecretSources: []v1.SecretSourceRef{ref}}
	bindings, err := Bindings(spec, Config{Bundle: bundle})
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 1 || bindings[0].Value != plaintext {
		t.Fatalf("bindings = %#v", bindings)
	}
	root := t.TempDir()
	result, err := secretfile.Apply(root, spec.ID, bindings)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(result.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
	if err := secretfile.CleanupRoot(root, spec.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(result.Files[0]); !os.IsNotExist(err) {
		t.Fatal("plaintext file remained after cleanup")
	}
}

func TestFilePathDefaultForEncrypted(t *testing.T) {
	ref := v1.SecretSourceRef{
		Type: v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{
			Ciphertext: "-----BEGIN AGE ENCRYPTED FILE-----\nxxxx\n-----END AGE ENCRYPTED FILE-----",
			Recipients: []string{"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"},
			KeyID:      "ql3z7hjy54pw",
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationFile},
	}
	if got := secretsource.FilePath(ref); got != "/var/run/zenthros/secrets/ql3z7hjy54pw/value" {
		t.Fatalf("FilePath = %q", got)
	}
}
