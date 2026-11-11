// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"strings"
	"testing"

	"filippo.io/age"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend"
)

func TestValidateEncryptedPayload(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := agepayload.Encrypt([]byte("token"), []string{identity.Recipient().String()})
	if err != nil {
		t.Fatal(err)
	}
	valid := testEncryptedRef(v1.SecretMaterializationEnv, ciphertext, identity.Recipient().String())
	tests := []struct {
		name    string
		ref     v1.SecretSourceRef
		wantErr bool
	}{
		{name: "env", ref: valid},
		{name: "sealed by the controller from a Workspace secret", ref: func() v1.SecretSourceRef {
			ref := valid
			ref.EncryptedPayload = valid.EncryptedPayload.DeepCopy()
			ref.EncryptedPayload.WorkspaceSecret = "hf-token"
			return ref
		}()},
		{name: "sealed from a name no Workspace secret has", ref: func() v1.SecretSourceRef {
			ref := valid
			ref.EncryptedPayload = valid.EncryptedPayload.DeepCopy()
			ref.EncryptedPayload.WorkspaceSecret = "HF_TOKEN"
			return ref
		}(), wantErr: true},
		{name: "missing ciphertext", ref: v1.SecretSourceRef{
			Type: v1.SecretSourceTypeEncryptedPayload, EncryptedPayload: &v1.EncryptedPayloadRef{Recipients: []string{identity.Recipient().String()}},
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "TOKEN"},
		}, wantErr: true},
		{name: "k8s coordinates mixed", ref: func() v1.SecretSourceRef {
			ref := valid
			ref.Namespace = "tenant-default"
			return ref
		}(), wantErr: true},
		{name: "env without envName", ref: v1.SecretSourceRef{
			Type: v1.SecretSourceTypeEncryptedPayload,
			EncryptedPayload: &v1.EncryptedPayloadRef{
				Ciphertext: ciphertext, Recipients: []string{identity.Recipient().String()},
			},
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv},
		}, wantErr: true},
		{name: "oversized ciphertext", ref: v1.SecretSourceRef{
			Type: v1.SecretSourceTypeEncryptedPayload,
			EncryptedPayload: &v1.EncryptedPayloadRef{
				Ciphertext: agepayload.ArmorBegin + "\n" + strings.Repeat("A", agepayload.MaxCiphertextBytes) + "\n" + agepayload.ArmorEnd,
				Recipients: []string{identity.Recipient().String()},
			},
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "TOKEN"},
		}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Validate([]v1.SecretSourceRef{test.ref})
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestMatchAdvertisedRecipients(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	other, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := agepayload.Encrypt([]byte("token"), []string{identity.Recipient().String()})
	if err != nil {
		t.Fatal(err)
	}
	ref := testEncryptedRef(v1.SecretMaterializationEnv, ciphertext, identity.Recipient().String())
	if err := MatchAdvertisedRecipients([]v1.SecretSourceRef{ref}, []string{identity.Recipient().String()}); err != nil {
		t.Fatalf("overlap: %v", err)
	}
	err = MatchAdvertisedRecipients([]v1.SecretSourceRef{ref}, []string{other.Recipient().String()})
	code, ok := backend.SecretSourceReason(err)
	if !ok || code != v1.ReasonSecretAccessDenied {
		t.Fatalf("wrong recipient error = %v", err)
	}
}

func TestEncryptedFingerprintOmitsPlaintext(t *testing.T) {
	const plaintext = "fingerprint-must-not-contain-this"
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := agepayload.Encrypt([]byte(plaintext), []string{identity.Recipient().String()})
	if err != nil {
		t.Fatal(err)
	}
	ref := testEncryptedRef(v1.SecretMaterializationEnv, ciphertext, identity.Recipient().String())
	got := Fingerprint(ref)
	if strings.Contains(got, plaintext) {
		t.Fatal("fingerprint contained plaintext")
	}
}

func testEncryptedRef(mode, ciphertext, recipient string) v1.SecretSourceRef {
	mat := v1.SecretMaterialization{Mode: mode}
	if mode == v1.SecretMaterializationEnv {
		mat.EnvName = "T15_SECRET"
	}
	return v1.SecretSourceRef{
		Type: v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{
			Ciphertext: ciphertext, Recipients: []string{recipient}, KeyID: agepayload.KeyID(recipient),
		},
		Materialization: mat,
	}
}
