// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"filippo.io/age"
	"filippo.io/age/armor"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

type secrets map[string]*corev1.Secret

func (s secrets) read(_ context.Context, namespace, name string) (*corev1.Secret, error) {
	found, ok := s[namespace+"/"+name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	return found, nil
}

// at is where the Secret keeping namespace's secret name is read.
func at(namespace, name string) string {
	return v1.DefaultWorkspaceSecretsNamespace + "/" + v1.WorkspaceSecretObject(namespace, name)
}

func workspaceSecret(namespace, name, value string, labelled bool) *corev1.Secret {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: v1.DefaultWorkspaceSecretsNamespace,
		Name: v1.WorkspaceSecretObject(namespace, name)},
		Data: map[string][]byte{v1.WorkspaceSecretKey: []byte(value)}}
	if labelled {
		secret.Labels = map[string]string{v1.LabelWorkspaceSecret: "true", v1.LabelWorkspaceNamespace: namespace}
	}
	return secret
}

func workspaceRef(name string, materialization v1.SecretMaterialization) v1.SecretSourceRef {
	return v1.SecretSourceRef{Type: v1.SecretSourceTypeWorkspace, Name: name, Materialization: materialization}
}

func open(t *testing.T, ciphertext string, identity *age.X25519Identity) string {
	t.Helper()
	reader, err := age.Decrypt(armor.NewReader(strings.NewReader(ciphertext)), identity)
	if err != nil {
		t.Fatalf("the machine cannot open it: %v", err)
	}
	plain, _ := io.ReadAll(reader)
	return string(plain)
}

// A Workspace secret is sealed, as the job goes to its machine, to that
// machine's age key: a template's machine or one that slept is made after
// the job was written, so nothing could be sealed to it before.
func TestWorkspaceSecretIsSealedToTheMachineThatRunsTheJob(t *testing.T) {
	machine, _ := age.GenerateX25519Identity()
	store := secrets{at("tenant-a", "hf-token"): workspaceSecret("tenant-a", "hf-token", "hf_s3cr3t", true),
		at("tenant-a", "wandb-key"): workspaceSecret("tenant-a", "wandb-key", "wb", true)}
	plain := v1.SecretSourceRef{Type: v1.SecretSourceTypeKubernetes, Namespace: "tenant-a", Name: "db", Key: "url",
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "DB_URL"}}
	refs := []v1.SecretSourceRef{
		workspaceRef("hf-token", v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "HF_TOKEN"}),
		plain,
		workspaceRef("hf-token", v1.SecretMaterialization{Mode: v1.SecretMaterializationFile}),
		// No env name: the name's, as for any secret, not the payload's.
		workspaceRef("wandb-key", v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv}),
	}
	resolved, err := ResolveWorkspace(context.Background(), refs, "tenant-a", "", store.read, []string{machine.Recipient().String()})
	if err != nil {
		t.Fatal(err)
	}
	if refs[0].Type != v1.SecretSourceTypeWorkspace {
		t.Fatal("the job's own refs were changed")
	}
	env, file := resolved[0], resolved[2]
	for _, ref := range []v1.SecretSourceRef{env, file} {
		if ref.Type != v1.SecretSourceTypeEncryptedPayload || ref.Name != "" || ref.EncryptedPayload == nil {
			t.Fatalf("not sealed: %+v", ref)
		}
		if got := open(t, ref.EncryptedPayload.Ciphertext, machine); got != "hf_s3cr3t" {
			t.Fatalf("opened %q", got)
		}
		if err := MatchAdvertisedRecipients([]v1.SecretSourceRef{ref}, []string{machine.Recipient().String()}); err != nil {
			t.Fatal(err)
		}
	}
	if resolved[1] != plain {
		t.Fatal("a ref that is not the Workspace's changed")
	}
	// Where it lands is what the job asked: the same env name, the same file.
	if env.Materialization.EnvName != "HF_TOKEN" || resolved[3].Materialization.EnvName != "WANDB_KEY" {
		t.Fatalf("env %+v, %+v", env.Materialization, resolved[3].Materialization)
	}
	if want := FilePath(refs[2]); file.Materialization.Path != want || want != DefaultFileRoot+"/workspace/hf-token" {
		t.Fatalf("file %q, want %q", file.Materialization.Path, want)
	}
	if err := Validate(resolved); err != nil {
		t.Fatal(err)
	}
}

// Only Secrets the API wrote for the Workspace are ever read: a job cannot
// name another Secret of its namespace (a ServiceAccount token, the hub's
// own) and have it sealed to a machine it controls.
func TestOnlyTheWorkspacesOwnSecretsAreRead(t *testing.T) {
	machine, _ := age.GenerateX25519Identity()
	recipients := []string{machine.Recipient().String()}
	ref := []v1.SecretSourceRef{workspaceRef("hf-token", v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv})}
	for name, store := range map[string]secrets{
		"unlabelled":   {at("tenant-a", "hf-token"): workspaceSecret("tenant-a", "hf-token", "x", false)},
		"other tenant": {at("tenant-b", "hf-token"): workspaceSecret("tenant-b", "hf-token", "x", true)},
		// Kept under tenant-a's name, labelled for tenant-b: not tenant-a's.
		"other label": {at("tenant-a", "hf-token"): workspaceSecret("tenant-b", "hf-token", "x", true)},
		// The Workspace's own namespace is not where its secrets are kept.
		"own namespace": {"tenant-a/zenthros-secret-hf-token": {ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: "zenthros-secret-hf-token",
			Labels: map[string]string{v1.LabelWorkspaceSecret: "true", v1.LabelWorkspaceNamespace: "tenant-a"}},
			Data: map[string][]byte{v1.WorkspaceSecretKey: []byte("x")}}},
		"missing":        {},
		"no value field": {at("tenant-a", "hf-token"): {ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.LabelWorkspaceSecret: "true"}}}},
	} {
		_, err := ResolveWorkspace(context.Background(), ref, "tenant-a", "", store.read, recipients)
		var secretErr *backend.SecretSourceError
		if !errors.As(err, &secretErr) || secretErr.Code != v1.ReasonInvalidSecretSource || !strings.Contains(err.Error(), "hf-token") {
			t.Fatalf("%s: %v", name, err)
		}
	}
	store := secrets{at("tenant-a", "hf-token"): workspaceSecret("tenant-a", "hf-token", "x", true)}
	if _, err := ResolveWorkspace(context.Background(), ref, "tenant-a", "", store.read, nil); err == nil ||
		!strings.Contains(err.Error(), "age key") {
		t.Fatalf("a machine with no age key: %v", err)
	}
	// A failing read is not the user's mistake: it is retried, never a
	// permanent failure.
	broken := func(context.Context, string, string) (*corev1.Secret, error) {
		return nil, errors.New("apiserver down")
	}
	_, err := ResolveWorkspace(context.Background(), ref, "tenant-a", "", broken, recipients)
	var secretErr *backend.SecretSourceError
	if err == nil || errors.As(err, &secretErr) {
		t.Fatalf("a failed read: %v", err)
	}
}

func TestWorkspaceSecretRefIsValidatedLikeTheOthers(t *testing.T) {
	env := v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv}
	if err := Validate([]v1.SecretSourceRef{workspaceRef("hf-token", env)}); err != nil {
		t.Fatal(err)
	}
	if got := EnvName(workspaceRef("hf-token", env)); got != "HF_TOKEN" {
		t.Fatalf("default env name %q", got)
	}
	for name, ref := range map[string]v1.SecretSourceRef{
		"no name":   workspaceRef("", env),
		"bad name":  workspaceRef("HF Token", env),
		"namespace": {Type: v1.SecretSourceTypeWorkspace, Name: "a", Namespace: "other", Materialization: env},
		"key":       {Type: v1.SecretSourceTypeWorkspace, Name: "a", Key: "k", Materialization: env},
	} {
		if err := Validate([]v1.SecretSourceRef{ref}); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	// Unresolved, it reaches no agent: Negotiate refuses it.
	if err := Negotiate([]v1.SecretSourceRef{workspaceRef("a", env)}, backend.TypeVM, "", nil); err == nil {
		t.Fatal("an unresolved Workspace secret was negotiated")
	}
}
