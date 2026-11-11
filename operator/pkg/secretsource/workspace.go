// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend"
)

// SecretReader reads one Secret, uncached (the manager's APIReader): a
// Workspace secret is read as the job is sent, never kept in a cache.
type SecretReader func(ctx context.Context, namespace, name string) (*corev1.Secret, error)

// HasWorkspace says whether refs name a Workspace secret.
func HasWorkspace(refs []v1.SecretSourceRef) bool {
	for _, ref := range refs {
		if ref.Type == v1.SecretSourceTypeWorkspace {
			return true
		}
	}
	return false
}

// ResolveWorkspace returns refs with each workspace-secret sealed, as an
// encrypted-payload, to recipients: the age keys of the machine the job goes
// to. The value is read from secretsNamespace (empty:
// v1.DefaultWorkspaceSecretsNamespace), only from the Secret
// v1.WorkspaceSecretObject(namespace, name) labelled LabelWorkspaceSecret and
// LabelWorkspaceNamespace=namespace, which zenthros-api writes for the job's
// Workspace. It lands where the job asked (the same env name, the same file).
// refs is not changed: the job's spec keeps the reference, never the value.
func ResolveWorkspace(ctx context.Context, refs []v1.SecretSourceRef, namespace, secretsNamespace string, read SecretReader, recipients []string) ([]v1.SecretSourceRef, error) {
	if secretsNamespace == "" {
		secretsNamespace = v1.DefaultWorkspaceSecretsNamespace
	}
	if !HasWorkspace(refs) {
		return refs, nil
	}
	out := make([]v1.SecretSourceRef, len(refs))
	copy(out, refs)
	for index, ref := range refs {
		if ref.Type != v1.SecretSourceTypeWorkspace {
			continue
		}
		missing := func(why string) error {
			return fmt.Errorf("secretSources[%d]: %w", index, backend.NewSecretSourceError(v1.ReasonInvalidSecretSource,
				fmt.Sprintf("Workspace secret %q %s: set it with ws.secrets[%q] = value (PUT /secrets/%s)", ref.Name, why, ref.Name, ref.Name)))
		}
		secret, err := read(ctx, secretsNamespace, v1.WorkspaceSecretObject(namespace, ref.Name))
		switch {
		case apierrors.IsNotFound(err):
			return nil, missing("does not exist")
		case err != nil:
			return nil, fmt.Errorf("secretSources[%d]: read Workspace secret %q: %w", index, ref.Name, err)
		case secret.Labels[v1.LabelWorkspaceSecret] != "true" || secret.Labels[v1.LabelWorkspaceNamespace] != namespace:
			return nil, missing("is not one the Workspace keeps")
		}
		value, ok := secret.Data[v1.WorkspaceSecretKey]
		if !ok {
			return nil, missing("has no value")
		}
		if len(recipients) == 0 {
			return nil, fmt.Errorf("secretSources[%d]: %w", index, backend.NewSecretSourceError(v1.ReasonUnsupportedSecretSource,
				fmt.Sprintf("Workspace secret %q: the machine advertises no age key to seal it to", ref.Name)))
		}
		ciphertext, err := agepayload.Encrypt(value, recipients)
		if err != nil {
			return nil, fmt.Errorf("secretSources[%d]: seal Workspace secret %q: %w", index, ref.Name, err)
		}
		where := ref.Materialization
		if where.Mode == v1.SecretMaterializationEnv {
			where.EnvName = EnvName(ref)
		} else {
			where.Path = FilePath(ref)
		}
		out[index] = v1.SecretSourceRef{
			Type: v1.SecretSourceTypeEncryptedPayload,
			EncryptedPayload: &v1.EncryptedPayloadRef{Ciphertext: ciphertext, Recipients: append([]string(nil), recipients...),
				KeyID: agepayload.KeyID(recipients[0]), WorkspaceSecret: ref.Name},
			Materialization: where,
		}
	}
	return out, nil
}
