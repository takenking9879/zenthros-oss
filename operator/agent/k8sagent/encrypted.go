// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/secretsource"
)

const (
	encryptedSecretLabel  = "zenthros.ai/encrypted-payload"
	encryptedJobLabel     = "zenthros.ai/job-id"
	encryptedSecretKey    = "value"
	encryptedSecretPrefix = "zenthros-enc"
)

func (e *Executor) projectEncryptedPayloads(ctx context.Context, spec jobspec.JobSpec) (jobspec.JobSpec, error) {
	projected := append([]v1.SecretSourceRef(nil), spec.SecretSources...)
	for index, ref := range projected {
		if ref.Type != v1.SecretSourceTypeEncryptedPayload {
			continue
		}
		if ref.EncryptedPayload == nil {
			return spec, backend.NewSecretSourceError(v1.ReasonInvalidSecretSource, "encryptedPayload is required")
		}
		plaintext, err := e.Decryptor.Decrypt(ref.EncryptedPayload.Ciphertext)
		if err != nil {
			return spec, err
		}
		secretName := encryptedSecretName(spec.ID, index)
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: spec.Namespace,
				Name:      secretName,
				Labels: map[string]string{
					encryptedSecretLabel: "true",
					encryptedJobLabel:    spec.ID,
				},
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{encryptedSecretKey: plaintext},
		}
		if err := e.upsertSecret(ctx, secret); err != nil {
			return spec, err
		}
		projected[index] = v1.SecretSourceRef{
			Type:            v1.SecretSourceTypeKubernetes,
			Namespace:       spec.Namespace,
			Name:            secretName,
			Key:             encryptedSecretKey,
			Materialization: ref.Materialization,
		}
	}
	spec.SecretSources = projected
	if err := secretsource.Validate(spec.SecretSources); err != nil {
		return spec, backend.NewSecretSourceError(v1.ReasonInvalidSecretSource, err.Error())
	}
	return spec, nil
}

// upsertSecret creates the job's payload Secret, or writes it again over
// the one a Submit of the same job made. One that is not the job's (its
// job-id label) refuses the Submit, and is left as it is.
func (e *Executor) upsertSecret(ctx context.Context, secret *corev1.Secret) error {
	data := secret.Data
	job := owner.Ref{Kind: "ZenthrosJob", JobID: secret.Labels[encryptedJobLabel]}
	err := fault.CreateOwned(ctx, e.client, secret, func(existing client.Object) bool { return owner.Owns(job, existing) })
	if err != nil {
		return fmt.Errorf("create encrypted payload secret: %w", err)
	}
	if bytes.Equal(secret.Data[encryptedSecretKey], data[encryptedSecretKey]) {
		return nil
	}
	secret.Data = data
	if err := e.client.Update(ctx, secret); err != nil {
		return fmt.Errorf("update encrypted payload secret: %w", err)
	}
	return nil
}

// encryptedSecretName is the Secret of a job's index-th payload, by the
// job's ID: by its name, it was another incarnation's of that name, or a
// user's.
func encryptedSecretName(jobID string, index int) string {
	return fmt.Sprintf("%s-%s-%d", encryptedSecretPrefix, jobID, index)
}
