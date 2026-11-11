// SPDX-License-Identifier: Apache-2.0

// Package k8s translates a JobSpec into KubeRay/Spark native resources for a
// spoke Agent. The Controller never registers this package as an ExecutionBackend
// and never applies RayJob/Spark to the hub apiserver.
package k8s

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/pkg/backend/k8spod"
	"github.com/zenthros/operator/pkg/jobspec"
)

// EnsureInlineSecret creates the Secret holding the decoded inline tarball for spec,
// named by the bundle's digest, idempotently: an existing Secret of that name keeps
// the same bundle, one whose data differs is rewritten, and one another job made
// is refused (EnsureObject). No ownerReference is set:
// the Secret can live in a different cluster than the owning ZenthrosJob (agent path),
// where a cross-cluster owner ref would dangle and trigger garbage collection; cleanup
// is driven by Cancel, which selects it by its job-id label.
func EnsureInlineSecret(ctx context.Context, c client.Client, spec jobspec.JobSpec) error {
	name := InlineSecretNameFor(spec)
	data, err := base64.StdEncoding.DecodeString(spec.Source.Inline)
	if err != nil {
		return fmt.Errorf("decode inline base64: %w", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: spec.Namespace,
			Labels: map[string]string{
				"zenthros.ai/job-id":        spec.ID,
				"zenthros.ai/tenant":        spec.Tenant,
				k8spod.InlineSecretJobLabel: spec.Name,
			},
		},
		Data: map[string][]byte{"bundle.tar.gz": data},
	}
	var existing corev1.Secret
	_, err = EnsureObject(ctx, c, "Secret", secret, &existing, func() error {
		if bytes.Equal(existing.Data["bundle.tar.gz"], data) {
			return nil
		}
		existing.Data = map[string][]byte{"bundle.tar.gz": data}
		if err := c.Update(ctx, &existing); err != nil {
			return fmt.Errorf("update secret %s/%s: %w", spec.Namespace, name, err)
		}
		return nil
	})
	return err
}
