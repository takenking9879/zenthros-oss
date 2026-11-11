// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CertificateSecret is agent.Config.SaveCertificate for an agent whose TLS
// files are the Secret name in namespace, mounted read-only. It writes the
// renewed pair under the keys the files are named by, keeps the CA, and
// writes on the version it read. The kubelet then updates the mounted files
// (the TLS configuration reads them on every handshake), and a restart reads
// the new pair.
func CertificateSecret(c client.Client, namespace, name, keyKey, certKey string) func(ctx context.Context, keyPEM, certPEM []byte) error {
	return func(ctx context.Context, keyPEM, certPEM []byte) error {
		var secret corev1.Secret
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret); err != nil {
			return fmt.Errorf("get agent certificate secret %s/%s: %w", namespace, name, err)
		}
		secret.Data[keyKey], secret.Data[certKey] = keyPEM, certPEM
		if err := c.Update(ctx, &secret); err != nil {
			return fmt.Errorf("update agent certificate secret %s/%s: %w", namespace, name, err)
		}
		return nil
	}
}
