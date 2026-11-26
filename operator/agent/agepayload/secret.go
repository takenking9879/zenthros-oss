// SPDX-License-Identifier: Apache-2.0

package agepayload

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	secretIdentityKey         = "identity"
	secretPreviousIdentityKey = "identity.previous"
	DefaultSecretName         = "zenthros-agent-age"
)

// LoadSecret loads or creates the destination identity Secret. The hub
// controller never reads this object.
func LoadSecret(ctx context.Context, c client.Client, namespace, name string) (Bundle, error) {
	if name == "" {
		name = DefaultSecretName
	}
	var secret corev1.Secret
	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret)
	if apierrors.IsNotFound(err) {
		generated, genErr := Generate()
		if genErr != nil {
			return Bundle{}, genErr
		}
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Type:       corev1.SecretTypeOpaque,
			Data:       bundleData(generated),
		}
		if createErr := c.Create(ctx, &secret); createErr != nil {
			if !apierrors.IsAlreadyExists(createErr) {
				return Bundle{}, fmt.Errorf("create age identity secret: %w", createErr)
			}
			if getErr := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret); getErr != nil {
				return Bundle{}, fmt.Errorf("get age identity secret: %w", getErr)
			}
			return bundleFromSecret(secret)
		}
		return generated, nil
	}
	if err != nil {
		return Bundle{}, fmt.Errorf("get age identity secret: %w", err)
	}
	bundle, err := bundleFromSecret(secret)
	if err != nil {
		return Bundle{}, err
	}
	if bundle.current != nil {
		return bundle, nil
	}
	generated, err := Generate()
	if err != nil {
		return Bundle{}, err
	}
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data[secretIdentityKey] = []byte(generated.current.String() + "\n")
	if err := c.Update(ctx, &secret); err != nil {
		return Bundle{}, fmt.Errorf("store age identity secret: %w", err)
	}
	generated.previous = bundle.previous
	return generated, nil
}

func bundleFromSecret(secret corev1.Secret) (Bundle, error) {
	var bundle Bundle
	if raw, ok := secret.Data[secretIdentityKey]; ok && len(raw) > 0 {
		identity, err := parseIdentityBytes(raw)
		if err != nil {
			return Bundle{}, err
		}
		bundle.current = identity
	}
	if raw, ok := secret.Data[secretPreviousIdentityKey]; ok && len(raw) > 0 {
		identity, err := parseIdentityBytes(raw)
		if err != nil {
			return Bundle{}, err
		}
		bundle.previous = identity
	}
	return bundle, nil
}

func bundleData(bundle Bundle) map[string][]byte {
	data := map[string][]byte{}
	if bundle.current != nil {
		data[secretIdentityKey] = []byte(bundle.current.String() + "\n")
	}
	if bundle.previous != nil {
		data[secretPreviousIdentityKey] = []byte(bundle.previous.String() + "\n")
	}
	return data
}
