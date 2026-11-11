// SPDX-License-Identifier: Apache-2.0

package agepayload

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/agepayload"
)

func TestLoadSecretCreatesAndRotatesWithOverlap(t *testing.T) {
	scheme := testutil.Scheme(t, corev1.AddToScheme)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()
	original, err := LoadSecret(ctx, c, "zenthros-system", DefaultSecretName)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := agepayload.Encrypt([]byte("token"), original.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: "zenthros-system", Name: DefaultSecretName}, &secret); err != nil {
		t.Fatal(err)
	}
	secret.Data[secretPreviousIdentityKey] = secret.Data[secretIdentityKey]
	delete(secret.Data, secretIdentityKey)
	if err := c.Update(ctx, &secret); err != nil {
		t.Fatal(err)
	}
	rotated, err := LoadSecret(ctx, c, "zenthros-system", DefaultSecretName)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotated.Recipients()) != 2 {
		t.Fatalf("overlap recipients = %d, want 2", len(rotated.Recipients()))
	}
	if _, err := rotated.Decrypt(sealed); err != nil {
		t.Fatalf("overlap decrypt: %v", err)
	}
}

func TestLoadSecretEmptyNamespaceRejectedByGet(t *testing.T) {
	scheme := testutil.Scheme(t, corev1.AddToScheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: DefaultSecretName},
		Data:       map[string][]byte{},
	}).Build()
	if _, err := LoadSecret(context.Background(), c, "ns", DefaultSecretName); err != nil {
		t.Fatal(err)
	}
}
