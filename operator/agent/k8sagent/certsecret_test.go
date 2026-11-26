// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A Kubernetes agent's renewal lands in its own Secret, under the names of
// its files, with the CA it had: the kubelet updates the read-only files from
// it, and a restart starts with the new pair (E2E 2026-10-28: the agent could
// not write its files and was cut off when its certificate expired).
func TestAKubernetesAgentsRenewalLandsInItsSecret(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "zenthros-system", Name: "zenthros-agent-tls"},
		Data:       map[string][]byte{"agent.key": []byte("old key"), "agent.crt": []byte("old cert"), "ca.crt": []byte("ca")},
	}).Build()
	save := CertificateSecret(c, "zenthros-system", "zenthros-agent-tls", "agent.key", "agent.crt")
	if err := save(ctx, []byte("new key"), []byte("new cert")); err != nil {
		t.Fatal(err)
	}
	var got corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: "zenthros-system", Name: "zenthros-agent-tls"}, &got); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"agent.key": "new key", "agent.crt": "new cert", "ca.crt": "ca"} {
		if string(got.Data[key]) != want {
			t.Errorf("%s = %q, want %q", key, got.Data[key], want)
		}
	}
}
