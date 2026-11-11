// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/zenthros/operator/internal/testutil"
)

// The enrollment Job of an attached cluster's release writes the agent's
// TLS Secret where that release's agent mounts it: the Job's namespace and
// the chart's secret name. In zenthros-system it clobbered the hub's own
// agent's. With neither said, the hub's own agent's place, as before.
func TestTheK8sJoinWritesTheSecretWhereItsJobSays(t *testing.T) {
	state := t.TempDir()
	cert := testutil.NewCA(t, "zenthros-ca").Issue(t, testutil.Leaf{CommonName: "cluster-b"}).CertPEM
	for name, body := range map[string]string{"agent.crt": string(cert), "agent.key": "key", "ca.crt": "ca"} {
		if err := os.WriteFile(filepath.Join(state, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	scheme := testutil.Scheme(t, clientgoscheme.AddToScheme)
	for name, tc := range map[string]struct {
		namespace, secret string
		want              client.ObjectKey
	}{
		"its release's": {"zenthros-k8s-1a2b3c4d", "zenthros-agent-tls", client.ObjectKey{Namespace: "zenthros-k8s-1a2b3c4d", Name: "zenthros-agent-tls"}},
		"another name":  {"zenthros-k8s-1a2b3c4d", "other-tls", client.ObjectKey{Namespace: "zenthros-k8s-1a2b3c4d", Name: "other-tls"}},
		"the hub's own": {"", "", client.ObjectKey{Namespace: "zenthros-system", Name: "zenthros-agent-tls"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(envAgentNamespace, tc.namespace)
			t.Setenv(envAgentTLSSecret, tc.secret)
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			if err := writeAgentSecret(context.Background(), c, JoinParams{StateDir: state}); err != nil {
				t.Fatal(err)
			}
			var secret corev1.Secret
			if err := c.Get(context.Background(), tc.want, &secret); err != nil {
				t.Fatalf("no Secret %s: %v", tc.want, err)
			}
			if string(secret.Data["agent.crt"]) != string(cert) || string(secret.Data["ca.crt"]) != "ca" {
				t.Fatalf("Secret %s holds %v", tc.want, secret.Data)
			}
			if ready, err := agentSecretReady(context.Background(), c); err != nil || !ready {
				t.Fatalf("a re-run does not find the Secret it wrote (%v, %v): it would join again", ready, err)
			}
		})
	}
}

// A cluster whose agent certificate expired cannot renew it (the renewal
// needs the connection the certificate opens), so its Secret is no
// credential to reuse: the enrollment Job, which runs with
// --skip-if-existing, enrolls over it. It skipped, and the agent stayed cut
// off (E2E 2026-10-28).
func TestAnExpiredAgentSecretIsEnrolledOver(t *testing.T) {
	t.Setenv(envAgentNamespace, "")
	t.Setenv(envAgentTLSSecret, "")
	ca := testutil.NewCA(t, "zenthros-ca")
	scheme := testutil.Scheme(t, clientgoscheme.AddToScheme)
	for name, tc := range map[string]struct {
		notAfter time.Time
		ready    bool
	}{
		"valid":   {time.Now().Add(time.Hour), true},
		"expired": {time.Now().Add(-time.Minute), false},
	} {
		cert := ca.Issue(t, testutil.Leaf{CommonName: "cluster-b", NotAfter: tc.notAfter}).CertPEM
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "zenthros-system", Name: "zenthros-agent-tls"},
			Data:       map[string][]byte{"agent.crt": cert, "agent.key": []byte("key"), "ca.crt": []byte("ca")},
		}).Build()
		if ready, err := agentSecretReady(context.Background(), c); err != nil || ready != tc.ready {
			t.Errorf("%s certificate: ready=%v err=%v, want %v", name, ready, err, tc.ready)
		}
	}
}
