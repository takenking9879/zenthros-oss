// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The enrollment Job says where its agent's TLS Secret goes: its own
// namespace (the downward API) and the chart's agent.tls.secretName. A
// cluster attached to a Workspace installs the chart once per machine, each
// in a namespace of its own; the hub's own agent is in zenthros-system.
const (
	envAgentNamespace = "POD_NAMESPACE"
	envAgentTLSSecret = "ZENTHROS_AGENT_TLS_SECRET"
	defaultNamespace  = "zenthros-system"
	defaultTLSSecret  = "zenthros-agent-tls"
)

// agentSecretKey is the agent's TLS Secret, from getenv.
func agentSecretKey(getenv func(string) string) client.ObjectKey {
	key := client.ObjectKey{Namespace: strings.TrimSpace(getenv(envAgentNamespace)), Name: strings.TrimSpace(getenv(envAgentTLSSecret))}
	if key.Namespace == "" {
		key.Namespace = defaultNamespace
	}
	if key.Name == "" {
		key.Name = defaultTLSSecret
	}
	return key
}

func deployK8s(ctx context.Context, p JoinParams) error {
	c, err := k8sClient()
	if err != nil {
		return err
	}
	if err := writeAgentSecret(ctx, c, p); err != nil {
		return err
	}
	return nil
}

func agentSecretReady(ctx context.Context, c client.Client) (bool, error) {
	secret := &corev1.Secret{}
	key := agentSecretKey(os.Getenv)
	if err := c.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read %s: %w", key, err)
	}
	for _, key := range []string{"agent.crt", "agent.key", "ca.crt"} {
		if len(secret.Data[key]) == 0 {
			return false, nil
		}
	}
	return unexpired(secret.Data["agent.crt"]), nil
}

func k8sClient() (client.Client, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add core scheme: %w", err)
	}
	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("resolve kubeconfig (in-cluster or KUBECONFIG): %w", err)
	}
	c, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	return c, nil
}

func writeAgentSecret(ctx context.Context, c client.Client, p JoinParams) error {
	certPEM, keyPEM, caPEM, err := readCredentialFiles(p.StateDir)
	if err != nil {
		return err
	}
	key := agentSecretKey(os.Getenv)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
		Data: map[string][]byte{
			"agent.crt": certPEM,
			"agent.key": keyPEM,
			"ca.crt":    caPEM,
		},
	}
	existing := &corev1.Secret{}
	err = c.Get(ctx, client.ObjectKeyFromObject(sec), existing)
	if err == nil {
		existing.Data = sec.Data
		return c.Update(ctx, existing)
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("read %s/%s: %w", sec.Namespace, sec.Name, err)
	}
	return c.Create(ctx, sec)
}
