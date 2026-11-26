// SPDX-License-Identifier: Apache-2.0

// Package reportkey holds the agent's report-token key. A job's report token
// is HMAC(key, job id): the agent issues the same token for a job after a
// restart or the loss of its spool, so a running job's reports keep being
// accepted. The key never leaves the agent's machine or namespace.
package reportkey

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Size is the key length in bytes.
	Size = 32
	// DefaultPath is where a machine keeps its key.
	DefaultPath = "/var/lib/zenthros/report.key"
	// DefaultSecretName is the Secret a Kubernetes agent keeps its key in.
	DefaultSecretName = "zenthros-agent-report-key"
	secretKey         = "key"
)

func generate() ([]byte, error) {
	key := make([]byte, Size)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate report key: %w", err)
	}
	return key, nil
}

// LoadFile reads the key at path, creating it (0600) on first use. Two agents
// racing to create it both end up with the one that was linked first.
func LoadFile(path string) ([]byte, error) {
	key, err := os.ReadFile(path)
	if err == nil {
		if len(key) != Size {
			return nil, fmt.Errorf("report key %s has %d bytes, want %d", path, len(key), Size)
		}
		return key, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read report key: %w", err)
	}
	key, err = generate()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create report key directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".report.key-*")
	if err != nil {
		return nil, fmt.Errorf("create report key: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(key); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("write report key: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("write report key: %w", err)
	}
	// Link, not rename: an existing key is never replaced.
	if err := os.Link(tmp.Name(), path); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("install report key: %w", err)
	}
	return LoadFile(path)
}

// LoadSecret reads the key from a Secret in namespace, creating it on first
// use; a create race is settled by the API server.
func LoadSecret(ctx context.Context, c client.Client, namespace, name string) ([]byte, error) {
	if name == "" {
		name = DefaultSecretName
	}
	var secret corev1.Secret
	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret)
	if apierrors.IsNotFound(err) {
		key, genErr := generate()
		if genErr != nil {
			return nil, genErr
		}
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{secretKey: key},
		}
		if createErr := c.Create(ctx, &secret); createErr == nil {
			return key, nil
		} else if !apierrors.IsAlreadyExists(createErr) {
			return nil, fmt.Errorf("create report key secret %s/%s: %w", namespace, name, createErr)
		}
		err = c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret)
	}
	if err != nil {
		return nil, fmt.Errorf("get report key secret %s/%s: %w", namespace, name, err)
	}
	key := secret.Data[secretKey]
	if len(key) != Size {
		return nil, fmt.Errorf("report key secret %s/%s has a %d-byte %q, want %d", namespace, name, len(key), secretKey, Size)
	}
	return key, nil
}
