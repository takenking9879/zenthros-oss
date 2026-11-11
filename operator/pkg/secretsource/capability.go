// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"fmt"
	"strings"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/capability"
)

// Negotiate rejects secret sources the destination backend cannot resolve.
// Kubernetes secrets stay on Kubernetes agents. AWS Secrets Manager requires a
// Managed VM worker identity; External VMs, containers and Kubernetes agents
// are rejected before the envelope is sent. stores are the types the agent
// declared it resolves (capability.Host.SecretStores): a type outside them is
// refused too, so a GCP VM no longer takes aws-secrets-manager. nil is a
// host that has not reported yet, judged by its class alone. The
// destination is known: a backend nobody can describe is waited for before
// anything is negotiated, never refused.
func Negotiate(refs []v1.SecretSourceRef, backendClass string, ownership infrav1alpha1.BackendOwnership, stores []string) error {
	for index, ref := range refs {
		if !(capability.Host{SecretStores: stores}).Resolves(ref.Type) {
			return fmt.Errorf("secretSources[%d]: %w", index, backend.NewSecretSourceError(
				v1.ReasonUnsupportedSecretSource,
				fmt.Sprintf("the destination resolves %s secrets, not %q", strings.Join(stores, ", "), ref.Type),
			))
		}
		if err := negotiateRef(ref, backendClass, ownership); err != nil {
			return fmt.Errorf("secretSources[%d]: %w", index, err)
		}
	}
	return nil
}

func negotiateRef(ref v1.SecretSourceRef, backendClass string, ownership infrav1alpha1.BackendOwnership) error {
	switch ref.Type {
	case v1.SecretSourceTypeKubernetes:
		if backendClass != backend.TypeK8s {
			return backend.NewSecretSourceError(
				v1.ReasonUnsupportedSecretSource,
				"k8s-secret references require a Kubernetes agent",
			)
		}
		return nil
	case v1.SecretSourceTypeAWSSecretsManager:
		if backendClass != backend.TypeVM {
			return backend.NewSecretSourceError(
				v1.ReasonUnsupportedSecretSource,
				"aws-secrets-manager references require a Managed VM worker identity",
			)
		}
		if infrav1alpha1.NormalizeOwnership(ownership) != infrav1alpha1.OwnershipManaged {
			return backend.NewSecretSourceError(
				v1.ReasonUnsupportedSecretSource,
				"aws-secrets-manager references require a Managed VM worker identity",
			)
		}
		return nil
	case v1.SecretSourceTypeEncryptedPayload:
		if backendClass != backend.TypeK8s && backendClass != backend.TypeVM && backendClass != backend.TypeContainer {
			return backend.NewSecretSourceError(
				v1.ReasonUnsupportedSecretSource,
				"encrypted-payload requires a destination agent that advertised an age recipient",
			)
		}
		return nil
	default:
		return backend.NewSecretSourceError(
			v1.ReasonUnsupportedSecretSource,
			fmt.Sprintf("unsupported secret source type %q", ref.Type),
		)
	}
}

// MatchAdvertisedRecipients rejects encrypted payloads sealed to a different
// destination than the connected agent. It compares public recipients only.
func MatchAdvertisedRecipients(refs []v1.SecretSourceRef, advertised []string) error {
	for index, ref := range refs {
		if ref.Type != v1.SecretSourceTypeEncryptedPayload {
			continue
		}
		var recipients []string
		if ref.EncryptedPayload != nil {
			recipients = ref.EncryptedPayload.Recipients
		}
		if !RecipientsOverlap(recipients, advertised) {
			return fmt.Errorf("secretSources[%d]: %w", index, backend.NewSecretSourceError(
				v1.ReasonSecretAccessDenied,
				"ciphertext is not sealed to the destination agent",
			))
		}
	}
	return nil
}
