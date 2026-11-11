// SPDX-License-Identifier: Apache-2.0

package capability

import (
	"slices"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/infrastructure/provider/traits"
)

// SecretStoresVM is what a VM resolves on provider ("" when it does not
// know where it runs): sealed payloads, with its age identity, and the
// stores its provider declares (traits.SecretStores). Another provider's
// store is refused here: aws-secrets-manager on a GCP VM would fail there
// after the VM was paid.
func SecretStoresVM(provider string) []string {
	stores := append([]string{v1.SecretSourceTypeEncryptedPayload}, traits.SecretStores(provider)...)
	slices.Sort(stores)
	return stores
}

// SecretStoresContainer is what a container resolves: sealed payloads only.
// It has no machine identity and no cluster.
func SecretStoresContainer() []string {
	return []string{v1.SecretSourceTypeEncryptedPayload}
}

// SecretStoresKubernetes is what the in-cluster agent resolves: the
// cluster's Secrets and sealed payloads.
func SecretStoresKubernetes() []string {
	return []string{v1.SecretSourceTypeEncryptedPayload, v1.SecretSourceTypeKubernetes}
}

// SecretStoresForPool is what a pool's machines will declare, read from its
// spec before any of them runs: the Scheduler skips a pool that cannot
// resolve a job's secrets instead of paying for a machine that refuses it.
func SecretStoresForPool(spec infrav1alpha1.VMPoolSpec) []string {
	if spec.MachineKind == infrav1alpha1.MachineKindContainer {
		return SecretStoresContainer()
	}
	return SecretStoresVM(spec.ProviderRef.Name)
}
