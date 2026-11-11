// SPDX-License-Identifier: Apache-2.0

package capability

import (
	"slices"
	"testing"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	v1 "github.com/zenthros/operator/api/v1"
)

// A provider's own store resolves only on its VMs; sealed payloads resolve
// everywhere; a cluster resolves its Secrets.
func TestSecretStoresFollowTheHost(t *testing.T) {
	aws, enc, k8s := v1.SecretSourceTypeAWSSecretsManager, v1.SecretSourceTypeEncryptedPayload, v1.SecretSourceTypeKubernetes
	vm := func(provider string) infrav1alpha1.VMPoolSpec {
		return infrav1alpha1.VMPoolSpec{ProviderRef: infrav1alpha1.ProviderReference{Name: provider}}
	}
	container := vm("runpod")
	container.MachineKind = infrav1alpha1.MachineKindContainer
	for name, c := range map[string]struct{ got, want []string }{
		"aws vm":           {SecretStoresVM("aws"), []string{aws, enc}},
		"gcp vm":           {SecretStoresVM("gcp"), []string{enc}},
		"runpod vm":        {SecretStoresVM("runpod"), []string{enc}},
		"vastai vm":        {SecretStoresVM("vastai"), []string{enc}},
		"vm nowhere known": {SecretStoresVM(""), []string{enc}},
		"container":        {SecretStoresContainer(), []string{enc}},
		"cluster":          {SecretStoresKubernetes(), []string{enc, k8s}},
		"aws pool":         {SecretStoresForPool(vm("aws")), []string{aws, enc}},
		"gcp pool":         {SecretStoresForPool(vm("gcp")), []string{enc}},
		"container pool":   {SecretStoresForPool(container), []string{enc}},
	} {
		if !slices.Equal(c.got, c.want) {
			t.Errorf("%s resolves %v, want %v", name, c.got, c.want)
		}
	}
	if !(Host{}).Resolves(aws) {
		t.Error("a host that declared nothing is not judged")
	}
	if (Host{SecretStores: SecretStoresVM("gcp")}).Resolves(aws) {
		t.Error("a GCP VM resolves aws-secrets-manager")
	}
}
