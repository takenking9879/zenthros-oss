// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"strings"
	"testing"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/capability"
)

func TestNegotiate(t *testing.T) {
	k8sRef := testRef(v1.SecretMaterializationEnv)
	awsRef := testAWSRef(v1.SecretMaterializationEnv)
	tests := []struct {
		name      string
		refs      []v1.SecretSourceRef
		class     string
		ownership infrav1alpha1.BackendOwnership
		stores    []string
		wantCode  string
	}{
		{name: "k8s on kubernetes", refs: []v1.SecretSourceRef{k8sRef}, class: backend.TypeK8s},
		{name: "k8s on vm", refs: []v1.SecretSourceRef{k8sRef}, class: backend.TypeVM, wantCode: v1.ReasonUnsupportedSecretSource},
		{name: "aws on managed vm", refs: []v1.SecretSourceRef{awsRef}, class: backend.TypeVM, ownership: infrav1alpha1.OwnershipManaged},
		{name: "aws on external vm", refs: []v1.SecretSourceRef{awsRef}, class: backend.TypeVM, ownership: infrav1alpha1.OwnershipExternal, wantCode: v1.ReasonUnsupportedSecretSource},
		{name: "aws on kubernetes", refs: []v1.SecretSourceRef{awsRef}, class: backend.TypeK8s, ownership: infrav1alpha1.OwnershipManaged, wantCode: v1.ReasonUnsupportedSecretSource},
		{name: "encrypted on kubernetes", refs: []v1.SecretSourceRef{testEncryptedNegotiateRef()}, class: backend.TypeK8s},
		{name: "encrypted on managed vm", refs: []v1.SecretSourceRef{testEncryptedNegotiateRef()}, class: backend.TypeVM, ownership: infrav1alpha1.OwnershipManaged},
		{name: "encrypted on external vm", refs: []v1.SecretSourceRef{testEncryptedNegotiateRef()}, class: backend.TypeVM, ownership: infrav1alpha1.OwnershipExternal},
		{name: "k8s on container", refs: []v1.SecretSourceRef{k8sRef}, class: backend.TypeContainer, wantCode: v1.ReasonUnsupportedSecretSource},
		{name: "aws on managed container", refs: []v1.SecretSourceRef{awsRef}, class: backend.TypeContainer, ownership: infrav1alpha1.OwnershipManaged, wantCode: v1.ReasonUnsupportedSecretSource},
		{name: "encrypted on container", refs: []v1.SecretSourceRef{testEncryptedNegotiateRef()}, class: backend.TypeContainer},
		// The agent declares the stores it resolves.
		{name: "aws on managed aws vm", refs: []v1.SecretSourceRef{awsRef}, class: backend.TypeVM, ownership: infrav1alpha1.OwnershipManaged, stores: capability.SecretStoresVM("aws")},
		{name: "aws on managed gcp vm", refs: []v1.SecretSourceRef{awsRef}, class: backend.TypeVM, ownership: infrav1alpha1.OwnershipManaged, stores: capability.SecretStoresVM("gcp"), wantCode: v1.ReasonUnsupportedSecretSource},
		{name: "aws on a managed vm that does not know its provider", refs: []v1.SecretSourceRef{awsRef}, class: backend.TypeVM, ownership: infrav1alpha1.OwnershipManaged, stores: capability.SecretStoresVM(""), wantCode: v1.ReasonUnsupportedSecretSource},
		{name: "aws on external aws vm", refs: []v1.SecretSourceRef{awsRef}, class: backend.TypeVM, ownership: infrav1alpha1.OwnershipExternal, stores: capability.SecretStoresVM("aws"), wantCode: v1.ReasonUnsupportedSecretSource},
		{name: "k8s on a cluster that declared", refs: []v1.SecretSourceRef{k8sRef}, class: backend.TypeK8s, stores: capability.SecretStoresKubernetes()},
		{name: "encrypted on a container that declared", refs: []v1.SecretSourceRef{testEncryptedNegotiateRef()}, class: backend.TypeContainer, stores: capability.SecretStoresContainer()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Negotiate(test.refs, test.class, test.ownership, test.stores)
			if test.wantCode == "" {
				if err != nil {
					t.Fatalf("Negotiate() error = %v", err)
				}
				return
			}
			code, ok := backend.SecretSourceReason(err)
			if !ok || code != test.wantCode {
				t.Fatalf("Negotiate() error = %v, want %s", err, test.wantCode)
			}
			if strings.Contains(strings.ToLower(err.Error()), "plaintext") {
				t.Fatal("capability error mentioned a secret value")
			}
		})
	}
}

func testEncryptedNegotiateRef() v1.SecretSourceRef {
	return v1.SecretSourceRef{
		Type: v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{
			Ciphertext: "-----BEGIN AGE ENCRYPTED FILE-----\nxxxx\n-----END AGE ENCRYPTED FILE-----",
			Recipients: []string{"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"},
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T15_SECRET"},
	}
}
