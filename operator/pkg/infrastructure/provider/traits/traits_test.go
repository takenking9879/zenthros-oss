// SPDX-License-Identifier: Apache-2.0

package traits

import (
	"slices"
	"testing"
	"time"

	v1 "github.com/zenthros/operator/api/v1"
)

// AWS's Secrets Manager is AWS's alone; only Vast.ai's hosts are a third
// party's; the EKS blueprint builds in AWS.
func TestProvidersDeclareTheirStoresAndBlueprints(t *testing.T) {
	if got := SecretStores("aws"); !slices.Equal(got, []string{v1.SecretSourceTypeAWSSecretsManager}) {
		t.Errorf("aws VMs resolve %v", got)
	}
	for _, p := range []string{"gcp", "runpod", "vastai", "fake", ""} {
		if got := SecretStores(p); len(got) > 0 {
			t.Errorf("%q VMs resolve %v", p, got)
		}
	}
	for p, untrusted := range map[string]bool{"vastai": true, "runpod": false, "aws": false, "gcp": false, "fake": false, "": false} {
		if UntrustedHost(p) != untrusted {
			t.Errorf("%q hosts untrusted: %v, want %v", p, !untrusted, untrusted)
		}
	}
	if got := ClusterProvider("aws-eks-aiml/v1"); got != "aws" {
		t.Errorf("the EKS blueprint builds in %q", got)
	}
	SecretStores("aws")[0] = "changed"
	if SecretStores("aws")[0] != v1.SecretSourceTypeAWSSecretsManager {
		t.Error("a caller changed the table")
	}
}

// Each cloud's notice drains for the notice less 10 s; a source no cloud
// declares has no cap here.
func TestCloudsDeclareTheirDrainCaps(t *testing.T) {
	for source, want := range map[string]time.Duration{NoticeAWSIMDS: 110 * time.Second, NoticeGCPMetadata: 110 * time.Second} {
		if got, ok := DrainCap(source); !ok || got != want {
			t.Errorf("%s drains for %v (declared %v), want %v", source, got, ok, want)
		}
	}
	if _, ok := DrainCap("container-sigterm"); ok {
		t.Error("a container's SIGTERM is a cloud's notice")
	}
}
