// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import "testing"

func TestAutoscalingProfileConstantsArePortable(t *testing.T) {
	if PurchaseOnDemand != "on-demand" {
		t.Fatalf("purchase option %q must stay the Karpenter capacity-type value", PurchaseOnDemand)
	}
	if ArchAMD64 != "amd64" {
		t.Fatalf("architecture %q must stay kubernetes.io/arch", ArchAMD64)
	}
	if NodeOwnerKarpenter != "karpenter" {
		t.Fatalf("node owner %q must name Karpenter, not Zenthros", NodeOwnerKarpenter)
	}
	if ReconcilesNodeClaimsNever != "false" {
		t.Fatalf("reconciles-nodeclaims annotation must stay false, got %q", ReconcilesNodeClaimsNever)
	}
}
