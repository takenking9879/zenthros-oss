// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import "testing"

func TestManagedClusterSpecClusterARN(t *testing.T) {
	spec := ManagedClusterSpec{
		ClusterName: "zenthros-eks-ml-1",
		Region:      "us-east-2",
		AccountID:   "123456789012",
	}
	want := "arn:aws:eks:us-east-2:123456789012:cluster/zenthros-eks-ml-1"
	if got := spec.ClusterARN(); got != want {
		t.Fatalf("ClusterARN = %q, want %q", got, want)
	}
}
