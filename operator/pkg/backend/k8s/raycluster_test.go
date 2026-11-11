// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	"errors"
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

func TestValidateRayClusterReferenceRejectsMissingAndCrossNamespace(t *testing.T) {
	scheme := testutil.Scheme(t, rayv1.AddToScheme)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "tenant-other"}},
	).Build()

	tests := []struct {
		name string
		ref  v1.RayClusterReference
	}{
		{name: "missing", ref: v1.RayClusterReference{Name: "missing", Namespace: "tenant-default"}},
		{name: "cross namespace", ref: v1.RayClusterReference{Name: "shared", Namespace: "tenant-other"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := jobspec.JobSpec{Namespace: "tenant-default", RayCluster: &tc.ref}
			err := ValidateRayClusterReference(context.Background(), reader, spec)
			if !errors.Is(err, backend.ErrAdmissionRejected) {
				t.Fatalf("error = %v, want ErrAdmissionRejected", err)
			}
		})
	}
}

func TestValidateRayClusterReferenceAcceptsSameNamespace(t *testing.T) {
	scheme := testutil.Scheme(t, rayv1.AddToScheme)
	cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "tenant-default"}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	spec := jobspec.JobSpec{
		Namespace:  "tenant-default",
		RayCluster: &v1.RayClusterReference{Name: "shared", Namespace: "tenant-default"},
	}
	if err := ValidateRayClusterReference(context.Background(), reader, spec); err != nil {
		t.Fatalf("ValidateRayClusterReference: %v", err)
	}
}
