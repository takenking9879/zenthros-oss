// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	"fmt"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

// ValidateRayClusterReference proves the user-selected cluster is an exact,
// same-namespace object before its name becomes a KubeRay cluster selector.
func ValidateRayClusterReference(ctx context.Context, reader client.Reader, spec jobspec.JobSpec) error {
	if spec.RayCluster == nil {
		return nil
	}
	if spec.RayCluster.Namespace != spec.Namespace {
		return fmt.Errorf("%w: RayCluster namespace %q is outside workload namespace %q", backend.ErrAdmissionRejected, spec.RayCluster.Namespace, spec.Namespace)
	}

	var cluster rayv1.RayCluster
	key := client.ObjectKey{Namespace: spec.Namespace, Name: spec.RayCluster.Name}
	if err := reader.Get(ctx, key, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: referenced RayCluster %s/%s was not found", backend.ErrAdmissionRejected, key.Namespace, key.Name)
		}
		if apierrors.IsForbidden(err) {
			return fmt.Errorf("%w: referenced RayCluster %s/%s is not authorized", backend.ErrAdmissionRejected, key.Namespace, key.Name)
		}
		return fmt.Errorf("get referenced RayCluster %s/%s: %w", key.Namespace, key.Name, err)
	}
	return nil
}
