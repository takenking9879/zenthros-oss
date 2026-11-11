// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/backend/spark"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := testutil.Scheme(t, rayv1.AddToScheme, corev1.AddToScheme, v1.AddToScheme, spark.AddToScheme)
	return scheme
}
