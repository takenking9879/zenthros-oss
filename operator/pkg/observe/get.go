// SPDX-License-Identifier: Apache-2.0

package observe

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetConfirmed reads key into obj from cached, and asks live again when the
// cache does not hold it: a cache is behind the API server, so its NotFound
// is not "does not exist". A caller that ends or refuses something because
// an object is missing reads it this way; only live's NotFound is returned.
func GetConfirmed(ctx context.Context, cached, live client.Reader, key client.ObjectKey, obj client.Object) error {
	err := cached.Get(ctx, key, obj)
	if !apierrors.IsNotFound(err) {
		return err
	}
	return live.Get(ctx, key, obj)
}
