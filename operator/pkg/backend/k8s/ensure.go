// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
)

// EnsureObject creates desired, or takes the object of that name this job
// already made: one a Get finds, or one a create whose answer was lost wrote
// (AlreadyExists is read again, never a mismatch: C-09). An object another
// job made is refused (N11): it is a leftover of an earlier job of that name,
// and adopting it would run that job's spec. One of this job goes to adopt,
// which says whether it is what this submit wants.
func EnsureObject(ctx context.Context, c client.Client, kind string, desired, existing client.Object, adopt func() error) (created bool, err error) {
	key := client.ObjectKeyFromObject(desired)
	err = c.Get(ctx, key, existing)
	if apierrors.IsNotFound(err) {
		if err = c.Create(ctx, desired); err == nil {
			return true, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return false, fmt.Errorf("create %s %s: %w", kind, key, err)
		}
		err = c.Get(ctx, key, existing)
	}
	if err != nil {
		return false, fmt.Errorf("get %s %s: %w", kind, key, err)
	}
	if want, have := desired.GetLabels()[k8spod.LabelJobID], existing.GetLabels()[k8spod.LabelJobID]; have != want {
		return false, fmt.Errorf("%w: %s %s belongs to job %q, not this one (%q); delete it or give the job another name",
			backend.ErrSpecMismatch, kind, key, have, want)
	}
	return false, adopt()
}
