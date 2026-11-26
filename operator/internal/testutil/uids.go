// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// CreateWithUID is a fake client's Create as the API server does it: the
// object gets a UID, which a delete of the incarnation that was read
// (owner.DeleteExact, owner.DeleteIncarnation) needs. The fake client
// assigns none. Use it as interceptor.Funcs{Create: testutil.CreateWithUID}.
func CreateWithUID(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	if obj.GetUID() == "" {
		obj.SetUID(uuid.NewUUID())
	}
	return c.Create(ctx, obj, opts...)
}

// WithUIDs gives each fixture without a UID one, as the API server would
// have: what a test seeds a fake client with was, in a cluster, created.
func WithUIDs(objects ...client.Object) []client.Object {
	for _, obj := range objects {
		if obj.GetUID() == "" {
			obj.SetUID(uuid.NewUUID())
		}
	}
	return objects
}

// ApplyWithUID is a fake client's server-side apply as the API server does
// it: an object the apply created gets a UID. Use it as
// interceptor.Funcs{Apply: testutil.ApplyWithUID}.
func ApplyWithUID(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	if err := c.Apply(ctx, obj, opts...); err != nil {
		return err
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	applied := &unstructured.Unstructured{}
	if err := applied.UnmarshalJSON(raw); err != nil {
		return err
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(applied), applied); err != nil {
		return err
	}
	if applied.GetUID() != "" {
		return nil
	}
	applied.SetUID(uuid.NewUUID())
	return c.Update(ctx, applied)
}

// DeleteWithUIDPrecondition is a fake client's Delete as the API server
// does it: a UID precondition that is not the stored object's is a
// Conflict. The fake client checks only a resourceVersion precondition.
// Use it as interceptor.Funcs{Delete: testutil.DeleteWithUIDPrecondition}.
func DeleteWithUIDPrecondition(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	var o client.DeleteOptions
	o.ApplyOptions(opts)
	if o.Preconditions != nil && o.Preconditions.UID != nil {
		stored := obj.DeepCopyObject().(client.Object)
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), stored); err != nil {
			return err
		}
		if stored.GetUID() != *o.Preconditions.UID {
			gvk, _ := apiutil.GVKForObject(obj, c.Scheme())
			return apierrors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, obj.GetName(),
				fmt.Errorf("precondition failed: UID in precondition: %s, UID in object meta: %s", *o.Preconditions.UID, stored.GetUID()))
		}
	}
	return c.Delete(ctx, obj, opts...)
}
