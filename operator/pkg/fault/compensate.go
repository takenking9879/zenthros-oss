// SPDX-License-Identifier: Apache-2.0

package fault

import (
	"context"
	"errors"
	"reflect"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CleanupTimeout bounds a compensating action.
const CleanupTimeout = 10 * time.Second

// Cleanup is the context a compensating action runs with (undo a half-made
// create, delete what a failed submit left): it outlives the caller's
// cancellation, which is often why the compensation is needed, and is
// bounded on its own.
func Cleanup(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)
}

// ErrNotOurs is CreateOwned's refusal: the object exists and is someone
// else's.
var ErrNotOurs = errors.New("the object exists and belongs to someone else")

// CreateOwned creates obj and resolves the outcomes a plain Create leaves
// open. AlreadyExists, an outcome nobody knows (a timeout after the request
// was sent) or a transient answer (a 500 from etcd timing out may have
// persisted it) is settled by reading the object directly: if owns
// says the object there is ours (a retry of our own create, or the create
// that timed out), that is success and obj is filled with it; otherwise the
// error is Exists, wrapping ErrNotOurs.
func CreateOwned(ctx context.Context, c client.Client, obj client.Object, owns func(existing client.Object) bool) error {
	err := c.Create(ctx, obj)
	if err == nil {
		return nil
	}
	if k := Of(err); k != Exists && k != Unknown && k != Transient {
		return err
	}
	existing, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return err
	}
	if getErr := c.Get(ctx, client.ObjectKeyFromObject(obj), existing); getErr != nil {
		if Of(err) == Exists {
			return Wrap(Unknown, "read back the existing object", getErr)
		}
		return err
	}
	if !owns(existing) {
		return &Error{Kind: Exists, Op: "create " + client.ObjectKeyFromObject(obj).String(), Err: ErrNotOurs}
	}
	// Same concrete type: DeepCopyObject returned a copy of obj.
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(existing).Elem())
	return nil
}
