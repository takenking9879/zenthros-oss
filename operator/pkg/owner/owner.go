// SPDX-License-Identifier: Apache-2.0

// Package owner is how Zenthros says whose an object is, and the only way it
// selects or deletes by that. Ownership used to be read from names: a prefix
// (wl-, zenthros-enc-<job>-), a device called X-1, or an empty string that
// selected everything. A job named a prefix of another's deleted that one's
// Secrets; a request recreated under the same name lost its machine.
//
// A Ref is the identity; Labels stamps it on what is created; Select finds
// exactly what carries it, never by prefix and never with an empty field;
// DeleteExact deletes only the incarnation that was read.
package owner

import (
	"context"
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/pkg/fault"
)

// The labels a Ref is stamped with.
const (
	LabelJobID        = "zenthros.ai/job-id"
	LabelAttempt      = "zenthros.ai/attempt"
	LabelBackendID    = "zenthros.ai/backend-id"
	LabelWorkspaceUID = "zenthros.ai/workspace-uid"
	LabelRequestUID   = "zenthros.ai/capacityrequest-uid"
)

// Ref is who owns an object. Kind names the owner's kind for messages
// (ZenthrosJob, CapacityRequest); the other fields are identities, each one
// exact: a job's ID, the run of it, a backend's ID, a Workspace's or a
// CapacityRequest's UID.
type Ref struct {
	Kind         string
	JobID        string
	Attempt      *int32
	BackendID    string
	WorkspaceUID string
	RequestUID   string
}

// Run is the attempt n, for Ref.Attempt.
func Run(n int32) *int32 { return &n }

func (r Ref) String() string {
	return fmt.Sprintf("%s%v", r.Kind, Labels(r))
}

// Labels are the labels that stamp r on an object: one per field set.
func Labels(r Ref) map[string]string {
	out := map[string]string{}
	for key, value := range map[string]string{
		LabelJobID:        r.JobID,
		LabelBackendID:    r.BackendID,
		LabelWorkspaceUID: r.WorkspaceUID,
		LabelRequestUID:   r.RequestUID,
	} {
		if value != "" {
			out[key] = value
		}
	}
	if r.Attempt != nil {
		out[LabelAttempt] = strconv.Itoa(int(*r.Attempt))
	}
	return out
}

// Select matches exactly the objects stamped with every field of r. A Ref
// with no identity would select every object of the kind: that is a
// programming error, and Select panics rather than delete everything.
func Select(r Ref) client.MatchingLabels {
	if r.JobID == "" && r.BackendID == "" && r.WorkspaceUID == "" && r.RequestUID == "" {
		panic(fmt.Sprintf("owner.Select: %s has no identity; it would select everything", r.Kind))
	}
	return client.MatchingLabels(Labels(r))
}

// Owns reports whether obj carries every label of r.
func Owns(r Ref, obj client.Object) bool {
	labels := obj.GetLabels()
	for key, value := range Labels(r) {
		if labels[key] != value {
			return false
		}
	}
	return len(Labels(r)) > 0
}

// DeleteExact deletes obj only if it still is the incarnation that was read:
// the UID and resourceVersion are preconditions, so a namesake recreated
// since, or a change nobody saw, is not deleted (a Conflict comes back).
// Already gone is success. obj must have been read (it has a UID).
func DeleteExact(ctx context.Context, c client.Client, obj client.Object, opts ...client.DeleteOption) error {
	uid, version := obj.GetUID(), obj.GetResourceVersion()
	if uid == "" {
		panic(fmt.Sprintf("owner.DeleteExact: %s/%s was not read (no UID)", obj.GetNamespace(), obj.GetName()))
	}
	pre := metav1.Preconditions{UID: &uid}
	if version != "" {
		pre.ResourceVersion = &version
	}
	err := c.Delete(ctx, obj, append([]client.DeleteOption{client.Preconditions(pre)}, opts...)...)
	if fault.Of(err) == fault.Absent {
		return nil
	}
	return err
}

// DeleteIncarnation deletes obj only if it still is the incarnation that
// was read (a UID precondition): a namesake recreated since is not deleted.
// Unlike DeleteExact a change since to the same object does not stop it: it
// is for a delete that follows the caller's own status write. Already gone
// is success.
func DeleteIncarnation(ctx context.Context, c client.Client, obj client.Object, opts ...client.DeleteOption) error {
	uid := obj.GetUID()
	if uid == "" {
		panic(fmt.Sprintf("owner.DeleteIncarnation: %s/%s was not read (no UID)", obj.GetNamespace(), obj.GetName()))
	}
	err := c.Delete(ctx, obj, append([]client.DeleteOption{client.Preconditions(metav1.Preconditions{UID: &uid})}, opts...)...)
	if fault.Of(err) == fault.Absent {
		return nil
	}
	return err
}

// DeleteOwned deletes every object of list's kind in namespace that Select(r)
// matches, each with DeleteExact. list is filled with what was found.
func DeleteOwned(ctx context.Context, c client.Client, list client.ObjectList, namespace string, r Ref) error {
	if err := c.List(ctx, list, client.InNamespace(namespace), Select(r)); err != nil {
		return err
	}
	items, err := metaItems(list)
	if err != nil {
		return err
	}
	for _, obj := range items {
		if err := DeleteExact(ctx, c, obj); err != nil {
			return err
		}
	}
	return nil
}

func metaItems(list client.ObjectList) ([]client.Object, error) {
	raw, err := meta.ExtractList(list)
	if err != nil {
		return nil, err
	}
	out := make([]client.Object, 0, len(raw))
	for _, item := range raw {
		obj, ok := item.(client.Object)
		if !ok {
			return nil, fmt.Errorf("owner: %T in a list is not an object", item)
		}
		out = append(out, obj)
	}
	return out, nil
}
