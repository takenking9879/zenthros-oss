// SPDX-License-Identifier: Apache-2.0

package owner_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/owner"
)

// For two different IDs, one's selector never matches the other's labels:
// not when one is a prefix of the other (job "a" and job "a-1", backend X and
// X-1), and not across fields.
func TestSelectNeverMatchesAnotherID(t *testing.T) {
	ids := []string{"a", "a-1", "a1", "ab", "b", "x", "x-1", "x-10", "wl-a", "zenthros-enc-a"}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 2000; i++ {
		a, b := ids[rng.IntN(len(ids))], ids[rng.IntN(len(ids))]
		if a == b {
			continue
		}
		for _, mk := range []func(string) owner.Ref{
			func(id string) owner.Ref { return owner.Ref{JobID: id} },
			func(id string) owner.Ref { return owner.Ref{BackendID: id} },
			func(id string) owner.Ref { return owner.Ref{RequestUID: id} },
			func(id string) owner.Ref { return owner.Ref{JobID: "j", Attempt: owner.Run(int32(len(id)))} },
		} {
			ra, rb := mk(a), mk(b)
			if owner.Labels(ra)[owner.LabelAttempt] == owner.Labels(rb)[owner.LabelAttempt] && ra.JobID == "j" {
				continue // same attempt number from two IDs of one length
			}
			if labels.SelectorFromSet(labels.Set(owner.Select(ra))).Matches(labels.Set(owner.Labels(rb))) {
				t.Fatalf("Select(%v) matches the labels of %v", ra, rb)
			}
		}
	}
}

func TestSelectWithoutIdentityPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an empty Ref must not select")
		}
	}()
	owner.Select(owner.Ref{Kind: "Secret"})
}

func TestOwns(t *testing.T) {
	ref := owner.Ref{JobID: "a", Attempt: owner.Run(2)}
	obj := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Labels: owner.Labels(ref)}}
	if !owner.Owns(ref, obj) || owner.Owns(owner.Ref{JobID: "a", Attempt: owner.Run(3)}, obj) || owner.Owns(owner.Ref{}, obj) {
		t.Fatal("Owns must match exactly and never for an empty Ref")
	}
}

func TestDeleteExactSparesANamesake(t *testing.T) {
	read := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns", UID: types.UID("old"), ResourceVersion: "1"}}
	recreated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns", UID: types.UID("new")}}
	c := fake.NewClientBuilder().WithObjects(recreated).Build()
	if err := owner.DeleteExact(context.Background(), c, read); fault.Of(err) != fault.Conflict {
		t.Fatalf("deleting a namesake: %v", err)
	}
	var still corev1.Secret
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(recreated), &still); err != nil {
		t.Fatalf("the namesake was deleted: %v", err)
	}
	if err := owner.DeleteExact(context.Background(), c, &still); err != nil {
		t.Fatalf("deleting what was read: %v", err)
	}
	if err := owner.DeleteExact(context.Background(), c, &still); err != nil {
		t.Fatalf("already gone must be success: %v", err)
	}
}

func TestDeleteOwnedDeletesOnlyTheJobsOwn(t *testing.T) {
	mk := func(name, job string) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name),
			Labels: owner.Labels(owner.Ref{JobID: job})}}
	}
	c := fake.NewClientBuilder().WithObjects(mk("zenthros-enc-a-1", "a"), mk("zenthros-enc-a-1-x", "a-1"), mk("other", "ab")).Build()
	if err := owner.DeleteOwned(context.Background(), c, &corev1.SecretList{}, "ns", owner.Ref{Kind: "Secret", JobID: "a"}); err != nil {
		t.Fatal(err)
	}
	var left corev1.SecretList
	if err := c.List(context.Background(), &left); err != nil {
		t.Fatal(err)
	}
	if len(left.Items) != 2 {
		t.Fatalf("left %d secrets, want the two of other jobs", len(left.Items))
	}
}

// DeleteIncarnation spares a namesake recreated since the read, and deletes
// the one it read even after that one changed. The fake client checks only
// a resourceVersion precondition; this one checks the UID too, as the API
// server does.
func TestDeleteIncarnationSparesANamesakeAndToleratesAChange(t *testing.T) {
	read := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns", UID: types.UID("old"), ResourceVersion: "1"}}
	recreated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns", UID: types.UID("new")}}
	c := fake.NewClientBuilder().WithObjects(recreated).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			var o client.DeleteOptions
			o.ApplyOptions(opts)
			var stored corev1.Secret
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &stored); err != nil {
				return err
			}
			if o.Preconditions == nil || o.Preconditions.UID == nil {
				return errors.New("a delete without a UID precondition")
			}
			if *o.Preconditions.UID != stored.UID {
				return apierrorsConflict(obj)
			}
			return c.Delete(ctx, obj, opts...)
		},
	}).Build()
	if err := owner.DeleteIncarnation(context.Background(), c, read); fault.Of(err) != fault.Conflict {
		t.Fatalf("deleting a namesake: %v", err)
	}
	stale := recreated.DeepCopy()
	stale.ResourceVersion = "0" // read before a write since
	if err := owner.DeleteIncarnation(context.Background(), c, stale); err != nil {
		t.Fatalf("deleting the incarnation that was read: %v", err)
	}
	if err := owner.DeleteIncarnation(context.Background(), c, stale); err != nil {
		t.Fatalf("already gone must be success: %v", err)
	}
}

func apierrorsConflict(obj client.Object) error {
	return apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, obj.GetName(), errors.New("UID precondition failed"))
}
