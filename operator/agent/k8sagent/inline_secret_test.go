// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"strings"
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	agentage "github.com/zenthros/operator/agent/agepayload"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend/k8s"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
)

// A Secret left by an earlier job of the same name held that job's code: a
// new job reusing the name ran the OLD code, since the Secret was kept as it
// was. The Secret is named by the bundle's digest
// now: the job mounts one holding its own code.
func TestEnsureInlineSecretReplacesStaleCode(t *testing.T) {
	spec := inlineJobSpec()
	stale := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: spec.Namespace, Name: "zenthros-src-" + spec.Name},
		Data:       map[string][]byte{"bundle.tar.gz": []byte("OLD CODE")},
	}
	e := newTestExecutor(t, stale)
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var job rayv1.RayJob
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}, &job); err != nil {
		t.Fatal(err)
	}
	mounted := ""
	for _, volume := range job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Volumes {
		if volume.Secret != nil && strings.HasPrefix(volume.Secret.SecretName, "zenthros-src-") {
			mounted = volume.Secret.SecretName
		}
	}
	var secret corev1.Secret
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: mounted}, &secret); err != nil {
		t.Fatalf("the job mounts Secret %q: %v", mounted, err)
	}
	if got := string(secret.Data["bundle.tar.gz"]); got != "bundle-bytes" {
		t.Fatalf("the job mounts %q holding %q, want this job's code", mounted, got)
	}
}

// Cancel removes the job's inline Secret.
func TestCancelRemovesInlineSecrets(t *testing.T) {
	spec := inlineJobSpec()
	e := newTestExecutor(t)
	handle, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	var left corev1.SecretList
	if err := e.client.List(context.Background(), &left, client.InNamespace(spec.Namespace)); err != nil {
		t.Fatal(err)
	}
	for _, secret := range left.Items {
		if strings.HasPrefix(secret.Name, "zenthros-src-") {
			t.Errorf("Cancel left inline Secret %s", secret.Name)
		}
	}
}

// A Submit refused after it wrote the job's Secrets removes them all, its
// code and its decrypted payloads (06#5): the controller cancels only a job
// with a handle, and a later job of the same name must not find them. It
// does so even when the request's own context has ended (c4-E5). An outcome
// nobody knows (a timeout: the workload may exist and mount them) or an
// object of the name keeps them for Cancel.
func TestAFailedSubmitRemovesOnlyWhatNothingUses(t *testing.T) {
	webhook := apierrors.NewForbidden(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, "inline-test",
		errors.New(`admission webhook "policy.example" denied the request`))
	for name, tc := range map[string]struct {
		createErr error
		cancelCtx bool
		removed   bool
	}{
		"admission refusal":          {createErr: webhook, removed: true},
		"refusal, request cancelled": {createErr: webhook, cancelCtx: true, removed: true},
		"timeout":                    {createErr: apierrors.NewTimeoutError("the create may have happened", 1), removed: false},
		"another job's object":       {createErr: apierrors.NewAlreadyExists(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, "inline-test"), removed: false},
	} {
		t.Run(name, func(t *testing.T) {
			bundle, err := agentage.Generate()
			if err != nil {
				t.Fatal(err)
			}
			ciphertext, err := agepayload.Encrypt([]byte("token"), bundle.Recipients())
			if err != nil {
				t.Fatal(err)
			}
			spec := inlineJobSpec()
			spec.SecretSources = []v1.SecretSourceRef{{
				Type:             v1.SecretSourceTypeEncryptedPayload,
				EncryptedPayload: &v1.EncryptedPayloadRef{Ciphertext: ciphertext, Recipients: bundle.Recipients(), KeyID: agepayload.KeyID(bundle.Recipients()[0])},
				Materialization:  v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "TOKEN"},
			}}
			e := newTestExecutor(t)
			e.Decryptor = bundle
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e.client = interceptor.NewClient(e.client.(client.WithWatch), interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*rayv1.RayJob); ok {
						if tc.cancelCtx {
							cancel()
						}
						return tc.createErr
					}
					return testutil.CreateWithUID(ctx, c, obj, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					return c.Delete(ctx, obj, opts...)
				},
			})
			if _, err := e.Submit(ctx, spec, nil); err == nil {
				t.Fatal("Submit succeeded")
			}
			for _, key := range []types.NamespacedName{
				{Namespace: spec.Namespace, Name: k8s.InlineSecretNameFor(spec)},
				{Namespace: spec.Namespace, Name: encryptedSecretName(spec.ID, 0)},
			} {
				err := e.client.Get(context.Background(), key, &corev1.Secret{})
				if gone := apierrors.IsNotFound(err); gone != tc.removed {
					t.Errorf("Secret %s removed=%v (err=%v), want %v", key.Name, gone, err, tc.removed)
				}
			}
		})
	}
}

// A cancel deletes the Secrets that carry the job's ID and no other: a job
// named "train" deleted "train-2"'s decrypted payload, whose name its prefix
// matched (K1/06#1). A Secret with the job's name label but no job ID is
// not the job's, and stays.
func TestACancelDeletesOnlyTheJobsOwnSecrets(t *testing.T) {
	secret := func(name string, labels map[string]string) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-default", Name: name, Labels: labels}}
	}
	train := jobspec.JobSpec{ID: "uid-a", Name: "train", Namespace: "tenant-default", Tenant: "default",
		Kind: v1.WorkloadKindRayJob, Entrypoint: "python main.py", Image: "team/train:1"}
	e := newTestExecutor(t,
		secret("zenthros-enc-train-0", map[string]string{encryptedSecretLabel: "true", owner.LabelJobID: "uid-a"}),
		secret("zenthros-enc-train-2-0", map[string]string{encryptedSecretLabel: "true", owner.LabelJobID: "uid-b"}),
		secret("zenthros-src-train-unowned", map[string]string{k8s.InlineSecretJobLabel: "train"}))
	h, err := e.Submit(context.Background(), train, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(context.Background(), h, jobOf(train)); err != nil {
		t.Fatal(err)
	}
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: "tenant-default", Name: "zenthros-enc-train-0"}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("Secret zenthros-enc-train-0 stayed (err=%v)", err)
	}
	for _, name := range []string{"zenthros-enc-train-2-0", "zenthros-src-train-unowned"} {
		if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: "tenant-default", Name: name}, &corev1.Secret{}); err != nil {
			t.Errorf("Secret %s, not uid-a's, was deleted: %v", name, err)
		}
	}
}
