// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	"encoding/base64"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/jobspec"
)

// The inline Secret a lost create answer (or a racing retry of the same
// submit) already wrote is the job's: the submit goes on, where it failed
// SPEC_MISMATCH as if the name were taken. Another job's Secret of that name
// is still refused.
func TestAnInlineSecretThisJobWroteIsItsOwn(t *testing.T) {
	scheme := testutil.Scheme(t, corev1.AddToScheme)
	spec := jobspec.JobSpec{ID: "uid-1", Name: "job-a", Namespace: "ns", Tenant: "default"}
	spec.Source.Inline = base64.StdEncoding.EncodeToString([]byte("bundle"))
	racing := func(owner string) client.Client {
		base := fake.NewClientBuilder().WithScheme(scheme).Build()
		return interceptor.NewClient(base, interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				// Between this submit's Get and its Create, the Secret
				// appears: this job's own earlier try, or another job's.
				raced := obj.DeepCopyObject().(*corev1.Secret)
				raced.Labels["zenthros.ai/job-id"] = owner
				if err := c.Create(ctx, raced, opts...); err != nil {
					return err
				}
				return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, obj.GetName())
			},
		})
	}
	if err := EnsureInlineSecret(context.Background(), racing("uid-1"), spec); err != nil {
		t.Fatalf("this job's own Secret: %v", err)
	}
	if err := EnsureInlineSecret(context.Background(), racing("uid-2"), spec); err == nil {
		t.Fatal("another job's Secret of the name was taken as this job's")
	}
}
