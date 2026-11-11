// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

// Submit builds the RayJob (BuildRayJob) and stores it. Retain must not shut
// the cluster down, and the workspace label must match the spec.
func TestSubmitRetainRayJob(t *testing.T) {
	e := newTestExecutor(t)
	spec := jobspec.JobSpec{
		ID:         "job-retain",
		Name:       "train-iris",
		Namespace:  "tenant-acme",
		Tenant:     "acme",
		Workspace:  "acme",
		Kind:       v1.WorkloadKindRayJob,
		Image:      "ray:latest",
		Entrypoint: "python train.py",
		Lifespan:   v1.LifespanRetain,
	}
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var rayJob rayv1.RayJob
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}, &rayJob); err != nil {
		t.Fatalf("get RayJob: %v", err)
	}
	if rayJob.Spec.ShutdownAfterJobFinishes {
		t.Error("Retain RayJob set ShutdownAfterJobFinishes; the cluster (and submitter logs) die at terminal")
	}
	if got := rayJob.Labels["zenthros.ai/workspace"]; got != spec.Workspace {
		t.Errorf("RayJob workspace label = %q, want %q (Job/Pod set this; RayJob does not)", got, spec.Workspace)
	}
}

// Submit calls ValidateWorkload before it admits the kind: the RayCluster the
// CRD enum lists is admitted and created (TestCRDKindRayClusterIsAcceptedByValidateWorkload).
func TestSubmitAdmitsARayCluster(t *testing.T) {
	e := newTestExecutor(t)
	spec := jobspec.JobSpec{
		ID:        "ray-cluster",
		Name:      "ray-cluster",
		Namespace: "tenant-default",
		Kind:      v1.WorkloadKindRayCluster,
		Image:     "ray:latest",
	}
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("CRD enum lists RayCluster but ValidateWorkload rejected it: %v", err)
	}
	var cluster rayv1.RayCluster
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}, &cluster); err != nil {
		t.Fatalf("admitted RayCluster was not created: %v", err)
	}
}
