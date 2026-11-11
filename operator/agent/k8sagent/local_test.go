// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
)

// usingLocal is an executor as the agent of no hub starts it.
func usingLocal(t *testing.T, objects ...client.Object) *Executor {
	t.Helper()
	e := newTestExecutor(t, objects...)
	e.UseLocal(&LocalCluster{Folder: "/home/me/.zenthros/k8s", UID: 1000, GID: 1001}, "zenthros-local")
	return e
}

func localJobSpec() jobspec.JobSpec {
	return jobspec.JobSpec{
		ID: "uid-local", Name: "suma", Namespace: "zenthros-local", Tenant: "local", Workspace: "local", WorkspaceUID: "local",
		Kind: v1.WorkloadKindJob, Image: "python:3.13-slim-bookworm", Command: []string{"python", "-c", "print(1)"},
	}
}

// A person's cluster: the chart gave the agent its namespace, so nothing else
// is created or bound, and the agent's own namespace is where its jobs go.
func TestLocalJobStaysInTheAgentsNamespace(t *testing.T) {
	e := usingLocal(t)
	if _, err := e.Submit(context.Background(), localJobSpec(), nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var ns corev1.Namespace
	if err := e.client.Get(context.Background(), client.ObjectKey{Name: "zenthros-local"}, &ns); !apierrors.IsNotFound(err) {
		t.Errorf("namespace: %v, want none made", err)
	}
	var bindings rbacv1.RoleBindingList
	if err := e.client.List(context.Background(), &bindings); err != nil {
		t.Fatal(err)
	}
	if len(bindings.Items) != 0 {
		t.Errorf("bound %d roles, want none", len(bindings.Items))
	}
}

// A job's pod sees the shared folder at the path the SDK wrote to, so a URI
// means the same on both sides, and what it writes there is that person's.
func TestLocalJobHasTheSharedFolderAtItsOwnPath(t *testing.T) {
	e := usingLocal(t)
	folder := e.Local.Folder
	if _, err := e.Submit(context.Background(), localJobSpec(), nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var job batchv1.Job
	if err := e.client.Get(context.Background(), client.ObjectKey{Namespace: "zenthros-local", Name: "suma"}, &job); err != nil {
		t.Fatal(err)
	}
	pod := job.Spec.Template.Spec
	var host string
	for _, volume := range pod.Volumes {
		if volume.HostPath != nil {
			host = volume.HostPath.Path
		}
	}
	if host != folder {
		t.Errorf("hostPath = %q, want %q", host, folder)
	}
	var mount string
	for _, m := range pod.Containers[0].VolumeMounts {
		if m.MountPath == folder && !m.ReadOnly {
			mount = m.MountPath
		}
	}
	if mount == "" {
		t.Errorf("mounts = %v, want %s writable", pod.Containers[0].VolumeMounts, folder)
	}
	context := pod.SecurityContext
	if context == nil || context.RunAsUser == nil || *context.RunAsUser != 1000 || context.RunAsGroup == nil || *context.RunAsGroup != 1001 {
		t.Errorf("security context = %+v, want 1000:1001", context)
	}
	var home string
	for _, env := range pod.Containers[0].Env {
		if env.Name == "HOME" {
			home = env.Value
		}
	}
	if home == "" {
		t.Error("no HOME: a uid the image does not know has none, and Python's caches need one")
	}
}

// What it may create it may create only here, so here is where it asks: asked
// about the whole cluster the answer is no, and it would declare no Job at all
// (found by the E2E: "backend local runs Manifest; not Job").
func TestLocalAgentAsksWhatItMayDoInItsNamespace(t *testing.T) {
	asked := []string{}
	clientset := kubefake.NewClientset()
	clientset.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
		asked = append(asked, review.Spec.ResourceAttributes.Namespace)
		review.Status.Allowed = true
		return true, review, nil
	})
	d := newDiscovery(clientset)
	if _, err := d.allowed(context.Background(), "batch", "jobs"); err != nil {
		t.Fatal(err)
	}
	e := usingLocal(t)
	d.namespace = e.discovery.namespace
	if _, err := d.allowed(context.Background(), "batch", "jobs"); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 2 || asked[0] != "" || asked[1] != "zenthros-local" {
		t.Errorf("asked in %q, want the cluster with a hub and its namespace with none", asked)
	}
}

// Its list of what is installed stays there too: anywhere else it is forbidden,
// and another's anyway.
func TestLocalAgentListsOnlyItsNamespace(t *testing.T) {
	job := func(namespace string) *batchv1.Job {
		return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: namespace, Labels: map[string]string{owner.LabelBackendID: "local", owner.LabelJobID: "id-" + namespace}}}
	}
	e := usingLocal(t, job("zenthros-local"), job("someone-else"))
	jobs, err := e.InstalledJobs(context.Background(), "local")
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].JobID != "id-zenthros-local" {
		t.Errorf("installed = %+v, want its namespace's job alone", jobs)
	}
}

// With a hub nothing of this applies: a job there keeps to what it is given.
func TestAHubsJobHasNoSharedFolder(t *testing.T) {
	e := newTestExecutor(t)
	e.Namespace = "zenthros-system"
	spec := localJobSpec()
	spec.Namespace = "tenant-default"
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var job batchv1.Job
	if err := e.client.Get(context.Background(), client.ObjectKey{Namespace: "tenant-default", Name: "suma"}, &job); err != nil {
		t.Fatal(err)
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.HostPath != nil {
			t.Errorf("a hub's job mounts %s", volume.HostPath.Path)
		}
	}
}
