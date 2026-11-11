// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/podsecurity"
)

const psaEnforce = "pod-security.kubernetes.io/enforce"

func isolationSpec(namespace, ws string) jobspec.JobSpec {
	return jobspec.JobSpec{ID: "uid-a", Name: "job-a", Namespace: namespace, Workspace: ws, WorkspaceUID: "uid-" + ws}
}

func getNamespace(t *testing.T, e *Executor, name string) corev1.Namespace {
	t.Helper()
	var ns corev1.Namespace
	if err := e.client.Get(context.Background(), client.ObjectKey{Name: name}, &ns); err != nil {
		t.Fatalf("get namespace %s: %v", name, err)
	}
	return ns
}

func hasIsolationPolicy(t *testing.T, e *Executor, namespace string) bool {
	t.Helper()
	var policy networkingv1.NetworkPolicy
	err := e.client.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: podsecurity.IsolationPolicyName}, &policy)
	return err == nil
}

// A spoke shared by several Workspaces isolates them as the hub does: the
// namespace the agent creates marks its Workspace, enforces Pod Security and
// carries the ingress policy. Without them one Workspace's pod reached
// another's Ray head. E2E: none yet (needs a spoke separate from the hub).
func TestEnsureNamespaceCreatesIsolatedWorkspaceNamespace(t *testing.T) {
	e := newTestExecutor(t)
	if err := e.ensureNamespace(context.Background(), isolationSpec("tenant-a", "team-a")); err != nil {
		t.Fatalf("ensureNamespace: %v", err)
	}
	ns := getNamespace(t, e, "tenant-a")
	if ns.Labels[v1.LabelWorkspace] != "team-a" {
		t.Errorf("workspace label = %q, want team-a", ns.Labels[v1.LabelWorkspace])
	}
	for key, want := range map[string]string{
		psaEnforce:                         podsecurity.Default,
		"pod-security.kubernetes.io/warn":  "restricted",
		"pod-security.kubernetes.io/audit": "restricted",
	} {
		if ns.Labels[key] != want {
			t.Errorf("label %s = %q, want %q", key, ns.Labels[key], want)
		}
	}
	if !hasIsolationPolicy(t, e, "tenant-a") {
		t.Error("the namespace has no isolation NetworkPolicy")
	}
}

// A namespace already marked for the Workspace (the hub's own, when the
// spoke is the hub) gets the PSA labels it lacks and the policy; a level it
// already enforces is its owner's choice and stays.
func TestEnsureNamespaceCompletesMarkedNamespace(t *testing.T) {
	e := newTestExecutor(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant-a", Labels: map[string]string{v1.LabelWorkspace: "team-a"}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant-p", Labels: map[string]string{v1.LabelWorkspace: "team-p", psaEnforce: "privileged"}}},
	)
	for _, spec := range []jobspec.JobSpec{isolationSpec("tenant-a", "team-a"), isolationSpec("tenant-p", "team-p")} {
		if err := e.ensureNamespace(context.Background(), spec); err != nil {
			t.Fatalf("ensureNamespace %s: %v", spec.Namespace, err)
		}
		if !hasIsolationPolicy(t, e, spec.Namespace) {
			t.Errorf("%s has no isolation NetworkPolicy", spec.Namespace)
		}
	}
	if got := getNamespace(t, e, "tenant-a").Labels[psaEnforce]; got != podsecurity.Default {
		t.Errorf("tenant-a enforce = %q, want %q added", got, podsecurity.Default)
	}
	if got := getNamespace(t, e, "tenant-p").Labels[psaEnforce]; got != "privileged" {
		t.Errorf("tenant-p enforce = %q, want the owner's privileged kept", got)
	}
}

// A namespace that marks no Workspace belongs to the cluster's admin. A
// Workspace's job is refused there: any Workspace could land in it, with no
// isolation between them. The admin labels it for
// the Workspace to let its jobs in. The agent labels nothing it does not own.
func TestEnsureNamespaceRefusesUnmarkedNamespaceForAWorkspace(t *testing.T) {
	e := newTestExecutor(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ml-shared"}})
	err := e.ensureNamespace(context.Background(), isolationSpec("ml-shared", "team-a"))
	if !errors.Is(err, backend.ErrAdmissionRejected) {
		t.Fatalf("err = %v, want admission rejected", err)
	}
	if !strings.Contains(err.Error(), v1.LabelWorkspace+"=team-a") {
		t.Errorf("refusal %q does not say how to admit the Workspace", err)
	}
	if labels := getNamespace(t, e, "ml-shared").Labels; len(labels) != 0 {
		t.Errorf("an admin's namespace was labelled %v", labels)
	}
	if hasIsolationPolicy(t, e, "ml-shared") {
		t.Error("an admin's namespace got a NetworkPolicy")
	}
}

// A namespace is one incarnation's of its Workspace: it carries the
// Workspace's UID, and a Workspace re-created under the name, or another
// hub's of the name, is refused there, with the old one's Secrets (N7). One
// an older agent marked by name only takes the UID of the first job of the
// name (the one-time backfill).
func TestANamespaceIsOneIncarnationsOfItsWorkspace(t *testing.T) {
	earlier := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant-a",
		Labels: map[string]string{v1.LabelWorkspace: "team-a", v1.LabelWorkspaceUID: "uid-earlier"}}}
	legacy := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant-l", Labels: map[string]string{v1.LabelWorkspace: "team-l"}}}
	e := newTestExecutor(t, earlier, legacy)

	err := e.ensureNamespace(context.Background(), isolationSpec("tenant-a", "team-a"))
	if !errors.Is(err, backend.ErrAdmissionRejected) || !strings.Contains(err.Error(), "uid-earlier") {
		t.Fatalf("a re-created Workspace in its predecessor's namespace: %v, want admission rejected", err)
	}
	if err := e.ensureNamespace(context.Background(), isolationSpec("tenant-new", "team-n")); err != nil {
		t.Fatal(err)
	}
	if got := getNamespace(t, e, "tenant-new").Labels[v1.LabelWorkspaceUID]; got != "uid-team-n" {
		t.Errorf("a created namespace marks UID %q, want uid-team-n", got)
	}
	if err := e.ensureNamespace(context.Background(), isolationSpec("tenant-l", "team-l")); err != nil {
		t.Fatalf("a namespace marked by name only: %v", err)
	}
	if got := getNamespace(t, e, "tenant-l").Labels[v1.LabelWorkspaceUID]; got != "uid-team-l" {
		t.Errorf("the backfill marked UID %q, want uid-team-l", got)
	}
}

// The cluster's namespaces, the default one and the agent's own are never a
// job's; nor is a namespace that marks another Workspace. Both refusals are
// permanent: a retry lands in the same place.
func TestEnsureNamespaceRefusesPlatformAndForeignNamespaces(t *testing.T) {
	e := newTestExecutor(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant-b", Labels: map[string]string{v1.LabelWorkspace: "team-b"}}})
	e.Namespace = "zenthros-system"
	for _, name := range []string{"default", "kube-system", "kube-public", "zenthros-system", "tenant-b"} {
		err := e.ensureNamespace(context.Background(), isolationSpec(name, "team-a"))
		if !errors.Is(err, backend.ErrAdmissionRejected) {
			t.Errorf("namespace %s: err = %v, want admission rejected", name, err)
		}
	}
	if hasIsolationPolicy(t, e, "tenant-b") {
		t.Error("a refused namespace got a NetworkPolicy")
	}
}

// Submits of different jobs run concurrently (F12), so a namespace can
// appear between this Submit's read and its create. The job it lost that
// race to may be another Workspace's: an AlreadyExists is checked like any
// existing namespace, never taken as this Workspace's own.
func TestEnsureNamespaceChecksNamespaceCreatedConcurrently(t *testing.T) {
	theirs := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant-x", Labels: map[string]string{v1.LabelWorkspace: "team-b"}}}
	e := newTestExecutor(t, theirs)
	missedOnce := false
	e.client = interceptor.NewClient(e.client.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Namespace); ok && !missedOnce {
				missedOnce = true // read before the other Submit's create landed
				return apierrors.NewNotFound(corev1.Resource("namespaces"), key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})

	err := e.ensureNamespace(context.Background(), isolationSpec("tenant-x", "team-a"))
	if !errors.Is(err, backend.ErrAdmissionRejected) {
		t.Fatalf("ensureNamespace into a namespace team-b created concurrently = %v, want admission rejected", err)
	}
	if !missedOnce {
		t.Fatal("the interleaving was not exercised")
	}
}
