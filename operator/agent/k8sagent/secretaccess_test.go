// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1 "github.com/zenthros/operator/api/v1"
)

// rbacGatedExecutor is an executor whose API server refuses every Secret
// operation in a namespace until the agent's RoleBinding is there, as the
// real one does once the agent holds no cluster-wide Secret grant.
// It records what is created, in order.
func rbacGatedExecutor(t *testing.T, objects ...client.Object) (*Executor, *[]string) {
	t.Helper()
	base := newTestExecutor(t, objects...)
	var created []string
	bound := func(ctx context.Context, c client.WithWatch, namespace string) bool {
		var binding rbacv1.RoleBinding
		return c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: base.SecretAccess.bindingName()}, &binding) == nil
	}
	forbidden := func(namespace string) error {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "", nil)
	}
	gated := interceptor.NewClient(base.client.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			switch obj.(type) {
			case *corev1.Secret:
				if !bound(ctx, c, obj.GetNamespace()) {
					return forbidden(obj.GetNamespace())
				}
				created = append(created, "Secret")
			case *rbacv1.RoleBinding:
				created = append(created, "RoleBinding")
			}
			return c.Create(ctx, obj, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Secret); ok && !bound(ctx, c, key.Namespace) {
				return forbidden(key.Namespace)
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Secret); ok && !bound(ctx, c, obj.GetNamespace()) {
				return forbidden(obj.GetNamespace())
			}
			return c.Delete(ctx, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.SecretList); ok {
				var o client.ListOptions
				o.ApplyOptions(opts)
				if !bound(ctx, c, o.Namespace) {
					return forbidden(o.Namespace)
				}
			}
			return c.List(ctx, list, opts...)
		},
	})
	base.client = gated
	return base, &created
}

// The agent binds its Secret role in a job's namespace before it writes the
// job's first Secret there: RoleBinding, then Secret.
func TestAgentBindsSecretRoleBeforeTheSecret(t *testing.T) {
	e, created := rbacGatedExecutor(t)
	spec := inlineJobSpec()
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(*created) < 2 || (*created)[0] != "RoleBinding" || (*created)[1] != "Secret" {
		t.Fatalf("created %v, want the RoleBinding before the Secret", *created)
	}
	var binding rbacv1.RoleBinding
	if err := e.client.Get(context.Background(), client.ObjectKey{Namespace: spec.Namespace, Name: e.SecretAccess.bindingName()}, &binding); err != nil {
		t.Fatal(err)
	}
	if binding.RoleRef.Kind != "ClusterRole" || binding.RoleRef.Name != "zenthros-agent-job-secrets" ||
		len(binding.Subjects) != 1 || binding.Subjects[0].Name != "zenthros-agent" || binding.Subjects[0].Namespace != "zenthros-system" {
		t.Fatalf("binding = %+v", binding)
	}
}

// A referenced Secret is read after the binding exists, so a job in a new
// namespace can name one.
func TestReferencedSecretIsCheckedAfterTheBinding(t *testing.T) {
	ns := &corev1.Namespace{}
	ns.Name, ns.Labels = "tenant-default", map[string]string{v1.LabelWorkspace: "default"}
	ref := &corev1.Secret{Data: map[string][]byte{"token": []byte("x")}}
	ref.Namespace, ref.Name = "tenant-default", "creds"
	e, _ := rbacGatedExecutor(t, ns, ref)
	spec := inlineJobSpec()
	spec.SecretSources = []v1.SecretSourceRef{{Type: v1.SecretSourceTypeKubernetes, Namespace: "tenant-default", Name: "creds", Key: "token",
		Materialization: v1.SecretMaterialization{Mode: "env", EnvName: "TOKEN"}}}
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit with a referenced Secret in its namespace: %v", err)
	}
}

// A job whose namespace lost its binding: its Cancel binds first, then
// removes its Secrets.
func TestCancelBindsBeforeRemovingSecrets(t *testing.T) {
	e, _ := rbacGatedExecutor(t)
	spec := inlineJobSpec()
	handle, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	var binding rbacv1.RoleBinding
	if err := e.client.Get(context.Background(), client.ObjectKey{Namespace: spec.Namespace, Name: e.SecretAccess.bindingName()}, &binding); err != nil {
		t.Fatal(err)
	}
	if err := e.client.Delete(context.Background(), &binding); err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel in a namespace without the binding: %v", err)
	}
}

// A namespace deleted and made again under its name has no binding: the
// agent bound it again only after a restart, and refused its jobs until
// then.
func TestARecreatedNamespaceIsBoundAgain(t *testing.T) {
	e, _ := rbacGatedExecutor(t)
	ctx := context.Background()
	spec := inlineJobSpec()
	if _, err := e.Submit(ctx, spec, nil); err != nil {
		t.Fatal(err)
	}
	var ns corev1.Namespace
	if err := e.client.Get(ctx, client.ObjectKey{Name: spec.Namespace}, &ns); err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: spec.Namespace, Name: e.SecretAccess.bindingName()}}
	if err := e.client.Delete(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := e.client.Delete(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	again := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: spec.Namespace, UID: "made-again", Labels: ns.Labels}}
	if err := e.client.Create(ctx, again); err != nil {
		t.Fatal(err)
	}
	spec.ID, spec.Name = "uid-inline-2", "inline-test-2"
	if _, err := e.Submit(ctx, spec, nil); err != nil {
		t.Fatalf("Submit in the namespace made again: %v", err)
	}
}

// Two agents in one cluster, the hub's own and a cluster attached to a
// Workspace (a release of its own), each bind their own Secret role in a
// job namespace: the hub's keeps zenthros-agent-secrets. With one name for
// both, the attached agent found the hub's binding and waited 15 s for a
// grant it never had (lab, 2026-10-18). A binding of that name that grants
// another agent is said at once, not waited on.
func TestTwoAgentsInOneClusterBindTheirOwnSecretRole(t *testing.T) {
	ctx := context.Background()
	hub, _ := rbacGatedExecutor(t)
	spec := inlineJobSpec()
	if _, err := hub.Submit(ctx, spec, nil); err != nil {
		t.Fatal(err)
	}
	if got := hub.SecretAccess.bindingName(); got != "zenthros-agent-secrets" {
		t.Fatalf("the hub's agent binds %s, want zenthros-agent-secrets as before", got)
	}

	attached := SecretAccess{ClusterRole: "zenthros-k8s-1a2b3c4d-job-secrets", ServiceAccount: "zenthros-agent", Namespace: "zenthros-k8s-1a2b3c4d"}
	if attached.bindingName() == hub.SecretAccess.bindingName() {
		t.Fatalf("both agents bind %s", attached.bindingName())
	}
	// Its role, which helm uninstall deletes: the binding's owner, so the
	// garbage collector takes it from every namespace (lab, 2026-10-18: it
	// outlived the release).
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: attached.ClusterRole, UID: "role-uid"}}
	if err := hub.client.Create(ctx, role); err != nil {
		t.Fatal(err)
	}
	hub.SecretAccess = attached
	hub.secretAccess.bound = nil // another agent: it has bound nothing
	if err := hub.ensureSecretAccess(ctx, spec.Namespace); err != nil {
		t.Fatalf("the attached agent beside the hub's: %v", err)
	}
	var binding rbacv1.RoleBinding
	if err := hub.client.Get(ctx, client.ObjectKey{Namespace: spec.Namespace, Name: "zenthros-k8s-1a2b3c4d-secrets"}, &binding); err != nil {
		t.Fatal(err)
	}
	if binding.RoleRef.Name != attached.ClusterRole || binding.Subjects[0].Namespace != attached.Namespace {
		t.Fatalf("the attached agent's binding = %+v", binding)
	}
	if owners := binding.OwnerReferences; len(owners) != 1 || owners[0].Kind != "ClusterRole" || owners[0].Name != attached.ClusterRole ||
		owners[0].UID != "role-uid" {
		t.Fatalf("the binding's owners = %+v, want its role, which goes with the release", owners)
	}

	// A binding of its name that grants another agent: said, at once.
	hub.SecretAccess = SecretAccess{ClusterRole: "zenthros-agent-job-secrets", ServiceAccount: "zenthros-agent", Namespace: "elsewhere"}
	hub.secretAccess.bound = nil // another agent: it has bound nothing
	err := hub.ensureSecretAccess(ctx, spec.Namespace)
	if err == nil || !strings.Contains(err.Error(), "not this agent's Secret role") {
		t.Fatalf("an agent whose binding name is taken by another: %v", err)
	}
}

// A binding taken from a live namespace (its ClusterRole made again, and the
// garbage collector took the bindings the old one owned) stayed bound in the
// agent's memory: every Submit and Cancel there was Forbidden until a
// restart (06#6). A Forbidden binds again, once.
func TestABindingTakenFromALiveNamespaceIsMadeAgain(t *testing.T) {
	e, _ := rbacGatedExecutor(t)
	ctx := context.Background()
	spec := inlineJobSpec()
	handle, err := e.Submit(ctx, spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	unbind := func() {
		binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: spec.Namespace, Name: e.SecretAccess.bindingName()}}
		if err := e.client.Delete(ctx, binding); err != nil {
			t.Fatal(err)
		}
	}
	unbind()
	second := inlineJobSpec()
	second.ID, second.Name = "uid-inline-2", "inline-test-2"
	if _, err := e.Submit(ctx, second, nil); err != nil {
		t.Fatalf("Submit after the binding went: %v", err)
	}
	unbind()
	if err := e.Cancel(ctx, handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel after the binding went: %v", err)
	}
}

// The chart always makes the agent's Secret role. Without it the agent
// binds nothing and says so; a Cancel then fails rather than reading the
// missing role as a namespace gone, which would leave the job's Secrets.
func TestAMissingSecretRoleIsRefusedNotTakenForAGoneNamespace(t *testing.T) {
	e, _ := rbacGatedExecutor(t)
	ctx := context.Background()
	spec := inlineJobSpec()
	handle, err := e.Submit(ctx, spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.client.Delete(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: testSecretRole}}); err != nil {
		t.Fatal(err)
	}
	e.secretAccess.bound = nil
	err = e.Cancel(ctx, handle, jobOf(spec))
	if err == nil || !strings.Contains(err.Error(), "Secret role "+testSecretRole+" is missing") {
		t.Fatalf("Cancel without the Secret role: %v", err)
	}
	second := inlineJobSpec()
	second.ID, second.Name = "uid-inline-2", "inline-test-2"
	if _, err := e.Submit(ctx, second, nil); err == nil || !strings.Contains(err.Error(), "is missing") {
		t.Fatalf("Submit without the Secret role: %v", err)
	}
}
