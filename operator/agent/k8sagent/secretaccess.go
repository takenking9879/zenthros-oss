// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// bindingName is the RoleBinding that gives this agent Secrets in one job
// namespace, named after its role: <release>-secrets, zenthros-agent-secrets
// for the hub's own agent. Two agents in one cluster (the hub's and a
// cluster attached to a Workspace) each bind their own role there, and a
// binding's role cannot change: with one name for all, the second agent
// found the first's binding and waited for a grant it never had (lab,
// 2026-10-18).
func (s SecretAccess) bindingName() string {
	return strings.TrimSuffix(s.ClusterRole, "-job-secrets") + "-secrets"
}

// secretAccessWait bounds how long the agent waits for the API server's
// authorizer to see a binding it just made.
const secretAccessWait = 15 * time.Second

// SecretAccess is how the agent comes by Secrets in the namespaces it runs
// jobs in. The chart binds no Secret verb cluster-wide:
// ClusterRole holds them unbound, and the agent binds it to its own
// ServiceAccount in each job namespace before it touches a Secret there.
type SecretAccess struct {
	ClusterRole    string
	ServiceAccount string
	// Namespace is the ServiceAccount's.
	Namespace string
}

// secretAccessCache remembers the namespaces already bound, by the UID of
// the namespace: one deleted and made again under its name has no binding
// (its jobs were refused until a restart).
type secretAccessCache struct {
	mu    sync.Mutex
	bound map[string]types.UID
}

// ensureSecretAccess binds the agent's Secret role in namespace and waits
// until the API server honours it: the RoleBinding is there before the
// first Secret is.
func (e *Executor) ensureSecretAccess(ctx context.Context, namespace string) error {
	if namespace == "" || e.Local != nil {
		return nil
	}
	var ns corev1.Namespace
	if err := e.client.Get(ctx, client.ObjectKey{Name: namespace}, &ns); err != nil {
		return fmt.Errorf("namespace %s: %w", namespace, err)
	}
	e.secretAccess.mu.Lock()
	uid, done := e.secretAccess.bound[namespace]
	e.secretAccess.mu.Unlock()
	if done && uid == ns.UID {
		return nil
	}
	// Owned by its role: when the release goes (helm uninstall deletes the
	// role), the garbage collector takes the binding from every namespace.
	var role rbacv1.ClusterRole
	if err := e.client.Get(ctx, client.ObjectKey{Name: e.SecretAccess.ClusterRole}, &role); apierrors.IsNotFound(err) {
		// Not wrapped: a NotFound out of here says the namespace is gone.
		return fmt.Errorf("the agent's Secret role %s is missing: reinstall the agent's release", e.SecretAccess.ClusterRole)
	} else if err != nil {
		return fmt.Errorf("read the agent's Secret role %s: %w", e.SecretAccess.ClusterRole, err)
	}
	me := rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Name: e.SecretAccess.ServiceAccount, Namespace: e.SecretAccess.Namespace}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: e.SecretAccess.bindingName(),
			Labels: map[string]string{managedByLabel: "zenthros-agent"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "ClusterRole",
				Name: role.Name, UID: role.UID}}},
		Subjects: []rbacv1.Subject{me},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
	}
	if err := e.client.Create(ctx, binding); apierrors.IsAlreadyExists(err) {
		// One there already: this agent's from an earlier run, or not this agent's
		// at all, which no wait makes honoured.
		var found rbacv1.RoleBinding
		if err := e.client.Get(ctx, client.ObjectKeyFromObject(binding), &found); err != nil {
			return fmt.Errorf("read RoleBinding %s/%s: %w", namespace, binding.Name, err)
		}
		if found.RoleRef != binding.RoleRef || !slices.Contains(found.Subjects, me) {
			return fmt.Errorf("RoleBinding %s/%s binds %s to %v, not this agent's Secret role to it: remove it, or give this agent's release another name",
				namespace, binding.Name, found.RoleRef.Name, found.Subjects)
		}
	} else if err != nil {
		return fmt.Errorf("bind the agent's Secret role in namespace %s: %w", namespace, err)
	}
	deadline := time.Now().Add(secretAccessWait)
	for {
		allowed, err := e.secretsAllowed(ctx, namespace)
		if err == nil && allowed {
			break
		}
		if time.Now().After(deadline) {
			return notHonoured(namespace, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	e.secretAccess.mu.Lock()
	if e.secretAccess.bound == nil {
		e.secretAccess.bound = map[string]types.UID{}
	}
	e.secretAccess.bound[namespace] = ns.UID
	e.secretAccess.mu.Unlock()
	return nil
}

// rebindingOnForbidden runs op, whose Secret access in namespace
// ensureSecretAccess made. A Forbidden means the binding went from a live
// namespace (its ClusterRole made again, and the garbage collector took the
// bindings the old one owned): the namespace is bound again and op runs
// once more. Remembered as bound, every Submit and Cancel there was refused
// until a restart (06#6).
func (e *Executor) rebindingOnForbidden(ctx context.Context, namespace string, op func() error) error {
	err := op()
	if !apierrors.IsForbidden(err) {
		return err
	}
	e.secretAccess.mu.Lock()
	delete(e.secretAccess.bound, namespace)
	e.secretAccess.mu.Unlock()
	if err := e.ensureSecretAccess(ctx, namespace); err != nil {
		return err
	}
	return op()
}

// secretsAllowedByReview asks the API server whether the agent may create
// Secrets in namespace.
func (e *Executor) secretsAllowedByReview(ctx context.Context, namespace string) (bool, error) {
	review, err := e.clientset.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{
			Namespace: namespace, Resource: "secrets", Verb: "create",
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return review.Status.Allowed, nil
}

// notHonoured is the error of a Secret role the API server did not honour
// within secretAccessWait: err is the last check's, nil when it answered
// "not allowed". It reaches the job's status.
func notHonoured(namespace string, err error) error {
	if err == nil {
		return fmt.Errorf("the agent's Secret role in namespace %s is not honoured after %s", namespace, secretAccessWait)
	}
	return fmt.Errorf("the agent's Secret role in namespace %s is not honoured after %s: %w", namespace, secretAccessWait, err)
}
