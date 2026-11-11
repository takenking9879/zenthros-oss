// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/metrics"
)

// Manifest: any namespaced object the user wrote. The operator that owns the
// kind runs it; the agent applies it, reads its status by the job's contract
// and relays every refusal (RBAC, an operator's webhook, a validating
// admission policy, an unknown kind) exactly as the API server returned it.

const (
	manifestHandlePrefix = "manifest:"
	// manifestFieldOwner is the server-side apply field manager. The apply is
	// never forced: a field another manager owns is a conflict the user reads.
	manifestFieldOwner = "zenthros-agent"
	// ManifestSpecHashAnnotation is the hash of the object as the job wrote
	// it; a resubmit with a different object is a spec mismatch, not an
	// overwrite.
	ManifestSpecHashAnnotation = "zenthros.ai/spec-hash"
	// ManifestContractAnnotation keeps the job's status contract on the object
	// itself, so status and logs need nothing but the handle.
	ManifestContractAnnotation = "zenthros.ai/manifest-contract"
)

// manifestDenied are kinds a Manifest never creates, whatever RBAC allows:
// they would grant the workload, or anyone in the namespace, more than the
// job asked for.
var manifestDenied = map[schema.GroupKind]bool{
	{Group: "rbac.authorization.k8s.io", Kind: "Role"}:               true,
	{Group: "rbac.authorization.k8s.io", Kind: "RoleBinding"}:        true,
	{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole"}:        true,
	{Group: "rbac.authorization.k8s.io", Kind: "ClusterRoleBinding"}: true,
	{Group: "", Kind: "ServiceAccount"}:                              true,
}

// manifestContract is how the object's status reads as a Zenthros phase.
type manifestContract struct {
	SuccessWhen []v1.ConditionMatch   `json:"successWhen,omitempty"`
	FailureWhen []v1.ConditionMatch   `json:"failureWhen,omitempty"`
	RunningWhen []v1.ConditionMatch   `json:"runningWhen,omitempty"`
	PhasePath   *v1.PhasePath         `json:"phasePath,omitempty"`
	LogSelector *metav1.LabelSelector `json:"logSelector,omitempty"`
	// EndpointFrom is where a Service object publishes its URLs.
	EndpointFrom []v1.EndpointSource `json:"endpointFrom,omitempty"`
}

func contractOf(m *v1.ManifestSpec) manifestContract {
	return manifestContract{
		SuccessWhen: m.SuccessWhen, FailureWhen: m.FailureWhen, RunningWhen: m.RunningWhen,
		PhasePath: m.PhasePath, LogSelector: m.LogSelector, EndpointFrom: m.EndpointFrom,
	}
}

func invalidManifest(format string, args ...any) error {
	return backend.Classified(backend.ErrInvalid, fmt.Errorf(format, args...))
}

// buildManifest turns the job's manifest into the object the agent applies:
// named after the job, in the job's namespace, labelled like every Zenthros
// workload and annotated with its spec hash and status contract. It refuses
// what a Manifest never runs, naming both sides of every mismatch.
func buildManifest(spec jobspec.JobSpec) (*unstructured.Unstructured, error) {
	if spec.Manifest == nil || len(spec.Manifest.Object.Raw) == 0 {
		return nil, invalidManifest("Manifest requires manifest.object")
	}
	obj := &unstructured.Unstructured{}
	// utiljson keeps integers int64, as the API server's own decoder does.
	if err := utiljson.Unmarshal(spec.Manifest.Object.Raw, &obj.Object); err != nil {
		return nil, invalidManifest("manifest.object is not a Kubernetes object: %v", err)
	}
	gvk := obj.GroupVersionKind()
	if gvk.Kind == "" || gvk.Version == "" {
		return nil, invalidManifest("manifest.object needs apiVersion and kind (got apiVersion %q, kind %q)", obj.GetAPIVersion(), obj.GetKind())
	}
	if manifestDenied[gvk.GroupKind()] {
		return nil, invalidManifest("a Manifest cannot create %s: RBAC objects and ServiceAccounts are never applied for a job", gvk.GroupKind())
	}
	if serviceAccountToken(obj) {
		return nil, invalidManifest("a Manifest cannot create a ServiceAccount token Secret: it would hand the job another account's identity")
	}
	if generate := obj.GetGenerateName(); generate != "" {
		return nil, invalidManifest("manifest.object.metadata.generateName %q is not supported: Zenthros names the object %q", generate, spec.Name)
	}
	if ns := obj.GetNamespace(); ns != "" && ns != spec.Namespace {
		return nil, invalidManifest("manifest.object.metadata.namespace %q is not the job's namespace %q", ns, spec.Namespace)
	}
	obj.SetName(spec.Name)
	obj.SetNamespace(spec.Namespace)
	unstructured.RemoveNestedField(obj.Object, "status")

	hash, err := manifestHash(obj)
	if err != nil {
		return nil, invalidManifest("manifest.object: %v", err)
	}
	contract, err := json.Marshal(contractOf(spec.Manifest))
	if err != nil {
		return nil, invalidManifest("manifest status contract: %v", err)
	}
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels["zenthros.ai/job-id"] = spec.ID
	labels["zenthros.ai/tenant"] = spec.Tenant
	if spec.Workspace != "" {
		labels[v1.LabelWorkspace] = spec.Workspace
	}
	obj.SetLabels(labels)
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[ManifestSpecHashAnnotation] = hash
	annotations[ManifestContractAnnotation] = string(contract)
	obj.SetAnnotations(annotations)
	return obj, nil
}

// manifestHash is a hash of the object as the job wrote it, after the name
// and namespace are forced. encoding/json sorts map keys, so the same object
// always hashes the same.
func manifestHash(obj *unstructured.Unstructured) (string, error) {
	raw, err := json.Marshal(obj.Object)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8]), nil
}

// manifestHandle is manifest:<group>/<version>/<kind>/<namespace>/<name>; the
// core group is empty.
func manifestHandle(gvk schema.GroupVersionKind, namespace, name string) string {
	return manifestHandlePrefix + strings.Join([]string{gvk.Group, gvk.Version, gvk.Kind, namespace, name}, "/")
}

func parseManifestRef(rest string) (ref, error) {
	parts := strings.Split(rest, "/")
	if len(parts) != 5 || parts[1] == "" || parts[2] == "" || parts[3] == "" || parts[4] == "" {
		return ref{}, fmt.Errorf("invalid handle %q (want %s<group>/<version>/<kind>/<namespace>/<name>)", manifestHandlePrefix+rest, manifestHandlePrefix)
	}
	return ref{
		gvk:       schema.GroupVersionKind{Group: parts[0], Version: parts[1], Kind: parts[2]},
		namespace: parts[3],
		name:      parts[4],
	}, nil
}

func objectFor(r ref) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(r.gvk)
	u.SetNamespace(r.namespace)
	u.SetName(r.name)
	return u
}

func (r ref) String() string {
	return r.gvk.Kind + " " + r.namespace + "/" + r.name
}

func (e *Executor) submitManifest(ctx context.Context, spec jobspec.JobSpec) (string, error) {
	desired, err := buildManifest(spec)
	if err != nil {
		return "", err
	}
	gvk := desired.GroupVersionKind()
	mapping, err := e.client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		// NoMatch: the cluster does not serve the kind (no CRD, no operator).
		return "", err
	}
	if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
		return "", invalidManifest("%s is cluster-scoped: a Manifest runs namespaced objects in the job's namespace %q", gvk.GroupKind(), spec.Namespace)
	}
	if err := e.refuseUserAccounts(ctx, desired); err != nil {
		return "", err
	}
	r := ref{gvk: gvk, namespace: desired.GetNamespace(), name: desired.GetName()}
	existing := objectFor(r)
	err = e.client.Get(ctx, client.ObjectKeyFromObject(existing), existing)
	switch {
	case apierrors.IsNotFound(err):
		if err := e.client.Apply(ctx, client.ApplyConfigurationFromUnstructured(desired), client.FieldOwner(manifestFieldOwner)); err != nil {
			return "", fmt.Errorf("apply %s: %w", r, err)
		}
		e.logger.Info("applied Manifest", "kind", gvk.String(), "namespace", r.namespace, "name", r.name)
	case err != nil:
		return "", fmt.Errorf("get %s: %w", r, err)
	case existing.GetLabels()[k8spod.LabelJobID] != "" && existing.GetLabels()[k8spod.LabelJobID] != desired.GetLabels()[k8spod.LabelJobID]:
		// An earlier job of this name applied it (N11): adopting it would
		// run that job's object as this one.
		return "", fmt.Errorf("%w: %s belongs to job %q, not this one (%q); delete it or give the job another name",
			backend.ErrSpecMismatch, r, existing.GetLabels()[k8spod.LabelJobID], desired.GetLabels()[k8spod.LabelJobID])
	default:
		want := desired.GetAnnotations()[ManifestSpecHashAnnotation]
		if have := existing.GetAnnotations()[ManifestSpecHashAnnotation]; have != want {
			if have == "" {
				have = "none: the object was not applied by Zenthros"
			}
			return "", backend.NewSpecMismatchError("manifest.object", want, have)
		}
		e.logger.Info("Manifest already applied, reconciled idempotently", "kind", gvk.String(), "namespace", r.namespace, "name", r.name)
	}
	return manifestHandle(gvk, r.namespace, r.name), nil
}

// serviceAccountToken reports a Secret that asks Kubernetes for an
// account's token.
func serviceAccountToken(obj *unstructured.Unstructured) bool {
	if obj.GroupVersionKind().GroupKind() != (schema.GroupKind{Kind: "Secret"}) {
		return false
	}
	kind, _, _ := unstructured.NestedString(obj.Object, "type")
	_, named := obj.GetAnnotations()[corev1.ServiceAccountNameKey]
	return kind == string(corev1.SecretTypeServiceAccountToken) || named
}

// manifestServiceAccounts are the accounts the object's pods would run as:
// every serviceAccountName (pod specs) and serviceAccount (the deprecated pod
// field, and an operator's own, as Spark's driver) anywhere in it.
func manifestServiceAccounts(value any, found map[string]bool) {
	switch v := value.(type) {
	case map[string]any:
		for key, field := range v {
			if name, ok := field.(string); ok && (key == "serviceAccountName" || key == "serviceAccount") {
				found[name] = true
				continue
			}
			manifestServiceAccounts(field, found)
		}
	case []any:
		for _, item := range v {
			manifestServiceAccounts(item, found)
		}
	}
}

// refuseUserAccounts refuses a Manifest whose pods run as a Workspace user's
// ServiceAccount. The agent applies the object with its own rights, and a
// Workspace editor could otherwise run a pod as a user who provisions
// machines and read that user's token from its own logs.
// An account the agent cannot read is refused too: it cannot tell.
func (e *Executor) refuseUserAccounts(ctx context.Context, obj *unstructured.Unstructured) error {
	found := map[string]bool{}
	manifestServiceAccounts(obj.Object, found)
	for name := range found {
		if name == "" || name == "default" {
			continue
		}
		account := &corev1.ServiceAccount{}
		err := e.client.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: name}, account)
		switch {
		case apierrors.IsNotFound(err):
			// No such account: Kubernetes refuses the pods themselves.
			continue
		case err != nil:
			return invalidManifest("manifest.object runs as ServiceAccount %q, which the agent cannot read to check (%v)", name, err)
		case account.Labels[v1.LabelUser] != "":
			return invalidManifest("manifest.object runs as ServiceAccount %q, a Workspace user's identity: a job never runs as a person", name)
		}
	}
	return nil
}

func (e *Executor) getManifest(ctx context.Context, r ref) (*unstructured.Unstructured, error) {
	obj := objectFor(r)
	started := time.Now()
	err := e.client.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	metrics.RecordK8sAPI(err, time.Since(started))
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", r, err)
	}
	return obj, nil
}

// readContract is the status contract the object was applied with. An
// annotation that does not parse (someone edited it) reads as the defaults.
func readContract(obj *unstructured.Unstructured) manifestContract {
	var c manifestContract
	if raw := obj.GetAnnotations()[ManifestContractAnnotation]; raw != "" {
		_ = json.Unmarshal([]byte(raw), &c)
	}
	return c
}
