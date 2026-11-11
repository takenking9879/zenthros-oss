// SPDX-License-Identifier: Apache-2.0

// Package k8sagent is the agent's Kubernetes executor: it runs jobs as native
// resources (KubeRay RayJobs) in the agent's own cluster. It is the in-cluster
// counterpart to the systemd executor — the same control-channel handlers drive both;
// the agent simply manages its local cluster like a kubelet (architecture §3).
package k8sagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/agent/agepayload"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8s"
	"github.com/zenthros/operator/pkg/backend/k8serr"
	"github.com/zenthros/operator/pkg/backend/k8spod"
	"github.com/zenthros/operator/pkg/capability"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/metrics"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/podsecurity"
	"github.com/zenthros/operator/pkg/secretsource"
)

// Executor runs jobs as RayJobs in the agent's local cluster via a controller-runtime
// client. It is stateless: live state lives in the RayJobs it manages.
type Executor struct {
	client    client.Client
	clientset kubernetes.Interface
	logger    logr.Logger
	Decryptor agepayload.Bundle
	// rayLogs fetches driver logs from the Ray Jobs API. Tests inject a fake.
	rayLogs    func(ctx context.Context, dashboardURL, jobID string) (string, error)
	httpClient *http.Client
	discovery  *discovery
	// Namespace is the agent's own: Prewarm DaemonSets and Jobs run there.
	Namespace string
	// PodSecurity is the PSA level enforced on the Workspace namespaces the
	// agent creates; empty means podsecurity.Default.
	PodSecurity string
	// SecretAccess binds the agent's Secret role in each job namespace.
	SecretAccess SecretAccess
	// Local is set on the agent of no hub, on a person's own cluster.
	Local *LocalCluster
	// StatusCache, when set, holds the objects and pods the agent made
	// (StatusCacheLabel): status reads come from it (statusReads).
	StatusCache client.Reader
	// events keeps a waiting job's event reads for eventCacheTTL; used with
	// StatusCache.
	events eventCache

	secretAccess   secretAccessCache
	secretsAllowed func(ctx context.Context, namespace string) (bool, error)

	restCfg *rest.Config

	warmMu sync.Mutex
	warm   map[string]bool
}

// New builds a Kubernetes executor backed by the given client (whose scheme must know
// the RayJob CRD). The rest config is used to build a clientset for pod log streaming.
func New(c client.Client, restCfg *rest.Config, logger logr.Logger) (*Executor, error) {
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset for log streaming: %w", err)
	}
	e := &Executor{
		client: c, clientset: clientset, restCfg: restCfg,
		logger:     logger.WithName("k8s-agent"),
		httpClient: &http.Client{Timeout: 10 * time.Second},
		discovery:  newDiscovery(clientset),
	}
	e.secretsAllowed = e.secretsAllowedByReview
	return e, nil
}

// Client returns the destination Kubernetes client used by this executor.
func (e *Executor) Client() client.Client {
	return e.client
}

// Submit creates the job's RayJob (or reconciles it idempotently) and returns a
// "namespace/name" handle. No ownerReference is set: the ZenthrosJob lives in the
// controller's cluster, so cleanup here is driven by Cancel.
// Submit creates the workload and relays the destination's error verbatim,
// classified for retry (k8serr).
func (e *Executor) Submit(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (string, error) {
	h, err := e.submit(ctx, spec, env)
	return h, k8serr.Classify(err)
}

func (e *Executor) submit(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (string, error) {
	if err := jobspec.ValidateWorkload(spec); err != nil {
		return "", fmt.Errorf("%w: %w", backend.ErrAdmissionRejected, err)
	}
	t, ok := translatorForKind(spec.Kind)
	if !ok {
		return "", fmt.Errorf("%w: %q", backend.ErrUnsupportedKind, spec.Kind)
	}
	spec.Env = env
	if e.Local == nil {
		if err := e.ensureNamespace(ctx, spec); err != nil {
			return "", err
		}
	}
	// The agent's Secret role is bound in the namespace before any Secret
	// there is read or written.
	if err := e.ensureSecretAccess(ctx, spec.Namespace); err != nil {
		return "", err
	}
	var handle string
	err := e.rebindingOnForbidden(ctx, spec.Namespace, func() error {
		if err := e.validateSecretSources(ctx, spec); err != nil {
			return err
		}
		var err error
		handle, err = e.create(ctx, t, spec)
		return err
	})
	if err != nil && nothingCreated(err) {
		// A failed submit leaves no code or decrypted secret behind: the
		// controller cancels only a job that has a handle (06#5). It runs
		// with its own context, which the request's end does not cancel
		// (c4-E5).
		cleanup, cancel := fault.Cleanup(ctx)
		defer cancel()
		if err := e.deleteOwnSecrets(cleanup, spec.Namespace, owner.Ref{Kind: "ZenthrosJob", JobID: spec.ID}); err != nil {
			e.logger.Error(err, "delete the Secrets of a failed submit", "job_id", spec.ID)
		}
	}
	return handle, err
}

// create writes what the job runs from: its Secrets, then its workload.
func (e *Executor) create(ctx context.Context, t translator, spec jobspec.JobSpec) (string, error) {
	spec, err := e.projectEncryptedPayloads(ctx, spec)
	if err != nil {
		return "", err
	}
	if spec.Source.Inline != "" {
		if err := k8s.EnsureInlineSecret(ctx, e.client, spec); err != nil {
			return "", fmt.Errorf("create inline source secret for %s: %w", spec.Name, err)
		}
	}
	return t.submit(e, ctx, spec)
}

// nothingCreated is whether a failed submit's outcome is known to have left
// no workload that runs from its Secrets. An object of the name (its own,
// whose spec changed, or another job's) or an outcome nobody knows (a
// timeout, a cancelled request) may be one, and its Secrets stay for
// Cancel, which selects them by the job's ID.
// The cluster's own error is read, before k8serr turns a timeout into an
// unavailable destination.
func nothingCreated(err error) bool {
	if errors.Is(err, backend.ErrSpecMismatch) {
		return false
	}
	switch fault.Of(err) {
	case fault.Refused, fault.Invalid, fault.Unauthorized, fault.Transient:
		return true
	}
	return false
}

const rayClusterHandlePrefix = "raycluster:"

func (e *Executor) submitRayCluster(ctx context.Context, spec jobspec.JobSpec) (string, error) {
	desired := k8s.BuildRayCluster(spec)
	var existing rayv1.RayCluster
	created, err := k8s.EnsureObject(ctx, e.client, "RayCluster", desired, &existing, func() error {
		if mismatch := k8s.RayClusterSpecMismatch(&existing, desired); mismatch != nil {
			return mismatch
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	e.logSubmitted("RayCluster", desired, created)
	return rayClusterHandle(desired.Namespace, desired.Name), nil
}

func rayClusterHandle(namespace, name string) string {
	return rayClusterHandlePrefix + namespace + "/" + name
}

func rayClusterState(cluster *rayv1.RayCluster) backend.JobState {
	switch cluster.Status.State {
	case rayv1.Ready:
		return backend.JobState{Phase: v1.PhaseRunning, Message: "RayCluster ready"}
	case rayv1.Failed:
		return backend.JobState{Phase: v1.PhaseFailed, Message: withKubeRayDetail("RayCluster failed", cluster)}
	default:
		return backend.JobState{Phase: v1.PhasePending, Message: withKubeRayDetail("RayCluster admitted", cluster)}
	}
}

// withKubeRayDetail appends what KubeRay itself reported about the cluster:
// status.reason and every condition that is not True, word for word.
func withKubeRayDetail(summary string, cluster *rayv1.RayCluster) string {
	parts := []string{summary}
	if cluster.Status.Reason != "" {
		parts = append(parts, "reason: "+cluster.Status.Reason)
	}
	for _, condition := range cluster.Status.Conditions {
		if condition.Status == metav1.ConditionTrue || condition.Message == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s %s: %s", condition.Type, condition.Status, condition.Reason, condition.Message))
	}
	return strings.Join(parts, "; ")
}

func (e *Executor) validateSecretSources(ctx context.Context, spec jobspec.JobSpec) error {
	if err := secretsource.Validate(spec.SecretSources); err != nil {
		return backend.NewSecretSourceError(v1.ReasonInvalidSecretSource, err.Error())
	}
	if err := secretsource.Negotiate(spec.SecretSources, backend.TypeK8s, "", capability.SecretStoresKubernetes()); err != nil {
		return err
	}
	for _, ref := range spec.SecretSources {
		if ref.Type != v1.SecretSourceTypeKubernetes {
			continue
		}
		if ref.Namespace != spec.Namespace {
			return backend.NewSecretSourceError(
				v1.ReasonSecretNamespaceForbidden,
				fmt.Sprintf("referenced namespace %q is outside workload namespace %q", ref.Namespace, spec.Namespace),
			)
		}
		var secret corev1.Secret
		err := e.client.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &secret)
		switch {
		case apierrors.IsNotFound(err):
			return backend.NewSecretSourceError(v1.ReasonSecretNotFound, "referenced Kubernetes Secret was not found: "+err.Error())
		case apierrors.IsForbidden(err):
			return backend.NewSecretSourceError(v1.ReasonSecretAccessDenied, "agent is not authorized to read the referenced Kubernetes Secret: "+err.Error())
		case err != nil:
			// Throttled, timed out, the server failing: nothing is known of
			// the Secret, and the submit is retried (c4-A1).
			return fmt.Errorf("read the referenced Kubernetes Secret: %w", k8serr.Classify(err))
		}
		if _, ok := secret.Data[ref.Key]; !ok {
			return backend.NewSecretSourceError(v1.ReasonSecretKeyNotFound, "referenced key was not found in the Kubernetes Secret")
		}
	}
	return nil
}

// Cancel deletes the job's native resource and the Secrets the job owns.
// Idempotent: a missing resource or Secret is not an error. Ownership is the
// job's ID, never the name (K1/06#1): an object of the name that another job
// made is left, and Secrets are those labelled with the job's ID, so a job
// named a prefix of another's no longer deletes that one's. Pods and a
// RayJob's cluster follow through owner references; the Secrets have none
// (they may outlive a cross-cluster owner) and hold user code and secrets,
// so they are removed here.
func (e *Executor) Cancel(ctx context.Context, h string, job owner.Ref) error {
	return k8serr.Classify(e.cancel(ctx, h, job))
}

func (e *Executor) cancel(ctx context.Context, h string, job owner.Ref) error {
	t, r, err := translatorForHandle(h)
	if err != nil {
		return err
	}
	if err := e.deleteOwn(ctx, t.object(r), job); err != nil {
		return err
	}
	if err := e.ensureSecretAccess(ctx, r.namespace); apierrors.IsNotFound(err) {
		// The namespace is gone, and its Secrets with it.
		e.logger.Info("cancelled "+t.kind, "handle", h)
		return nil
	} else if err != nil {
		return err
	}
	if err := e.rebindingOnForbidden(ctx, r.namespace, func() error {
		return e.deleteOwnSecrets(ctx, r.namespace, job)
	}); err != nil {
		return err
	}
	e.logger.Info("cancelled "+t.kind, "handle", h)
	return nil
}

// deleteOwn deletes obj if it is the job's: read, its job-id checked, then
// deleted as the incarnation read.
func (e *Executor) deleteOwn(ctx context.Context, obj client.Object, job owner.Ref) error {
	if err := e.client.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
	}
	if !owner.Owns(job, obj) {
		e.logger.Info("cancel leaves an object of the job's name that another job made",
			"object", obj.GetNamespace()+"/"+obj.GetName(), "job_id", job.JobID, "its_job_id", obj.GetLabels()[owner.LabelJobID])
		return nil
	}
	if err := owner.DeleteIncarnation(ctx, e.client, obj, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		return fmt.Errorf("delete %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}

// deleteOwnSecrets deletes the Secrets labelled with the job's ID: its
// inline source and its decrypted payloads. One by one: the agent's role
// lists and deletes Secrets, and is not given deletecollection on them.
func (e *Executor) deleteOwnSecrets(ctx context.Context, namespace string, job owner.Ref) error {
	if err := owner.DeleteOwned(ctx, e.client, &corev1.SecretList{}, namespace, job); err != nil {
		return fmt.Errorf("delete the Secrets of job %s in %s: %w", job.JobID, namespace, err)
	}
	return nil
}

// Status reads the job's native resource and maps it to a Zenthros JobState.
func (e *Executor) Status(ctx context.Context, h string) (backend.JobState, error) {
	state, err := e.status(ctx, h)
	return state, k8serr.Classify(err)
}

// status is the one status rule for every kind: the kind reads its object
// and maps its state, and what its pods and events add is k8spod.Enrich's.
// Reads come from the status cache when the agent has one.
func (e *Executor) status(ctx context.Context, h string) (backend.JobState, error) {
	t, r, err := translatorForHandle(h)
	if err != nil {
		return backend.JobState{}, err
	}
	reads := e.statusReads()
	obj, state, err := t.read(e, ctx, reads, r)
	if err != nil || k8spod.Settled(state) {
		return state, err
	}
	objects := []string{obj.GetName()}
	if t.objects != nil {
		objects = t.objects(obj)
	}
	return k8spod.Enrich(ctx, reads, k8spod.Observed{
		Namespace: obj.GetNamespace(), Objects: objects, State: state,
		Pods: t.pods(e, ctx, reads, obj), Container: t.container, DetailOnly: t.detailOnly,
	}), nil
}

// statusReads times what reaches the apiserver, and reads the status cache
// first when there is one.
func (e *Executor) statusReads() client.Client {
	reads := client.Client(recordingAPIClient{Client: e.client})
	if e.StatusCache != nil {
		reads = cachedReads{Client: reads, cache: e.StatusCache, events: &e.events}
	}
	return reads
}

// recordingAPIClient times destination-cluster reads used during a status sweep.
type recordingAPIClient struct {
	client.Client
}

func (c recordingAPIClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	started := time.Now()
	err := c.Client.Get(ctx, key, obj, opts...)
	metrics.RecordK8sAPI(err, time.Since(started))
	return err
}

func (c recordingAPIClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	started := time.Now()
	err := c.Client.List(ctx, list, opts...)
	metrics.RecordK8sAPI(err, time.Since(started))
	return err
}

// managedByLabel marks a namespace the agent created, so a later Submit may
// complete its isolation without touching a namespace an admin owns.
const managedByLabel = "app.kubernetes.io/managed-by"

// ensureNamespace creates the Workspace namespace in the agent's cluster if
// absent, so a job can land on a fresh remote cluster without manual
// pre-provisioning. The namespace gets what the hub's Workspace reconciler
// gives its own: the Workspace label, Pod Security Admission labels and the
// ingress isolation policy. Without them one Workspace's pod on a shared
// spoke reached another's Ray head, whose job API runs any code it is sent.
// A namespace that already exists and marks no Workspace belongs to its
// admin: a Workspace's job is refused there until the admin labels it. One
// that marks another Workspace than the job's is refused.
func (e *Executor) ensureNamespace(ctx context.Context, spec jobspec.JobSpec) error {
	name := spec.Namespace
	if e.protectedNamespace(name) {
		return fmt.Errorf("%w: namespace %q belongs to the platform", backend.ErrAdmissionRejected, name)
	}
	var ns corev1.Namespace
	err := e.client.Get(ctx, client.ObjectKey{Name: name}, &ns)
	switch {
	case apierrors.IsNotFound(err):
		labels := map[string]string{v1.LabelWorkspace: spec.Workspace, v1.LabelWorkspaceUID: spec.WorkspaceUID, managedByLabel: "zenthros-agent"}
		for key, value := range podsecurity.Labels(e.PodSecurity, nil) {
			labels[key] = value
		}
		ns = corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
		if err := e.client.Create(ctx, &ns); apierrors.IsAlreadyExists(err) {
			// Another job's Submit created it first (they run concurrently):
			// it may be another Workspace's, so check it like any existing one.
			if err := e.client.Get(ctx, client.ObjectKey{Name: name}, &ns); err != nil {
				return fmt.Errorf("get namespace %s: %w", name, err)
			}
			return e.adoptNamespace(ctx, spec, &ns)
		} else if err != nil {
			return fmt.Errorf("create namespace %s: %w", name, err)
		}
		e.logger.Info("created workspace namespace", "namespace", name, "workspace", spec.Workspace)
	case err != nil:
		return fmt.Errorf("get namespace %s: %w", name, err)
	default:
		return e.adoptNamespace(ctx, spec, &ns)
	}
	return e.ensureIsolationPolicy(ctx, name)
}

// adoptNamespace admits spec into an existing namespace: one no Workspace
// marks is its admin's, refused to a Workspace's job (any Workspace could
// land in it, with no isolation between them); one marked for another
// Workspace, or for an earlier Workspace of the name (another UID), is
// refused; one marked for this Workspace gets what isolation it lacks, and
// the Workspace's UID when it is marked by name only (an admin's label).
func (e *Executor) adoptNamespace(ctx context.Context, spec jobspec.JobSpec, ns *corev1.Namespace) error {
	marked, ok := ns.Labels[v1.LabelWorkspace]
	if !ok {
		return fmt.Errorf("%w: namespace %q exists and marks no Workspace; label it %s=%s to run Workspace %q's jobs there",
			backend.ErrAdmissionRejected, ns.Name, v1.LabelWorkspace, spec.Workspace, spec.Workspace)
	}
	if marked != spec.Workspace {
		return fmt.Errorf("%w: namespace %q belongs to Workspace %q, not %q", backend.ErrAdmissionRejected, ns.Name, marked, spec.Workspace)
	}
	switch uid := ns.Labels[v1.LabelWorkspaceUID]; uid {
	case spec.WorkspaceUID:
	case "":
		if err := e.markWorkspaceUID(ctx, ns, spec.WorkspaceUID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: namespace %q belongs to an earlier Workspace %q (UID %s); delete it to run this one's jobs there",
			backend.ErrAdmissionRejected, ns.Name, marked, uid)
	}
	if err := e.completePodSecurity(ctx, ns); err != nil {
		return err
	}
	return e.ensureIsolationPolicy(ctx, ns.Name)
}

// markWorkspaceUID stamps the UID on a namespace marked by Workspace name
// only (an admin labelled it by hand), as read: once, for the first job of
// the name.
func (e *Executor) markWorkspaceUID(ctx context.Context, ns *corev1.Namespace, uid string) error {
	patch := client.MergeFromWithOptions(ns.DeepCopy(), client.MergeFromWithOptimisticLock{})
	ns.Labels[v1.LabelWorkspaceUID] = uid
	if err := e.client.Patch(ctx, ns, patch); err != nil {
		return fmt.Errorf("mark namespace %s with its Workspace's UID: %w", ns.Name, err)
	}
	return nil
}

// completePodSecurity adds the PSA labels a Workspace namespace lacks and
// never changes one it has: the hub's reconciler, or the admin, chose it.
func (e *Executor) completePodSecurity(ctx context.Context, ns *corev1.Namespace) error {
	base := ns.DeepCopy()
	changed := false
	for key, value := range podsecurity.Labels(e.PodSecurity, ns.Labels) {
		if _, ok := ns.Labels[key]; ok {
			continue
		}
		ns.Labels[key] = value
		changed = true
	}
	if !changed {
		return nil
	}
	if err := e.client.Patch(ctx, ns, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("label namespace %s: %w", ns.Name, err)
	}
	return nil
}

// ensureIsolationPolicy creates the Workspace ingress policy if absent. One
// that exists is left as it is: the hub's reconciler may own it.
func (e *Executor) ensureIsolationPolicy(ctx context.Context, namespace string) error {
	var policy networkingv1.NetworkPolicy
	err := e.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podsecurity.IsolationPolicyName}, &policy)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get network policy %s/%s: %w", namespace, podsecurity.IsolationPolicyName, err)
	}
	created := podsecurity.IsolationPolicy(namespace, map[string]string{managedByLabel: "zenthros-agent"})
	if err := e.client.Create(ctx, created); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create network policy %s/%s: %w", namespace, podsecurity.IsolationPolicyName, err)
	}
	return nil
}

// protectedNamespace is a namespace no Workspace job may land in: the
// cluster's own, the default one and the agent's.
func (e *Executor) protectedNamespace(name string) bool {
	return name == metav1.NamespaceDefault || strings.HasPrefix(name, "kube-") ||
		(e.Namespace != "" && name == e.Namespace)
}

func handle(namespace, name string) string { return namespace + "/" + name }

func parseHandle(h string) (namespace, name string, err error) {
	parts := strings.SplitN(h, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid handle %q (want namespace/name)", h)
	}
	return parts[0], parts[1], nil
}
