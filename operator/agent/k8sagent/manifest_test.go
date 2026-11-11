// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

var (
	pytorchJobGVK = schema.GroupVersionKind{Group: "kubeflow.org", Version: "v1", Kind: "PyTorchJob"}
	widgetGVK     = schema.GroupVersionKind{Group: "e2e.zenthros.ai", Version: "v1", Kind: "Widget"}
	batchJobGVK   = batchv1.SchemeGroupVersion.WithKind("Job")
)

// manifestExecutor is an executor whose cluster serves the kinds a Manifest
// test applies: PyTorchJob and Widget (namespaced CRDs), batch Jobs, pods,
// and PersistentVolumes (cluster-scoped).
func manifestExecutor(t *testing.T, funcs *interceptor.Funcs, objects ...client.Object) *Executor {
	t.Helper()
	scheme := testutil.Scheme(t, corev1.AddToScheme, batchv1.AddToScheme, networkingv1.AddToScheme, rbacv1.AddToScheme)
	mapper := meta.NewDefaultRESTMapper(nil)
	for _, gvk := range []schema.GroupVersionKind{pytorchJobGVK, widgetGVK, batchJobGVK, corev1.SchemeGroupVersion.WithKind("Pod"), corev1.SchemeGroupVersion.WithKind("ServiceAccount"),
		rbacv1.SchemeGroupVersion.WithKind("RoleBinding")} {
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}
	mapper.Add(corev1.SchemeGroupVersion.WithKind("PersistentVolume"), meta.RESTScopeRoot)
	mapper.Add(rbacv1.SchemeGroupVersion.WithKind("ClusterRole"), meta.RESTScopeRoot)
	// Created objects get a UID, as the API server gives them.
	with := withUIDs(funcs)
	builder := fake.NewClientBuilder().WithScheme(scheme).WithRESTMapper(mapper).WithObjects(withSecretRole(objects)...).WithInterceptorFuncs(with)
	e := &Executor{client: builder.Build(), logger: logr.Discard()}
	e.clientset = kubefake.NewSimpleClientset()
	withSecretAccess(e)
	return e
}

func manifestSpec(object string) jobspec.JobSpec {
	return jobspec.JobSpec{
		ID: "uid-train", Name: "train", Namespace: "tenant-a", Tenant: "a",
		Kind:     v1.WorkloadKindManifest,
		Manifest: &v1.ManifestSpec{Object: runtime.RawExtension{Raw: []byte(object)}},
	}
}

const pytorchObject = `{"apiVersion":"kubeflow.org/v1","kind":"PyTorchJob","metadata":{"name":"whatever","labels":{"team":"ml"}},
"spec":{"pytorchReplicaSpecs":{"Master":{"replicas":1}}}}`

// The object is named after the job, in the job's namespace, with the
// Zenthros labels next to the user's own and the hash and contract that let
// status and resubmits work from the handle alone.
func TestManifestForcesNameNamespaceLabels(t *testing.T) {
	spec := manifestSpec(pytorchObject)
	spec.Manifest.FailureWhen = []v1.ConditionMatch{{Type: "Broken", Status: "True"}}
	obj, err := buildManifest(spec)
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetName() != "train" || obj.GetNamespace() != "tenant-a" {
		t.Fatalf("object %s/%s, want tenant-a/train", obj.GetNamespace(), obj.GetName())
	}
	labels := obj.GetLabels()
	if labels["team"] != "ml" || labels["zenthros.ai/job-id"] != "uid-train" || labels["zenthros.ai/tenant"] != "a" {
		t.Fatalf("labels = %v", labels)
	}
	if obj.GetAnnotations()[ManifestSpecHashAnnotation] == "" {
		t.Fatal("no spec-hash annotation")
	}
	if c := readContract(obj); len(c.FailureWhen) != 1 || c.FailureWhen[0].Type != "Broken" {
		t.Fatalf("contract read back = %+v", c)
	}
	if replicas, _, _ := unstructured.NestedInt64(obj.Object, "spec", "pytorchReplicaSpecs", "Master", "replicas"); replicas != 1 {
		t.Fatalf("the user's spec was not kept: %v", obj.Object["spec"])
	}
}

// A Manifest runs namespaced objects only; the error names the kind and the
// job's namespace, and nothing is created.
func TestManifestRejectsClusterScoped(t *testing.T) {
	e := manifestExecutor(t, nil)
	_, err := e.Submit(context.Background(), manifestSpec(`{"apiVersion":"v1","kind":"PersistentVolume","metadata":{}}`), nil)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), `PersistentVolume is cluster-scoped: a Manifest runs namespaced objects in the job's namespace "tenant-a"`) {
		t.Fatalf("err = %v", err)
	}
}

// An object for another namespace is refused with both namespaces named, and
// so is generateName, which would break idempotent resubmits.
func TestManifestRejectsNamespaceMismatch(t *testing.T) {
	e := manifestExecutor(t, nil)
	_, err := e.Submit(context.Background(), manifestSpec(`{"apiVersion":"e2e.zenthros.ai/v1","kind":"Widget","metadata":{"namespace":"kube-system"}}`), nil)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), `manifest.object.metadata.namespace "kube-system" is not the job's namespace "tenant-a"`) {
		t.Fatalf("namespace: err = %v", err)
	}
	_, err = e.Submit(context.Background(), manifestSpec(`{"apiVersion":"e2e.zenthros.ai/v1","kind":"Widget","metadata":{"generateName":"w-"}}`), nil)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), `generateName "w-" is not supported: Zenthros names the object "train"`) {
		t.Fatalf("generateName: err = %v", err)
	}
}

// RBAC objects and ServiceAccounts are never applied, whatever the agent's
// own RBAC would allow.
func TestManifestDenyList(t *testing.T) {
	e := manifestExecutor(t, nil)
	for _, object := range []string{
		`{"apiVersion":"v1","kind":"ServiceAccount","metadata":{}}`,
		`{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{}}`,
	} {
		_, err := e.Submit(context.Background(), manifestSpec(object), nil)
		if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), "RBAC objects and ServiceAccounts are never applied") {
			t.Fatalf("%s: err = %v", object, err)
		}
	}
}

// A kind the cluster does not serve is the RESTMapper's NoMatch, word for
// word, classified as an unsupported kind.
func TestManifestUnknownKindIsVerbatim(t *testing.T) {
	e := manifestExecutor(t, nil)
	_, err := e.Submit(context.Background(), manifestSpec(`{"apiVersion":"volcano.sh/v1alpha1","kind":"Job","metadata":{}}`), nil)
	if !errors.Is(err, backend.ErrUnsupportedKind) || !strings.Contains(err.Error(), `no matches for kind "Job" in version "volcano.sh/v1alpha1"`) {
		t.Fatalf("err = %v", err)
	}
}

// What the API server or an operator's webhook says when the apply is refused
// reaches the user unchanged; only the object is named in front.
func TestManifestApplyErrorIsVerbatim(t *testing.T) {
	webhook := `admission webhook "validator.pytorchjob.training-operator.kubeflow.org" denied the request: PyTorchJobSpec is not valid: There is no Master replica`
	funcs := &interceptor.Funcs{Apply: func(context.Context, client.WithWatch, runtime.ApplyConfiguration, ...client.ApplyOption) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "kubeflow.org", Resource: "pytorchjobs"}, "train", errors.New(webhook))
	}}
	e := manifestExecutor(t, funcs)
	_, err := e.Submit(context.Background(), manifestSpec(pytorchObject), nil)
	if !errors.Is(err, backend.ErrAdmissionRejected) || !strings.Contains(err.Error(), "apply PyTorchJob tenant-a/train: ") || !strings.Contains(err.Error(), webhook) {
		t.Fatalf("err = %v", err)
	}
}

// Resubmitting the same object is idempotent; a different object, or one
// Zenthros did not apply, is a spec mismatch and is never overwritten.
func TestManifestSpecHashMismatch(t *testing.T) {
	e := manifestExecutor(t, nil)
	ctx := context.Background()
	first, err := e.Submit(ctx, manifestSpec(pytorchObject), nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := "manifest:kubeflow.org/v1/PyTorchJob/tenant-a/train"; first != want {
		t.Fatalf("handle = %q, want %q", first, want)
	}
	again, err := e.Submit(ctx, manifestSpec(pytorchObject), nil)
	if err != nil || again != first {
		t.Fatalf("resubmit = %q, %v", again, err)
	}
	changed := strings.Replace(pytorchObject, `"replicas":1`, `"replicas":2`, 1)
	if _, err := e.Submit(ctx, manifestSpec(changed), nil); !errors.Is(err, backend.ErrSpecMismatch) {
		t.Fatalf("changed object: err = %v, want a spec mismatch", err)
	}

	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(widgetGVK)
	foreign.SetNamespace("tenant-a")
	foreign.SetName("train")
	e = manifestExecutor(t, nil, foreign)
	_, err = e.Submit(ctx, manifestSpec(`{"apiVersion":"e2e.zenthros.ai/v1","kind":"Widget","metadata":{}}`), nil)
	if !errors.Is(err, backend.ErrSpecMismatch) || !strings.Contains(err.Error(), "not applied by Zenthros") {
		t.Fatalf("foreign object: err = %v", err)
	}
}

// The handle carries the kind, so status, logs and cancel need nothing else;
// the core group is empty.
func TestManifestHandleRoundTrip(t *testing.T) {
	for _, gvk := range []schema.GroupVersionKind{pytorchJobGVK, {Version: "v1", Kind: "ConfigMap"}} {
		h := manifestHandle(gvk, "tenant-a", "train")
		tr, r, err := translatorForHandle(h)
		if err != nil || tr.kind != v1.WorkloadKindManifest || r.gvk != gvk || r.namespace != "tenant-a" || r.name != "train" {
			t.Fatalf("%q -> %s %+v %v", h, tr.kind, r, err)
		}
	}
	for _, bad := range []string{"manifest:kubeflow.org/v1/PyTorchJob/train", "manifest:kubeflow.org//PyTorchJob/a/b"} {
		if _, _, err := translatorForHandle(bad); err == nil {
			t.Fatalf("malformed handle %q accepted", bad)
		}
	}
}

func objectWithStatus(kind string, status map[string]any) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{"status": status}}
	obj.SetKind(kind)
	obj.SetName("train")
	return obj
}

func conds(pairs ...string) map[string]any {
	var list []any
	for i := 0; i+1 < len(pairs); i += 2 {
		list = append(list, map[string]any{"type": pairs[i], "status": pairs[i+1], "reason": pairs[i] + "Reason", "message": pairs[i] + " said so"})
	}
	return map[string]any{"conditions": list}
}

// The default contract reads batch/v1 Jobs and the Kubeflow training jobs;
// phasePath reads operators that report a phase string (Volcano).
func TestStatusContractDefaults(t *testing.T) {
	volcano := manifestContract{PhasePath: &v1.PhasePath{Path: "status.state.phase", Succeeded: []string{"Completed"}, Failed: []string{"Failed", "Aborted"}, Running: []string{"Running"}}}
	serving := manifestContract{RunningWhen: []v1.ConditionMatch{{Type: "Ready", Status: "True"}}}
	for name, tc := range map[string]struct {
		obj      *unstructured.Unstructured
		contract manifestContract
		want     v1.Phase
	}{
		"batch Complete":             {objectWithStatus("Job", conds("Complete", "True")), manifestContract{}, v1.PhaseSucceeded},
		"batch Failed":               {objectWithStatus("Job", conds("Failed", "True")), manifestContract{}, v1.PhaseFailed},
		"batch active":               {objectWithStatus("Job", map[string]any{"active": int64(1)}), manifestContract{}, v1.PhaseRunning},
		"pytorch created":            {objectWithStatus("PyTorchJob", conds("Created", "True")), manifestContract{}, v1.PhaseSubmitted},
		"pytorch running":            {objectWithStatus("PyTorchJob", conds("Created", "True", "Running", "True")), manifestContract{}, v1.PhaseRunning},
		"pytorch restarting":         {objectWithStatus("PyTorchJob", conds("Running", "False", "Restarting", "True")), manifestContract{}, v1.PhaseRunning},
		"pytorch succeeded":          {objectWithStatus("PyTorchJob", conds("Running", "False", "Succeeded", "True")), manifestContract{}, v1.PhaseSucceeded},
		"pytorch failed":             {objectWithStatus("PyTorchJob", conds("Running", "False", "Failed", "True")), manifestContract{}, v1.PhaseFailed},
		"complete wins over failed":  {objectWithStatus("Job", conds("Failed", "True", "Complete", "True")), manifestContract{}, v1.PhaseSucceeded},
		"trainjob complete":          {objectWithStatus("TrainJob", conds("Complete", "True")), manifestContract{}, v1.PhaseSucceeded},
		"false condition is ignored": {objectWithStatus("PyTorchJob", conds("Failed", "False")), manifestContract{}, v1.PhaseSubmitted},
		"custom failure":             {objectWithStatus("Widget", conds("Broken", "True")), manifestContract{FailureWhen: []v1.ConditionMatch{{Type: "Broken"}}}, v1.PhaseFailed},
		"volcano running":            {objectWithStatus("Job", map[string]any{"state": map[string]any{"phase": "Running"}}), volcano, v1.PhaseRunning},
		"volcano completed":          {objectWithStatus("Job", map[string]any{"state": map[string]any{"phase": "Completed"}}), volcano, v1.PhaseSucceeded},
		"volcano aborted":            {objectWithStatus("Job", map[string]any{"state": map[string]any{"phase": "Aborted"}}), volcano, v1.PhaseFailed},
		"volcano pending":            {objectWithStatus("Job", map[string]any{"state": map[string]any{"phase": "Pending"}}), volcano, v1.PhaseSubmitted},
		// Serving (docs/serving.md): RayService and InferenceService say Ready,
		// not Running, so the documented contract is runningWhen Ready.
		"rayservice ready":            {objectWithStatus("RayService", conds("Ready", "True")), serving, v1.PhaseRunning},
		"inferenceservice ready":      {objectWithStatus("InferenceService", conds("Ready", "True")), serving, v1.PhaseRunning},
		"rayservice without contract": {objectWithStatus("RayService", conds("Ready", "True")), manifestContract{}, v1.PhaseSubmitted},
	} {
		t.Run(name, func(t *testing.T) {
			if got := manifestState(tc.obj, tc.contract); got.Phase != tc.want {
				t.Fatalf("phase = %s (%q), want %s", got.Phase, got.Message, tc.want)
			}
		})
	}
}

// A failure is the operator's condition as it wrote it, after the object's
// kind and name.
func TestManifestFailureMessageIsConditionMessage(t *testing.T) {
	obj := objectWithStatus("PyTorchJob", map[string]any{"conditions": []any{
		map[string]any{"type": "Failed", "status": "True", "reason": "PyTorchJobFailed", "message": "boom: CUDA OOM"},
	}})
	state := manifestState(obj, manifestContract{})
	const want = "PyTorchJob/train condition Failed=True reason=PyTorchJobFailed: boom: CUDA OOM"
	if state.Message != want || len(state.Reasons) != 1 || state.Reasons[0].Detail != want || state.Reasons[0].Code != v1.ReasonProcessFailed {
		t.Fatalf("state = %+v, want message %q", state, want)
	}
	aborted := objectWithStatus("Job", map[string]any{"state": map[string]any{"phase": "Aborted"}, "message": "queue closed"})
	state = manifestState(aborted, manifestContract{PhasePath: &v1.PhasePath{Path: "status.state.phase", Failed: []string{"Aborted"}}})
	if state.Message != `Job/train status.state.phase="Aborted": queue closed` {
		t.Fatalf("phase failure message = %q", state.Message)
	}
}

func owned(kind, apiVersion, name string, uid types.UID) []metav1.OwnerReference {
	return []metav1.OwnerReference{{APIVersion: apiVersion, Kind: kind, Name: name, UID: uid}}
}

// The job's log is its pods' logs, found through the owner chain (object ->
// batch Job -> pod here) and never a neighbour's pod; each line says which
// pod and container it came from.
func TestManifestLogsFollowOwnerChain(t *testing.T) {
	root := &unstructured.Unstructured{}
	root.SetGroupVersionKind(widgetGVK)
	root.SetNamespace("tenant-a")
	root.SetName("train")
	root.SetUID("uid-widget")
	middle := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: "train-worker", UID: "uid-job",
		OwnerReferences: owned("Widget", "e2e.zenthros.ai/v1", "train", "uid-widget")}}
	pod := func(name string, owners []metav1.OwnerReference, age time.Duration) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: name, OwnerReferences: owners, CreationTimestamp: metav1.NewTime(time.Now().Add(-age))},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		}
	}
	direct := pod("train-master-0", owned("Widget", "e2e.zenthros.ai/v1", "train", "uid-widget"), time.Minute)
	chained := pod("train-worker-abc", owned("Job", "batch/v1", "train-worker", "uid-job"), 0)
	neighbour := pod("other-0", owned("Job", "batch/v1", "other", "uid-other"), 0)
	e := manifestExecutor(t, nil, root, middle, direct, chained, neighbour)
	e.clientset = kubefake.NewSimpleClientset(direct, chained, neighbour)

	pods, err := e.manifestPods(context.Background(), root, manifestContract{})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range pods {
		names = append(names, p.Name)
	}
	if fmt.Sprint(names) != "[train-worker-abc train-master-0]" {
		t.Fatalf("pods = %v, want the chained and the direct pod, newest first", names)
	}

	selected, err := e.manifestPods(context.Background(), root, manifestContract{LogSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "none"}}})
	if err != nil || len(selected) != 0 {
		t.Fatalf("logSelector matched %v, %v", selected, err)
	}

	ch, err := e.Logs(context.Background(), manifestHandle(widgetGVK, "tenant-a", "train"), false, -1)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for chunk := range ch {
		lines = append(lines, string(chunk.Data))
	}
	joined := strings.Join(lines, "")
	for _, want := range []string{"[train-worker-abc/main] fake logs\n", "[train-master-0/main] fake logs\n"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("logs %q lack %q", joined, want)
		}
	}
	if strings.Contains(joined, "other-0") {
		t.Fatalf("logs include a neighbour's pod: %q", joined)
	}
}

// Status reads the object by its handle and the contract stored on it;
// cancel deletes it and a second cancel is a no-op.
func TestManifestStatusAndCancelByHandle(t *testing.T) {
	e := manifestExecutor(t, nil)
	ctx := context.Background()
	spec := manifestSpec(`{"apiVersion":"e2e.zenthros.ai/v1","kind":"Widget","metadata":{}}`)
	spec.Manifest.SuccessWhen = []v1.ConditionMatch{{Type: "Done"}}
	h, err := e.Submit(ctx, spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if state, err := e.Status(ctx, h); err != nil || state.Phase != v1.PhaseSubmitted {
		t.Fatalf("fresh status = %+v, %v", state, err)
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(widgetGVK)
	if err := e.client.Get(ctx, client.ObjectKey{Namespace: "tenant-a", Name: "train"}, obj); err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedSlice(obj.Object, []any{map[string]any{"type": "Done", "status": "True"}}, "status", "conditions")
	if err := e.client.Update(ctx, obj); err != nil {
		t.Fatal(err)
	}
	if state, err := e.Status(ctx, h); err != nil || state.Phase != v1.PhaseSucceeded {
		t.Fatalf("status after Done = %+v, %v", state, err)
	}
	for i := 0; i < 2; i++ {
		if err := e.Cancel(ctx, h, jobOf(spec)); err != nil {
			t.Fatalf("cancel %d: %v", i, err)
		}
	}
	if _, err := e.Status(ctx, h); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("status after cancel: %v, want not found", err)
	}
}

// While a Manifest waits, the user reads the newest Warning its operator or
// Kubernetes recorded on it or its pods, word for word, even one Zenthros has
// no reason code for (a quota that wants requests set).
func TestManifestPendingShowsOperatorWarning(t *testing.T) {
	root := &unstructured.Unstructured{}
	root.SetGroupVersionKind(pytorchJobGVK)
	root.SetNamespace("tenant-a")
	root.SetName("train")
	root.SetUID("uid-train")
	event := func(name, reason, message string, age time.Duration) *corev1.Event {
		return &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "tenant-a", Name: name},
			InvolvedObject: corev1.ObjectReference{Kind: "PyTorchJob", Namespace: "tenant-a", Name: "train"},
			Type:           corev1.EventTypeWarning, Reason: reason, Message: message,
			LastTimestamp: metav1.NewTime(time.Now().Add(-age)),
		}
	}
	const quota = `Error creating: pods "train-master-0" is forbidden: failed quota: zenthros-workspace-quota: must specify requests.cpu for: pytorch`
	e := manifestExecutor(t, nil, root,
		event("old", "FailedCreate", "an older complaint", time.Hour),
		event("new", "FailedCreate", quota, time.Second))
	state, err := e.status(context.Background(), manifestHandle(pytorchJobGVK, "tenant-a", "train"))
	if err != nil {
		t.Fatal(err)
	}
	want := "Warning FailedCreate PyTorchJob/train: " + quota
	if state.Phase != v1.PhaseSubmitted || !strings.HasSuffix(state.Message, want) {
		t.Fatalf("state = %s %q, want Submitted ending %q", state.Phase, state.Message, want)
	}
}

// A Manifest's pods never run as a Workspace user: the agent applies with
// its own rights, and an editor could run a batch Job as a user who
// provisions machines and read that user's token from its logs.
// The Workspace's workload account, and an account the
// user did not name, still run; a token Secret is never made.
func TestManifestNeverRunsAsAUser(t *testing.T) {
	user := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "tenant-a", Labels: map[string]string{v1.LabelUser: "true"}}}
	workload := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: v1.WorkspaceServiceAccount, Namespace: "tenant-a"}}
	e := manifestExecutor(t, nil, user, workload)
	asAlice := `{"apiVersion":"batch/v1","kind":"Job","metadata":{},"spec":{"template":{"spec":{"serviceAccountName":"alice","containers":[{"name":"c","image":"busybox"}]}}}}`
	_, err := e.Submit(context.Background(), manifestSpec(asAlice), nil)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), `ServiceAccount "alice", a Workspace user's identity`) {
		t.Fatalf("a Job as user alice: err = %v", err)
	}
	// Deep in an operator's object too (a PyTorchJob's replica template).
	nested := `{"apiVersion":"kubeflow.org/v1","kind":"PyTorchJob","metadata":{},"spec":{"pytorchReplicaSpecs":{"Worker":{"template":{"spec":{"serviceAccount":"alice"}}}}}}`
	if _, err := e.Submit(context.Background(), manifestSpec(nested), nil); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("a PyTorchJob worker as alice: err = %v", err)
	}
	asWorkload := `{"apiVersion":"batch/v1","kind":"Job","metadata":{},"spec":{"template":{"spec":{"serviceAccountName":"` + v1.WorkspaceServiceAccount + `","containers":[{"name":"c","image":"busybox"}]}}}}`
	if _, err := e.Submit(context.Background(), manifestSpec(asWorkload), nil); err != nil {
		t.Fatalf("a Job as the Workspace's workload account: %v", err)
	}
	token := `{"apiVersion":"v1","kind":"Secret","metadata":{"annotations":{"kubernetes.io/service-account.name":"alice"}},"type":"kubernetes.io/service-account-token"}`
	if _, err := buildManifest(manifestSpec(token)); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("a token Secret for alice: err = %v", err)
	}
}
