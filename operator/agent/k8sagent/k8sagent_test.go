// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	agentage "github.com/zenthros/operator/agent/agepayload"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8s"
	"github.com/zenthros/operator/pkg/backend/spark"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
)

func newTestExecutor(t *testing.T, objects ...client.Object) *Executor {
	t.Helper()
	scheme := testutil.Scheme(t, rayv1.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme, appsv1.AddToScheme, spark.AddToScheme, networkingv1.AddToScheme, rbacv1.AddToScheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(withSecretRole(objects)...).WithStatusSubresource(&batchv1.Job{}, &appsv1.DaemonSet{}).
		WithInterceptorFuncs(interceptor.Funcs{Create: testutil.CreateWithUID}).Build()
	e, err := New(c, &rest.Config{Host: "http://localhost:1"}, logr.Discard())
	if err != nil {
		t.Fatalf("build executor: %v", err)
	}
	withSecretAccess(e)
	return e
}

// testSecretRole is the ClusterRole the agent chart makes and names in
// ZENTHROS_JOB_SECRETS_CLUSTER_ROLE.
const testSecretRole = "zenthros-agent-job-secrets"

// withSecretRole is a test cluster's objects, with UIDs, and the chart's
// Secret role among them.
func withSecretRole(objects []client.Object) []client.Object {
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: testSecretRole}}
	return testutil.WithUIDs(append([]client.Object{role}, objects...)...)
}

// withSecretAccess gives e the chart's Secret role (withSecretRole puts it
// in the cluster), and an API server that honours the agent's binding of it
// in a namespace once that binding exists. e.client is read at each call,
// so a client the test wraps later is the one asked.
func withSecretAccess(e *Executor) {
	e.SecretAccess = SecretAccess{ClusterRole: testSecretRole, ServiceAccount: "zenthros-agent", Namespace: "zenthros-system"}
	e.secretsAllowed = func(ctx context.Context, namespace string) (bool, error) {
		var binding rbacv1.RoleBinding
		return e.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: e.SecretAccess.bindingName()}, &binding) == nil, nil
	}
}

func TestSubmitSparkApplicationThroughKubernetesExecutor(t *testing.T) {
	e := newTestExecutor(t)
	spec := jobspec.JobSpec{
		ID: "uid-spark", Name: "spark-pi", Namespace: "tenant-default", Tenant: "default",
		Kind: v1.WorkloadKindSparkApplication, Image: "zenthros/spark-base:4.1.1",
		Spark: &v1.SparkSpec{
			MainApplicationFile: "local:///opt/spark/examples/jars/spark-examples.jar",
			MainClass:           "org.apache.spark.examples.SparkPi", ExecutorCount: 1,
		},
	}
	handle, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !strings.HasPrefix(handle, spark.AgentHandlePrefix) {
		t.Fatalf("handle = %q, want Spark prefix", handle)
	}
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("idempotent Submit: %v", err)
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}

func TestSubmitUsesExistingRayClusterWithoutOwningIt(t *testing.T) {
	cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "shared-ray", Namespace: "tenant-default"}}
	e := newTestExecutor(t, cluster)
	spec := jobspec.JobSpec{
		ID: "uid-shared", Name: "shared-job", Namespace: cluster.Namespace, Tenant: "default",
		Kind: v1.WorkloadKindRayJob, Entrypoint: "python main.py", Image: "ray:latest",
		RayCluster: &v1.RayClusterReference{Name: cluster.Name, Namespace: cluster.Namespace},
	}
	handle, err := e.Submit(context.Background(), spec, map[string]string{"JOB_MARKER": "one"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var rayJob rayv1.RayJob
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}, &rayJob); err != nil {
		t.Fatalf("get RayJob: %v", err)
	}
	if rayJob.Spec.ClusterSelector["ray.io/cluster"] != cluster.Name || rayJob.Spec.RayClusterSpec != nil {
		t.Fatalf("RayJob spec = %+v", rayJob.Spec)
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	var preserved rayv1.RayCluster
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, &preserved); err != nil {
		t.Fatalf("shared cluster was deleted: %v", err)
	}
}

func secretJobSpec(mode string) jobspec.JobSpec {
	return jobspec.JobSpec{
		ID: "uid-secret", Name: "secret-test", Namespace: "tenant-default", Tenant: "default",
		Kind: v1.WorkloadKindRayJob, Entrypoint: "python main.py", Image: "ray:latest",
		SecretSources: []v1.SecretSourceRef{{
			Type: v1.SecretSourceTypeKubernetes, Namespace: "tenant-default", Name: "api-token", Key: "token",
			Materialization: v1.SecretMaterialization{Mode: mode, EnvName: "API_TOKEN"},
		}},
	}
}

func TestSubmitResolvesSecretOnlyAtKubernetesAgent(t *testing.T) {
	const plaintext = "local-secret-value"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-default", Name: "api-token"},
		Data:       map[string][]byte{"token": []byte(plaintext)},
	}
	e := newTestExecutor(t, secret)
	spec := secretJobSpec(v1.SecretMaterializationEnv)
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var rayJob rayv1.RayJob
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}, &rayJob); err != nil {
		t.Fatalf("get RayJob: %v", err)
	}
	raw, err := json.Marshal(rayJob)
	if err != nil {
		t.Fatalf("marshal RayJob: %v", err)
	}
	if strings.Contains(string(raw), plaintext) {
		t.Fatal("RayJob contains plaintext secret")
	}
}

func TestCancelRemovesWorkloadWithoutDeletingReferencedSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-default", Name: "api-token"},
		Data:       map[string][]byte{"token": []byte("local-secret-value")},
	}
	e := newTestExecutor(t, secret)
	spec := secretJobSpec(v1.SecretMaterializationEnv)
	handle, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	var referenced corev1.Secret
	if err := e.client.Get(context.Background(), client.ObjectKeyFromObject(secret), &referenced); err != nil {
		t.Fatalf("referenced Secret must remain customer-owned: %v", err)
	}
	var rayJob rayv1.RayJob
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}, &rayJob); !apierrors.IsNotFound(err) {
		t.Fatalf("RayJob still present after Cancel: %v", err)
	}
}

func TestSubmitSecretSourceFailuresAreStableAndRedacted(t *testing.T) {
	const plaintext = "must-not-leak"
	tests := []struct {
		name   string
		mutate func(*jobspec.JobSpec)
		secret *corev1.Secret
		code   string
	}{
		{name: "missing secret", code: v1.ReasonSecretNotFound},
		{name: "missing key", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-default", Name: "api-token"}, Data: map[string][]byte{"other": []byte(plaintext)}}, code: v1.ReasonSecretKeyNotFound},
		{name: "forbidden namespace", mutate: func(spec *jobspec.JobSpec) { spec.SecretSources[0].Namespace = "tenant-other" }, code: v1.ReasonSecretNamespaceForbidden},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var e *Executor
			if test.secret == nil {
				e = newTestExecutor(t)
			} else {
				e = newTestExecutor(t, test.secret)
			}
			spec := secretJobSpec(v1.SecretMaterializationEnv)
			if test.mutate != nil {
				test.mutate(&spec)
			}
			_, err := e.Submit(context.Background(), spec, nil)
			if err == nil || !strings.Contains(err.Error(), test.code+":") {
				t.Fatalf("Submit error = %v, want code %s", err, test.code)
			}
			if strings.Contains(err.Error(), plaintext) {
				t.Fatal("error contains plaintext secret")
			}
		})
	}
}

func TestSubmitRejectsAWSSecretSourceOnKubernetesAgent(t *testing.T) {
	e := newTestExecutor(t)
	spec := secretJobSpec(v1.SecretMaterializationEnv)
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeAWSSecretsManager,
		AWSSecretsManager: &v1.AWSSecretsManagerRef{
			SecretID: "workload/token", Region: "us-east-2",
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T14_SECRET"},
	}}
	_, err := e.Submit(context.Background(), spec, nil)
	if err == nil || !strings.Contains(err.Error(), v1.ReasonUnsupportedSecretSource+":") {
		t.Fatalf("Submit error = %v", err)
	}
}

func TestSubmitProjectsEncryptedPayloadWithoutPlaintextInRayJob(t *testing.T) {
	const plaintext = "local-encrypted-value"
	bundle, err := agentage.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := agepayload.Encrypt([]byte(plaintext), bundle.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	e := newTestExecutor(t)
	e.Decryptor = bundle
	spec := secretJobSpec(v1.SecretMaterializationEnv)
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{
			Ciphertext: ciphertext, Recipients: bundle.Recipients(),
			KeyID: agepayload.KeyID(bundle.Recipients()[0]),
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T15_SECRET"},
	}}
	handle, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var projected corev1.Secret
	key := types.NamespacedName{Namespace: spec.Namespace, Name: encryptedSecretName(spec.ID, 0)}
	if err := e.client.Get(context.Background(), key, &projected); err != nil {
		t.Fatalf("projected Secret missing: %v", err)
	}
	if string(projected.Data[encryptedSecretKey]) != plaintext {
		t.Fatalf("projected Secret = %q", projected.Data[encryptedSecretKey])
	}
	var rayJob rayv1.RayJob
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}, &rayJob); err != nil {
		t.Fatalf("get RayJob: %v", err)
	}
	raw, err := json.Marshal(rayJob)
	if err != nil {
		t.Fatalf("marshal RayJob: %v", err)
	}
	if strings.Contains(string(raw), plaintext) {
		t.Fatal("RayJob contains plaintext secret")
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := e.client.Get(context.Background(), key, &projected); !apierrors.IsNotFound(err) {
		t.Fatalf("projected Secret remained after Cancel: %v", err)
	}
}

func TestSubmitEncryptedPayloadWrongRecipientIsDenied(t *testing.T) {
	destination, err := agentage.Generate()
	if err != nil {
		t.Fatal(err)
	}
	other, err := agentage.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := agepayload.Encrypt([]byte("token"), other.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	e := newTestExecutor(t)
	e.Decryptor = destination
	spec := secretJobSpec(v1.SecretMaterializationEnv)
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{
			Ciphertext: ciphertext, Recipients: other.Recipients(),
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T15_SECRET"},
	}}
	_, err = e.Submit(context.Background(), spec, nil)
	if err == nil || !strings.Contains(err.Error(), v1.ReasonSecretAccessDenied+":") {
		t.Fatalf("Submit error = %v", err)
	}
}

type forbiddenSecretClient struct {
	client.Client
}

func (c forbiddenSecretClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if _, ok := object.(*corev1.Secret); ok {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, key.Name, errors.New("denied"))
	}
	return c.Client.Get(ctx, key, object, opts...)
}

func TestSubmitMapsSecretRBACDenial(t *testing.T) {
	e := newTestExecutor(t)
	e.client = forbiddenSecretClient{Client: e.client}
	_, err := e.Submit(context.Background(), secretJobSpec(v1.SecretMaterializationEnv), nil)
	if err == nil || !strings.Contains(err.Error(), v1.ReasonSecretAccessDenied+":") {
		t.Fatalf("Submit error = %v", err)
	}
}

// A Secret read the remote API server throttled or could not answer is a
// transient Submit error, retried within the submit budget: it says nothing
// of the Secret (c4-A1).
func TestSubmitRetriesASecretReadTheClusterCouldNotAnswer(t *testing.T) {
	for name, fail := range map[string]error{
		"throttled": apierrors.NewTooManyRequests("slow down", 1),
		"timed out": apierrors.NewTimeoutError("read", 1),
	} {
		e := newTestExecutor(t)
		e.client = failingSecretClient{Client: e.client, err: fail}
		_, err := e.Submit(context.Background(), secretJobSpec(v1.SecretMaterializationEnv), nil)
		if _, refused := backend.SecretSourceReason(err); err == nil || refused || backend.Classify(err) != backend.ClassTransient {
			t.Errorf("%s: Submit error = %v (class %v), want a transient error, no secret-source refusal", name, err, backend.Classify(err))
		}
	}
}

type failingSecretClient struct {
	client.Client
	err error
}

func (c failingSecretClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if _, ok := object.(*corev1.Secret); ok {
		return c.err
	}
	return c.Client.Get(ctx, key, object, opts...)
}

func inlineJobSpec() jobspec.JobSpec {
	return jobspec.JobSpec{
		ID:        "uid-inline",
		Name:      "inline-test",
		Namespace: "tenant-default",
		Tenant:    "default",
		Workspace: "default", WorkspaceUID: "uid-default",
		Kind:       "RayJob",
		Entrypoint: "python main.py",
		Image:      "takenking9879/ray-train:2.53.0",
		Source:     v1.Source{Inline: base64.StdEncoding.EncodeToString([]byte("bundle-bytes"))},
	}
}

// TestSubmitCreatesInlineSecret locks in the fix for the inline-source defect: the
// agent's executor builds an inline-extract initContainer that mounts a per-job
// Secret, so Submit must create that Secret or the pod stalls at Init.
func TestSubmitCreatesInlineSecret(t *testing.T) {
	e := newTestExecutor(t)
	spec := inlineJobSpec()

	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	var secret corev1.Secret
	key := types.NamespacedName{Namespace: spec.Namespace, Name: k8s.InlineSecretNameFor(spec)}
	if err := e.client.Get(context.Background(), key, &secret); err != nil {
		t.Fatalf("inline secret not created: %v", err)
	}
	if got := string(secret.Data["bundle.tar.gz"]); got != "bundle-bytes" {
		t.Errorf("bundle.tar.gz = %q, want %q", got, "bundle-bytes")
	}
}

// TestSubmitInlineIdempotent confirms a repeated Submit does not error on the
// already-existing Secret (the agent reconciles periodically).
func TestSubmitInlineIdempotent(t *testing.T) {
	e := newTestExecutor(t)
	spec := inlineJobSpec()

	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("second Submit: %v", err)
	}
}

// TestSubmitNoSecretWithoutInline confirms non-inline jobs create no source Secret.
func TestSubmitNoSecretWithoutInline(t *testing.T) {
	e := newTestExecutor(t)
	spec := inlineJobSpec()
	spec.Source = v1.Source{HostPath: "/data/src"}

	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	var secret corev1.Secret
	key := types.NamespacedName{Namespace: spec.Namespace, Name: k8s.InlineSecretNameFor(spec)}
	err := e.client.Get(context.Background(), key, &secret)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected no inline secret, got err=%v", err)
	}
}

// TestCancelRemovesWorkloadAndSecret proves Cancel cleans up everything the job
// left in the cluster (Phase L): the RayJob AND the inline-source Secret holding
// user code. Idempotent — a second Cancel on the already-removed objects succeeds.
func TestCancelRemovesWorkloadAndSecret(t *testing.T) {
	e := newTestExecutor(t)
	spec := inlineJobSpec()

	handle, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	var rayJob rayv1.RayJob
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}, &rayJob); !apierrors.IsNotFound(err) {
		t.Errorf("RayJob still present after Cancel (err=%v)", err)
	}
	var secret corev1.Secret
	if err := e.client.Get(context.Background(), types.NamespacedName{Namespace: spec.Namespace, Name: k8s.InlineSecretNameFor(spec)}, &secret); !apierrors.IsNotFound(err) {
		t.Errorf("inline secret still present after Cancel (err=%v)", err)
	}

	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("second Cancel must be idempotent: %v", err)
	}
}

func TestSparkSubmitAndCancelCleanInlineSourceButKeepReferencedSecret(t *testing.T) {
	referenced := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-default", Name: "api-token"},
		Data:       map[string][]byte{"token": []byte("destination-only")},
	}
	e := newTestExecutor(t, referenced)
	spec := jobspec.JobSpec{
		ID: "uid-spark-inline", Name: "spark-inline", Namespace: "tenant-default", Tenant: "default",
		Kind: v1.WorkloadKindSparkApplication, Image: "zenthros/spark-base:4.1.1",
		Source: v1.Source{Inline: base64.StdEncoding.EncodeToString([]byte("bundle"))},
		SecretSources: []v1.SecretSourceRef{{
			Type: v1.SecretSourceTypeKubernetes, Namespace: "tenant-default", Name: "api-token", Key: "token",
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "API_TOKEN"},
		}},
		Spark: &v1.SparkSpec{MainApplicationFile: "local:///work/main.py", ExecutorCount: 1},
	}
	handle, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var source corev1.Secret
	sourceKey := types.NamespacedName{Namespace: spec.Namespace, Name: k8s.InlineSecretNameFor(spec)}
	if err := e.client.Get(context.Background(), sourceKey, &source); err != nil {
		t.Fatalf("inline source Secret missing: %v", err)
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := e.Cancel(context.Background(), handle, jobOf(spec)); err != nil {
		t.Fatalf("idempotent Cancel: %v", err)
	}
	if err := e.client.Get(context.Background(), sourceKey, &source); !apierrors.IsNotFound(err) {
		t.Fatalf("inline source Secret remains: %v", err)
	}
	var customerSecret corev1.Secret
	if err := e.client.Get(context.Background(), client.ObjectKeyFromObject(referenced), &customerSecret); err != nil {
		t.Fatalf("referenced Secret must remain: %v", err)
	}
}

// A Kubernetes API error while resolving a secret reaches the user with the
// API server's own words after Zenthros's reason; the value is never read.
func TestSecretSourceErrorAppendsAPIServerText(t *testing.T) {
	e := newTestExecutor(t)
	e.client = forbiddenSecretClient{Client: e.client}
	want := apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "api-token", errors.New("denied")).Error()
	_, err := e.Submit(context.Background(), secretJobSpec(v1.SecretMaterializationEnv), nil)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Submit error %v does not carry the API server text %q", err, want)
	}
	if code, ok := backend.SecretSourceReason(err); !ok || code != v1.ReasonSecretAccessDenied {
		t.Fatalf("reason = %q, want %s", code, v1.ReasonSecretAccessDenied)
	}
}

func TestRayClusterFailedMessageIncludesStatusReason(t *testing.T) {
	cluster := &rayv1.RayCluster{Status: rayv1.RayClusterStatus{
		State:  rayv1.Failed,
		Reason: "RayCluster head Pod failed: container ray-head OOMKilled",
		Conditions: []metav1.Condition{{
			Type: "HeadPodReady", Status: metav1.ConditionFalse, Reason: "HeadPodNotReady",
			Message: "containers with unready status: [ray-head]",
		}},
	}}
	got := rayClusterState(cluster).Message
	for _, want := range []string{"RayCluster head Pod failed: container ray-head OOMKilled", "containers with unready status: [ray-head]"} {
		if !strings.Contains(got, want) {
			t.Fatalf("message %q lacks KubeRay's %q", got, want)
		}
	}
}

// A Job on Kubernetes is a batch/v1 Job running the command in the workload
// container, with the env the controller sent. Its handle is job:ns/name,
// a resubmit adopts it, a changed command is a spec mismatch, the Job's
// Failed condition reaches Status word for word, and Cancel deletes it.
func TestK8sExecutorSubmitsBatchJob(t *testing.T) {
	e := newTestExecutor(t)
	spec := jobspec.JobSpec{
		ID: "uid-batch", Name: "batch-job", Namespace: "tenant-default", Tenant: "default",
		Kind: v1.WorkloadKindJob, Command: []string{"node", "main.js"}, Image: "node:22-slim", BackoffLimit: 2,
	}
	ctx := context.Background()
	handle, err := e.Submit(ctx, spec, map[string]string{"JOB_MARKER": "one"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if handle != "job:tenant-default/batch-job" {
		t.Fatalf("handle = %q", handle)
	}
	var job batchv1.Job
	key := types.NamespacedName{Namespace: spec.Namespace, Name: spec.Name}
	if err := e.client.Get(ctx, key, &job); err != nil {
		t.Fatalf("get Job: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Image != "node:22-slim" || !slices.Equal(c.Command, []string{"node", "main.js"}) || *job.Spec.BackoffLimit != 2 {
		t.Fatalf("Job = image %q command %v backoff %d", c.Image, c.Command, *job.Spec.BackoffLimit)
	}
	if !slices.ContainsFunc(c.Env, func(v corev1.EnvVar) bool { return v.Name == "JOB_MARKER" && v.Value == "one" }) {
		t.Fatalf("env = %v", c.Env)
	}
	if again, err := e.Submit(ctx, spec, map[string]string{"JOB_MARKER": "one"}); err != nil || again != handle {
		t.Fatalf("resubmit = %q, %v", again, err)
	}
	changed := spec
	changed.Command = []string{"node", "other.js"}
	if _, err := e.Submit(ctx, changed, nil); !errors.Is(err, backend.ErrSpecMismatch) {
		t.Fatalf("changed command = %v, want ErrSpecMismatch", err)
	}

	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}
	if err := e.client.Status().Update(ctx, &job); err != nil {
		t.Fatalf("update status: %v", err)
	}
	state, err := e.Status(ctx, handle)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	want := "Job/batch-job condition Failed=True reason=BackoffLimitExceeded: Job has reached the specified backoff limit"
	if state.Phase != v1.PhaseFailed || state.Message != want {
		t.Fatalf("state = %s %q, want Failed %q", state.Phase, state.Message, want)
	}

	if err := e.Cancel(ctx, handle, jobOf(spec)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := e.client.Get(ctx, key, &job); !apierrors.IsNotFound(err) {
		t.Fatalf("Job after Cancel: %v", err)
	}
	if _, err := e.Status(ctx, handle); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Status after Cancel = %v, want ErrNotFound (the controller resubmits)", err)
	}
}

// A decrypted payload's Secret is the job's by its ID: a Secret already
// under the job's name pattern (a user's, or another incarnation's of the
// same name) is not written over, and one under the ID's name that is not
// the job's refuses the Submit instead of being taken.
func TestAnEncryptedPayloadWritesNoSecretItDoesNotOwn(t *testing.T) {
	bundle, err := agentage.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := agepayload.Encrypt([]byte("value"), bundle.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	spec := secretJobSpec(v1.SecretMaterializationEnv)
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{
			Ciphertext: ciphertext, Recipients: bundle.Recipients(),
			KeyID: agepayload.KeyID(bundle.Recipients()[0]),
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T15_SECRET"},
	}}
	theirs := func(name string) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: spec.Namespace, Name: name,
			Labels: map[string]string{owner.LabelJobID: "uid-other"}}, Data: map[string][]byte{"value": []byte("theirs")}}
	}

	byName := theirs("zenthros-enc-" + spec.Name + "-0")
	e := newTestExecutor(t)
	e.Decryptor = bundle
	if err := e.client.Create(context.Background(), byName); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	kept := &corev1.Secret{}
	if err := e.client.Get(context.Background(), client.ObjectKeyFromObject(byName), kept); err != nil || string(kept.Data["value"]) != "theirs" {
		t.Fatalf("the Secret under the job's name was written over: %v %q", err, kept.Data["value"])
	}

	byID := theirs(encryptedSecretName(spec.ID, 0))
	e = newTestExecutor(t)
	e.Decryptor = bundle
	if err := e.client.Create(context.Background(), byID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(context.Background(), spec, nil); err == nil {
		t.Fatal("Submit took a Secret that is not the job's")
	}
	if err := e.client.Get(context.Background(), client.ObjectKeyFromObject(byID), kept); err != nil || string(kept.Data["value"]) != "theirs" {
		t.Fatalf("the Secret that is not the job's was written over: %v %q", err, kept.Data["value"])
	}
}

// A role the API server answered "not allowed" for says so plainly in the
// job's status: the check had no error to wrap ("%!w(<nil>)").
func TestARoleNotHonouredSaysSoPlainly(t *testing.T) {
	if msg := notHonoured("team-a", nil).Error(); strings.Contains(msg, "%!") {
		t.Fatalf("message = %q", msg)
	}
	cause := errors.New("connection refused")
	if err := notHonoured("team-a", cause); !errors.Is(err, cause) {
		t.Fatalf("the check's error is lost: %v", err)
	}
}
