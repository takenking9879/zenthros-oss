// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"fmt"
	"strings"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8s"
	"github.com/zenthros/operator/pkg/backend/spark"
	"github.com/zenthros/operator/pkg/jobspec"
)

// translator is how one workload kind maps onto native resources in this
// cluster. Every handle is prefix + what parse reads back (namespace/name
// unless the kind says otherwise); the RayJob handle predates prefixes and has
// none, so it is matched last. resource is the API resource the kind creates:
// discovery declares the kind only when the cluster serves it and the agent
// may create it. A kind with no resource (Manifest) is always declared; what
// it may create is whatever RBAC the admin granted, and a refusal reaches the
// user as the API server wrote it.
//
// A kind says only what is its own about status: how to read its object and
// map its state (read), which pods run it (pods), the objects whose events
// speak for it (objects; nil is the object alone), and its workload container.
// What the pods and events add is one rule for every kind (Executor.status,
// k8spod.Enrich).
type translator struct {
	kind      string
	resource  schema.GroupVersionResource
	prefix    string
	parse     func(rest string) (ref, error)
	submit    func(e *Executor, ctx context.Context, spec jobspec.JobSpec) (string, error)
	read      func(e *Executor, ctx context.Context, reads client.Client, r ref) (client.Object, backend.JobState, error)
	pods      func(e *Executor, ctx context.Context, reads client.Client, obj client.Object) []corev1.Pod
	objects   func(obj client.Object) []string
	container string
	// detailOnly: the kind's operator decides its fate (k8spod.Observed).
	detailOnly bool
	// object is the native resource r names, unread: Cancel reads it and
	// deletes it only if it is the job's.
	object func(r ref) client.Object
	logs   func(e *Executor, ctx context.Context, r ref, follow bool, tailLines int) (<-chan backend.LogChunk, error)
}

// ref is the workload a handle names. gvk is set only for a Manifest, whose
// kind is whatever the user wrote.
type ref struct {
	namespace, name string
	gvk             schema.GroupVersionKind
}

func (r ref) meta() metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: r.namespace, Name: r.name}
}

func parseNamespaced(rest string) (ref, error) {
	namespace, name, err := parseHandle(rest)
	return ref{namespace: namespace, name: name}, err
}

// The fixed kinds address their resource by namespace and name alone.

func logsByName(f func(*Executor, context.Context, string, string, bool, int) (<-chan backend.LogChunk, error)) func(*Executor, context.Context, ref, bool, int) (<-chan backend.LogChunk, error) {
	return func(e *Executor, ctx context.Context, r ref, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
		return f(e, ctx, r.namespace, r.name, follow, tailLines)
	}
}

// translators lists every kind this executor can run.
var translators = []translator{
	{
		kind:      v1.WorkloadKindJob,
		resource:  batchv1.SchemeGroupVersion.WithResource("jobs"),
		prefix:    k8s.AgentHandleJobPrefix,
		submit:    (*Executor).submitBatchJob,
		read:      readBatchJob,
		pods:      batchJobPods,
		container: k8s.WorkloadContainerName,
		object:    func(r ref) client.Object { return &batchv1.Job{ObjectMeta: r.meta()} },
		logs:      logsByName((*Executor).batchJobLogs),
	},
	{
		kind:      v1.WorkloadKindRayCluster,
		resource:  rayv1.GroupVersion.WithResource("rayclusters"),
		prefix:    rayClusterHandlePrefix,
		submit:    (*Executor).submitRayCluster,
		read:      readRayCluster,
		pods:      rayClusterPods,
		container: k8s.RayContainerName,
		object:    func(r ref) client.Object { return &rayv1.RayCluster{ObjectMeta: r.meta()} },
		logs:      logsByName((*Executor).rayClusterLogs),
	},
	{
		kind:      v1.WorkloadKindSparkApplication,
		resource:  spark.GroupVersionKind.GroupVersion().WithResource("sparkapplications"),
		prefix:    spark.AgentHandlePrefix,
		submit:    (*Executor).submitSpark,
		read:      readSpark,
		pods:      sparkPods,
		container: spark.DriverContainerName,
		object:    func(r ref) client.Object { return spark.NewApplication(r.namespace, r.name) },
		logs:      logsByName((*Executor).sparkLogs),
	},
	{
		kind:      v1.WorkloadKindRayJob,
		resource:  rayv1.GroupVersion.WithResource("rayjobs"),
		submit:    (*Executor).submitRayJob,
		read:      readRayJob,
		pods:      rayJobPods,
		objects:   func(obj client.Object) []string { return k8s.RayJobObjects(obj.(*rayv1.RayJob)) },
		container: k8s.RayContainerName,
		object:    func(r ref) client.Object { return &rayv1.RayJob{ObjectMeta: r.meta()} },
		logs:      logsByName((*Executor).rayJobLogs),
	},
	{
		kind:       v1.WorkloadKindManifest,
		prefix:     manifestHandlePrefix,
		parse:      parseManifestRef,
		submit:     (*Executor).submitManifest,
		read:       readManifest,
		pods:       manifestPodsOf,
		detailOnly: true,
		object:     func(r ref) client.Object { return objectFor(r) },
		logs:       (*Executor).manifestLogs,
	},
}

func translatorForKind(kind string) (translator, bool) {
	for _, t := range translators {
		if t.kind == kind {
			return t, true
		}
	}
	return translator{}, false
}

// translatorForHandle finds the translator that issued h and reads the
// workload it names.
func translatorForHandle(h string) (translator, ref, error) {
	for _, t := range translators {
		if t.prefix == "" || !strings.HasPrefix(h, t.prefix) {
			continue
		}
		r, err := t.parseRef(strings.TrimPrefix(h, t.prefix))
		return t, r, err
	}
	rayJob, _ := translatorForKind(v1.WorkloadKindRayJob)
	r, err := parseNamespaced(h)
	return rayJob, r, err
}

func (t translator) parseRef(rest string) (ref, error) {
	if t.parse != nil {
		return t.parse(rest)
	}
	return parseNamespaced(rest)
}

// prefixed is the handle a translator issues for namespace/name.
func (t translator) handle(namespace, name string) string {
	return t.prefix + namespace + "/" + name
}

// Job: a batch/v1 Job running the command in the workload container.

func (e *Executor) submitBatchJob(ctx context.Context, spec jobspec.JobSpec) (string, error) {
	desired := k8s.BuildJob(spec)
	if e.Local != nil {
		e.Local.apply(&desired.Spec.Template.Spec)
	}
	var existing batchv1.Job
	created, err := k8s.EnsureObject(ctx, e.client, "Job", desired, &existing, func() error {
		if mismatch := k8s.BatchJobSpecMismatch(&existing, desired); mismatch != nil {
			return mismatch
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	e.logSubmitted("Job", desired, created)
	return k8s.JobAgentHandle(desired.Namespace, desired.Name), nil
}

func readBatchJob(_ *Executor, ctx context.Context, reads client.Client, r ref) (client.Object, backend.JobState, error) {
	var job batchv1.Job
	if err := reads.Get(ctx, client.ObjectKey{Namespace: r.namespace, Name: r.name}, &job); err != nil {
		return nil, backend.JobState{}, fmt.Errorf("get Job %s/%s: %w", r.namespace, r.name, err)
	}
	return &job, k8s.MapBatchJob(&job), nil
}

func batchJobPods(_ *Executor, ctx context.Context, reads client.Client, obj client.Object) []corev1.Pod {
	return k8s.BatchJobPods(ctx, reads, obj.(*batchv1.Job))
}

// cachedReads reads what the status cache holds (the objects and pods the
// agent made) from it and events through the agent's event cache; it asks
// the apiserver for everything else, and for an object the cache does not
// hold yet (just created): not found there is not found.
type cachedReads struct {
	client.Client
	cache  client.Reader
	events *eventCache
}

func (c cachedReads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.cache.Get(ctx, key, obj, opts...); err == nil {
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c cachedReads) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	switch typed := list.(type) {
	case *corev1.PodList:
		return c.cache.List(ctx, list, opts...)
	case *corev1.EventList:
		if c.events != nil {
			return c.events.list(ctx, c.Client, typed, opts...)
		}
	}
	return c.Client.List(ctx, list, opts...)
}

func (e *Executor) batchJobLogs(ctx context.Context, namespace, name string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	var job batchv1.Job
	if err := e.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &job); err != nil {
		return nil, fmt.Errorf("get Job %s/%s: %w", namespace, name, err)
	}
	pods := k8s.BatchJobPods(ctx, e.client, &job)
	if len(pods) == 0 {
		return nil, fmt.Errorf("Job %s/%s has no pods yet", namespace, name)
	}
	return e.streamPodLogs(ctx, namespace, pods[0].Name, k8s.WorkloadContainerName, follow, tailLines)
}

// RayCluster: a standing cluster, a service until cancelled.

func readRayCluster(_ *Executor, ctx context.Context, reads client.Client, r ref) (client.Object, backend.JobState, error) {
	var cluster rayv1.RayCluster
	if err := reads.Get(ctx, client.ObjectKey{Namespace: r.namespace, Name: r.name}, &cluster); err != nil {
		return nil, backend.JobState{}, fmt.Errorf("get RayCluster %s/%s: %w", r.namespace, r.name, err)
	}
	return &cluster, rayClusterState(&cluster), nil
}

func rayClusterPods(_ *Executor, ctx context.Context, reads client.Client, obj client.Object) []corev1.Pod {
	return k8s.RayClusterPods(ctx, reads, obj.(*rayv1.RayCluster))
}

func (e *Executor) rayClusterLogs(context.Context, string, string, bool, int) (<-chan backend.LogChunk, error) {
	return nil, fmt.Errorf("%w: a RayCluster has no single log; read its head pod", backend.ErrUnsupportedKind)
}

// SparkApplication: the Spark Operator's resource.

func (e *Executor) submitSpark(ctx context.Context, spec jobspec.JobSpec) (string, error) {
	desired, err := spark.Build(spec)
	if err != nil {
		return "", err
	}
	existing := spark.NewApplication(desired.GetNamespace(), desired.GetName())
	created, err := k8s.EnsureObject(ctx, e.client, "SparkApplication", desired, existing, func() error {
		if mismatch := spark.SpecMismatch(existing, desired); mismatch != nil {
			return mismatch
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	e.logSubmitted("SparkApplication", desired, created)
	return spark.AgentHandlePrefix + desired.GetNamespace() + "/" + desired.GetName(), nil
}

func readSpark(_ *Executor, ctx context.Context, reads client.Client, r ref) (client.Object, backend.JobState, error) {
	application := spark.NewApplication(r.namespace, r.name)
	if err := reads.Get(ctx, client.ObjectKeyFromObject(application), application); err != nil {
		return nil, backend.JobState{}, fmt.Errorf("get SparkApplication %s/%s: %w", r.namespace, r.name, err)
	}
	return application, spark.MapJobState(application), nil
}

func sparkPods(_ *Executor, ctx context.Context, reads client.Client, obj client.Object) []corev1.Pod {
	return spark.ApplicationPods(ctx, reads, obj.(*unstructured.Unstructured))
}

func (e *Executor) sparkLogs(ctx context.Context, namespace, name string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	ns, podName, containerName, err := e.sparkLogTarget(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return e.streamPodLogs(ctx, ns, podName, containerName, follow, tailLines)
}

// RayJob: KubeRay's job resource, on a dedicated or an existing cluster.

func (e *Executor) submitRayJob(ctx context.Context, spec jobspec.JobSpec) (string, error) {
	if err := k8s.ValidateRayClusterReference(ctx, e.client, spec); err != nil {
		return "", err
	}
	desired := k8s.BuildRayJob(spec)
	var existing rayv1.RayJob
	created, err := k8s.EnsureObject(ctx, e.client, "RayJob", desired, &existing, func() error {
		if mismatch := k8s.SpecMismatch(&existing, desired); mismatch != nil {
			return mismatch
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	e.logSubmitted("RayJob", desired, created)
	return handle(desired.Namespace, desired.Name), nil
}

func readRayJob(_ *Executor, ctx context.Context, reads client.Client, r ref) (client.Object, backend.JobState, error) {
	var rayJob rayv1.RayJob
	if err := reads.Get(ctx, client.ObjectKey{Namespace: r.namespace, Name: r.name}, &rayJob); err != nil {
		return nil, backend.JobState{}, fmt.Errorf("get RayJob %s/%s: %w", r.namespace, r.name, err)
	}
	return &rayJob, k8s.MapJobState(&rayJob), nil
}

func rayJobPods(_ *Executor, ctx context.Context, reads client.Client, obj client.Object) []corev1.Pod {
	return k8s.RayJobPods(ctx, reads, obj.(*rayv1.RayJob))
}

// logSubmitted says whether a submit made its object or found the one this
// job made before.
func (e *Executor) logSubmitted(kind string, obj client.Object, created bool) {
	if created {
		e.logger.Info("submitted "+kind, "namespace", obj.GetNamespace(), "name", obj.GetName())
		return
	}
	e.logger.Info(kind+" already exists, reconciled idempotently", "namespace", obj.GetNamespace(), "name", obj.GetName())
}
