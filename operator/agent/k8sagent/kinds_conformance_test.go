// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"fmt"
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
	"github.com/zenthros/operator/pkg/backend/spark"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
)

// kindCase is what the conformance test needs of one kind: a spec, how its
// native object reports running, and the labels its kind's pod lister picks.
// A kind whose operator decides its fate (Manifest) has no status case.
type kindCase struct {
	spec      jobspec.JobSpec
	running   func(t *testing.T, c client.Client, spec jobspec.JobSpec)
	podLabels func(t *testing.T, c client.Client, spec jobspec.JobSpec) map[string]string
}

func kindCases() map[string]kindCase {
	base := func(kind string) jobspec.JobSpec {
		return jobspec.JobSpec{ID: "uid-" + strings.ToLower(kind), Name: "w", Namespace: "tenant-default", Tenant: "default",
			Kind: kind, Image: "team/train:1"}
	}
	batchSpec := base(v1.WorkloadKindJob)
	batchSpec.Command = []string{"python", "main.py"}
	rayJobSpec := base(v1.WorkloadKindRayJob)
	rayJobSpec.Entrypoint = "python main.py"
	sparkSpec := base(v1.WorkloadKindSparkApplication)
	sparkSpec.Spark = &v1.SparkSpec{MainApplicationFile: "local:///app.py", ExecutorCount: 1}
	return map[string]kindCase{
		v1.WorkloadKindJob: {
			spec: batchSpec,
			running: func(t *testing.T, c client.Client, spec jobspec.JobSpec) {
				var job batchv1.Job
				get(t, c, spec, &job)
				// The apiserver gives every Job its own selector.
				job.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"controller-uid": "c-1"}}
				update(t, c, &job)
				job.Status.Active = 1
				if err := c.Status().Update(context.Background(), &job); err != nil {
					t.Fatal(err)
				}
			},
			podLabels: func(_ *testing.T, _ client.Client, spec jobspec.JobSpec) map[string]string {
				return map[string]string{"controller-uid": "c-1", k8spod.LabelJobID: spec.ID}
			},
		},
		v1.WorkloadKindRayJob: {
			spec: rayJobSpec,
			running: func(t *testing.T, c client.Client, spec jobspec.JobSpec) {
				var rayJob rayv1.RayJob
				get(t, c, spec, &rayJob)
				rayJob.Status.JobDeploymentStatus, rayJob.Status.JobStatus, rayJob.Status.RayClusterName = rayv1.JobDeploymentStatusRunning, rayv1.JobStatusRunning, "w-ray"
				update(t, c, &rayJob)
			},
			podLabels: func(_ *testing.T, _ client.Client, spec jobspec.JobSpec) map[string]string {
				return map[string]string{"ray.io/cluster": "w-ray", "job-name": spec.Name, k8spod.LabelJobID: spec.ID}
			},
		},
		v1.WorkloadKindRayCluster: {
			spec: base(v1.WorkloadKindRayCluster),
			running: func(t *testing.T, c client.Client, spec jobspec.JobSpec) {
				var cluster rayv1.RayCluster
				get(t, c, spec, &cluster)
				cluster.Status.State = rayv1.Ready
				update(t, c, &cluster)
			},
			podLabels: func(_ *testing.T, _ client.Client, spec jobspec.JobSpec) map[string]string {
				return map[string]string{"ray.io/cluster": spec.Name, k8spod.LabelJobID: spec.ID}
			},
		},
		v1.WorkloadKindSparkApplication: {
			spec: sparkSpec,
			running: func(t *testing.T, c client.Client, spec jobspec.JobSpec) {
				application := spark.NewApplication(spec.Namespace, spec.Name)
				get(t, c, spec, application)
				if err := unstructured.SetNestedField(application.Object, "RunningHealthy", "status", "currentState", "currentStateSummary"); err != nil {
					t.Fatal(err)
				}
				update(t, c, application)
			},
			podLabels: func(_ *testing.T, _ client.Client, spec jobspec.JobSpec) map[string]string {
				return map[string]string{spark.ApplicationNameLabel: spec.Name, spark.JobIDLabel: spec.ID}
			},
		},
		v1.WorkloadKindManifest: {spec: manifestSpec(pytorchObject)},
	}
}

func get(t *testing.T, c client.Client, spec jobspec.JobSpec, obj client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: spec.Namespace, Name: spec.Name}, obj); err != nil {
		t.Fatal(err)
	}
}

func update(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	if err := c.Update(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
}

// conformanceExecutor runs a kind's case: the Manifest one needs its REST
// mapper, the others the scheme of the kinds they create.
func conformanceExecutor(t *testing.T, kind string, funcs *interceptor.Funcs, objects ...client.Object) *Executor {
	t.Helper()
	if kind == v1.WorkloadKindManifest {
		return manifestExecutor(t, funcs, objects...)
	}
	scheme := testutil.Scheme(t, rayv1.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme, appsv1.AddToScheme, spark.AddToScheme, networkingv1.AddToScheme, rbacv1.AddToScheme)
	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(withSecretRole(objects)...).WithStatusSubresource(&batchv1.Job{}).
		WithInterceptorFuncs(withUIDs(funcs))
	e, err := New(builder.Build(), &rest.Config{Host: "http://localhost:1"}, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	withSecretAccess(e)
	return e
}

// withUIDs is funcs with what the fake client lacks: a created or applied
// object gets a UID, as the API server gives it. A funcs Create that passes
// on to the client gets none; one that needs it calls testutil.CreateWithUID.
func withUIDs(funcs *interceptor.Funcs) interceptor.Funcs {
	with := interceptor.Funcs{}
	if funcs != nil {
		with = *funcs
	}
	if with.Create == nil {
		with.Create = testutil.CreateWithUID
	}
	if with.Apply == nil {
		with.Apply = testutil.ApplyWithUID
	}
	return with
}

func workloadPod(name string, labels map[string]string, phase corev1.PodPhase, state corev1.ContainerState, container string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-default", Labels: labels},
		Status: corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{{
			Name: container, State: state, ImageID: "docker.io/team/train@sha256:ab",
		}}},
	}
}

// Every kind in the translators table keeps the same promises: a resubmit
// adopts its own object, a create whose answer was lost is its own (C-09),
// another job's object of that name is refused (N11); a dead worker of a
// running workload is the native controller's to retry (C-03), a workload
// that cannot pull its image says so (c2-F1#4, RayCluster included), and
// the digest its pods pulled is reported for every kind (c2-F1#5), and a
// cancel deletes only what carries its job ID (K1/06#1).
func TestEveryKindKeepsTheSamePromises(t *testing.T) {
	cases := kindCases()
	for _, tr := range translators {
		c, ok := cases[tr.kind]
		if !ok {
			t.Fatalf("kind %s has no conformance case", tr.kind)
		}
		t.Run(tr.kind, func(t *testing.T) {
			ctx := context.Background()
			t.Run("a resubmit adopts its own object", func(t *testing.T) {
				e := conformanceExecutor(t, tr.kind, nil)
				first, err := e.Submit(ctx, c.spec, nil)
				if err != nil {
					t.Fatal(err)
				}
				if again, err := e.Submit(ctx, c.spec, nil); err != nil || again != first {
					t.Fatalf("resubmit = %q, %v", again, err)
				}
			})
			t.Run("a create whose answer was lost is its own", func(t *testing.T) {
				lost := &interceptor.Funcs{Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, secret := obj.(*corev1.Secret); secret || obj.GetName() != c.spec.Name {
						return cl.Create(ctx, obj, opts...)
					}
					if err := cl.Create(ctx, obj, opts...); err != nil {
						return err
					}
					return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "workload"}, obj.GetName())
				}}
				if _, err := conformanceExecutor(t, tr.kind, lost).Submit(ctx, c.spec, nil); err != nil {
					t.Fatalf("Submit after a lost create answer: %v", err)
				}
			})
			t.Run("another job's object of the name is refused", func(t *testing.T) {
				e := conformanceExecutor(t, tr.kind, nil)
				earlier := c.spec
				earlier.ID = "uid-earlier"
				if _, err := e.Submit(ctx, earlier, nil); err != nil {
					t.Fatal(err)
				}
				_, err := e.Submit(ctx, c.spec, nil)
				if !errors.Is(err, backend.ErrSpecMismatch) || !strings.Contains(err.Error(), "uid-earlier") {
					t.Fatalf("Submit over another job's object = %v", err)
				}
			})
			t.Run("a cancel deletes its own object and leaves another job's", func(t *testing.T) {
				e := conformanceExecutor(t, tr.kind, nil)
				earlier := c.spec
				earlier.ID = "uid-earlier"
				h, err := e.Submit(ctx, earlier, nil)
				if err != nil {
					t.Fatal(err)
				}
				_, r, err := translatorForHandle(h)
				if err != nil {
					t.Fatal(err)
				}
				workload := tr.object(r)
				if err := e.Cancel(ctx, h, jobOf(c.spec)); err != nil {
					t.Fatal(err)
				}
				if err := e.client.Get(ctx, client.ObjectKeyFromObject(workload), workload); err != nil {
					t.Fatalf("a cancel of %s deleted uid-earlier's object of the name: %v", c.spec.ID, err)
				}
				if err := e.Cancel(ctx, h, jobOf(earlier)); err != nil {
					t.Fatal(err)
				}
				if err := e.client.Get(ctx, client.ObjectKeyFromObject(workload), workload); !apierrors.IsNotFound(err) {
					t.Fatalf("its own job's cancel left the object: %v", err)
				}
			})
			if c.running == nil {
				return
			}
			t.Run("a dead worker of a running workload is retried by its controller", func(t *testing.T) {
				e := conformanceExecutor(t, tr.kind, nil)
				h, err := e.Submit(ctx, c.spec, nil)
				if err != nil {
					t.Fatal(err)
				}
				c.running(t, e.client, c.spec)
				exited := corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "Error"}}
				if err := e.client.Create(ctx, workloadPod("w-worker-0", c.podLabels(t, e.client, c.spec), corev1.PodFailed, exited, tr.container)); err != nil {
					t.Fatal(err)
				}
				state, err := e.Status(ctx, h)
				if err != nil || state.Phase != v1.PhaseRunning {
					t.Fatalf("state = %s %+v, %v; want Running", state.Phase, state.Reasons, err)
				}
				if state.ResolvedImage != "docker.io/team/train@sha256:ab" {
					t.Fatalf("resolved image = %q", state.ResolvedImage)
				}
			})
			t.Run("a workload that cannot pull its image says so", func(t *testing.T) {
				e := conformanceExecutor(t, tr.kind, nil)
				h, err := e.Submit(ctx, c.spec, nil)
				if err != nil {
					t.Fatal(err)
				}
				pulling := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "pull access denied"}}
				if err := e.client.Create(ctx, workloadPod("w-0", c.podLabels(t, e.client, c.spec), corev1.PodPending, pulling, tr.container)); err != nil {
					t.Fatal(err)
				}
				if tr.kind == v1.WorkloadKindJob {
					var job batchv1.Job
					get(t, e.client, c.spec, &job)
					job.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"controller-uid": "c-1"}}
					update(t, e.client, &job)
				}
				state, err := e.Status(ctx, h)
				if err != nil || state.Phase != v1.PhaseFailed || len(state.Reasons) == 0 || state.Reasons[0].Code != v1.ReasonImagePullFailed {
					t.Fatalf("state = %s %+v, %v; want Failed IMAGE_PULL_FAILED", state.Phase, state.Reasons, err)
				}
			})
		})
	}
}

// everyCall counts what reaches the cluster's apiserver.
type everyCall struct {
	client.Client
	calls []string
}

func (c *everyCall) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.calls = append(c.calls, fmt.Sprintf("get %T", obj))
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *everyCall) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.calls = append(c.calls, fmt.Sprintf("list %T", list))
	return c.Client.List(ctx, list, opts...)
}

// A running workload's 2-second poll asks the apiserver nothing, whatever its
// kind: its object and pods come from the agent's watch (C-11/12). Only
// Batch Jobs did; a RayJob, a RayCluster or a SparkApplication was a GET and
// pod and event LISTs per poll.
func TestARunningWorkloadOfEveryKindIsReadFromTheCache(t *testing.T) {
	cases := kindCases()
	for _, tr := range translators {
		c := cases[tr.kind]
		if c.running == nil {
			continue
		}
		t.Run(tr.kind, func(t *testing.T) {
			ctx := context.Background()
			e := conformanceExecutor(t, tr.kind, nil)
			h, err := e.Submit(ctx, c.spec, nil)
			if err != nil {
				t.Fatal(err)
			}
			c.running(t, e.client, c.spec)
			alive := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			if err := e.client.Create(ctx, workloadPod("w-0", c.podLabels(t, e.client, c.spec), corev1.PodRunning, alive, tr.container)); err != nil {
				t.Fatal(err)
			}
			e.StatusCache = e.client
			calls := &everyCall{Client: e.client}
			e.client = calls
			state, err := e.Status(ctx, h)
			if err != nil || state.Phase != v1.PhaseRunning {
				t.Fatalf("state = %s, %v", state.Phase, err)
			}
			if len(calls.calls) != 0 {
				t.Fatalf("a steady poll reached the apiserver: %v", calls.calls)
			}
		})
	}
}

// jobOf is the owner a Cancel of spec's workload names, as the agent builds
// it from CancelRequest.id.
func jobOf(spec jobspec.JobSpec) owner.Ref { return owner.Ref{Kind: "ZenthrosJob", JobID: spec.ID} }

// A restarted agent finds the workloads it installed by their job-id label,
// whatever their kind, so its first heartbeat does not report an idle
// cluster running jobs. One that ended is not work.
func TestARestartedAgentFindsTheRunningWorkloadOfEveryKind(t *testing.T) {
	cases := kindCases()
	for _, tr := range translators {
		c := cases[tr.kind]
		if c.running == nil {
			continue
		}
		t.Run(tr.kind, func(t *testing.T) {
			ctx := context.Background()
			e := conformanceExecutor(t, tr.kind, nil)
			c.spec.TargetBackend = "k8s-self"
			h, err := e.Submit(ctx, c.spec, nil)
			if err != nil {
				t.Fatal(err)
			}
			c.running(t, e.client, c.spec)
			jobs, err := e.InstalledJobs(ctx, "k8s-self")
			if err != nil {
				t.Fatal(err)
			}
			if want := []backend.InstalledJob{{JobID: c.spec.ID, Handle: h}}; !slices.Equal(jobs, want) {
				t.Fatalf("installed = %v, want %v", jobs, want)
			}
		})
	}
}

// A cluster can run another agent beside this one (an attached cluster beside
// the hub's own), in the same namespaces. Its workloads are not this agent's: a
// restart tracked them, and a detach then stopped them, running or ended.
func TestAWorkloadOfAnotherBackendIsNotThisAgents(t *testing.T) {
	cases := kindCases()
	for _, tr := range translators {
		c := cases[tr.kind]
		if c.running == nil {
			continue
		}
		t.Run(tr.kind, func(t *testing.T) {
			ctx := context.Background()
			e := conformanceExecutor(t, tr.kind, nil)
			c.spec.TargetBackend = "k8s-other"
			if _, err := e.Submit(ctx, c.spec, nil); err != nil {
				t.Fatal(err)
			}
			c.running(t, e.client, c.spec)
			installed, err := e.InstalledJobs(ctx, "k8s-self")
			if err != nil {
				t.Fatal(err)
			}
			if len(installed) != 0 {
				t.Fatalf("another backend's running workload is this agent's: %v", installed)
			}
			ended, err := e.EndedJobs(ctx, "k8s-self")
			if err != nil {
				t.Fatal(err)
			}
			if len(ended) != 0 {
				t.Fatalf("another backend's workload is listed as this agent's: %v", ended)
			}
			if mine, err := e.InstalledJobs(ctx, "k8s-other"); err != nil || len(mine) != 1 {
				t.Fatalf("its own agent finds %v, %v; want its workload", mine, err)
			}
		})
	}
}

// A kind the agent may not list (custom RBAC without RayJobs) hides only
// that kind: the restarted agent still tracks the workloads of the others,
// and says what it could not list. The tracker only adds, so a partial list
// is no claim that nothing else runs.
func TestAKindTheAgentMayNotListHidesOnlyThatKind(t *testing.T) {
	ctx := context.Background()
	c := kindCases()[v1.WorkloadKindJob]
	forbidden := &interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if l, ok := list.(*metav1.PartialObjectMetadataList); ok && l.Kind == "RayJobList" {
			return apierrors.NewForbidden(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, "", errors.New("rbac"))
		}
		return cl.List(ctx, list, opts...)
	}}
	e := conformanceExecutor(t, v1.WorkloadKindJob, forbidden)
	c.spec.TargetBackend = "k8s-self"
	h, err := e.Submit(ctx, c.spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.running(t, e.client, c.spec)
	jobs, err := e.InstalledJobs(ctx, "k8s-self")
	if want := []backend.InstalledJob{{JobID: c.spec.ID, Handle: h}}; !slices.Equal(jobs, want) {
		t.Fatalf("installed = %v, want %v", jobs, want)
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("err = %v, want the RayJob list's Forbidden", err)
	}
}
