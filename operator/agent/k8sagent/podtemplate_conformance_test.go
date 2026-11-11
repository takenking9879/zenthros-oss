// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend/k8s"
	"github.com/zenthros/operator/pkg/backend/k8spod"
	"github.com/zenthros/operator/pkg/backend/spark"
	"github.com/zenthros/operator/pkg/jobspec"
)

// Every kind's workload pods carry the job's placement and the labels other
// jobs' placement rules select (C-10, c2-F1#7), and every object its
// Workspace (F1#8): Spark's pods had neither, Ray's lacked the tenant and
// Workspace.
func TestEveryWorkloadPodCarriesTheJobsPlacementAndLabels(t *testing.T) {
	placed := func(spec jobspec.JobSpec) jobspec.JobSpec {
		spec.Workspace = "ws"
		spec.Labels = map[string]string{"team": "a"}
		spec.Placement = &v1.PlacementPolicy{Affinity: []v1.JobAffinityTerm{{JobName: "producer", TopologyKey: v1.TopologyMachine}}}
		return spec
	}
	type built struct {
		object metav1.Object
		pods   []corev1.PodTemplateSpec
	}
	builds := map[string]func(jobspec.JobSpec) built{
		v1.WorkloadKindJob: func(spec jobspec.JobSpec) built {
			job := k8s.BuildJob(spec)
			return built{job, []corev1.PodTemplateSpec{job.Spec.Template}}
		},
		v1.WorkloadKindRayJob: func(spec jobspec.JobSpec) built {
			rayJob := k8s.BuildRayJob(spec)
			cluster := rayJob.Spec.RayClusterSpec
			pods := []corev1.PodTemplateSpec{cluster.HeadGroupSpec.Template}
			for _, group := range cluster.WorkerGroupSpecs {
				pods = append(pods, group.Template)
			}
			return built{rayJob, pods}
		},
		v1.WorkloadKindRayCluster: func(spec jobspec.JobSpec) built {
			cluster := k8s.BuildRayCluster(spec)
			pods := []corev1.PodTemplateSpec{cluster.Spec.HeadGroupSpec.Template}
			for _, group := range cluster.Spec.WorkerGroupSpecs {
				pods = append(pods, group.Template)
			}
			return built{cluster, pods}
		},
		v1.WorkloadKindSparkApplication: func(spec jobspec.JobSpec) built {
			application, err := spark.Build(spec)
			if err != nil {
				t.Fatal(err)
			}
			var pods []corev1.PodTemplateSpec
			for _, role := range []string{"driverSpec", "executorSpec"} {
				raw, _, _ := unstructured.NestedMap(application.Object, "spec", role, "podTemplateSpec")
				var pod corev1.PodTemplateSpec
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &pod); err != nil {
					t.Fatal(err)
				}
				pods = append(pods, pod)
			}
			return built{application, pods}
		},
	}
	cases := kindCases()
	for kind, build := range builds {
		t.Run(kind, func(t *testing.T) {
			spec := placed(cases[kind].spec)
			got := build(spec)
			if got.object.GetLabels()[v1.LabelWorkspace] != "ws" || got.object.GetLabels()[k8spod.LabelTenant] != spec.Tenant {
				t.Errorf("object labels %v lack the tenant or Workspace", got.object.GetLabels())
			}
			if len(got.pods) == 0 {
				t.Fatal("no pod templates")
			}
			for _, pod := range got.pods {
				for k, v := range map[string]string{"team": "a", k8spod.LabelJobID: spec.ID, k8spod.LabelJobName: spec.Name,
					k8spod.LabelTenant: spec.Tenant, v1.LabelWorkspace: "ws"} {
					if pod.Labels[k] != v {
						t.Errorf("pod label %s=%q, want %q (labels %v)", k, pod.Labels[k], v, pod.Labels)
					}
				}
				if pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
					t.Errorf("a pod template has no rendered placement: %+v", pod.Spec.Affinity)
				}
			}
		})
	}
}
