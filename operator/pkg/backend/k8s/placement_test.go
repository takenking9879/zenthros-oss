// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

// spec.placement reaches the pods as native constraints: spread by zone is a
// topologySpreadConstraint over the job's labels, anti-affinity by machine a
// required podAntiAffinity on kubernetes.io/hostname that never selects the
// job's own pods, and provider rules are not rendered.
func TestK8sPlacementRendersTopologySpread(t *testing.T) {
	two := int32(2)
	spec := jobspec.JobSpec{ID: "uid-1", Name: "shard-a", Namespace: "tenant-a", Tenant: "a", Entrypoint: "true", Image: "busybox",
		Labels: map[string]string{"app": "shard"},
		Placement: &v1.PlacementPolicy{
			Spread: []v1.SpreadRule{{TopologyKey: v1.TopologyZone, MaxSkew: 1, MinDomains: 2}, {TopologyKey: v1.TopologyProvider, MaxSkew: 1}},
			AntiAffinity: []v1.JobAffinityTerm{
				{JobSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "x"}}, TopologyKey: v1.TopologyMachine}},
			Affinity: []v1.JobAffinityTerm{{JobName: "etl", TopologyKey: v1.TopologyRegion}},
		}}
	job := BuildJob(spec)
	pod := job.Spec.Template

	for k, v := range map[string]string{"app": "shard", "zenthros.ai/job-id": "uid-1", "zenthros.ai/job-name": "shard-a", "zenthros.ai/tenant": "a"} {
		if pod.Labels[k] != v {
			t.Errorf("pod label %s=%q, want %q (%v)", k, pod.Labels[k], v, pod.Labels)
		}
	}
	wantSpread := []corev1.TopologySpreadConstraint{{
		MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "shard"}}, MinDomains: &two,
	}}
	if !reflect.DeepEqual(pod.Spec.TopologySpreadConstraints, wantSpread) {
		t.Fatalf("spread:\n got %+v\nwant %+v", pod.Spec.TopologySpreadConstraints, wantSpread)
	}
	notSelf := metav1.LabelSelectorRequirement{Key: "zenthros.ai/job-id", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"uid-1"}}
	wantAnti := []corev1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname", LabelSelector: &metav1.LabelSelector{
		MatchLabels: map[string]string{"team": "x"}, MatchExpressions: []metav1.LabelSelectorRequirement{notSelf}}}}
	if pod.Spec.Affinity == nil || !reflect.DeepEqual(pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, wantAnti) {
		t.Fatalf("anti-affinity: %+v", pod.Spec.Affinity)
	}
	wantAff := []corev1.PodAffinityTerm{{TopologyKey: "topology.kubernetes.io/region", LabelSelector: &metav1.LabelSelector{
		MatchLabels: map[string]string{"zenthros.ai/job-name": "etl"}, MatchExpressions: []metav1.LabelSelectorRequirement{notSelf}}}}
	if !reflect.DeepEqual(pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, wantAff) {
		t.Fatalf("affinity: %+v", pod.Spec.Affinity.PodAffinity)
	}

	// Ray head and workers get the same constraints and labels.
	spec.Workers = v1.WorkerGroup{Min: 1, Max: 1}
	ray := BuildRayJob(spec)
	for _, tpl := range []corev1.PodTemplateSpec{ray.Spec.RayClusterSpec.HeadGroupSpec.Template, ray.Spec.RayClusterSpec.WorkerGroupSpecs[0].Template} {
		if !reflect.DeepEqual(tpl.Spec.TopologySpreadConstraints, wantSpread) || tpl.Labels["app"] != "shard" {
			t.Fatalf("ray pod: labels %v spread %+v", tpl.Labels, tpl.Spec.TopologySpreadConstraints)
		}
	}

	// No placement, no constraints; a name too long for a label is left off.
	spec.Placement = nil
	spec.Name = "a-very-long-job-name-that-is-longer-than-sixty-three-characters-long"
	pod = BuildJob(spec).Spec.Template
	if pod.Spec.Affinity != nil || pod.Spec.TopologySpreadConstraints != nil {
		t.Fatalf("constraints without placement: %+v", pod.Spec)
	}
	if _, ok := pod.Labels["zenthros.ai/job-name"]; ok {
		t.Fatal("an invalid label value was set")
	}
}
