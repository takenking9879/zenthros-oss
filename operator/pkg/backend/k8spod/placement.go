// SPDX-License-Identifier: Apache-2.0

package k8spod

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	v1 "github.com/zenthros/operator/api/v1"
)

// Pod labels every workload pod carries, so other jobs' placement rules can
// select it.
const (
	LabelJobID   = "zenthros.ai/job-id"
	LabelJobName = "zenthros.ai/job-name"
)

// NodeTopologyKey is the node label a Zenthros topology key means inside a
// cluster; "" for provider, which is the cluster's, not a node's.
func NodeTopologyKey(key v1.TopologyKey) string {
	switch key {
	case v1.TopologyMachine:
		return corev1.LabelHostname
	case v1.TopologyZone:
		return corev1.LabelTopologyZone
	case v1.TopologyRegion:
		return corev1.LabelTopologyRegion
	}
	return ""
}

// PodLabels are the labels a workload pod carries: the job's own, then
// Zenthros's, which win. The job name is a label only when it is a valid
// label value (63 characters at most).
func PodLabels(jobID, jobName string, jobLabels map[string]string) map[string]string {
	labels := make(map[string]string, len(jobLabels)+2)
	for k, v := range jobLabels {
		labels[k] = v
	}
	labels[LabelJobID] = jobID
	if len(validation.IsValidLabelValue(jobName)) == 0 {
		labels[LabelJobName] = jobName
	}
	return labels
}

// ApplyPlacement renders spec.placement as the pod's native constraints:
// spread rules as topologySpreadConstraints, affinity and anti-affinity as
// required pod (anti-)affinity. Every rule is hard (DoNotSchedule,
// required): the kube-scheduler leaves a pod no node satisfies Pending and
// says why. Provider rules, and pack/separateMachines (members of one job),
// have no pod form and are not rendered. Affinity terms never select the
// job's own pods.
func ApplyPlacement(pod *corev1.PodSpec, jobID string, jobLabels map[string]string, p *v1.PlacementPolicy) {
	if p == nil {
		return
	}
	for _, rule := range p.Spread {
		key := NodeTopologyKey(rule.TopologyKey)
		if key == "" {
			continue
		}
		selector := rule.JobSelector
		if selector == nil {
			selector = &metav1.LabelSelector{MatchLabels: copyLabels(jobLabels)}
		}
		constraint := corev1.TopologySpreadConstraint{
			MaxSkew:           rule.MaxSkew,
			TopologyKey:       key,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     selector.DeepCopy(),
		}
		if rule.MinDomains > 0 {
			minDomains := rule.MinDomains
			constraint.MinDomains = &minDomains
		}
		pod.TopologySpreadConstraints = append(pod.TopologySpreadConstraints, constraint)
	}
	affinity := terms(jobID, p.Affinity)
	anti := terms(jobID, p.AntiAffinity)
	if len(affinity) == 0 && len(anti) == 0 {
		return
	}
	if pod.Affinity == nil {
		pod.Affinity = &corev1.Affinity{}
	}
	if len(affinity) > 0 {
		pod.Affinity.PodAffinity = &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: affinity}
	}
	if len(anti) > 0 {
		pod.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: anti}
	}
}

func terms(jobID string, in []v1.JobAffinityTerm) []corev1.PodAffinityTerm {
	var out []corev1.PodAffinityTerm
	for _, term := range in {
		key := NodeTopologyKey(term.TopologyKey)
		if key == "" {
			continue
		}
		// Admission takes exactly one of jobName and jobSelector; a term
		// stored before that rule held neither (20#4), names no job, and
		// is no constraint.
		selector := term.JobSelector.DeepCopy()
		if term.JobName != "" {
			selector = &metav1.LabelSelector{MatchLabels: map[string]string{LabelJobName: term.JobName}}
		}
		if selector == nil {
			continue
		}
		selector.MatchExpressions = append(selector.MatchExpressions, metav1.LabelSelectorRequirement{
			Key: LabelJobID, Operator: metav1.LabelSelectorOpNotIn, Values: []string{jobID}})
		out = append(out, corev1.PodAffinityTerm{LabelSelector: selector, TopologyKey: key})
	}
	return out
}

func copyLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
