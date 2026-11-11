// SPDX-License-Identifier: Apache-2.0

// Package podsecurity is how a Workspace's namespace is locked: its Pod
// Security Admission labels and the NetworkPolicy that keeps other
// Workspaces out. The controller's Workspace reconciler and a cluster
// agent's namespaces (agent/k8sagent) apply the same; a leaf, as the agent
// links it.
package podsecurity

import (
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
)

// Pod Security Admission labels on a Workspace namespace.
const (
	psaEnforce = "pod-security.kubernetes.io/enforce"
	psaWarn    = "pod-security.kubernetes.io/warn"
	psaAudit   = "pod-security.kubernetes.io/audit"

	// Default is the enforced level when the controller is not
	// told otherwise. baseline refuses privileged pods, host namespaces and
	// hostPath volumes; Ray, Spark and the source init containers all run
	// under it.
	Default = "baseline"

	// IsolationPolicyName is the Workspace namespace's NetworkPolicy.
	IsolationPolicyName = "zenthros-workspace-isolation"
)

var podSecurityRank = map[string]int{"privileged": 0, "baseline": 1, "restricted": 2}

// Labels are the PSA labels for a Workspace namespace that has
// existing labels: enforce is level, unless the namespace already enforces a
// stricter one (an adopted namespace keeps what its owner chose); warn and
// audit are restricted, so what baseline lets through is still visible.
func Labels(level string, existing map[string]string) map[string]string {
	if _, ok := podSecurityRank[level]; !ok {
		level = Default
	}
	if current, ok := podSecurityRank[existing[psaEnforce]]; ok && current > podSecurityRank[level] {
		level = existing[psaEnforce]
	}
	return map[string]string{psaEnforce: level, psaWarn: "restricted", psaAudit: "restricted"}
}

// IsolationPolicy admits ingress to the Workspace's pods from its own
// namespace and from namespaces that are not a Workspace's (the KubeRay and
// Spark operators, monitoring, ingress controllers), and from no other
// Workspace. Without it one tenant's pod reached another's Ray head, whose
// job API runs any code it is sent. Egress is left open: the other side's
// own policy is what refuses it.
func IsolationPolicy(namespace string, labels map[string]string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: IsolationPolicyName, Namespace: namespace, Labels: labels},
		Spec:       IsolationPolicySpec(),
	}
}

func IsolationPolicySpec() networkingv1.NetworkPolicySpec {
	return networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		Ingress: []networkingv1.NetworkPolicyIngressRule{{
			From: []networkingv1.NetworkPolicyPeer{
				{PodSelector: &metav1.LabelSelector{}},
				{NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key: v1.LabelWorkspace, Operator: metav1.LabelSelectorOpDoesNotExist,
				}}}},
			},
		}},
	}
}
