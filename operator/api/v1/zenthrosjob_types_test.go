// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResolvedWorkerLostDefaults(t *testing.T) {
	var spec ZenthrosJobSpec
	if spec.ResolvedOnWorkerLost() != OnWorkerLostReschedule {
		t.Fatalf("default onWorkerLost = %q, want %s", spec.ResolvedOnWorkerLost(), OnWorkerLostReschedule)
	}
	if spec.ResolvedWorkerLostRetryLimit() != DefaultWorkerLostRetryLimit {
		t.Fatalf("default retry limit = %d, want %d", spec.ResolvedWorkerLostRetryLimit(), DefaultWorkerLostRetryLimit)
	}
	zero := int32(0)
	spec.Resilience = &ResilienceSpec{OnWorkerLost: OnWorkerLostFail, WorkerLostRetryLimit: &zero}
	if spec.ResolvedOnWorkerLost() != OnWorkerLostFail {
		t.Fatalf("onWorkerLost = %q, want Fail", spec.ResolvedOnWorkerLost())
	}
	if spec.ResolvedWorkerLostRetryLimit() != 0 {
		t.Fatalf("retry limit = %d, want 0", spec.ResolvedWorkerLostRetryLimit())
	}
	grace := metav1.Duration{Duration: 60 * time.Second}
	spec.Resilience.JobGrace = &grace
	if spec.Resilience.JobGrace.Duration != 60*time.Second {
		t.Fatalf("jobGrace override = %s, want 60s", spec.Resilience.JobGrace.Duration)
	}
}

func TestTargetBackendsHelpers(t *testing.T) {
	target := Target{Backends: []string{"vm-a", "vm-b", BackendFallbackAny}}
	if !target.HasBackends() {
		t.Fatal("HasBackends = false, want true")
	}
	if target.FirstBackend() != "vm-a" {
		t.Fatalf("FirstBackend = %q, want vm-a", target.FirstBackend())
	}
	if !target.ContainsBackend("vm-b") || target.ContainsBackend(BackendFallbackAny) {
		t.Fatalf("ContainsBackend named vs any: %+v", target)
	}
	got := target.NamedBackends()
	if len(got) != 2 || got[0] != "vm-a" || got[1] != "vm-b" {
		t.Fatalf("NamedBackends = %v, want [vm-a vm-b]", got)
	}
	if (Target{}).HasBackends() {
		t.Fatal("empty HasBackends = true, want false")
	}
}

func TestZenthrosJobGVK(t *testing.T) {
	job := &ZenthrosJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: GroupVersion.String(),
			Kind:       "ZenthrosJob",
		},
	}
	gvk := job.GroupVersionKind()
	if gvk.Group != GroupName || gvk.Version != Version || gvk.Kind != "ZenthrosJob" {
		t.Fatalf("GVK = %s, want %s/ZenthrosJob", gvk, GroupVersion)
	}
}
