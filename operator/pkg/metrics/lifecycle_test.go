// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
)

func TestExclusiveIdleUnit(t *testing.T) {
	cases := []struct {
		name       string
		typ        string
		ownership  string
		sharedPool bool
		want       bool
	}{
		{name: "dedicated vm", typ: v1.BackendKindVM, ownership: "External", want: true},
		{name: "managed slot", typ: v1.BackendKindVM, ownership: OwnershipManaged, want: true},
		{name: "dedicated container", typ: v1.BackendKindContainer, ownership: "External", want: true},
		{name: "managed without type", typ: "", ownership: OwnershipManaged, want: true},
		{name: "shared k8s ray cluster", typ: v1.BackendKindK8s, ownership: "External", sharedPool: true, want: false},
		{name: "ephemeral per-job raycluster", typ: v1.BackendKindK8s, ownership: OwnershipManaged, sharedPool: true, want: false},
		{name: "shared rayClusterRef pool", typ: v1.BackendKindVM, ownership: "External", sharedPool: true, want: false},
		{name: "k8s rayClusterRef", typ: v1.BackendKindK8s, sharedPool: false, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExclusiveIdleUnit(tc.typ, tc.ownership, tc.sharedPool)
			if got != tc.want {
				t.Fatalf("ExclusiveIdleUnit(%q,%q,%v) = %v, want %v", tc.typ, tc.ownership, tc.sharedPool, got, tc.want)
			}
		})
	}
}

func TestIdleCollectorEmitsOnlyExclusiveUnits(t *testing.T) {
	collector := NewIdleCollector(func() []IdleSnapshot {
		return []IdleSnapshot{
			{ID: "vm-01", Type: v1.BackendKindVM, Ownership: "External", RunningJobs: 0},
			{ID: "slot-a", Type: v1.BackendKindVM, Ownership: OwnershipManaged, RunningJobs: 0},
			{ID: "cluster-b", Type: v1.BackendKindK8s, Ownership: "External", SharedPool: true, RunningJobs: 0},
			{ID: "ray-shared", Type: v1.BackendKindK8s, Ownership: OwnershipManaged, RunningJobs: 0, SharedPool: true},
			{ID: "busy-vm", Type: v1.BackendKindVM, Ownership: "External", RunningJobs: 1},
		}
	})
	fixed := time.Date(2026, 8, 17, 12, 0, 10, 0, time.UTC)
	collector.now = func() time.Time { return fixed }
	collector.idleSince["vm-01"] = fixed.Add(-10 * time.Second)
	collector.idleSince["slot-a"] = fixed.Add(-4 * time.Second)

	want := `
# HELP zenthros_controller_backend_idle_seconds Seconds an exclusive durable unit (dedicated VM, managed slot, or rayClusterRef) has been idle. Shared or ephemeral RayClusters are never emitted.
# TYPE zenthros_controller_backend_idle_seconds gauge
zenthros_controller_backend_idle_seconds{backend_id="slot-a"} 4
zenthros_controller_backend_idle_seconds{backend_id="vm-01"} 10
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(want)); err != nil {
		t.Errorf("idle collector: %v", err)
	}
}

func TestIdleSnapshotsFromRayClusterJobs(t *testing.T) {
	jobs := []v1.ZenthrosJob{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hold", Namespace: "tenant-default", Annotations: map[string]string{v1.AnnotationExclusiveHold: "raycluster/tenant-default/rc-a"}},
			Spec:       v1.ZenthrosJobSpec{Target: v1.Target{RayCluster: &v1.RayClusterReference{Name: "rc-a", Namespace: "tenant-default"}}},
			Status:     v1.ZenthrosJobStatus{Phase: v1.PhasePending},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "tenant-default"},
			Spec:       v1.ZenthrosJobSpec{Target: v1.Target{RayCluster: &v1.RayClusterReference{Name: "rc-b", Namespace: "tenant-default"}}},
			Status:     v1.ZenthrosJobStatus{Phase: v1.PhaseSucceeded},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "ephemeral", Namespace: "tenant-default"},
			Spec:       v1.ZenthrosJobSpec{Workload: v1.Workload{Template: v1.WorkloadTemplate{Entrypoint: "echo"}}},
			Status:     v1.ZenthrosJobStatus{Phase: v1.PhaseRunning},
		},
	}
	got := IdleSnapshotsFromRayClusterJobs(jobs)
	if len(got) != 2 {
		t.Fatalf("snapshots = %d, want 2 exclusive rayClusterRef units: %+v", len(got), got)
	}
	byID := map[string]IdleSnapshot{}
	for _, snap := range got {
		byID[snap.ID] = snap
		if snap.SharedPool || snap.Type != v1.BackendKindK8s {
			t.Errorf("%s must be exclusive k8s, got %+v", snap.ID, snap)
		}
	}
	if byID["raycluster/tenant-default/rc-a"].RunningJobs != 1 {
		t.Errorf("held cluster running = %d, want 1", byID["raycluster/tenant-default/rc-a"].RunningJobs)
	}
	if byID["raycluster/tenant-default/rc-b"].RunningJobs != 0 {
		t.Errorf("finished cluster running = %d, want 0 (idle)", byID["raycluster/tenant-default/rc-b"].RunningJobs)
	}
}

func TestIdleCollectorEmitsRayClusterRef(t *testing.T) {
	collector := NewIdleCollector(func() []IdleSnapshot {
		return []IdleSnapshot{
			{ID: "raycluster/tenant-default/rc-a", Type: v1.BackendKindK8s, RunningJobs: 0},
			{ID: "cluster-b", Type: v1.BackendKindK8s, SharedPool: true, RunningJobs: 0},
		}
	})
	fixed := time.Date(2026, 8, 17, 12, 0, 10, 0, time.UTC)
	collector.now = func() time.Time { return fixed }
	collector.idleSince["raycluster/tenant-default/rc-a"] = fixed.Add(-7 * time.Second)

	want := `
# HELP zenthros_controller_backend_idle_seconds Seconds an exclusive durable unit (dedicated VM, managed slot, or rayClusterRef) has been idle. Shared or ephemeral RayClusters are never emitted.
# TYPE zenthros_controller_backend_idle_seconds gauge
zenthros_controller_backend_idle_seconds{backend_id="raycluster/tenant-default/rc-a"} 7
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(want)); err != nil {
		t.Errorf("idle collector rayClusterRef: %v", err)
	}
}

func TestObservePendingAndBackendBusy(t *testing.T) {
	before := testutil.ToFloat64(backendBusyTotal)
	ObservePending(3 * time.Second)
	RecordBackendBusy()
	if testutil.CollectAndCount(pendingSeconds) == 0 {
		t.Fatal("pending histogram emitted no samples")
	}
	if got := testutil.ToFloat64(backendBusyTotal) - before; got != 1 {
		t.Fatalf("backend_busy = %v, want +1", got)
	}
}
