// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/zenthros/operator/pkg/backend/k8s"
)

// A waiting job's events are read once per eventCacheTTL, not on every
// 2-second poll: two LISTs per waiting job per sweep were 600 a sweep with
// 300 jobs waiting, each a round trip to a remote cluster.
// A warning that appears is still reported, a TTL later at most.
func TestAWaitingJobReadsItsEventsOncePerTTL(t *testing.T) {
	job, pod := runningBatchJob("queued", corev1.PodPending)
	e := newTestExecutor(t)
	// The apiserver selects events by involvedObject.name; the fake needs an
	// index for it.
	indexed := fake.NewClientBuilder().WithScheme(e.client.Scheme()).WithObjects(job.DeepCopy(), pod.DeepCopy()).
		WithIndex(&corev1.Event{}, "involvedObject.name", func(o client.Object) []string {
			return []string{o.(*corev1.Event).InvolvedObject.Name}
		}).Build()
	calls := &apiserverCalls{Client: indexed}
	e.client = calls
	e.StatusCache = fake.NewClientBuilder().WithScheme(calls.Scheme()).WithObjects(job.DeepCopy(), pod.DeepCopy()).Build()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	e.events.now = func() time.Time { return now }

	poll := func() string {
		t.Helper()
		state, err := e.status(context.Background(), k8s.JobAgentHandle("tenant-default", "queued"))
		if err != nil {
			t.Fatal(err)
		}
		return state.Message
	}
	poll()
	first := calls.evLists
	if first == 0 {
		t.Fatal("a waiting job read no events: its diagnosis is lost")
	}
	for range 4 {
		now = now.Add(2 * time.Second)
		poll()
	}
	if calls.evLists != first {
		t.Fatalf("five polls inside %s made %d event LISTs, want the first poll's %d", eventCacheTTL, calls.evLists, first)
	}

	// A quota warning appears; the next poll past the TTL reports it.
	warning := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "queued.quota", Namespace: "tenant-default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Job", Name: "queued", Namespace: "tenant-default"},
		Type:           corev1.EventTypeWarning, Reason: "FailedCreate",
		Message: `pods "queued-x" is forbidden: exceeded quota: compute, requested: cpu=4, used: cpu=8, limited: cpu=8`,
	}
	if err := calls.Client.Create(context.Background(), warning); err != nil {
		t.Fatal(err)
	}
	now = now.Add(eventCacheTTL)
	if message := poll(); !strings.Contains(message, "quota") {
		t.Fatalf("a TTL after the quota warning the job reads %q", message)
	}
	if calls.evLists == first {
		t.Fatal("the poll past the TTL did not read events again")
	}

	// A job whose pod runs reads no events at all.
	running, runningPod := runningBatchJob("busy", corev1.PodRunning)
	e.StatusCache = fake.NewClientBuilder().WithScheme(calls.Scheme()).WithObjects(running, runningPod).Build()
	before := calls.evLists
	if _, err := e.status(context.Background(), k8s.JobAgentHandle("tenant-default", "busy")); err != nil {
		t.Fatal(err)
	}
	if calls.evLists != before {
		t.Fatal("a running job read events")
	}
}
