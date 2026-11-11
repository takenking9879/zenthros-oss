// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"sync"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend/k8s"
)

// apiserverCalls counts what reaches the cluster's apiserver.
type apiserverCalls struct {
	client.Client
	mu                         sync.Mutex
	jobGets, podLists, evLists int
}

func (c *apiserverCalls) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*batchv1.Job); ok {
		c.mu.Lock()
		c.jobGets++
		c.mu.Unlock()
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *apiserverCalls) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.mu.Lock()
	switch list.(type) {
	case *corev1.PodList:
		c.podLists++
	case *corev1.EventList:
		c.evLists++
	}
	c.mu.Unlock()
	return c.Client.List(ctx, list, opts...)
}

func runningBatchJob(name string, podPhase corev1.PodPhase) (*batchv1.Job, *corev1.Pod) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-default", Labels: map[string]string{StatusCacheLabel: name + "-uid"}},
		Spec:       batchv1.JobSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"controller-uid": name + "-cuid"}}},
		Status:     batchv1.JobStatus{Active: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-abcde", Namespace: "tenant-default",
			Labels: map[string]string{"job-name": name, "controller-uid": name + "-cuid", StatusCacheLabel: name + "-uid"}},
		Status: corev1.PodStatus{Phase: podPhase},
	}
	return job, pod
}

// A running job's 2-second status poll asks the cluster's apiserver
// nothing: its Job and pods come from the agent's watch, and a job with a
// running pod LISTs no events. Before, every poll of every job was a GET, a
// pod LIST and an event LIST per pod.
func TestRunningJobStatusAsksTheApiserverNothing(t *testing.T) {
	job, pod := runningBatchJob("steady", corev1.PodRunning)
	e := newTestExecutor(t, job.DeepCopy(), pod.DeepCopy())
	calls := &apiserverCalls{Client: e.client}
	e.client = calls
	e.StatusCache = fake.NewClientBuilder().WithScheme(calls.Scheme()).WithObjects(job.DeepCopy(), pod.DeepCopy()).Build()

	state, err := e.status(context.Background(), k8s.JobAgentHandle("tenant-default", "steady"))
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != v1.PhaseRunning {
		t.Fatalf("phase = %s, want Running", state.Phase)
	}
	if calls.jobGets != 0 || calls.podLists != 0 || calls.evLists != 0 {
		t.Fatalf("a steady poll reached the apiserver: %d Job GETs, %d pod LISTs, %d event LISTs",
			calls.jobGets, calls.podLists, calls.evLists)
	}
}

// A Job the watch has not shown yet (just created) is read from the
// apiserver: not in the cache is not gone.
func TestJobNotYetCachedIsReadFromTheApiserver(t *testing.T) {
	job, pod := runningBatchJob("fresh", corev1.PodRunning)
	e := newTestExecutor(t, job.DeepCopy(), pod.DeepCopy())
	calls := &apiserverCalls{Client: e.client}
	e.client = calls
	e.StatusCache = fake.NewClientBuilder().WithScheme(calls.Scheme()).Build()

	state, err := e.status(context.Background(), k8s.JobAgentHandle("tenant-default", "fresh"))
	if err != nil {
		t.Fatalf("a Job the cache does not hold yet read as %v", err)
	}
	if state.Phase != v1.PhaseRunning || calls.jobGets != 1 {
		t.Fatalf("phase %s after %d apiserver GETs, want Running from one", state.Phase, calls.jobGets)
	}
}

// A job whose pod has not started still gets its events read: they say why
// (image, quota, scheduling).
func TestPendingJobStillReadsItsEvents(t *testing.T) {
	job, pod := runningBatchJob("waiting", corev1.PodPending)
	e := newTestExecutor(t, job.DeepCopy(), pod.DeepCopy())
	calls := &apiserverCalls{Client: e.client}
	e.client = calls
	if _, err := e.status(context.Background(), k8s.JobAgentHandle("tenant-default", "waiting")); err != nil {
		t.Fatal(err)
	}
	if calls.evLists == 0 {
		t.Fatal("a job whose pod has not started read no events: its pending diagnosis is lost")
	}
}
