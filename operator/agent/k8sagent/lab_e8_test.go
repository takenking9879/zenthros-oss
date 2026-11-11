// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	k8s "github.com/zenthros/operator/pkg/backend/k8s"
)

// E8 of the audit of 2026-10-16 (section G), the Kubernetes agent's half:
// what its 2-second status sweep costs with 100/300/500 Jobs, its pods
// running or still pending, and what its capacity read (every node, every
// pod that has not ended, a page at a time) holds with 2,000 nodes and
// 20,000 pods. A real apiserver (envtest) with the agent's own status cache
// and executor; no kubelet, so pod phases are written by the test, and no
// kwok. Criteria: a sweep of 300 jobs under 2 s; the capacity read under
// 80 % of the agent's 256 MiB limit. A lab, not a check of the suite: it runs
// only when ZENTHROS_E2E_LAB names E8 (and KUBEBUILDER_ASSETS is set), and
// writes its numbers to ZENTHROS_E2E_LAB_OUT when set.
//
//	ZENTHROS_E2E_LAB=E8 KUBEBUILDER_ASSETS=$PWD/bin/envtest/k8s/1.34.0-linux-amd64 \
//	  go test ./agent/k8sagent/ -run '^TestLabE8' -v -timeout 30m
type e8Sweep struct {
	Jobs        int     `json:"jobs"`
	Pods        string  `json:"pods"`
	SweepMillis float64 `json:"sweepMillis"`
	APICalls    int64   `json:"apiCalls"`
	// The first sweep of a phase, before any event read is kept: what one
	// sweep in eventCacheTTL/2s pays.
	FirstMillis   float64 `json:"firstMillis"`
	FirstAPICalls int64   `json:"firstAPICalls"`
}

type e8Capacity struct {
	Nodes         int     `json:"nodes"`
	Pods          int     `json:"pods"`
	Millis        float64 `json:"millis"`
	PeakHeapBytes uint64  `json:"peakHeapBytes"`
	NodesDeclared int     `json:"nodesDeclared"`
}

const e8AgentMemoryLimit = 256 << 20

func TestLabE8K8sAgent(t *testing.T) {
	if !strings.Contains(os.Getenv("ZENTHROS_E2E_LAB"), "E8") || os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("a lab: set ZENTHROS_E2E_LAB=E8 and KUBEBUILDER_ASSETS")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	defer func() { _ = env.Stop() }()
	cfg.QPS, cfg.Burst = -1, 0 // the lab's own writes are not what is measured

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newTestExecutor(t)
	scheme := e.client.Scheme()
	direct, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	calls := &countingClient{Client: direct}
	e.client = calls
	if e.StatusCache, err = StartStatusCache(ctx, cfg, scheme, ""); err != nil {
		t.Fatal(err)
	}
	if err := direct.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant-e8"}}); err != nil {
		t.Fatal(err)
	}

	var sweeps []e8Sweep
	made := 0
	for _, jobs := range []int{100, 300, 500} {
		e8MakeJobs(t, direct, made, jobs)
		made = jobs
		for _, phase := range []corev1.PodPhase{corev1.PodRunning, corev1.PodPending} {
			e8SetPodPhases(t, direct, jobs, phase)
			e8WaitCached(t, e.StatusCache, jobs, phase)
			sweeps = append(sweeps, e8MeasureSweep(t, e, calls, jobs, phase))
		}
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	capacity := e8MeasureCapacity(t, cfg, clientset, direct, 2000, 20000)

	if path := os.Getenv("ZENTHROS_E2E_LAB_OUT"); path != "" {
		raw, _ := json.MarshalIndent(map[string]any{"sweeps": sweeps, "capacity": capacity}, "", "  ")
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Errorf("write %s: %v", path, err)
		}
	}
	for _, s := range sweeps {
		t.Logf("E8 sweep %3d jobs, pods %-7s: first %7.1f ms (%d apiserver calls), then %7.1f ms (%d)",
			s.Jobs, s.Pods, s.FirstMillis, s.FirstAPICalls, s.SweepMillis, s.APICalls)
		if s.Jobs == 300 && s.FirstMillis >= 2000 {
			t.Errorf("E8: a sweep of 300 jobs (pods %s) took %.0f ms, criterion < 2 s", s.Pods, s.FirstMillis)
		}
	}
	t.Logf("E8 capacity %d nodes, %d pods: %.0f ms, peak heap %d MiB, %d nodes declared",
		capacity.Nodes, capacity.Pods, capacity.Millis, capacity.PeakHeapBytes>>20, capacity.NodesDeclared)
	if capacity.PeakHeapBytes >= e8AgentMemoryLimit*8/10 {
		t.Errorf("E8: the capacity read peaked at %d MiB, criterion < 80 %% of %d MiB", capacity.PeakHeapBytes>>20, e8AgentMemoryLimit>>20)
	}
}

// countingClient counts what reaches the apiserver.
type countingClient struct {
	client.Client
	n atomic.Int64
}

func (c *countingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.n.Add(1)
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *countingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.n.Add(1)
	return c.Client.List(ctx, list, opts...)
}

func e8Name(i int) string { return fmt.Sprintf("e8-%04d", i) }

// e8MakeJobs creates Jobs from..to-1 and a pod each, labeled as the agent
// labels its own.
func e8MakeJobs(t *testing.T, c client.Client, from, to int) {
	t.Helper()
	parallel(t, from, to, func(i int) error {
		name := e8Name(i)
		labels := map[string]string{StatusCacheLabel: name + "-uid"}
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-e8", Labels: labels},
			Spec: batchv1.JobSpec{
				// Suspended: no job controller runs here, and none must
				// make pods of its own.
				Suspend: ptr.To(true),
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{{Name: "main", Image: "python:3.12-slim", Command: []string{"python3", "train.py"}}}},
				},
			},
		}
		if err := c.Create(context.Background(), job); err != nil {
			return err
		}
		podLabels := map[string]string{StatusCacheLabel: name + "-uid", "job-name": name, "batch.kubernetes.io/job-name": name}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-abcde", Namespace: "tenant-e8", Labels: podLabels},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{Name: "main", Image: "python:3.12-slim"}}},
		}
		return c.Create(context.Background(), pod)
	})
}

// e8SetPodPhases writes every pod's phase, and its Job's active count, as a
// kubelet and the job controller would.
func e8SetPodPhases(t *testing.T, c client.Client, jobs int, phase corev1.PodPhase) {
	t.Helper()
	parallel(t, 0, jobs, func(i int) error {
		pod := &corev1.Pod{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: "tenant-e8", Name: e8Name(i) + "-abcde"}, pod); err != nil {
			return err
		}
		pod.Status.Phase = phase
		if err := c.Status().Update(context.Background(), pod); err != nil {
			return err
		}
		job := &batchv1.Job{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: "tenant-e8", Name: e8Name(i)}, job); err != nil {
			return err
		}
		if job.Status.Active == 1 {
			return nil
		}
		now := metav1.Now()
		job.Status.Active, job.Status.StartTime = 1, &now
		return c.Status().Update(context.Background(), job)
	})
}

func e8WaitCached(t *testing.T, cache client.Reader, jobs int, phase corev1.PodPhase) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var pods corev1.PodList
		if err := cache.List(context.Background(), &pods, client.InNamespace("tenant-e8")); err != nil {
			t.Fatal(err)
		}
		ready := 0
		for _, pod := range pods.Items {
			if pod.Status.Phase == phase {
				ready++
			}
		}
		if ready >= jobs {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the status cache shows %d of %d pods %s", ready, jobs, phase)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// e8MeasureSweep is the agent's sweep: Status of every job in turn, as
// StatusWatcher.pollAll does; the best of three.
func e8MeasureSweep(t *testing.T, e *Executor, calls *countingClient, jobs int, phase corev1.PodPhase) e8Sweep {
	t.Helper()
	best := time.Duration(1 << 62)
	var apiCalls, firstCalls int64
	var first time.Duration
	e.events = eventCache{} // a new phase: nothing kept
	for i := range 3 {
		before := calls.n.Load()
		started := time.Now()
		for i := 0; i < jobs; i++ {
			if _, err := e.Status(context.Background(), k8s.JobAgentHandle("tenant-e8", e8Name(i))); err != nil {
				t.Fatalf("status of %s: %v", e8Name(i), err)
			}
		}
		took := time.Since(started)
		if i == 0 {
			first, firstCalls = took, calls.n.Load()-before
		}
		if took < best {
			best, apiCalls = took, calls.n.Load()-before
		}
	}
	return e8Sweep{Jobs: jobs, Pods: string(phase), SweepMillis: float64(best.Microseconds()) / 1000, APICalls: apiCalls,
		FirstMillis: float64(first.Microseconds()) / 1000, FirstAPICalls: firstCalls}
}

// e8MeasureCapacity fills the cluster with nodes and pods that have not
// ended, then times listNodes and samples the heap while it runs.
func e8MeasureCapacity(t *testing.T, cfg *rest.Config, clientset kubernetes.Interface, c client.Client, nodes, pods int) e8Capacity {
	t.Helper()
	parallel(t, 0, nodes, func(i int) error {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%04d", i), Labels: map[string]string{"kubernetes.io/hostname": fmt.Sprintf("node-%04d", i)}},
			Status: corev1.NodeStatus{
				Capacity:    corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("64Gi"), corev1.ResourcePods: resource.MustParse("110")},
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("15800m"), corev1.ResourceMemory: resource.MustParse("62Gi"), corev1.ResourcePods: resource.MustParse("110")},
				Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()}},
			},
		}
		status := node.Status // Create answers with the status dropped
		if err := c.Create(context.Background(), node); err != nil {
			return err
		}
		node.Status = status
		if err := c.Status().Update(context.Background(), node); err != nil {
			return err
		}
		// Admission taints a new node not-ready; the node lifecycle
		// controller, absent here, lifts it once the node is Ready.
		node.Spec.Taints = nil
		return c.Update(context.Background(), node)
	})
	for i := 0; i < pods/1000; i++ {
		if err := c.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("shared-%02d", i)}}); err != nil {
			t.Fatal(err)
		}
	}
	parallel(t, 0, pods, func(i int) error {
		// What a shared cluster's pods look like to the read: requests, a
		// node, and the labels and env a real workload carries.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("app-%05d", i), Namespace: fmt.Sprintf("shared-%02d", i/1000),
				Labels: map[string]string{"app": fmt.Sprintf("app-%d", i%200), "team": "data", "pod-template-hash": "7d9f8c6b5"}},
			Spec: corev1.PodSpec{
				NodeName: fmt.Sprintf("node-%04d", i%nodes),
				Containers: []corev1.Container{{Name: "app", Image: "registry.example.com/team/app:1.2.3",
					Command: []string{"/app/server", "--port=8080", "--log-level=info"},
					Env:     []corev1.EnvVar{{Name: "MODE", Value: "serve"}, {Name: "REGION", Value: "us-east-1"}, {Name: "WORKERS", Value: "4"}},
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")}}}},
			},
		}
		if err := c.Create(context.Background(), pod); err != nil {
			return err
		}
		pod.Status.Phase = corev1.PodRunning
		return c.Status().Update(context.Background(), pod)
	})

	read := listNodes(clientset)
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	done := make(chan struct{})
	go func() {
		var m runtime.MemStats
		for {
			select {
			case <-done:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peak.Load() {
				peak.Store(m.HeapInuse)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	started := time.Now()
	declared, err := read(context.Background())
	took := time.Since(started)
	close(done)
	if err != nil {
		t.Fatal(err)
	}
	if len(declared) != nodes || declared[0].FreeMilliCPU != 13000 {
		t.Fatalf("declared %d nodes (first %+v), want %d with 15800m - 10 × 250m rounded to 13000m", len(declared), declared[:min(1, len(declared))], nodes)
	}
	var used uint64
	if p := peak.Load(); p > base.HeapInuse {
		used = p - base.HeapInuse
	}
	return e8Capacity{Nodes: nodes, Pods: pods, Millis: float64(took.Milliseconds()), PeakHeapBytes: used, NodesDeclared: len(declared)}
}

// parallel runs fn for from..to-1 on 32 workers and fails on the first error.
func parallel(t *testing.T, from, to int, fn func(i int) error) {
	t.Helper()
	next := atomic.Int64{}
	next.Store(int64(from))
	var wg sync.WaitGroup
	var first atomic.Value
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= to || first.Load() != nil {
					return
				}
				if err := fn(i); err != nil {
					first.CompareAndSwap(nil, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err, ok := first.Load().(error); ok {
		t.Fatal(err)
	}
}
