// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8serr"
	"github.com/zenthros/operator/pkg/backend/spark"
)

// TestIsContainerStarting locks in which log-open errors the retry loop treats as
// the transient "container still starting" condition (Phase M task 100): a RayJob
// can report Running while its submitter container is ContainerCreating, and a
// log watcher attaching at that edge must wait, not fail. Anything else (missing
// pod/container, terminated, generic API errors) must surface immediately so a
// real problem is not masked by retries.
func TestIsContainerStarting(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "container creating",
			err:  errors.New(`container "ray-job-submitter" in pod "j-abc" is waiting to start: ContainerCreating`),
			want: true,
		},
		{
			name: "waiting to start",
			err:  errors.New(`container "main" in pod "j-abc" is waiting to start: PodInitializing`),
			want: true,
		},
		{
			name: "pod initializing",
			err:  errors.New("pod j-abc container main PodInitializing"),
			want: true,
		},
		{
			name: "container not found",
			err:  errors.New(`container "main" in pod "j-abc" is not valid`),
			want: false,
		},
		{
			name: "pod not found",
			err:  errors.New(`pods "j-abc" not found`),
			want: false,
		},
		{
			name: "terminated container",
			err:  errors.New("container main is terminated"),
			want: false,
		},
		{
			name: "generic api error",
			err:  errors.New("the server rejected our request for an unknown reason"),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := k8serr.ContainerStarting(tc.err); got != tc.want {
				t.Errorf("ContainerStarting(%q) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestFindSparkDriverPodAndMissingDriverError(t *testing.T) {
	driver := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-pi-driver", Namespace: "tenant-default",
			Labels: map[string]string{spark.ApplicationNameLabel: "spark-pi", "spark-role": "driver"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: spark.DriverContainerName}}},
	}
	executor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-pi-exec-1", Namespace: "tenant-default",
			Labels: map[string]string{spark.ApplicationNameLabel: "spark-pi", "spark-role": "executor"},
		},
	}
	executorClient := newTestExecutor(t, executor, driver)
	name, err := executorClient.findSparkDriverPod(context.Background(), "tenant-default", "spark-pi")
	if err != nil || name != driver.Name {
		t.Fatalf("findSparkDriverPod = %q, %v", name, err)
	}
	_, err = executorClient.findSparkDriverPod(context.Background(), "tenant-default", "missing")
	if err == nil || !strings.Contains(err.Error(), "driver pod not found") {
		t.Fatalf("missing driver error = %v", err)
	}
}

func TestRayJobLogsURL(t *testing.T) {
	got, err := rayJobLogsURL("playground-ray-head-svc.tenant-default.svc.cluster.local:8265", "abc-1")
	if err != nil {
		t.Fatal(err)
	}
	want := "http://playground-ray-head-svc.tenant-default.svc.cluster.local:8265/api/jobs/abc-1/logs"
	if got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
}

func TestTailLogText(t *testing.T) {
	got := tailLogText("a\nb\nc\n", 2)
	if got != "b\nc\n" {
		t.Fatalf("tail = %q", got)
	}
}

func TestNextLogDeltaKeepsBoundedFingerprint(t *testing.T) {
	first, tail, seen := nextLogDelta("hello\nworld\n", "", 0, true, -1)
	if first != "hello\nworld\n" {
		t.Fatalf("first chunk = %q", first)
	}
	if len(tail) > logTailFingerprint {
		t.Fatalf("fingerprint grew to %d", len(tail))
	}
	delta, _, seen2 := nextLogDelta("hello\nworld\nmore\n", tail, seen, false, -1)
	if delta != "more\n" {
		t.Fatalf("delta = %q, want more\\n", delta)
	}
	if seen2 != len("hello\nworld\nmore\n") {
		t.Fatalf("seen = %d", seen2)
	}
}

func TestHTTPModeLogsUseRayJobsAPI(t *testing.T) {
	job := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: "zj-http", Namespace: "tenant-default"},
		Spec:       rayv1.RayJobSpec{SubmissionMode: rayv1.HTTPMode},
		Status: rayv1.RayJobStatus{
			JobId:        "raysub-1",
			DashboardURL: "ray-head:8265",
			JobStatus:    rayv1.JobStatusSucceeded,
		},
	}
	e := newTestExecutor(t, job)
	e.rayLogs = func(_ context.Context, dashboard, id string) (string, error) {
		if dashboard != "ray-head:8265" || id != "raysub-1" {
			t.Fatalf("dashboard=%s id=%s", dashboard, id)
		}
		return "driver line 1\ndriver line 2\n", nil
	}
	ch, err := e.Logs(context.Background(), "tenant-default/zj-http", false, -1)
	if err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for chunk := range ch {
		got.Write(chunk.Data)
	}
	if got.String() != "driver line 1\ndriver line 2\n" {
		t.Fatalf("logs = %q", got.String())
	}
}

func TestMissingSubmitterFallsBackToRayJobsAPI(t *testing.T) {
	job := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: "zj-k8s", Namespace: "tenant-default"},
		Spec:       rayv1.RayJobSpec{SubmissionMode: rayv1.K8sJobMode},
		Status: rayv1.RayJobStatus{
			JobId:        "raysub-2",
			DashboardURL: "ray-head:8265",
			JobStatus:    rayv1.JobStatusSucceeded,
		},
	}
	e := newTestExecutor(t, job)
	e.rayLogs = func(_ context.Context, dashboard, id string) (string, error) {
		return "head-driver\n", nil
	}
	ch, err := e.Logs(context.Background(), "tenant-default/zj-k8s", false, -1)
	if err != nil {
		t.Fatal(err)
	}
	chunk := <-ch
	if string(chunk.Data) != "head-driver\n" {
		t.Fatalf("fallback logs = %q", chunk.Data)
	}
}

func TestPipeToChannelFlushesFinalUnterminatedLine(t *testing.T) {
	chunks := make(chan backend.LogChunk, 1)
	pipeToChannel(context.Background(), strings.NewReader("final-line"), chunks)
	chunk := <-chunks
	if string(chunk.Data) != "final-line\n" {
		t.Fatalf("final chunk = %q, want final-line newline", chunk.Data)
	}
}

// A log read that the cluster refused crosses the wire classified, as a
// Submit, Status or Cancel does: a missing RayJob is ErrNotFound, not an
// unclassified string the hub retries (c2-F1#11).
func TestALogErrorIsClassified(t *testing.T) {
	e := newTestExecutor(t)
	_, err := e.Logs(context.Background(), "tenant-default/gone", false, -1)
	if !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Logs of a missing RayJob = %v, want ErrNotFound", err)
	}
}

// A RayJob's submitter pod is its submitter Job's (the Job the RayJob
// controls, by that Job's UID), not any pod labelled with the RayJob's
// name: a pod a former RayJob of that name left is not this one's driver.
func TestTheSubmitterPodIsTheRayJobsOwn(t *testing.T) {
	controller := true
	rayJob := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{Name: "zj", Namespace: "tenant-default", UID: "ray-uid"}}
	submitter := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "zj", Namespace: "tenant-default", UID: "job-uid",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "ray.io/v1", Kind: "RayJob", Name: "zj", UID: "ray-uid", Controller: &controller}}}}
	pod := func(name, controllerUID string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-default",
			Labels: map[string]string{"job-name": "zj", batchv1.ControllerUidLabel: controllerUID}}}
	}
	e := newTestExecutor(t, rayJob, submitter, pod("zj-former", "former-job-uid"), pod("zj-own", "job-uid"))
	if got, err := e.findSubmitterPod(context.Background(), rayJob); err != nil || got != "zj-own" {
		t.Fatalf("submitter pod = %q, %v; want zj-own", got, err)
	}
}
