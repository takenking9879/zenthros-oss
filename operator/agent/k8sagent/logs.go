// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"fmt"
	"io"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8serr"
	"github.com/zenthros/operator/pkg/backend/spark"
)

// containerStartGrace bounds how long a log open waits for the submitter container
// to start. A RayJob can report Running while its submitter container is still
// ContainerCreating, and GetLogs on a waiting container fails immediately; log
// readers (kubectl logs -f) wait for the container instead of erroring, so a
// watcher attaching at the Running edge must not be refused.
const containerStartGrace = 60 * time.Second

// Logs streams the user-facing log of the workload behind h, as its kind's
// translator reads it.
func (e *Executor) Logs(ctx context.Context, h string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	t, r, err := translatorForHandle(h)
	if err != nil {
		return nil, err
	}
	chunks, err := t.logs(e, ctx, r, follow, tailLines)
	return chunks, k8serr.Classify(err)
}

// rayJobLogs streams a RayJob's driver log: the KubeRay submitter pod in
// K8sJobMode, else the Ray Jobs API on Status.DashboardURL
// (GET /api/jobs/{jobId}/logs). The hub never dials Ray; this stays on the
// spoke agent.
func (e *Executor) rayJobLogs(ctx context.Context, namespace, name string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	var rayJob rayv1.RayJob
	if err := e.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rayJob); err != nil {
		return nil, fmt.Errorf("get RayJob %s/%s: %w", namespace, name, err)
	}
	podName, podErr := e.findSubmitterPod(ctx, &rayJob)
	if rayJob.Spec.SubmissionMode == rayv1.HTTPMode || podErr != nil {
		return e.streamRayJobLogs(ctx, namespace, name, follow, tailLines)
	}
	return e.streamPodLogs(ctx, namespace, podName, "", follow, tailLines)
}

func (e *Executor) sparkLogTarget(ctx context.Context, namespace, applicationName string) (string, string, string, error) {
	podName, err := e.findSparkDriverPod(ctx, namespace, applicationName)
	if err != nil {
		return "", "", "", fmt.Errorf("find Spark driver pod for %s/%s: %w", namespace, applicationName, err)
	}
	return namespace, podName, spark.DriverContainerName, nil
}

func (e *Executor) streamPodLogs(ctx context.Context, namespace, podName, containerName string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {

	opts := &corev1.PodLogOptions{
		Follow:     follow,
		Timestamps: false,
		Container:  containerName,
	}
	if tailLines >= 0 {
		tl := int64(tailLines)
		opts.TailLines = &tl
	}

	stream, err := e.openLogStream(ctx, namespace, podName, opts)
	if err != nil {
		return nil, fmt.Errorf("open log stream for pod %s/%s: %w", namespace, podName, err)
	}

	ch := make(chan backend.LogChunk, 64)
	go func() {
		defer close(ch)
		defer stream.Close()
		pipeToChannel(ctx, stream, ch)
	}()
	return ch, nil
}

func (e *Executor) findSparkDriverPod(ctx context.Context, namespace, applicationName string) (string, error) {
	var pods corev1.PodList
	err := e.client.List(ctx, &pods,
		client.InNamespace(namespace),
		client.MatchingLabels{spark.ApplicationNameLabel: applicationName},
	)
	if err != nil {
		return "", fmt.Errorf("list pods for SparkApplication %s/%s: %w", namespace, applicationName, err)
	}
	for _, pod := range pods.Items {
		if pod.Labels["spark-role"] == "driver" || hasContainer(pod.Spec.Containers, spark.DriverContainerName) {
			return pod.Name, nil
		}
	}
	return "", fmt.Errorf("driver pod not found for SparkApplication %s/%s", namespace, applicationName)
}

func hasContainer(containers []corev1.Container, name string) bool {
	for _, container := range containers {
		if container.Name == name {
			return true
		}
	}
	return false
}

// openLogStream opens the pod log stream, retrying while the container is still
// starting (GetLogs on a ContainerCreating container fails immediately). It waits
// up to containerStartGrace for the container to become loggable, matching how
// kubectl logs -f blocks until the container starts. Any other error, or the grace
// window elapsing, surfaces to the caller.
func (e *Executor) openLogStream(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	deadline := time.Now().Add(containerStartGrace)
	for {
		req := e.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
		stream, err := req.Stream(ctx)
		if err == nil {
			return stream, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !k8serr.ContainerStarting(err) || time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// findSubmitterPod locates the RayJob's submitter pod: a pod of the Job the
// RayJob controls (KubeRay names it after the RayJob), by that Job's UID. By
// the job-name label alone, a pod a former RayJob of the name left was read.
func (e *Executor) findSubmitterPod(ctx context.Context, rayJob *rayv1.RayJob) (string, error) {
	var submitter batchv1.Job
	if err := e.client.Get(ctx, client.ObjectKeyFromObject(rayJob), &submitter); err != nil {
		return "", fmt.Errorf("get the submitter Job of RayJob %s/%s: %w", rayJob.Namespace, rayJob.Name, err)
	}
	if !metav1.IsControlledBy(&submitter, rayJob) {
		return "", fmt.Errorf("Job %s/%s is not RayJob %s's submitter", submitter.Namespace, submitter.Name, rayJob.Name)
	}
	var pods corev1.PodList
	if err := e.client.List(ctx, &pods, client.InNamespace(rayJob.Namespace),
		client.MatchingLabels{batchv1.ControllerUidLabel: string(submitter.UID)}); err != nil {
		return "", fmt.Errorf("list pods for RayJob %s/%s: %w", rayJob.Namespace, rayJob.Name, err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no submitter pod found for RayJob %s/%s", rayJob.Namespace, rayJob.Name)
	}
	return pods.Items[0].Name, nil
}

func pipeToChannel(ctx context.Context, r io.Reader, ch chan<- backend.LogChunk) {
	scanner := backend.LogLines(r)
	for scanner.Scan() {
		chunk := backend.LogChunk{
			Stream:    "stdout",
			Data:      []byte(scanner.Text() + "\n"),
			Timestamp: time.Now(),
		}
		select {
		case ch <- chunk:
		case <-ctx.Done():
			return
		}
	}
}
