// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

const agentNS = "zenthros-agent"

func prewarmRequest(key string, k *agentpb.PrepareKubernetes, items ...*agentpb.PrepareItem) *agentpb.PrepareRequest {
	return &agentpb.PrepareRequest{Key: key, Items: items, Kubernetes: k}
}

func imageItem(image string) *agentpb.PrepareItem {
	return &agentpb.PrepareItem{Ref: "image:" + image, Image: image}
}

func phases(t *testing.T, got []*agentpb.PrepareItemStatus) map[string]*agentpb.PrepareItemStatus {
	t.Helper()
	out := map[string]*agentpb.PrepareItemStatus{}
	for _, s := range got {
		out[s.GetRef()] = s
	}
	return out
}

func prewarmDS(t *testing.T, e *Executor, name string) *appsv1.DaemonSet {
	t.Helper()
	ds := &appsv1.DaemonSet{}
	if err := e.client.Get(context.Background(), client.ObjectKey{Namespace: agentNS, Name: prewarmObjectName(name)}, ds); err != nil {
		t.Fatal(err)
	}
	return ds
}

func settle(t *testing.T, e *Executor, ds *appsv1.DaemonSet, desired int32) {
	t.Helper()
	// The fake client leaves generation to the caller, as an API server
	// would not.
	if ds.Generation == 0 {
		ds.Generation = 1
		if err := e.client.Update(context.Background(), ds); err != nil {
			t.Fatal(err)
		}
	}
	ds.Status = appsv1.DaemonSetStatus{ObservedGeneration: ds.Generation, DesiredNumberScheduled: desired, UpdatedNumberScheduled: desired, NumberReady: desired}
	if err := e.client.Status().Update(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
}

// Images are warmed by a DaemonSet in the agent's namespace on the nodes the
// Prewarm names, one init container per image; an image a node cannot pull
// fails with the kubelet's reason and message, a new generation updates the
// DaemonSet in place, and a release removes it.
// E2E TestK8sPrewarmDaemonSet.
func TestK8sPrewarmDaemonSetScopedToTargets(t *testing.T) {
	e := newTestExecutor(t)
	e.Namespace = agentNS
	k := &agentpb.PrepareKubernetes{NodeSelector: map[string]string{"pool": "gpu"}}
	ctx := context.Background()
	got, err := e.Prepare(ctx, prewarmRequest("llama/2", k, imageItem("python:3.12-slim"), imageItem("registry.example/private:1")))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range got {
		if s.GetPhase() != "Running" {
			t.Fatalf("just created: %+v", s)
		}
	}
	ds := prewarmDS(t, e, "llama")
	spec := ds.Spec.Template.Spec
	if !reflect.DeepEqual(spec.NodeSelector, k.NodeSelector) || ds.Labels[LabelPrewarm] != "llama" || ds.Annotations[annotationPrewarmKey] != "llama/2" {
		t.Fatalf("daemonset %s selector %v labels %v annotations %v", ds.Name, spec.NodeSelector, ds.Labels, ds.Annotations)
	}
	var images []string
	for _, c := range spec.Containers {
		images = append(images, c.Image)
		if c.Command[0] != prewarmToolsDir+"/sleep" || c.ImagePullPolicy != corev1.PullIfNotPresent {
			t.Fatalf("image container %+v", c)
		}
	}
	// Each image is its own container: one the node cannot pull does not
	// hold back the next, as it would in a chain of init containers.
	if !reflect.DeepEqual(images, []string{"python:3.12-slim", "registry.example/private:1"}) || len(spec.InitContainers) != 1 ||
		spec.InitContainers[0].Image != "busybox:1.36.1-musl" {
		t.Fatalf("images %v init %+v", images, spec.InitContainers)
	}

	pull := `Failed to pull image "registry.example/private:1": rpc error: code = Unknown desc = failed to resolve reference: unexpected status from HEAD request: 403 Forbidden`
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p-a", Namespace: agentNS, Labels: map[string]string{LabelPrewarm: "llama"}},
		Spec: corev1.PodSpec{NodeName: "gpu-1"},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{Name: "tools", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "image-0", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				{Name: "image-1", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull", Message: pull}}},
			}}}
	if err := e.client.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	settle(t, e, ds, 1)
	got, _ = e.Prepare(ctx, prewarmRequest("llama/2", k, imageItem("python:3.12-slim"), imageItem("registry.example/private:1")))
	p := phases(t, got)
	if p["image:python:3.12-slim"].GetPhase() != "Ready" {
		t.Fatalf("pulled image: %+v", p["image:python:3.12-slim"])
	}
	if s := p["image:registry.example/private:1"]; s.GetPhase() != "Failed" || s.GetMessage() != "pod p-a on node gpu-1: ErrImagePull: "+pull {
		t.Fatalf("pull failure: %+v", s)
	}
	if warm := e.warmImages(); !reflect.DeepEqual(warm, []string{"python:3.12-slim"}) {
		t.Fatalf("declared warm %v", warm)
	}

	// A container that cannot run fails its image with the kubelet's words
	// (a tools binary that is not static cannot run inside the image).
	pod.Status.ContainerStatuses[0] = corev1.ContainerStatus{Name: "image-0",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off 20s restarting failed container"}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 255, Reason: "Error",
			Message: "exec /zenthros-prewarm/sleep: no such file or directory\n"}}}
	if err := e.client.Status().Update(ctx, pod); err != nil {
		if err := e.client.Update(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	got, _ = e.Prepare(ctx, prewarmRequest("llama/2", k, imageItem("python:3.12-slim"), imageItem("registry.example/private:1")))
	if s := phases(t, got)["image:python:3.12-slim"]; s.GetPhase() != "Failed" ||
		s.GetMessage() != "pod p-a on node gpu-1: python:3.12-slim exited 255: Error: exec /zenthros-prewarm/sleep: no such file or directory" {
		t.Fatalf("container that cannot run: %+v", s)
	}

	if _, err := e.Prepare(ctx, prewarmRequest("llama/3", k, imageItem("python:3.13-slim"))); err != nil {
		t.Fatal(err)
	}
	ds = prewarmDS(t, e, "llama")
	if c := ds.Spec.Template.Spec.Containers; ds.Annotations[annotationPrewarmKey] != "llama/3" || len(c) != 1 || c[0].Image != "python:3.13-slim" {
		t.Fatalf("new generation: %v %+v", ds.Annotations, c)
	}

	if _, err := e.Prepare(ctx, &agentpb.PrepareRequest{Key: "llama", Release: true}); err != nil {
		t.Fatal(err)
	}
	if err := e.client.Get(ctx, client.ObjectKeyFromObject(ds), &appsv1.DaemonSet{}); !apierrors.IsNotFound(err) {
		t.Fatalf("release left the daemonset: %v", err)
	}
}

// No node matching the selector is a failure that says which selector.
func TestK8sPrewarmNoMatchingNode(t *testing.T) {
	e := newTestExecutor(t)
	e.Namespace = agentNS
	k := &agentpb.PrepareKubernetes{NodeSelector: map[string]string{"pool": "tpu"}}
	if _, err := e.Prepare(context.Background(), prewarmRequest("x/1", k, imageItem("busybox:1.36"))); err != nil {
		t.Fatal(err)
	}
	settle(t, e, prewarmDS(t, e, "x"), 0)
	got, _ := e.Prepare(context.Background(), prewarmRequest("x/1", k, imageItem("busybox:1.36")))
	if got[0].GetPhase() != "Failed" || got[0].GetMessage() != "no node matches nodeSelector {pool=tpu}" {
		t.Fatalf("%+v", got[0])
	}
}

// An artifact is written to the claim by a Job; without a claim, or when
// the Job fails, the item fails in words that never repeat a presigned
// query. Toolchains are refused: they belong in the image.
func TestK8sPrewarmArtifactJobAndRefusals(t *testing.T) {
	e := newTestExecutor(t)
	e.Namespace = agentNS
	ctx := context.Background()
	uri := "https://bucket.example/w.bin?X-Amz-Signature=secret"
	artifact := &agentpb.PrepareItem{Ref: "artifact:w", ArtifactUri: uri, ArtifactSha256: strings.Repeat("a", 64), ArtifactPath: "models/w.bin"}
	toolchain := &agentpb.PrepareItem{Ref: "toolchain:node@22", ToolchainName: "node", ToolchainVersion: "22"}

	got, _ := e.Prepare(ctx, prewarmRequest("w/1", &agentpb.PrepareKubernetes{}, artifact, toolchain))
	p := phases(t, got)
	if s := p["artifact:w"]; s.GetPhase() != "Failed" || !strings.Contains(s.GetMessage(), "spec.kubernetes.pvcName") {
		t.Fatalf("no claim: %+v", s)
	}
	if s := p["toolchain:node@22"]; s.GetPhase() != "Failed" || !strings.Contains(s.GetMessage(), "put them in the job's image") {
		t.Fatalf("toolchain: %+v", s)
	}

	k := &agentpb.PrepareKubernetes{PvcName: "model-cache"}
	if got, _ := e.Prepare(ctx, prewarmRequest("w/1", k, artifact)); got[0].GetPhase() != "Running" {
		t.Fatalf("created: %+v", got[0])
	}
	var jobs batchv1.JobList
	if err := e.client.List(ctx, &jobs, client.InNamespace(agentNS), client.MatchingLabels{LabelPrewarm: "w"}); err != nil || len(jobs.Items) != 1 {
		t.Fatalf("jobs %d %v", len(jobs.Items), err)
	}
	job := &jobs.Items[0]
	pod := job.Spec.Template.Spec
	if pod.Volumes[0].PersistentVolumeClaim.ClaimName != "model-cache" || pod.Containers[0].TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Fatalf("job pod %+v", pod)
	}
	for _, env := range pod.Containers[0].Env {
		if env.Name == "SHOWN" && env.Value != "https://bucket.example/w.bin" {
			t.Fatalf("shown url %q", env.Value)
		}
	}
	wgetErr := "wget: server returned error: HTTP/1.1 403 Forbidden fetching " + uri
	failedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-x", Namespace: agentNS, Labels: map[string]string{"job-name": job.Name, batchv1.ControllerUidLabel: string(job.UID)}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "fetch",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: wgetErr + "\n"}}}}}}
	if err := e.client.Create(ctx, failedPod); err != nil {
		t.Fatal(err)
	}
	// A pod of an earlier Job of the name: its words are not this one's.
	former := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-former", Namespace: agentNS, Labels: map[string]string{"job-name": job.Name, batchv1.ControllerUidLabel: "uid-former"}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "fetch",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "disk full\n"}}}}}}
	if err := e.client.Create(ctx, former); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}
	if err := e.client.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	got, _ = e.Prepare(ctx, prewarmRequest("w/1", k, artifact))
	if got[0].GetPhase() != "Failed" || got[0].GetMessage() != "wget: server returned error: HTTP/1.1 403 Forbidden fetching https://bucket.example/w.bin" {
		t.Fatalf("failed job: %+v", got[0])
	}
}
