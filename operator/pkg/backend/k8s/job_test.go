// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	"github.com/zenthros/operator/pkg/jobspec"
)

func genericSpec() jobspec.JobSpec {
	spec := testSpec()
	spec.Kind = v1.WorkloadKindJob
	spec.Workspace = "default"
	spec.Command = []string{"python3", "-c", "print('ok')"}
	spec.Entrypoint = ""
	spec.Workers = v1.WorkerGroup{Min: 0, Max: 0, CPU: "250m", Memory: "256Mi"}
	return spec
}

func TestBuildJobUsesArgvNotRayEntrypoint(t *testing.T) {
	spec := genericSpec()
	job := BuildJob(spec)
	if job.Namespace != "tenant-default" || job.Name != "hello" {
		t.Fatalf("identity %s/%s", job.Namespace, job.Name)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Name != WorkloadContainerName {
		t.Errorf("container name %q", c.Name)
	}
	if got := c.Command; len(got) != 3 || got[0] != "python3" {
		t.Errorf("command = %v, want argv python3", c.Command)
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restart = %s", job.Spec.Template.Spec.RestartPolicy)
	}
	if job.Labels[v1.LabelWorkspace] != "default" {
		t.Errorf("workspace label = %q", job.Labels[v1.LabelWorkspace])
	}
	if job.Spec.TTLSecondsAfterFinished != nil {
		t.Errorf("ephemeral job kube TTL = %v, want nil (Zenthros GC owns cleanup)", job.Spec.TTLSecondsAfterFinished)
	}
}

func TestBuildJobRetainOmitsTTL(t *testing.T) {
	spec := genericSpec()
	spec.Lifespan = v1.LifespanRetain
	job := BuildJob(spec)
	if job.Spec.TTLSecondsAfterFinished != nil {
		t.Errorf("retain job ttl = %v, want nil", job.Spec.TTLSecondsAfterFinished)
	}
}

func TestBuildJobShellEntrypoint(t *testing.T) {
	spec := genericSpec()
	spec.Command = nil
	spec.Entrypoint = "echo hi"
	job := BuildJob(spec)
	c := job.Spec.Template.Spec.Containers[0]
	if len(c.Command) != 2 || c.Command[0] != "/bin/sh" || c.Command[1] != "-lc" {
		t.Errorf("command = %v", c.Command)
	}
	if len(c.Args) != 1 || c.Args[0] != "echo hi" {
		t.Errorf("args = %v", c.Args)
	}
}

func TestPrefixedHandles(t *testing.T) {
	h := JobAgentHandle("ns", "n")
	ns, name, ok, err := ParseJobHandle(h)
	if err != nil || !ok || ns != "ns" || name != "n" {
		t.Fatalf("job handle %q -> %s/%s ok=%v err=%v", h, ns, name, ok, err)
	}
	if _, _, ok, _ = ParseJobHandle("tenant-default/hello"); ok {
		t.Fatal("bare ns/name must not parse as job handle")
	}
}

// Only the Complete condition ends a Job: an indexed Job with one index
// succeeded and another still in backoff counts succeeded>0 and active==0,
// and read off the counts it was Succeeded (K9).
func TestOnlyTheCompleteConditionEndsAJob(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		succeeded, failed, active int32
		want                      v1.Phase
	}{
		{"one index done, another in backoff", 1, 1, 0, v1.PhaseSubmitted},
		{"a failed pod the Job may retry", 0, 1, 0, v1.PhaseSubmitted},
		{"running", 0, 0, 1, v1.PhaseRunning},
	} {
		job := &batchv1.Job{Status: batchv1.JobStatus{Succeeded: tc.succeeded, Failed: tc.failed, Active: tc.active}}
		if got := MapBatchJob(job).Phase; got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestMapBatchJobFailedConditionIsVerbatim(t *testing.T) {
	job := &batchv1.Job{}
	job.Name = "train"
	job.Status.Failed = 3
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"},
	}
	state := MapBatchJob(job)
	want := "Job/train condition Failed=True reason=BackoffLimitExceeded: Job has reached the specified backoff limit"
	if state.Phase != v1.PhaseFailed || state.Message != want {
		t.Fatalf("state = %s %q, want Failed %q", state.Phase, state.Message, want)
	}
	if len(state.Reasons) != 1 || state.Reasons[0].Code != v1.ReasonProcessFailed || state.Reasons[0].Detail != want {
		t.Fatalf("reasons = %+v", state.Reasons)
	}
	job.Status.Conditions[1].Reason = batchv1.JobReasonDeadlineExceeded
	if got := MapBatchJob(job).Reasons[0].Code; got != v1.ReasonDeadlineExceeded {
		t.Fatalf("deadline code = %s", got)
	}
}

func TestACompleteJobExitedZero(t *testing.T) {
	job := &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	if got := MapBatchJob(job).ExitCode; got == nil || *got != 0 {
		t.Fatalf("exit code %v, want 0", got)
	}
}

func TestMapBatchJobCompleteWins(t *testing.T) {
	job := &batchv1.Job{}
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if MapBatchJob(job).Phase != v1.PhaseSucceeded {
		t.Fatal("Complete=True must be Succeeded even after a retried pod failure")
	}
}

// Until the Job says Failed, a pod that exited non-zero is a retry the Job
// runs itself, not the job's end; a pod that cannot pull its image is shown.
func TestABatchJobLeavesRetriesToKubernetes(t *testing.T) {
	job := ownJob()
	job.Status.Failed, job.Status.Active = 1, 0
	pod := func(name string, status corev1.ContainerStatus) *corev1.Pod {
		p := &corev1.Pod{}
		p.Name, p.Namespace = name, "tenant-a"
		p.Labels = map[string]string{"job-name": "train", "controller-uid": "u1"}
		p.Status.ContainerStatuses = []corev1.ContainerStatus{status}
		return p
	}
	exited := pod("train-a", corev1.ContainerStatus{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 3, Reason: "Error"}}})
	cl := fake.NewClientBuilder().WithObjects(exited).Build()
	state := enrichBatchJob(cl, job)
	if state.Phase == v1.PhaseFailed {
		t.Fatalf("a retried pod failed the Job before Kubernetes did: %+v", state)
	}

	pulling := pod("train-b", corev1.ContainerStatus{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
		Reason: "ImagePullBackOff", Message: `Back-off pulling image "nope:1": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image "docker.io/library/nope:1": failed to resolve reference "docker.io/library/nope:1": docker.io/library/nope:1: not found`}}})
	cl = fake.NewClientBuilder().WithObjects(pulling).Build()
	state = enrichBatchJob(cl, job)
	if len(state.Reasons) == 0 || state.Reasons[0].Code != v1.ReasonImagePullFailed || !strings.Contains(state.Message, `Back-off pulling image "nope:1"`) {
		t.Fatalf("image pull not surfaced: %+v", state)
	}
}

// A distribution on a cluster is one Indexed Job: one pod per task, the
// task's index in TASK_INDEX from the pod's completion index. A task retries
// on its own and, FailFast, the first task that runs out fails the Job.
// E2E TestIndexedJobNodes (local k8s).
func TestIndexedBatchJobForDistribution(t *testing.T) {
	spec := testSpec()
	spec.Name = "train"
	spec.Env = map[string]string{"TASK_INDEX": "user-set", "EPOCHS": "3"}
	spec.Distribution = &v1.Distribution{Mode: v1.DistributionIndependent, Count: 4}
	spec.BackoffLimit = 2
	job := BuildJob(spec)
	if job.Spec.CompletionMode == nil || *job.Spec.CompletionMode != batchv1.IndexedCompletion ||
		*job.Spec.Completions != 4 || *job.Spec.Parallelism != 4 {
		t.Fatalf("job spec %+v", job.Spec)
	}
	env := map[string]corev1.EnvVar{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if _, dup := env[e.Name]; dup {
			t.Fatalf("%s set twice", e.Name)
		}
		env[e.Name] = e
	}
	if f := env["TASK_INDEX"].ValueFrom; f == nil || f.FieldRef == nil || !strings.Contains(f.FieldRef.FieldPath, "job-completion-index") {
		t.Fatalf("TASK_INDEX must come from the completion index: %+v", env["TASK_INDEX"])
	}
	if env["TASK_COUNT"].Value != "4" || env["EPOCHS"].Value != "3" {
		t.Fatalf("env %+v", env)
	}
	for _, gone := range []string{"RANK", "WORLD_SIZE", "MASTER_ADDR", "MASTER_PORT"} {
		if _, ok := env[gone]; ok {
			t.Fatalf("tasks do not rendezvous; %s set", gone)
		}
	}
	if job.Spec.Template.Spec.Subdomain != "" {
		t.Fatalf("no headless Service, no subdomain: %q", job.Spec.Template.Spec.Subdomain)
	}
	if job.Spec.BackoffLimitPerIndex == nil || *job.Spec.BackoffLimitPerIndex != 2 || job.Spec.BackoffLimit != nil ||
		job.Spec.MaxFailedIndexes == nil || *job.Spec.MaxFailedIndexes != 0 {
		t.Fatalf("independent job spec %+v", job.Spec)
	}
	spec.Distribution.FailurePolicy = v1.FailurePolicyContinueOnFailure
	if job = BuildJob(spec); job.Spec.MaxFailedIndexes != nil {
		t.Fatal("ContinueOnFailure lets every index finish")
	}
	spec.Distribution = nil
	if job = BuildJob(spec); job.Spec.CompletionMode != nil {
		t.Fatal("a job of one is a plain Job")
	}
}

// A pod that is a machine (create("k8s:<cluster>")) is that size and no
// more, as a container of create("local") is: its limits are its requests,
// so the agent in it sees its cgroup's quota and not the node. Any other
// Job keeps its CPU a request, and a limit on memory only if it asked for
// some (found by the k8s pod E2E: mi-pod showed the node's 24 CPUs).
func TestAMachinesPodIsLimitedToItsSize(t *testing.T) {
	// The SDK writes this label on the job (sdk/python/zenthros/machines.py POD_LABEL).
	if v1.LabelMachineBackend != "zenthros.ai/machine-backend" {
		t.Fatalf("the label is %q; the SDK writes zenthros.ai/machine-backend", v1.LabelMachineBackend)
	}
	spec := genericSpec()
	spec.Labels = map[string]string{v1.LabelMachineBackend: "ct-1"}
	resources := BuildJob(spec).Spec.Template.Spec.Containers[0].Resources
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		request, limit := resources.Requests[name], resources.Limits[name]
		if limit.IsZero() || limit.Cmp(request) != 0 {
			t.Errorf("%s: request %s, limit %s; want the limit at the request", name, request.String(), limit.String())
		}
	}
	plain := BuildJob(genericSpec()).Spec.Template.Spec.Containers[0].Resources
	if _, ok := plain.Limits[corev1.ResourceCPU]; ok {
		t.Errorf("a Job that is no machine has a CPU limit: %v", plain.Limits)
	}
}

// sizedPod is the container of a job's pod as the in-cluster agent builds it:
// from the ZenthrosJob, through the wire, as a Job. The agent only ever sees
// what crossed the envelope, so a size that stays behind is a pod of defaults.
func sizedPod(t *testing.T, compute *v1.ComputeConstraints, workers v1.WorkerGroup, labels map[string]string) corev1.Container {
	t.Helper()
	job := &v1.ZenthrosJob{
		ObjectMeta: metav1.ObjectMeta{Name: "sized", Namespace: "tenant-default", UID: "uid-sized", Labels: labels},
		Spec: v1.ZenthrosJobSpec{
			Compute: compute,
			Workload: v1.Workload{Kind: v1.WorkloadKindJob, Template: v1.WorkloadTemplate{
				Image:   "python:3.12",
				Command: []string{"python3"},
				Workers: workers,
			}},
		},
	}
	wire, err := envelope.Encode(jobspec.FromZenthrosJob(job, "k8s", job.Namespace))
	if err != nil {
		t.Fatal(err)
	}
	spec, err := envelope.Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	return BuildJob(spec).Spec.Template.Spec.Containers[0]
}

func wantQuantities(t *testing.T, what string, got, want corev1.ResourceList) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s = %v, want %v", what, got, want)
		return
	}
	for name, quantity := range want {
		if have, ok := got[name]; !ok || have.Cmp(quantity) != 0 {
			t.Errorf("%s[%s] = %s, want %s", what, name, have.String(), quantity.String())
		}
	}
}

// cpu=, memory= and gpu= from the SDK are spec.compute, and the pod is that
// size: sizing.HardwareOf is the rule the Scheduler chose the place with, so a
// pod of defaults would run a 16Gi job in 128Mi, on a node that may lack it.
// Memory is limited at what was asked; CPU is only requested, so a job may use
// an idle core.
func TestAJobsPodIsTheSizeSpecComputeAsked(t *testing.T) {
	c := sizedPod(t, &v1.ComputeConstraints{CPU: "2", Memory: "16Gi", GPUCount: 1}, v1.WorkerGroup{}, nil)
	wantQuantities(t, "requests", c.Resources.Requests, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("2"),
		corev1.ResourceMemory: resource.MustParse("16Gi"),
	})
	wantQuantities(t, "limits", c.Resources.Limits, corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("16Gi"),
		gpuResourceName:       resource.MustParse("1"),
	})
}

// HardwareOf's order: compute over workers, field by field, so what compute
// does not name still comes from workers.
func TestAJobsPodTakesWorkersWhereComputeIsSilent(t *testing.T) {
	workers := v1.WorkerGroup{CPU: "3", Memory: "6Gi", GPUsPerNode: 2}
	c := sizedPod(t, nil, workers, nil)
	wantQuantities(t, "workers alone: requests", c.Resources.Requests, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("3"),
		corev1.ResourceMemory: resource.MustParse("6Gi"),
	})
	wantQuantities(t, "workers alone: limits", c.Resources.Limits, corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("6Gi"),
		gpuResourceName:       resource.MustParse("2"),
	})

	c = sizedPod(t, &v1.ComputeConstraints{Memory: "8Gi"}, workers, nil)
	wantQuantities(t, "compute over workers: requests", c.Resources.Requests, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("3"),
		corev1.ResourceMemory: resource.MustParse("8Gi"),
	})
	wantQuantities(t, "compute over workers: limits", c.Resources.Limits, corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("8Gi"),
		gpuResourceName:       resource.MustParse("2"),
	})
}

// A job asking nothing is small and unlimited: a limit at a default nobody
// asked for would kill a job that never mentioned memory.
func TestAJobAskingNothingKeepsTheDefaultSizeAndNoLimit(t *testing.T) {
	c := sizedPod(t, nil, v1.WorkerGroup{}, nil)
	wantQuantities(t, "requests", c.Resources.Requests, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(defaultJobCPU),
		corev1.ResourceMemory: resource.MustParse(defaultJobMemory),
	})
	wantQuantities(t, "limits", c.Resources.Limits, corev1.ResourceList{})
}

// A pod that is a machine is the size it asked, whichever way it asked it:
// the machine branch limits what the requests already say.
func TestAMachinesPodIsLimitedToTheSizeSpecComputeAsked(t *testing.T) {
	c := sizedPod(t, &v1.ComputeConstraints{CPU: "2", Memory: "16Gi"}, v1.WorkerGroup{}, map[string]string{v1.LabelMachineBackend: "ct-1"})
	want := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("2"),
		corev1.ResourceMemory: resource.MustParse("16Gi"),
	}
	wantQuantities(t, "requests", c.Resources.Requests, want)
	wantQuantities(t, "limits", c.Resources.Limits, want)
}
