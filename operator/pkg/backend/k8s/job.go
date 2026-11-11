// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	"fmt"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
	"github.com/zenthros/operator/pkg/jobspec"
)

const (
	WorkloadContainerName = "workload"
	defaultJobCPU         = "100m"
	defaultJobMemory      = "128Mi"

	// AgentHandleJobPrefix distinguishes batch Jobs from RayJob ns/name handles.
	AgentHandleJobPrefix = "job:"
)

// BuildJob translates a JobSpec into a Kubernetes batch/v1 Job.
func BuildJob(spec jobspec.JobSpec) *batchv1.Job {
	backoff := spec.BackoffLimit
	command, args := containerCommand(spec)
	job := &batchv1.Job{
		ObjectMeta: workloadMeta(spec),
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template:     k8spod.WorkloadPodTemplate(spec, workloadPodSpec(spec, command, args, corev1.RestartPolicyNever), k8spod.DefaultRuntime),
		},
	}
	distribute(job, spec)
	// Ephemeral Jobs are not given kube TTL=0. That raced Zenthros log harvest
	// and deleted pods before zctl could read them. The controller GC Cancels
	// the native Job after retainLogsForSeconds; Retain omits that Cancel.
	return job
}

// completionIndex is the annotation an Indexed Job's pod carries its index in.
const completionIndex = "metadata.annotations['batch.kubernetes.io/job-completion-index']"

// distribute makes the Job Indexed for a distribution: one pod per task,
// TASK_INDEX from the pod's completion index. Tasks retry per index
// (backoffLimitPerIndex) and fail the Job at the first index that runs out
// under FailFast.
func distribute(job *batchv1.Job, spec jobspec.JobSpec) {
	d := spec.Distribution
	if d == nil || d.Count <= 1 {
		return
	}
	count := d.Count
	mode := batchv1.IndexedCompletion
	job.Spec.CompletionMode = &mode
	job.Spec.Completions = &count
	job.Spec.Parallelism = &count
	index := corev1.EnvVar{ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: completionIndex}}}
	c := &job.Spec.Template.Spec.Containers[0]
	own := map[string]bool{}
	add := func(name, value string, fromIndex bool) {
		own[name] = true
		v := corev1.EnvVar{Name: name, Value: value}
		if fromIndex {
			v.ValueFrom, v.Value = index.ValueFrom, ""
		}
		c.Env = append(c.Env, v)
	}
	env := c.Env
	c.Env = nil
	add(v1.EnvTaskIndex, "", true)
	add(v1.EnvTaskCount, fmt.Sprint(count), false)
	per := spec.BackoffLimit
	job.Spec.BackoffLimitPerIndex = &per
	job.Spec.BackoffLimit = nil
	if d.EffectiveFailurePolicy() == v1.FailurePolicyFailFast {
		zero := int32(0)
		job.Spec.MaxFailedIndexes = &zero
	}
	for _, e := range env {
		if !own[e.Name] {
			c.Env = append(c.Env, e)
		}
	}
}

func workloadMeta(spec jobspec.JobSpec) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      spec.Name,
		Namespace: spec.Namespace,
		Labels:    k8spod.WorkloadLabels(spec),
	}
}

func workloadPodSpec(spec jobspec.JobSpec, command, args []string, restart corev1.RestartPolicy) corev1.PodSpec {
	cpu, memory, gpus := v1.SizesOf(spec.Workers, spec.Compute)
	resources := containerResources(valueOr(cpu, defaultJobCPU), valueOr(memory, defaultJobMemory), gpus)
	// The memory a job asked for is all it gets, so an overrun is its own
	// OOM and not a neighbour's. Its CPU stays a request: an idle core harms
	// nobody. A job that asked nothing gets no limit at a size it never chose.
	if memory != "" {
		resources.Limits[corev1.ResourceMemory] = resources.Requests[corev1.ResourceMemory]
	}
	podSpec := corev1.PodSpec{
		RestartPolicy: restart,
		Containers: []corev1.Container{{
			Name:      WorkloadContainerName,
			Image:     spec.Image,
			Command:   command,
			Args:      args,
			Env:       envVars(spec.Env),
			Resources: resources,
		}},
	}
	if spec.Labels[v1.LabelMachineBackend] != "" {
		// A pod that is a machine is its size and no more, as a container
		// of create("local") is: its agent sees the quota, not the node.
		resources := &podSpec.Containers[0].Resources
		resources.Limits[corev1.ResourceCPU] = resources.Requests[corev1.ResourceCPU]
		resources.Limits[corev1.ResourceMemory] = resources.Requests[corev1.ResourceMemory]
	}
	return podSpec
}

// containerCommand returns argv. Structured Command is passed through; a shell
// entrypoint becomes /bin/sh -lc (not Ray's quoted entrypoint string).
func containerCommand(spec jobspec.JobSpec) (command, args []string) {
	if len(spec.Command) > 0 {
		return spec.Command, spec.Args
	}
	return []string{"/bin/sh", "-lc"}, []string{spec.Entrypoint}
}

// JobAgentHandle encodes a batch Job for the control channel.
func JobAgentHandle(namespace, name string) string {
	return AgentHandleJobPrefix + namespace + "/" + name
}

// ParseJobHandle decodes a job:ns/name handle.
func ParseJobHandle(value string) (namespace, name string, ok bool, err error) {
	return parsePrefixedHandle(value, AgentHandleJobPrefix)
}

func parsePrefixedHandle(value, prefix string) (namespace, name string, ok bool, err error) {
	if len(value) < len(prefix) || value[:len(prefix)] != prefix {
		return "", "", false, nil
	}
	rest := value[len(prefix):]
	slash := -1
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			slash = i
			break
		}
	}
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", true, fmt.Errorf("invalid handle %q", value)
	}
	return rest[:slash], rest[slash+1:], true, nil
}

// MapBatchJob maps a batch/v1 Job onto Zenthros phases. Only the Job's own
// terminal conditions end it, and a Failed condition's reason and message
// reach the user as Kubernetes wrote them.
func MapBatchJob(job *batchv1.Job) backend.JobState {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			// A Job is Complete only when its pods exited 0.
			exitCode := int32(0)
			return backend.JobState{Phase: v1.PhaseSucceeded, ExitCode: &exitCode}
		case batchv1.JobFailed:
			msg := fmt.Sprintf("Job/%s condition Failed=True reason=%s: %s", job.Name, c.Reason, c.Message)
			code := v1.ReasonProcessFailed
			if c.Reason == batchv1.JobReasonDeadlineExceeded {
				code = v1.ReasonDeadlineExceeded
			}
			return backend.JobState{Phase: v1.PhaseFailed, Message: msg,
				Reasons: []v1.Reason{{Code: code, Detail: msg}}}
		}
	}
	// Only the Complete condition ends a Job: counts say Succeeded for an
	// indexed Job whose other index is still in backoff (K9), and a failed
	// pod with none active may still be retried.
	if job.Status.Active > 0 {
		return backend.JobState{Phase: v1.PhaseRunning}
	}
	return backend.JobState{Phase: v1.PhaseSubmitted}
}

// BatchJobSpecMismatch reports the first field of what the job runs that
// differs between an existing Job and the one a resubmit would create. Env is
// left out: it carries per-attempt values the controller derives.
func BatchJobSpecMismatch(existing, desired *batchv1.Job) *backend.SpecMismatchError {
	have, want := workloadContainer(existing), workloadContainer(desired)
	switch {
	case have.Image != want.Image:
		return backend.NewSpecMismatchError("image", want.Image, have.Image)
	case !slices.Equal(have.Command, want.Command):
		return backend.NewSpecMismatchError("command", fmt.Sprint(want.Command), fmt.Sprint(have.Command))
	case !slices.Equal(have.Args, want.Args):
		return backend.NewSpecMismatchError("args", fmt.Sprint(want.Args), fmt.Sprint(have.Args))
	}
	return nil
}

func workloadContainer(job *batchv1.Job) corev1.Container {
	for _, c := range job.Spec.Template.Spec.Containers {
		if c.Name == WorkloadContainerName {
			return c
		}
	}
	return corev1.Container{}
}

// BatchJobPods lists the Job's pods, newest first, by the Job's own selector
// (its controller UID): a Job of the same name deleted in the background
// still has pods labelled job-name=<name> (N12).
func BatchJobPods(ctx context.Context, cl client.Client, job *batchv1.Job) []corev1.Pod {
	selector, err := metav1.LabelSelectorAsSelector(job.Spec.Selector)
	if err != nil {
		return nil
	}
	var pods corev1.PodList
	if err := cl.List(ctx, &pods, client.InNamespace(job.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil
	}
	items := pods.Items
	slices.SortFunc(items, func(a, b corev1.Pod) int {
		return b.CreationTimestamp.Compare(a.CreationTimestamp.Time)
	})
	return items
}
