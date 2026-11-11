// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"encoding/base64"
	"fmt"
	"github.com/zenthros/operator/pkg/shellword"
	"sort"
	"strconv"
	"strings"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
	"github.com/zenthros/operator/pkg/jobspec"
)

const (
	RayContainerName    = "ray-node"
	submitterName       = "ray-job-submitter"
	workerGroupName     = "workers"
	gpuResourceName     = "nvidia.com/gpu"
	defaultHeadCPU      = "1"
	defaultHeadMemory   = "2Gi"
	defaultWorkerCPU    = "1"
	defaultWorkerMemory = "2Gi"
	submitterCPU        = "100m"
	submitterMemory     = "128Mi"

	rayVersion            = "2.53.0"
	rayClusterSelectorKey = "ray.io/cluster"

	sourceVolumeName       = k8spod.SourceVolumeName
	sourceMountPath        = k8spod.SourceMountPath
	gitSyncImage           = k8spod.GitSyncImage
	gitSyncName            = k8spod.GitSyncName
	gitLinkPath            = k8spod.GitLinkPath
	gitSecretMountPath     = k8spod.GitSecretMountPath
	gitSyncUID             = k8spod.GitSyncUID
	gitRootMountPath       = k8spod.GitRootMountPath
	gitSecretVolumeName    = k8spod.GitSecretVolumeName
	gitSecretDefaultMode   = k8spod.GitSecretDefaultMode
	inlineInitName         = k8spod.InlineInitName
	inlineInitImage        = k8spod.InlineInitImage
	inlineSecretVolumeName = k8spod.InlineSecretVolumeName
	inlineSecretMountPath  = k8spod.InlineSecretMountPath
	secretRuntimeUID       = k8spod.SecretRuntimeUID
	secretInitImage        = k8spod.SecretInitImage
)

// BuildRayJob translates a JobSpec into a KubeRay RayJob. Durable data references
// are injected as env vars on the Ray nodes that run user code. No ownerReference is
// set: the ZenthrosJob lives on the hub, and only a spoke Agent applies the
// RayJob; cleanup on the spoke is driven by Cancel.
func BuildRayJob(spec jobspec.JobSpec) *rayv1.RayJob {
	backoffLimit := spec.BackoffLimit
	env := backend.WithDataReferences(spec.Env, spec.Artifacts)
	// Retain keeps the cluster until Zenthros GC harvests submitter logs.
	// Shutdown at finish deletes those logs before that harvest.
	shutdownAfterJobFinishes := spec.Lifespan != v1.LifespanRetain
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: spec.Namespace,
			Labels:    k8spod.WorkloadLabels(spec),
		},
		Spec: rayv1.RayJobSpec{
			Entrypoint:               renderEntrypoint(spec),
			JobId:                    spec.ID,
			BackoffLimit:             &backoffLimit,
			ShutdownAfterJobFinishes: shutdownAfterJobFinishes,
			RuntimeEnvYAML:           runtimeEnvYAML(env),
			RayClusterSpec: &rayv1.RayClusterSpec{
				RayVersion:       rayVersion,
				HeadGroupSpec:    headGroupSpec(spec, env),
				WorkerGroupSpecs: workerGroupSpecs(spec, env),
			},
		},
	}
	if spec.RayCluster != nil {
		rayJob.Spec.ClusterSelector = map[string]string{rayClusterSelectorKey: spec.RayCluster.Name}
		rayJob.Spec.RayClusterSpec = nil
		// KubeRay rejects BackoffLimit in ClusterSelector mode. Retries for jobs on
		// an external cluster are driven by the owning ZenthrosJob instead.
		rayJob.Spec.BackoffLimit = nil
		rayJob.Spec.ShutdownAfterJobFinishes = false
		// A standing cluster (target.rayCluster): submit over the Ray Jobs API. No
		// submitter pod — logs come from GET /api/jobs/{id}/logs on the agent.
		rayJob.Spec.SubmissionMode = rayv1.HTTPMode
	} else {
		rayJob.Spec.SubmitterPodTemplate = submitterPodTemplate(spec, env)
	}
	return rayJob
}

// BuildRayCluster translates a JobSpec into a standing KubeRay RayCluster.
// The CRD lists this kind beside RayJob; admission creates the cluster itself,
// not a RayJob that would shut the cluster down when an entrypoint exits.
func BuildRayCluster(spec jobspec.JobSpec) *rayv1.RayCluster {
	env := backend.WithDataReferences(spec.Env, spec.Artifacts)
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: spec.Namespace,
			Labels:    k8spod.WorkloadLabels(spec),
		},
		Spec: rayv1.RayClusterSpec{
			RayVersion:       rayVersion,
			HeadGroupSpec:    headGroupSpec(spec, env),
			WorkerGroupSpecs: workerGroupSpecs(spec, env),
		},
	}
}

// submitterPodTemplate sizes the pod that runs `ray job submit`. Explicit requests
// keep it admissible under a tenant ResourceQuota that enforces requests.
func submitterPodTemplate(spec jobspec.JobSpec, env map[string]string) *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: k8spod.WorkloadPodLabels(spec)},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyOnFailure,
			Containers: []corev1.Container{{
				Name:  submitterName,
				Image: spec.Image,
				Env:   envVars(env),
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse(submitterCPU),
						corev1.ResourceMemory: resource.MustParse(submitterMemory),
					},
				},
			}},
		},
	}
}

func headGroupSpec(spec jobspec.JobSpec, env map[string]string) rayv1.HeadGroupSpec {
	return rayv1.HeadGroupSpec{
		RayStartParams: map[string]string{"dashboard-host": "0.0.0.0"},
		Template:       podTemplate(spec, defaultHeadCPU, defaultHeadMemory, 0, env),
	}
}

// workerGroupSpecs returns nil for a head-only job (Max <= 0).
func workerGroupSpecs(spec jobspec.JobSpec, env map[string]string) []rayv1.WorkerGroupSpec {
	if spec.Workers.Max <= 0 {
		return nil
	}
	replicas := spec.Workers.Min
	minReplicas := spec.Workers.Min
	maxReplicas := spec.Workers.Max
	cpu := valueOr(spec.Workers.CPU, defaultWorkerCPU)
	memory := valueOr(spec.Workers.Memory, defaultWorkerMemory)
	return []rayv1.WorkerGroupSpec{{
		GroupName:      workerGroupName,
		Replicas:       &replicas,
		MinReplicas:    &minReplicas,
		MaxReplicas:    &maxReplicas,
		RayStartParams: map[string]string{},
		Template:       podTemplate(spec, cpu, memory, spec.Workers.GPUsPerNode, env),
	}}
}

func podTemplate(spec jobspec.JobSpec, cpu, memory string, gpus int32, env map[string]string) corev1.PodTemplateSpec {
	container := corev1.Container{
		Name:      RayContainerName,
		Image:     spec.Image,
		Resources: containerResources(cpu, memory, gpus),
		Env:       envVars(env),
	}
	return k8spod.WorkloadPodTemplate(spec, corev1.PodSpec{Containers: []corev1.Container{container}}, k8spod.DefaultRuntime)
}

// renderEntrypoint returns the RayJob entrypoint string: the spec's shell entrypoint
// verbatim, or Command+Args quoted into a single string when the structured form is
// used. Ray always runs the entrypoint through a shell, so structured tokens must be
// quoted to keep their exact meaning (the point of the structured spec).
//
// A dedicated cluster submits from a submitter pod (K8sJobMode), where the
// entrypoint meets a second shell first: KubeRay splices it into a bash line
// ("ray job submit ... -- <entrypoint>") and Ray rejoins the words with
// list2cmdline, which double-quotes only words with blanks. A command that
// would not come through that unchanged is shipped encoded instead
// (submitterEncoded). A standing cluster is submitted over HTTP and read by
// one shell only.
func renderEntrypoint(spec jobspec.JobSpec) string {
	command, survives := spec.Entrypoint, plainWords(spec.Entrypoint)
	if len(spec.Command) > 0 {
		tokens := make([]string, 0, len(spec.Command)+len(spec.Args))
		tokens = append(tokens, spec.Command...)
		tokens = append(tokens, spec.Args...)
		quoted := make([]string, len(tokens))
		for i, token := range tokens {
			quoted[i] = shellword.Quote(token)
		}
		command, survives = strings.Join(quoted, " "), tokensSurviveSubmitter(tokens)
	}
	if survives || spec.RayCluster != nil {
		return command
	}
	return submitterEncoded(spec.ID, command)
}

// plainWords reports whether every word of a shell command is plain: bash
// reads it back as the same word and list2cmdline leaves it alone, so the
// submitter's extra shell changes nothing.
func plainWords(command string) bool {
	for _, word := range strings.Fields(command) {
		if !plainWord(word) {
			return false
		}
	}
	return true
}

func plainWord(word string) bool {
	for _, r := range word {
		if !strings.ContainsRune(plainChars, r) {
			return false
		}
	}
	return word != ""
}

const plainChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_@%+=:,./-"

// tokensSurviveSubmitter reports whether structured tokens, quoted for bash,
// reach Ray's shell as the same tokens: a plain token passes bare, and a
// token with blanks is double-quoted by list2cmdline, which keeps it whole
// unless it holds a character a double-quoted shell word still expands.
func tokensSurviveSubmitter(tokens []string) bool {
	for i, token := range tokens {
		switch {
		case token == "":
		case strings.ContainsAny(token, " \t"):
			if strings.ContainsAny(token, "\"$`\\") {
				return false
			}
		case !plainWord(token), i == 0 && strings.Contains(token, "="):
			return false
		}
	}
	return true
}

// submitterEncoded wraps a shell command so it reaches Ray's job shell byte
// for byte through the submitter's bash and list2cmdline. The command travels
// as base64 inside a word with no character either shell expands; Ray's shell
// decodes it to a file and runs that, so it is read by one shell only.
func submitterEncoded(jobID, command string) string {
	name := strings.Map(func(r rune) rune {
		if r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			return r
		}
		return -1
	}, jobID)
	if name == "" {
		name = "job"
	}
	file := "/tmp/zenthros-entrypoint-" + name
	encoded := base64.StdEncoding.EncodeToString([]byte(command))
	return fmt.Sprintf("sh -c 'echo %s | base64 -d > %s && exec sh %s'", encoded, file, file)
}

// InlineSecretJobLabel names the job on its inline Secrets, for cleanup.
const InlineSecretJobLabel = k8spod.InlineSecretJobLabel

// InlineSecretNameFor is the Secret holding spec's inline bundle, named by its
// content (see k8spod.InlineSecretNameFor).
func InlineSecretNameFor(spec jobspec.JobSpec) string {
	return k8spod.InlineSecretNameFor(spec)
}

func containerResources(cpu, memory string, gpus int32) corev1.ResourceRequirements {
	requests := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}
	limits := corev1.ResourceList{}
	if gpus > 0 {
		limits[gpuResourceName] = *resource.NewQuantity(int64(gpus), resource.DecimalSI)
	}
	return corev1.ResourceRequirements{Requests: requests, Limits: limits}
}

func envVars(env map[string]string) []corev1.EnvVar {
	if len(env) == 0 {
		return nil
	}
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]corev1.EnvVar, 0, len(env))
	for _, name := range names {
		out = append(out, corev1.EnvVar{Name: name, Value: env[name]})
	}
	return out
}

// SpecMismatch returns a SpecMismatchError when the live resource diverges from
// the desired spec on a field Zenthros owns. Returning nil means "safe to reuse".
func SpecMismatch(existing, desired *rayv1.RayJob) *backend.SpecMismatchError {
	if existing.Spec.Entrypoint != desired.Spec.Entrypoint {
		return backend.NewSpecMismatchError("entrypoint", desired.Spec.Entrypoint, existing.Spec.Entrypoint)
	}
	if existingImage, desiredImage := headImage(existing), headImage(desired); existingImage != desiredImage {
		return backend.NewSpecMismatchError("image", desiredImage, existingImage)
	}
	if existingSelector, desiredSelector := clusterSelector(existing), clusterSelector(desired); existingSelector != desiredSelector {
		return backend.NewSpecMismatchError("target.rayCluster", desiredSelector, existingSelector)
	}
	if existingEnv, desiredEnv := userRuntimeEnv(existing.Spec.RuntimeEnvYAML), userRuntimeEnv(desired.Spec.RuntimeEnvYAML); existingEnv != desiredEnv {
		return backend.NewSpecMismatchError("runtimeEnv", desiredEnv, existingEnv)
	}
	if existingSecrets, desiredSecrets := secretBindingFingerprint(existing), secretBindingFingerprint(desired); existingSecrets != desiredSecrets {
		return backend.NewSpecMismatchError("secretSources", desiredSecrets, existingSecrets)
	}
	return nil
}

func clusterSelector(rayJob *rayv1.RayJob) string {
	if rayJob.Spec.ClusterSelector == nil {
		return ""
	}
	return rayJob.Spec.ClusterSelector[rayClusterSelectorKey]
}

// runtimeEnvYAML carries per-job environment into the Ray Job driver. Container
// env on the Ray node is not enough: Ray Job supervisors start the entrypoint
// with a filtered environment and do not reliably inherit TOKEN-like variables.
// JSON string quoting is valid YAML and prevents values from becoming YAML types.
func runtimeEnvYAML(env map[string]string) string {
	if len(env) == 0 {
		return ""
	}
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)

	var output strings.Builder
	output.WriteString("env_vars:\n")
	for _, name := range names {
		fmt.Fprintf(&output, "  %q: %q\n", name, env[name])
	}
	return output.String()
}

// userRuntimeEnv is a runtimeEnvYAML without the variables the control plane
// derives per run (jobspec.ControllerDerivedEnv): a resubmit after a
// checkpoint ack carries a newer cursor for the same RayJob.
func userRuntimeEnv(yaml string) string {
	var out strings.Builder
	for _, line := range strings.SplitAfter(yaml, "\n") {
		if name, ok := runtimeEnvName(line); ok && jobspec.ControllerDerivedEnv(name) {
			continue
		}
		out.WriteString(line)
	}
	return out.String()
}

// runtimeEnvName reads the variable name of one `  "NAME": "VALUE"` line.
func runtimeEnvName(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, `"`) {
		return "", false
	}
	quoted, err := strconv.QuotedPrefix(trimmed)
	if err != nil {
		return "", false
	}
	name, err := strconv.Unquote(quoted)
	return name, err == nil
}

func secretBindingFingerprint(rayJob *rayv1.RayJob) string {
	if rayJob.Spec.RayClusterSpec == nil {
		return ""
	}
	pod := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec
	if len(pod.Containers) == 0 {
		return ""
	}
	parts := make([]string, 0, 8)
	for _, env := range pod.Containers[0].Env {
		if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
			continue
		}
		ref := env.ValueFrom.SecretKeyRef
		parts = append(parts, fmt.Sprintf("env:%s=%s/%s", env.Name, ref.Name, ref.Key))
	}
	for _, mount := range pod.Containers[0].VolumeMounts {
		if strings.HasPrefix(mount.Name, "zenthros-secret-") {
			parts = append(parts, fmt.Sprintf("mount:%s:%s:%s", mount.Name, mount.MountPath, mount.SubPath))
		}
	}
	for _, volume := range pod.Volumes {
		if !strings.HasPrefix(volume.Name, "zenthros-secret-") {
			continue
		}
		secretName := ""
		if volume.Secret != nil {
			secretName = volume.Secret.SecretName
		}
		parts = append(parts, fmt.Sprintf("vol:%s=%s", volume.Name, secretName))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// RayClusterSpecMismatch reports a standing cluster of the job's name that
// runs another image than a resubmit would create (06#8): it was reused
// silently.
func RayClusterSpecMismatch(existing, desired *rayv1.RayCluster) *backend.SpecMismatchError {
	if have, want := clusterHeadImage(&existing.Spec), clusterHeadImage(&desired.Spec); have != want {
		return backend.NewSpecMismatchError("image", want, have)
	}
	return nil
}

func clusterHeadImage(spec *rayv1.RayClusterSpec) string {
	containers := spec.HeadGroupSpec.Template.Spec.Containers
	if len(containers) == 0 {
		return ""
	}
	return containers[0].Image
}

func headImage(rayJob *rayv1.RayJob) string {
	if rayJob.Spec.RayClusterSpec == nil {
		return ""
	}
	return clusterHeadImage(rayJob.Spec.RayClusterSpec)
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
