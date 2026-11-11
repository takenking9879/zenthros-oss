// SPDX-License-Identifier: Apache-2.0

// Package jobspec defines the backend-agnostic description of a job. Adapters
// translate a JobSpec into a native resource (RayJob, SparkApplication, process).
package jobspec

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/checkpoint"
	"github.com/zenthros/operator/pkg/resultdigest"
)

// JobSpec is the normalized, backend-neutral view of a ZenthrosJob plus the
// scheduler's placement decision. Adapters depend on this, never on the CRD status.
type JobSpec struct {
	// ID is the stable idempotency key; Submit deduplicates on it.
	ID string `json:"-"`
	// Name is the ZenthrosJob name.
	Name string `json:"-"`
	// Namespace is the tenant namespace resolved by the scheduler.
	Namespace string `json:"-"`
	// Tenant owning the job.
	Tenant string `json:"tenant,omitempty"`
	// Workspace is the job's Workspace, resolved from its namespace, and
	// WorkspaceUID that Workspace's UID: a remote cluster's namespace is one
	// incarnation's of the name (N7).
	Workspace    string `json:"workspace,omitempty"`
	WorkspaceUID string `json:"workspaceUID,omitempty"`
	// Backend type, e.g. "k8s" or "vm".
	Backend string `json:"backend,omitempty"`
	// TargetBackend is the effective backend id the job is routed to, from
	// spec.target.backends, the lease, or the automatic Scheduler decision.
	TargetBackend string `json:"targetBackend,omitempty"`
	// Kind is the workload kind, e.g. "RayJob".
	Kind string `json:"-"`
	// Entrypoint submitted to the engine (shell string). Empty when Command is set.
	Entrypoint string `json:"entrypoint,omitempty"`
	// Command is the structured executable. Adapters run it without shell
	// interpretation. Empty when Entrypoint is set.
	Command []string `json:"command,omitempty"`
	// Args are appended to Command verbatim.
	Args []string `json:"args,omitempty"`
	// WorkingDir overrides the working directory derived from Source.
	WorkingDir string `json:"workingDir,omitempty"`
	// Image is the runtime-only container image.
	Image string `json:"image,omitempty"`
	// Workers sizes the worker group.
	Workers v1.WorkerGroup `json:"workers"`
	// Compute is spec.compute: over Workers, the size of a Job's pod
	// (v1.SizesOf).
	Compute *v1.ComputeConstraints `json:"compute,omitempty"`
	// Env holds plain name/value pairs for the workload process.
	Env map[string]string `json:"env,omitempty"`
	// BackoffLimit bounds backend-native submission retries.
	BackoffLimit int32 `json:"backoffLimit,omitempty"`
	// Artifacts references object-storage input/output URIs.
	Artifacts v1.Artifacts `json:"artifacts,omitempty"`
	// StorageClass referenced by the workload.
	StorageClass string `json:"storageClass,omitempty"`
	// SecretSources contains destination-local references, never secret values.
	SecretSources []v1.SecretSourceRef `json:"secretSources,omitempty"`
	// Source declares the origin of user code.
	Source v1.Source `json:"source,omitempty"`
	// Spark contains native Spark intent for SparkApplication jobs only.
	Spark *v1.SparkSpec `json:"spark,omitempty"`
	// Service makes a Job a Service: the ports its process listens on,
	// whose endpoints the agent publishes once they accept connections.
	Service *v1.ServiceSpec `json:"service,omitempty"`
	// RayCluster is the existing RayCluster a RayJob runs on
	// (spec.target.rayCluster); nil for a dedicated cluster.
	RayCluster *v1.RayClusterReference `json:"rayCluster,omitempty"`
	// Manifest is the object a Manifest workload applies, with its status
	// contract. Nil for every other kind.
	Manifest *v1.ManifestSpec `json:"manifest,omitempty"`
	// Runtime is what the workload runs under, resolved by the controller from
	// spec.compute.runtime and the backend's declared default (systemd,
	// containerd, workspace-containers, kubernetes). Executors never infer it.
	Runtime string `json:"runtime,omitempty"`
	// Attempt is which run of the process this is on a machine: 0 first, n
	// for the n-th re-run after a failure. Each attempt is its own unit
	// (zenthros-<id>-a<n>) and sees ZENTHROS_ATTEMPT. Backends that restart
	// natively ignore it.
	Attempt int32 `json:"attempt,omitempty"`
	// IsolatedNetwork: the backend is shared between Workspaces, so a
	// container job gets a network of its own instead of the host's: no
	// instance metadata, no host ports, no other job.
	// The controller sets it from the backend's access policy.
	IsolatedNetwork bool `json:"isolatedNetwork,omitempty"`
	// Lifespan is Ephemeral (delete after GC) or Retain (keep finished Job/Pod for debug).
	Lifespan string `json:"lifespan,omitempty"`
	// RetainWorkdir tells the agent to keep the job's work directory when the
	// workload is cleaned up (VM only; k8s agents ignore it).
	RetainWorkdir bool `json:"retainWorkdir,omitempty"`

	// Labels are the ZenthrosJob's labels; Kubernetes pods carry them so
	// other jobs' placement rules can select them.
	Labels map[string]string `json:"labels,omitempty"`
	// Placement is spec.placement; a Kubernetes backend renders it as the
	// pods' spread and affinity constraints.
	Placement *v1.PlacementPolicy `json:"placement,omitempty"`

	// PreemptSignal is sent to the process after its machine's interruption
	// notice is written to ZENTHROS_PREEMPT_FILE; empty sends none.
	PreemptSignal string `json:"preemptSignal,omitempty"`

	// Distribution is spec.workload.template.distribution for a backend
	// that runs every task itself (a Kubernetes cluster: one Indexed Job).
	// Tasks on machines are jobs of their own and carry none.
	Distribution *v1.Distribution `json:"distribution,omitempty"`
	// Owner identity lets the adapter set a controller ownerReference on the
	// native resource, enabling owner-based watches and garbage collection.
	OwnerKind       string `json:"ownerKind,omitempty"`
	OwnerAPIVersion string `json:"ownerAPIVersion,omitempty"`
	OwnerName       string `json:"ownerName,omitempty"`
	OwnerUID        string `json:"ownerUID,omitempty"`
}

// FromZenthrosJob builds a JobSpec from a ZenthrosJob and a resolved namespace.
func FromZenthrosJob(job *v1.ZenthrosJob, backendType, namespace string) JobSpec {
	template := job.Spec.Workload.Template
	spec := JobSpec{
		ID:            job.JobID(),
		Name:          job.Name,
		Namespace:     namespace,
		Workspace:     job.Spec.Workspace,
		Backend:       backendType,
		TargetBackend: job.Spec.Target.FirstBackend(),
		Kind:          job.Spec.Workload.Kind,
		Entrypoint:    template.Entrypoint,
		Command:       template.Command,
		Args:          template.Args,
		WorkingDir:    template.WorkingDir,
		Image:         template.Image,
		Workers:       template.Workers,
		Compute:       job.Spec.Compute,
		Labels:        job.Labels,
		Placement:     job.Spec.Placement,
		Env:           resumeEnv(job, template.Env),
		BackoffLimit:  template.RestartPolicy.BackoffLimit,
		Artifacts:     template.Artifacts,
		StorageClass:  template.StorageClass,
		SecretSources: template.SecretSources,
		Source:        template.Source,
		Spark:         template.Spark,
		RayCluster:    job.Spec.Target.RayCluster,
		Manifest:      template.Manifest,
		Distribution:  template.Distribution,
		Service:       template.Service,

		OwnerKind:       "ZenthrosJob",
		OwnerAPIVersion: v1.GroupVersion.String(),
		OwnerName:       job.Name,
		OwnerUID:        string(job.UID),
	}
	spec.Attempt = job.Status.Run()
	if job.Spec.Compute != nil {
		spec.Runtime = job.Spec.Compute.Runtime
		spec.Lifespan = job.Spec.Compute.Lifespan
	}
	if job.Spec.GCPolicy != nil {
		spec.RetainWorkdir = job.Spec.GCPolicy.RetainWorkdir
		if spec.Lifespan == "" && job.Spec.GCPolicy.CleanupPolicy == v1.CleanupPolicyRetain {
			spec.Lifespan = v1.LifespanRetain
		}
	}
	if r := job.Spec.Resilience; r != nil && r.PreemptNotice != nil {
		spec.PreemptSignal = r.PreemptNotice.Signal
	}
	if spec.Lifespan == "" {
		spec.Lifespan = v1.LifespanEphemeral
	}
	return spec
}

// ValidateWorkload rejects kind/intent combinations before they cross an adapter
// boundary. The CRD enforces the same contract for Kubernetes API submissions.
func ValidateWorkload(spec JobSpec) error {
	if err := validSizes(spec); err != nil {
		return err
	}
	if spec.Manifest != nil && spec.Kind != v1.WorkloadKindManifest {
		return fmt.Errorf("%s cannot include a manifest; set workload.kind Manifest", spec.Kind)
	}
	switch spec.Kind {
	case v1.WorkloadKindRayJob:
		if spec.Spark != nil {
			return fmt.Errorf("RayJob cannot include spark intent")
		}
		if spec.RayCluster != nil {
			if spec.RayCluster.Name == "" {
				return fmt.Errorf("target.rayCluster requires name")
			}
			if spec.RayCluster.Namespace == "" {
				return fmt.Errorf("target.rayCluster requires namespace")
			}
			if spec.Namespace != "" && spec.RayCluster.Namespace != spec.Namespace {
				return fmt.Errorf("target.rayCluster namespace %q is outside workload namespace %q", spec.RayCluster.Namespace, spec.Namespace)
			}
			if len(spec.SecretSources) > 0 {
				return fmt.Errorf("target.rayCluster cannot apply secretSources to an externally owned cluster")
			}
			if spec.Source.HostPath != "" || spec.Source.Git != "" || spec.Source.S3 != "" || spec.Source.Inline != "" || spec.Source.Ref != "" {
				return fmt.Errorf("target.rayCluster cannot apply source mounts to an externally owned cluster")
			}
			if spec.Workers.Min != 0 || spec.Workers.Max != 0 || spec.Workers.GPUsPerNode != 0 || spec.Workers.CPU != "" || spec.Workers.Memory != "" || spec.Workers.ResourceClass != "" || spec.Workers.Interconnect != "" {
				return fmt.Errorf("target.rayCluster cannot resize or reconfigure an externally owned cluster")
			}
		}
		return nil
	case v1.WorkloadKindSparkApplication:
		if spec.RayCluster != nil {
			return fmt.Errorf("SparkApplication cannot use target.rayCluster")
		}
		if spec.Spark == nil {
			return fmt.Errorf("SparkApplication requires spark intent")
		}
		if spec.Spark.MainApplicationFile == "" {
			return fmt.Errorf("SparkApplication requires mainApplicationFile")
		}
		if spec.Entrypoint != "" || len(spec.Command) > 0 || len(spec.Args) > 0 {
			return fmt.Errorf("SparkApplication uses spark.arguments, not entrypoint or command")
		}
		return nil
	case v1.WorkloadKindRayCluster:
		if spec.Spark != nil || spec.RayCluster != nil {
			return fmt.Errorf("RayCluster cannot include spark intent or target.rayCluster")
		}
		if spec.Entrypoint != "" || len(spec.Command) > 0 || len(spec.Args) > 0 {
			return fmt.Errorf("a RayCluster runs no command; submit RayJobs to it with spec.target.rayCluster")
		}
		if spec.Image == "" {
			return fmt.Errorf("RayCluster requires image")
		}
		return nil
	case v1.WorkloadKindJob:
		if spec.Spark != nil {
			return fmt.Errorf("Job cannot include spark intent")
		}
		if spec.RayCluster != nil {
			return fmt.Errorf("Job cannot use target.rayCluster")
		}
		hasEntry := spec.Entrypoint != ""
		hasCmd := len(spec.Command) > 0
		if hasEntry == hasCmd {
			return fmt.Errorf("Job requires exactly one of entrypoint or command")
		}
		if len(spec.Args) > 0 && !hasCmd {
			return fmt.Errorf("args requires command")
		}
		// The image depends on the runtime the job resolved to (RequiresImage).
		return nil
	case v1.WorkloadKindManifest:
		if spec.Manifest == nil || len(spec.Manifest.Object.Raw) == 0 {
			return fmt.Errorf("Manifest requires manifest.object")
		}
		if spec.Spark != nil || spec.RayCluster != nil {
			return fmt.Errorf("Manifest cannot include spark intent or target.rayCluster")
		}
		if spec.Entrypoint != "" || len(spec.Command) > 0 || len(spec.Args) > 0 || spec.Image != "" {
			return fmt.Errorf("a Manifest carries its own object: entrypoint, command, args and image do not apply")
		}
		if len(spec.SecretSources) > 0 || spec.Source.HostPath != "" || spec.Source.Git != "" || spec.Source.S3 != "" || spec.Source.Inline != "" || spec.Source.Ref != "" {
			return fmt.Errorf("a Manifest carries its own object: source and secretSources do not apply")
		}
		return nil
	default:
		return fmt.Errorf("unsupported workload kind %q", spec.Kind)
	}
}

// validSizes refuses a size that does not parse: Encode and Decode both run
// ValidateWorkload, so no executor builds a pod from one.
func validSizes(spec JobSpec) error {
	sizes := map[string]string{"workers.cpu": spec.Workers.CPU, "workers.memory": spec.Workers.Memory}
	if compute := spec.Compute; compute != nil {
		sizes["compute.cpu"], sizes["compute.memory"] = compute.CPU, compute.Memory
	}
	for field, value := range sizes {
		if value == "" {
			continue
		}
		parsed, err := resource.ParseQuantity(value)
		if err != nil {
			return fmt.Errorf("invalid %s %q: %w", field, value, err)
		}
		if parsed.Sign() < 0 {
			return fmt.Errorf("invalid %s %q: quantity must be non-negative", field, value)
		}
	}
	return nil
}

// RequiresImage reports whether a workload under runtime needs
// workload.template.image: everything except a process on the machine, or
// one in a container, which runs in the container's own image.
func RequiresImage(runtime string) bool {
	return runtime != "" && runtime != v1.RuntimeSystemd && runtime != v1.RuntimeProcess
}

func envToMap(envs []v1.EnvVar) map[string]string {
	if len(envs) == 0 {
		return nil
	}
	out := make(map[string]string, len(envs))
	for _, e := range envs {
		out[e.Name] = e.Value
	}
	return out
}

// Variables the control plane sets per run. They say which run this is, not
// what runs.
const (
	EnvAttempt     = "ZENTHROS_ATTEMPT"
	EnvReportToken = "ZENTHROS_REPORT_TOKEN"
	// EnvPreemptFile is where the agent writes the machine's interruption
	// notice for this job.
	EnvPreemptFile = "ZENTHROS_PREEMPT_FILE"
	// EnvCacheDir is where a job finds the artifacts a Prewarm put on its
	// machine, by the paths the Prewarm named.
	EnvCacheDir = "ZENTHROS_CACHE_DIR"
	// EnvEnvsDir is where a job builds and finds its function environments
	// (the SDK's cache root): its Workspace's, on the machine.
	EnvEnvsDir = "ZENTHROS_ENV_CACHE"
	// EnvAgentBin is the agent binary as the job sees it, for
	// `$ZENTHROS_AGENT_BIN job ...` (reports, checkpoints, notices) from any
	// language. Where it is says where the agent is installed, not what runs.
	EnvAgentBin = "ZENTHROS_AGENT_BIN"
	// EnvSecretsDir is where the job finds its file secrets that name no
	// path, as <dir>/<secret-name>/<key>: where the agent keeps them, which
	// says nothing about what runs.
	EnvSecretsDir = "ZENTHROS_SECRETS_DIR"
)

var controllerDerived = map[string]bool{
	checkpoint.EnvGeneration: true,
	checkpoint.EnvURI:        true,
	checkpoint.EnvPhase:      true,
	checkpoint.EnvSeq:        true,
	checkpoint.EnvSHA256:     true,
	checkpoint.EnvComplete:   true,
	checkpoint.EnvCursors:    true,
	EnvAttempt:               true,
	EnvReportToken:           true,
	EnvPreemptFile:           true,
	EnvAgentBin:              true,
	EnvSecretsDir:            true,
}

// The agent sets these on the host it runs the job on: where it listens for
// reports, and the job's ID. Moving the socket
// changed the endpoint under the units that were running, and their
// resubmit failed SPEC_MISMATCH.
const (
	EnvReportEndpoint = "ZENTHROS_REPORT_ENDPOINT"
	EnvJobID          = "ZENTHROS_JOB_ID"
)

var agentDerived = map[string]bool{
	EnvReportEndpoint: true,
	EnvJobID:          true,
}

// ControllerDerivedEnv reports a variable the control plane derives per run:
// the lease generation, the attempt, the report token and the acked
// checkpoint cursor. A resubmit after a checkpoint ack or a new lease changes
// them while the workload is the same one, so every spec fingerprint (the
// systemd unit hash, a RayJob's runtimeEnv) leaves them out: the resubmit
// adopts the running workload instead of failing SPEC_MISMATCH. The
// checkpoint prefix is the user's and stays in.
func ControllerDerivedEnv(name string) bool {
	return controllerDerived[name] || agentDerived[name]
}

func resumeEnv(job *v1.ZenthrosJob, envs []v1.EnvVar) map[string]string {
	out := envToMap(envs)
	if out == nil {
		out = map[string]string{}
	}
	checkpoint.ApplyResumeEnv(out, job)
	// The upstream pins come from status, which only the control plane writes.
	if pins := resultdigest.EnvValue(job.Status.UpstreamResults); pins != "" {
		out[resultdigest.EnvUpstreamResults] = pins
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// AttemptScoped gives a re-run its own executor-side identity, keyed by
// <id>-a<n>, so it never adopts or overwrites the failed attempt's unit,
// process or work directory. The env says which run it is
// (ZENTHROS_ATTEMPT). Reports and checkpoints stay keyed by the job
// (ZENTHROS_JOB_ID is already in env).
func AttemptScoped(spec JobSpec, env map[string]string) (JobSpec, map[string]string) {
	out := make(map[string]string, len(env)+1)
	for k, v := range env {
		out[k] = v
	}
	out[EnvAttempt] = strconv.Itoa(int(spec.Attempt))
	if spec.Attempt > 0 {
		spec.ID = fmt.Sprintf("%s-a%d", spec.ID, spec.Attempt)
	}
	return spec, out
}

// ServiceFingerprint is what a Service adds to a spec's fingerprint: the
// ports, in order, by name, number and protocol.
func ServiceFingerprint(service *v1.ServiceSpec) string {
	var b strings.Builder
	for _, p := range service.Ports {
		fmt.Fprintf(&b, "%s/%d/%s;", p.Name, p.Port, p.Protocol)
	}
	return b.String()
}
