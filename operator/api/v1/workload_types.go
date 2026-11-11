// SPDX-License-Identifier: Apache-2.0

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Workload kind values. Each backend's agent declares which of these it runs
// (pkg/capability); nothing else in Zenthros decides it.
const (
	// WorkloadKindJob is a run-to-completion process: a systemd unit or a
	// containerd container on a machine, a batch/v1 Job on Kubernetes.
	WorkloadKindJob              = "Job"
	WorkloadKindRayJob           = "RayJob"
	WorkloadKindRayCluster       = "RayCluster"
	WorkloadKindSparkApplication = "SparkApplication"
	// WorkloadKindManifest is any namespaced Kubernetes object the user writes
	// (a batch Job, a PyTorchJob, a Volcano Job...). The operator that owns it
	// runs it; Zenthros applies it, reads its status by a declared contract and
	// relays the operator's errors word for word.
	WorkloadKindManifest = "Manifest"
)

// Workload describes what runs, independent of where it runs.
// +kubebuilder:validation:XValidation:rule="self.kind == 'SparkApplication' ? has(self.template.spark) : !has(self.template.spark)",message="spark must be set only for SparkApplication workloads"
// +kubebuilder:validation:XValidation:rule="self.kind == 'Manifest' ? has(self.template.manifest) : !has(self.template.manifest)",message="manifest must be set exactly when workload.kind is Manifest"
// +kubebuilder:validation:XValidation:rule="self.kind != 'Manifest' || (!has(self.template.entrypoint) && !has(self.template.command) && !has(self.template.image) && !has(self.template.secretSources) && (!has(self.template.source) || (!has(self.template.source.hostPath) && !has(self.template.source.git) && !has(self.template.source.s3) && !has(self.template.source.inline) && !has(self.template.source.ref))))",message="a Manifest carries its own object: entrypoint, command, image, source and secretSources do not apply"
// +kubebuilder:validation:XValidation:rule="self.kind in ['SparkApplication', 'RayCluster', 'Manifest'] || (has(self.template.entrypoint) != has(self.template.command))",message="Job and RayJob workloads require exactly one of entrypoint or command"
// +kubebuilder:validation:XValidation:rule="self.kind != 'RayCluster' || (!has(self.template.entrypoint) && !has(self.template.command))",message="a RayCluster runs no command; submit RayJobs to it with spec.target.rayCluster"
// +kubebuilder:validation:XValidation:rule="self.kind == 'SparkApplication' || !has(self.template.args) || has(self.template.command)",message="args requires command"
// +kubebuilder:validation:XValidation:rule="!(self.kind in ['SparkApplication', 'Manifest']) || !has(self.template.restartPolicy) || !has(self.template.restartPolicy.backoffLimit)",message="restartPolicy.backoffLimit does not apply to SparkApplication (use spark.restartPolicy) or Manifest (the object sets its own)"
// +kubebuilder:validation:XValidation:rule="!has(self.template.distribution) || self.kind == 'Job'",message="distribution splits a Job; RayJob, RayCluster, SparkApplication and Manifest distribute through their own operator"
// +kubebuilder:validation:XValidation:rule="!has(self.template.service) || self.kind == 'Job'",message="service makes a Job a Service; a Manifest declares lifecycle: Service and endpointFrom instead"
// +kubebuilder:validation:XValidation:rule="!has(self.template.service) || !has(self.template.distribution) || self.template.distribution.count <= 1",message="a Service is one replica; distribution does not apply"
type Workload struct {
	// Kind selects the native resource the adapter creates.
	// +kubebuilder:validation:Enum=Job;RayJob;RayCluster;SparkApplication;Manifest
	Kind string `json:"kind"`

	// Template is the logical workload specification.
	Template WorkloadTemplate `json:"template"`
}

// WorkloadTemplate is the backend-agnostic logical spec of the workload.
type WorkloadTemplate struct {
	// Entrypoint is the shell string submitted to the engine, e.g. "python train.py".
	// Mutually exclusive with the structured Command; prefer Command, which runs
	// without shell interpretation (Phase O).
	// +optional
	Entrypoint string `json:"entrypoint,omitempty"`

	// Command is the structured executable, e.g. ["python", "train.py"]. Mutually
	// exclusive with Entrypoint. Tokens pass to the backend uninterpreted by any
	// shell: k8s renders them quoted into the RayJob entrypoint, a VM runs them
	// directly as the systemd ExecStart.
	// +kubebuilder:validation:MinItems=1
	Command []string `json:"command,omitempty"`

	// Args are appended to Command verbatim (no shell interpretation). Only valid
	// with Command.
	Args []string `json:"args,omitempty"`

	// WorkingDir is the working directory the workload runs in. Overrides the
	// working directory derived from Source (the mount path on k8s, the prepared
	// source dir on a VM). Use an absolute path valid on the target backend.
	// +kubebuilder:validation:MaxLength=4096
	// +kubebuilder:validation:Pattern=`^/[^\x00-\x1f\x7f]*$`
	WorkingDir string `json:"workingDir,omitempty"`

	// Image is the runtime-only container image (no user code baked in): an
	// OCI reference, [registry[:port]/]path[:tag][@digest]. Agents hand it to
	// root-run tooling, so nothing outside that grammar is admitted. A Job
	// that runs as a process on the machine (runtime systemd) needs none;
	// every containerized runtime does, which admission checks once the
	// runtime is resolved.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._/:@-]*$`
	Image string `json:"image,omitempty"`

	// Workers describes the worker group sizing.
	// +optional
	Workers WorkerGroup `json:"workers,omitempty"`

	// Distribution splits the Job into Independent tasks across machines.
	// Workers still sizes each task.
	// +optional
	Distribution *Distribution `json:"distribution,omitempty"`

	// Env holds plain name/value pairs injected into the workload process.
	Env []EnvVar `json:"env,omitempty"`

	// RestartPolicy configures backend-native resilience (delegated, not reimplemented).
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`

	// Artifacts references object-storage URIs for inputs and outputs.
	Artifacts Artifacts `json:"artifacts,omitempty"`

	// StorageClass referenced by the workload; admin pre-provisions it.
	StorageClass string `json:"storageClass,omitempty"`

	// SecretSources carries destination-local references only. Secret values must
	// never be placed in this API or resolved by the controller.
	SecretSources []SecretSourceRef `json:"secretSources,omitempty"`

	// Source declares where the user code comes from (exactly one mode set).
	Source Source `json:"source,omitempty"`

	// Spark contains Spark-native intent. It is valid only when workload.kind is
	// SparkApplication; common image, env, source, and workspace fields stay outside it.
	// +optional
	Spark *SparkSpec `json:"spark,omitempty"`

	// Manifest is the Kubernetes object a Manifest workload applies and how
	// to read its status. Valid only when workload.kind is Manifest.
	// +optional
	Manifest *ManifestSpec `json:"manifest,omitempty"`

	// Service makes a Job a Service: a process that listens on these ports
	// and runs until it is cancelled. It never Succeeds; its agent publishes
	// each port's URL in status.endpoints once the port accepts connections.
	// Valid only when workload.kind is Job.
	// +optional
	Service *ServiceSpec `json:"service,omitempty"`
}

// ServiceSpec is what a Service listens on.
type ServiceSpec struct {
	// Ports the process listens on, on every interface of its machine.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:XValidation:rule="self.all(p, self.exists_one(q, q.port == p.port))",message="each port appears once"
	Ports []ServicePort `json:"ports"`

	// StopAfterSeconds stops the Service once it has served this long,
	// from its first start (a re-run or a new machine does not move it):
	// a server nobody remembers does not run, and cost, forever. It ends
	// Cancelled, reason STOP_AFTER. spec.activeDeadlineSeconds, which
	// fails a job, does not apply to a Service.
	// +optional
	// +kubebuilder:validation:Minimum=1
	StopAfterSeconds *int64 `json:"stopAfterSeconds,omitempty"`
}

// ServicePort is one port a Service listens on. Not 8080 or 8081: the
// Zenthros agent holds them on the machine (ExposedPort).
// +kubebuilder:validation:XValidation:rule="self.port != 8080 && self.port != 8081",message="8080 and 8081 are the Zenthros agent's metrics and health ports on the machine; a Service cannot listen there"
type ServicePort struct {
	// Name names the port's endpoint in status.endpoints.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// Port is the port number the process listens on.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`

	// Protocol is HTTP (the default) or TCP.
	// +optional
	// +kubebuilder:default=HTTP
	// +kubebuilder:validation:Enum=HTTP;TCP
	Protocol string `json:"protocol,omitempty"`
}

// Service port protocols.
const (
	ProtocolHTTP = "HTTP"
	ProtocolTCP  = "TCP"
)

// ManifestSpec is one namespaced Kubernetes object and the contract that says
// when it succeeded, failed or runs. Without a contract the object's
// status.conditions decide: Succeeded=True or Complete=True succeeds,
// Failed=True fails, Running=True (or status.active > 0) runs. That covers
// batch/v1 Jobs and the Kubeflow training jobs; other operators, such as
// Volcano, are read through phasePath.
// +kubebuilder:validation:XValidation:rule="!has(self.endpointFrom) || size(self.endpointFrom) == 0 || (has(self.lifecycle) && self.lifecycle == 'Service')",message="endpointFrom reads a Service's URLs; set lifecycle: Service"
type ManifestSpec struct {
	// Object is the object to apply, as written. Its namespace, if set, must
	// be the job's; its name is replaced by the job's.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:EmbeddedResource
	Object runtime.RawExtension `json:"object"`

	// SuccessWhen lists conditions any of which means the object succeeded.
	// +optional
	SuccessWhen []ConditionMatch `json:"successWhen,omitempty"`

	// FailureWhen lists conditions any of which means the object failed.
	// +optional
	FailureWhen []ConditionMatch `json:"failureWhen,omitempty"`

	// RunningWhen lists conditions any of which means the object runs.
	// +optional
	RunningWhen []ConditionMatch `json:"runningWhen,omitempty"`

	// PhasePath reads a phase string instead of conditions.
	// +optional
	PhasePath *PhasePath `json:"phasePath,omitempty"`

	// LogSelector picks the pods whose logs are the job's logs. Without it,
	// the pods the object owns (through up to three owner levels) are read.
	// +optional
	LogSelector *metav1.LabelSelector `json:"logSelector,omitempty"`

	// Lifecycle is RunToCompletion (default) or Service, for an object that
	// runs until it is cancelled.
	// +optional
	// +kubebuilder:validation:Enum=RunToCompletion;Service
	Lifecycle string `json:"lifecycle,omitempty"`

	// EndpointFrom reads the URLs a Service object publishes in its own
	// status (an InferenceService's status.url) into status.endpoints, once
	// the object runs by its contract. Only with lifecycle Service.
	// +optional
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=name
	EndpointFrom []EndpointSource `json:"endpointFrom,omitempty"`
}

// EndpointSource is where a Manifest's object publishes one URL.
type EndpointSource struct {
	// Name names the endpoint in status.endpoints.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// Path is the dotted path of a string field holding an absolute URL,
	// e.g. status.url.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*$`
	Path string `json:"path"`
}

// ConditionMatch matches one entry of the object's status.conditions.
type ConditionMatch struct {
	// Type is the condition type, e.g. Succeeded.
	// +kubebuilder:validation:MinLength=1
	Type string `json:"type"`

	// Status is the condition status that matches.
	// +optional
	// +kubebuilder:default="True"
	// +kubebuilder:validation:Enum="True";"False";"Unknown"
	Status string `json:"status,omitempty"`
}

// PhasePath reads the object's phase from a string field.
type PhasePath struct {
	// Path is the dotted path of the phase field, e.g. status.state.phase.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*$`
	Path string `json:"path"`

	// Succeeded lists phases that mean success.
	// +optional
	Succeeded []string `json:"succeeded,omitempty"`

	// Failed lists phases that mean failure.
	// +optional
	Failed []string `json:"failed,omitempty"`

	// Running lists phases that mean the object runs.
	// +optional
	Running []string `json:"running,omitempty"`
}

// RayClusterReference identifies one existing RayCluster without exposing a
// user-defined label selector.
type RayClusterReference struct {
	// Name is the exact RayCluster name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace must equal the ZenthrosJob namespace. It is explicit so a
	// cross-Workspace reference is rejected rather than silently redirected.
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
}

const (
	SparkRestartNever                   = "Never"
	SparkRestartAlways                  = "Always"
	SparkRestartOnFailure               = "OnFailure"
	SparkRestartOnInfrastructureFailure = "OnInfrastructureFailure"
)

// SparkSpec contains only the native Spark intent that has no common workload field.
type SparkSpec struct {
	// MainApplicationFile is a local://, s3a://, or other Spark-supported application URI.
	// +kubebuilder:validation:MinLength=1
	MainApplicationFile string `json:"mainApplicationFile"`

	// MainClass is required for JVM applications and omitted for Python applications.
	MainClass string `json:"mainClass,omitempty"`

	// Arguments are passed to the Spark driver without shell interpretation.
	Arguments []string `json:"arguments,omitempty"`

	// SparkConf contains Spark-native key/value configuration.
	SparkConf map[string]string `json:"sparkConf,omitempty"`

	Driver SparkResourceSpec `json:"driver,omitempty"`

	Executor SparkResourceSpec `json:"executor,omitempty"`

	// ExecutorCount is the fixed initial executor count. Zero defaults to one.
	// +kubebuilder:validation:Minimum=0
	ExecutorCount int32 `json:"executorCount,omitempty"`

	RestartPolicy SparkRestartPolicy `json:"restartPolicy,omitempty"`
}

// SparkResourceSpec declares CPU and memory for one Spark role.
type SparkResourceSpec struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// SparkRestartPolicy is delegated to Spark Operator.
type SparkRestartPolicy struct {
	// Type selects the Spark Operator restart behavior.
	// +kubebuilder:validation:Enum=Never;Always;OnFailure;OnInfrastructureFailure
	// +kubebuilder:default=Never
	Type string `json:"type,omitempty"`

	// MaxRestartAttempts bounds operator-managed restarts.
	// +kubebuilder:validation:Minimum=0
	MaxRestartAttempts int32 `json:"maxRestartAttempts,omitempty"`
}

// WorkerGroup sizes a group of worker nodes.
// +kubebuilder:validation:XValidation:rule="self.max >= self.min",message="max must be >= min"
type WorkerGroup struct {
	// Min replica count.
	// +kubebuilder:validation:Minimum=0
	Min int32 `json:"min"`

	// Max replica count.
	// +kubebuilder:validation:Minimum=0
	Max int32 `json:"max"`

	// ResourceClass is a coarse label the scheduler uses for placement.
	ResourceClass string `json:"resourceClass,omitempty"`

	// GPUsPerNode requested per worker.
	// +kubebuilder:validation:Minimum=0
	GPUsPerNode int32 `json:"gpusPerNode,omitempty"`

	// Interconnect preference, e.g. "nvlink" (intra-node, owned by Ray/Spark).
	Interconnect string `json:"interconnect,omitempty"`

	// CPU request per worker, e.g. "2".
	CPU string `json:"cpu,omitempty"`

	// Memory request per worker, e.g. "8Gi".
	Memory string `json:"memory,omitempty"`
}

// EnvVar is a plain environment variable.
type EnvVar struct {
	// Name is a POSIX environment variable name.
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_]*$`
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

// RestartPolicy configures backend resilience. Zenthros sets it; the backend enforces it.
type RestartPolicy struct {
	// BackoffLimit is how many times the job's process is re-run after it
	// fails, on every target: a Kubernetes Job's own backoffLimit, KubeRay's
	// for a RayJob, and the controller's on a machine (each re-run is a new
	// unit, zenthros-<id>-a<n>, with ZENTHROS_ATTEMPT set). A lost worker is
	// not a failure of the process and does not count. SparkApplication
	// restarts through spark.restartPolicy and a Manifest through its own
	// object, so neither takes it.
	// +kubebuilder:validation:Minimum=0
	BackoffLimit int32 `json:"backoffLimit,omitempty"`
}

// Artifacts references object-storage locations for data and results.
type Artifacts struct {
	// Input URI, e.g. s3://bucket/dataset.
	Input string `json:"input,omitempty"`

	// Output URI, e.g. s3://bucket/models.
	Output string `json:"output,omitempty"`
}

// Source declares the origin of user code. Exactly one mode field should be set.
// +kubebuilder:validation:XValidation:rule="(has(self.hostPath) ? 1 : 0) + (has(self.git) ? 1 : 0) + (has(self.s3) ? 1 : 0) + (has(self.inline) ? 1 : 0) + (has(self.ref) ? 1 : 0) <= 1",message="hostPath, git, s3, inline, and ref are mutually exclusive"
type Source struct {
	// HostPath mounts a node directory (dev/local, single-node only).
	HostPath string `json:"hostPath,omitempty"`

	// Git repository cloned into the workload (production). Use the SSH URL
	// (git@github.com:OWNER/REPO.git) when authenticating with a deploy key.
	Git string `json:"git,omitempty"`

	// S3 bundle (zip/tar) downloaded and mounted (production).
	S3 string `json:"s3,omitempty"`

	// Inline is a base64-encoded tarball of user code embedded in the submission.
	// It lives in the object, which etcd rewrites on every status write: the
	// controller admits 512 KiB by default and the API refuses more than 1 MiB.
	// Larger projects should use git or an artifact.
	// +kubebuilder:validation:MaxLength=1048576
	Inline string `json:"inline,omitempty"`

	// Ref names a ConfigMap in the job's namespace whose binaryData
	// "source.tar.gz" is a gzipped tarball of user code. It keeps the code out
	// of the ZenthrosJob, which etcd rewrites on every status write. The
	// Controller reads it at each Submit, so it must outlive the job's run;
	// `zctl run` creates it content-addressed and owned by the job.
	// +kubebuilder:validation:MaxLength=253
	Ref string `json:"ref,omitempty"`

	// GitRef is the branch/tag/hash to check out (default: HEAD of the default branch).
	GitRef string `json:"gitRef,omitempty"`

	// GitSecretRef names an opaque Secret (keys: ssh, known_hosts) for SSH auth.
	// Empty means a public repo (no auth). Referenced by name only, never read here.
	GitSecretRef string `json:"gitSecretRef,omitempty"`
}

// SourceRefKey is the ConfigMap binaryData key a source.ref bundle is read from.
const SourceRefKey = "source.tar.gz"
