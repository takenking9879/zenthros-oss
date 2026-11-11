// SPDX-License-Identifier: Apache-2.0

package jobspec

import (
	"k8s.io/apimachinery/pkg/runtime"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/zenthros/operator/api/v1"
)

func testJob(policy *v1.GCPolicy) *v1.ZenthrosJob {
	return &v1.ZenthrosJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hello",
			Namespace: "tenant-default",
			UID:       types.UID("uid-1"),
		},
		Spec: v1.ZenthrosJobSpec{
			Target:   v1.Target{Backends: []string{"vm-01"}},
			Workload: v1.Workload{Kind: v1.WorkloadKindRayJob},
			GCPolicy: policy,
		},
	}
}

func TestFromZenthrosJobCarriesStructuredExecution(t *testing.T) {
	job := testJob(nil)
	job.Spec.Workload.Template = v1.WorkloadTemplate{
		Command:    []string{"python", "train.py"},
		Args:       []string{"--epochs", "10"},
		WorkingDir: "/data/run",
		Image:      "ray:latest",
		Env:        []v1.EnvVar{{Name: "MODE", Value: "fast"}},
	}

	spec := FromZenthrosJob(job, "k8s", "tenant-default")

	if len(spec.Command) != 2 || spec.Command[0] != "python" || spec.Command[1] != "train.py" {
		t.Errorf("command = %v, want [python train.py]", spec.Command)
	}
	if len(spec.Args) != 2 || spec.Args[0] != "--epochs" || spec.Args[1] != "10" {
		t.Errorf("args = %v, want [--epochs 10]", spec.Args)
	}
	if spec.WorkingDir != "/data/run" {
		t.Errorf("workingDir = %q, want /data/run", spec.WorkingDir)
	}
	if spec.Entrypoint != "" {
		t.Errorf("entrypoint = %q, want empty for a structured spec", spec.Entrypoint)
	}
	if spec.Env["MODE"] != "fast" {
		t.Errorf("env = %+v, want MODE=fast", spec.Env)
	}
}

func TestFromZenthrosJobInjectsFrozenCheckpointCursor(t *testing.T) {
	job := testJob(nil)
	job.Spec.Workload.Template.Artifacts.Output = "s3://bucket/out"
	job.Status.Lease = &v1.JobLease{Generation: 2}
	job.Status.Progress = &v1.JobProgress{
		URI: "s3://bucket/out/jobs/uid-1/gen/1/train/4", Phase: "train", Seq: 4, SHA256: "deadbeef",
	}

	spec := FromZenthrosJob(job, "vm", "tenant-default")
	if spec.Env["ZENTHROS_CHECKPOINT_URI"] != "s3://bucket/out/jobs/uid-1/gen/1/train/4" {
		t.Fatalf("URI = %q, want frozen ack not a listing", spec.Env["ZENTHROS_CHECKPOINT_URI"])
	}
	if spec.Env["ZENTHROS_CHECKPOINT_SEQ"] != "4" {
		t.Fatalf("seq = %q, want 4 so resume is seq+1", spec.Env["ZENTHROS_CHECKPOINT_SEQ"])
	}
	if spec.Env["ZENTHROS_GENERATION"] != "2" {
		t.Fatalf("generation = %q, want 2 (new writes stay out of gen/1)", spec.Env["ZENTHROS_GENERATION"])
	}
}

func TestFromZenthrosJobCarriesRetainWorkdir(t *testing.T) {
	spec := FromZenthrosJob(testJob(&v1.GCPolicy{RetainWorkdir: true}), "vm", "tenant-default")
	if !spec.RetainWorkdir {
		t.Error("retainWorkdir from the GC policy must reach the JobSpec")
	}
}

func TestFromZenthrosJobDefaultsRetainWorkdirFalse(t *testing.T) {
	for _, policy := range []*v1.GCPolicy{nil, {}} {
		spec := FromZenthrosJob(testJob(policy), "vm", "tenant-default")
		if spec.RetainWorkdir {
			t.Errorf("retainWorkdir must default to false (policy=%+v)", policy)
		}
	}
}

func TestFromZenthrosJobCarriesSparkIntent(t *testing.T) {
	job := testJob(nil)
	job.Spec.Workload.Kind = v1.WorkloadKindSparkApplication
	job.Spec.Workload.Template.Spark = &v1.SparkSpec{
		MainApplicationFile: "local:///app.py", Arguments: []string{"--rows", "10"}, ExecutorCount: 2,
	}
	spec := FromZenthrosJob(job, "k8s", "tenant-default")
	if spec.Spark == nil || spec.Spark.MainApplicationFile != "local:///app.py" || spec.Spark.ExecutorCount != 2 {
		t.Fatalf("Spark intent = %+v", spec.Spark)
	}
}

func TestCRDKindRayClusterIsAcceptedByValidateWorkload(t *testing.T) {
	err := ValidateWorkload(JobSpec{
		Kind:  v1.WorkloadKindRayCluster,
		Image: "ray:latest",
	})
	if err != nil {
		t.Fatalf("CRD enum lists RayCluster but ValidateWorkload rejected it: %v", err)
	}
}

func TestValidateWorkloadAcceptsJobWithOrWithoutImage(t *testing.T) {
	if err := ValidateWorkload(JobSpec{Kind: v1.WorkloadKindJob, Command: []string{"echo", "hi"}, Image: "python:3.13"}); err != nil {
		t.Fatalf("Job with image: %v", err)
	}
	if err := ValidateWorkload(JobSpec{Kind: v1.WorkloadKindJob, Entrypoint: "echo hi"}); err != nil {
		t.Fatalf("Job without image: %v", err)
	}
}

func TestFromZenthrosJobCarriesRuntimeAndLifespan(t *testing.T) {
	job := testJob(nil)
	job.Spec.Compute = &v1.ComputeConstraints{Runtime: v1.RuntimeContainerd, Lifespan: v1.LifespanRetain}
	spec := FromZenthrosJob(job, "vm", "tenant-default")
	if spec.Runtime != v1.RuntimeContainerd || spec.Lifespan != v1.LifespanRetain {
		t.Fatalf("runtime/lifespan = %q %q", spec.Runtime, spec.Lifespan)
	}
}

func TestFromZenthrosJobCarriesRayClusterReference(t *testing.T) {
	job := testJob(nil)
	job.Spec.Target.RayCluster = &v1.RayClusterReference{
		Name: "shared-ray", Namespace: "tenant-default",
	}
	spec := FromZenthrosJob(job, "k8s", "tenant-default")
	if spec.RayCluster == nil || spec.RayCluster.Name != "shared-ray" || spec.RayCluster.Namespace != "tenant-default" {
		t.Fatalf("RayCluster reference = %+v", spec.RayCluster)
	}
}

func TestValidateWorkloadRejectsKindCapabilityMismatch(t *testing.T) {
	tests := []JobSpec{
		{Kind: v1.WorkloadKindSparkApplication},
		{Kind: v1.WorkloadKindRayJob, Spark: &v1.SparkSpec{MainApplicationFile: "local:///app.py"}},
		{Kind: v1.WorkloadKindSparkApplication, Spark: &v1.SparkSpec{MainApplicationFile: "local:///app.py"}, RayCluster: &v1.RayClusterReference{Name: "shared", Namespace: "tenant-default"}},
		{Kind: v1.WorkloadKindRayJob, Namespace: "tenant-default", RayCluster: &v1.RayClusterReference{Name: "", Namespace: "tenant-default"}},
		{Kind: v1.WorkloadKindRayJob, Namespace: "tenant-default", RayCluster: &v1.RayClusterReference{Name: "shared", Namespace: "tenant-other"}},
		{Kind: v1.WorkloadKindRayJob, Namespace: "tenant-default", RayCluster: &v1.RayClusterReference{Name: "shared", Namespace: "tenant-default"}, Source: v1.Source{Git: "https://example.invalid/repo.git"}},
		{Kind: v1.WorkloadKindRayJob, Namespace: "tenant-default", RayCluster: &v1.RayClusterReference{Name: "shared", Namespace: "tenant-default"}, Workers: v1.WorkerGroup{Max: 1}},
		{Kind: "SparkJob"},
		{Kind: v1.WorkloadKindJob, Image: "python:3.13"},
		{Kind: v1.WorkloadKindRayCluster, Image: ""},
		{Kind: v1.WorkloadKindRayCluster, Image: "ray:2", Entrypoint: "echo hi"},
		{Kind: "Pod", Command: []string{"echo"}, Image: "python:3.13"},
		{Kind: v1.WorkloadKindJob, Command: []string{"echo"}, Entrypoint: "echo hi", Image: "python:3.13"},
	}
	for _, spec := range tests {
		if err := ValidateWorkload(spec); err == nil {
			t.Errorf("ValidateWorkload(%+v) accepted mismatched intent", spec)
		}
	}
}

// A Manifest carries its object from the ZenthrosJob to the agent, and
// nothing that belongs to a command (entrypoint, image, source) rides along;
// a manifest on another kind is refused.
func TestValidateWorkloadManifest(t *testing.T) {
	job := &v1.ZenthrosJob{}
	job.Spec.Workload.Kind = v1.WorkloadKindManifest
	job.Spec.Workload.Template.Manifest = &v1.ManifestSpec{Object: runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap"}`)}}
	spec := FromZenthrosJob(job, "agent", "tenant-a")
	if spec.Manifest == nil || string(spec.Manifest.Object.Raw) != `{"apiVersion":"v1","kind":"ConfigMap"}` {
		t.Fatalf("manifest not carried: %+v", spec.Manifest)
	}
	if err := ValidateWorkload(spec); err != nil {
		t.Fatalf("valid Manifest: %v", err)
	}
	for name, mutate := range map[string]func(*JobSpec){
		"no object":  func(s *JobSpec) { s.Manifest = &v1.ManifestSpec{} },
		"entrypoint": func(s *JobSpec) { s.Entrypoint = "echo" },
		"image":      func(s *JobSpec) { s.Image = "busybox" },
		"source":     func(s *JobSpec) { s.Source.Git = "https://example.com/r.git" },
		"other kind": func(s *JobSpec) { s.Kind = v1.WorkloadKindJob; s.Entrypoint = "echo" },
	} {
		bad := spec
		mutate(&bad)
		if err := ValidateWorkload(bad); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The agent's boundary: a spec with a size that does not parse is refused as
// it decodes, whoever sent it (it panicked in resource.MustParse).
func TestValidateWorkloadRefusesASizeThatDoesNotParse(t *testing.T) {
	spec := JobSpec{Kind: v1.WorkloadKindJob, Command: []string{"true"}, Image: "i"}
	if err := ValidateWorkload(spec); err != nil {
		t.Fatal(err)
	}
	for _, workers := range []v1.WorkerGroup{{CPU: "abc"}, {Memory: "12x"}, {CPU: "-1"}} {
		spec.Workers = workers
		if err := ValidateWorkload(spec); err == nil {
			t.Fatalf("workers %+v was taken", workers)
		}
	}
}

// A size spec.compute names reaches a pod, where one that does not parse would
// panic the agent: it is refused here with the field's name, as workers' is.
func TestValidateWorkloadRefusesASizeComputeCannotParse(t *testing.T) {
	for _, compute := range []*v1.ComputeConstraints{{CPU: "lots"}, {Memory: "lots"}, {Memory: "-1Gi"}} {
		job := testJob(nil)
		job.Spec.Workload.Kind = v1.WorkloadKindJob
		job.Spec.Workload.Template.Command = []string{"true"}
		job.Spec.Compute = compute
		err := ValidateWorkload(FromZenthrosJob(job, "k8s", "tenant-default"))
		if err == nil || !strings.Contains(err.Error(), "compute.") {
			t.Errorf("compute %+v: got %v, want the field named", compute, err)
		}
	}
}
