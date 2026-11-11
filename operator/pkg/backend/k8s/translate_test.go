// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"strings"
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

func testSpec() jobspec.JobSpec {
	return jobspec.JobSpec{
		ID:         "uid-123",
		Name:       "hello",
		Namespace:  "tenant-default",
		Tenant:     "default",
		Backend:    backend.TypeK8s,
		Kind:       "RayJob",
		Entrypoint: "python hello.py",
		Image:      "takenking9879/ray-train:2.53.0",
		Workers:    v1.WorkerGroup{Min: 1, Max: 2, CPU: "2", Memory: "4Gi", GPUsPerNode: 1},
		Env:        map[string]string{"FOO": "bar", "BAZ": "qux"},
		Source:     v1.Source{HostPath: "/data/src"},

		OwnerKind:       "ZenthrosJob",
		OwnerAPIVersion: "zenthros.ai/v1",
		OwnerName:       "hello",
		OwnerUID:        "uid-123",
	}
}

func retainRaySpec() jobspec.JobSpec {
	return jobspec.JobSpec{
		ID:        "job-retain",
		Name:      "train-iris",
		Namespace: "tenant-acme",
		Tenant:    "acme",
		Workspace: "acme",
		Kind:      v1.WorkloadKindRayJob,
		Image:     "ray:latest",
	}
}

func TestEphemeralRayJobDoesNotShutdownBeforeRetain(t *testing.T) {
	spec := retainRaySpec()
	spec.Lifespan = v1.LifespanRetain
	rayJob := BuildRayJob(spec)
	if rayJob.Spec.ShutdownAfterJobFinishes {
		t.Fatal("Retain RayJob set ShutdownAfterJobFinishes; the cluster (and submitter logs) die at terminal")
	}
}

func TestRayJobCarriesWorkspaceLabel(t *testing.T) {
	spec := retainRaySpec()
	rayJob := BuildRayJob(spec)
	if got := rayJob.Labels["zenthros.ai/workspace"]; got != spec.Workspace {
		t.Fatalf("RayJob workspace label = %q, want %q (Job/Pod set this; RayJob does not)", got, spec.Workspace)
	}
}

func TestBuildRayJobCoreFields(t *testing.T) {
	spec := testSpec()

	rayJob := BuildRayJob(spec)

	if rayJob.Name != "hello" || rayJob.Namespace != "tenant-default" {
		t.Fatalf("unexpected identity: %s/%s", rayJob.Namespace, rayJob.Name)
	}
	if rayJob.Spec.Entrypoint != "python hello.py" {
		t.Errorf("entrypoint = %q", rayJob.Spec.Entrypoint)
	}
	if rayJob.Spec.JobId != "uid-123" {
		t.Errorf("jobId = %q, want uid-123", rayJob.Spec.JobId)
	}
	if rayJob.Spec.RayClusterSpec == nil {
		t.Fatal("rayClusterSpec is nil")
	}
	if rayJob.Spec.RayClusterSpec.RayVersion != rayVersion {
		t.Errorf("rayVersion = %q", rayJob.Spec.RayClusterSpec.RayVersion)
	}
}

func TestBuildRayJobUsesExistingRayCluster(t *testing.T) {
	spec := testSpec()
	spec.RayCluster = &v1.RayClusterReference{Name: "shared-ray", Namespace: spec.Namespace}
	spec.Env["ZENTHROS_DATA_INPUT"] = "s3://bucket/input"

	rayJob := BuildRayJob(spec)
	if rayJob.Spec.ClusterSelector[rayClusterSelectorKey] != "shared-ray" {
		t.Fatalf("clusterSelector = %+v", rayJob.Spec.ClusterSelector)
	}
	if rayJob.Spec.RayClusterSpec != nil {
		t.Fatal("existing-cluster RayJob must not embed rayClusterSpec")
	}
	if rayJob.Spec.ShutdownAfterJobFinishes {
		t.Fatal("existing-cluster RayJob must not shut down the shared cluster")
	}
	if rayJob.Spec.BackoffLimit != nil {
		t.Fatalf("existing-cluster RayJob backoffLimit = %d, want omitted", *rayJob.Spec.BackoffLimit)
	}
	if !strings.Contains(rayJob.Spec.RuntimeEnvYAML, `"ZENTHROS_DATA_INPUT": "s3://bucket/input"`) {
		t.Fatalf("runtimeEnvYAML = %q", rayJob.Spec.RuntimeEnvYAML)
	}
	if rayJob.Spec.SubmissionMode != rayv1.HTTPMode {
		t.Fatalf("standing-cluster SubmissionMode = %q, want HTTPMode", rayJob.Spec.SubmissionMode)
	}
	if rayJob.Spec.SubmitterPodTemplate != nil {
		t.Fatal("HTTPMode standing-cluster RayJob must not set a submitter pod")
	}
}

func TestBuildRayJobWithoutReferencePreservesPerJobCluster(t *testing.T) {
	rayJob := BuildRayJob(testSpec())
	if rayJob.Spec.RayClusterSpec == nil || len(rayJob.Spec.ClusterSelector) != 0 || !rayJob.Spec.ShutdownAfterJobFinishes {
		t.Fatalf("per-job cluster translation changed: %+v", rayJob.Spec)
	}
	if rayJob.Spec.BackoffLimit == nil {
		t.Fatal("per-job cluster backoffLimit was removed")
	}
	if rayJob.Spec.SubmissionMode == rayv1.HTTPMode {
		t.Fatal("ephemeral per-job cluster must not default to HTTPMode")
	}
	if rayJob.Spec.SubmitterPodTemplate == nil {
		t.Fatal("ephemeral per-job cluster still needs a submitter pod")
	}
	if !strings.Contains(rayJob.Spec.RuntimeEnvYAML, `"FOO": "bar"`) {
		t.Fatalf("ephemeral RayJob must set runtimeEnvYAML, got %q", rayJob.Spec.RuntimeEnvYAML)
	}
}

func TestSpecMismatchDetectsRayClusterReferenceChange(t *testing.T) {
	spec := testSpec()
	spec.RayCluster = &v1.RayClusterReference{Name: "shared-a", Namespace: spec.Namespace}
	existing := BuildRayJob(spec)
	spec.RayCluster.Name = "shared-b"
	desired := BuildRayJob(spec)
	if mismatch := SpecMismatch(existing, desired); mismatch == nil {
		t.Fatal("changed RayCluster reference was accepted as idempotent")
	}
}

func TestBuildRayJobHasNoOwnerReference(t *testing.T) {

	rayJob := BuildRayJob(testSpec())

	// Only a spoke Agent applies the RayJob; the ZenthrosJob lives on the hub,
	// so translation never sets an ownerReference. Cleanup is driven by Cancel.
	if len(rayJob.OwnerReferences) != 0 {
		t.Fatalf("expected no ownerReference from translation, got %d", len(rayJob.OwnerReferences))
	}
}

func TestBuildRayJobEnvIsDeterministic(t *testing.T) {

	rayJob := BuildRayJob(testSpec())
	containers := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("expected 1 head container, got %d", len(containers))
	}
	env := containers[0].Env
	if len(env) != 2 || env[0].Name != "BAZ" || env[1].Name != "FOO" {
		t.Errorf("env not sorted deterministically: %+v", env)
	}
}

func TestBuildRayJobHostPathSource(t *testing.T) {

	rayJob := BuildRayJob(testSpec())
	podSpec := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec

	if len(podSpec.Volumes) != 1 || podSpec.Volumes[0].HostPath == nil || podSpec.Volumes[0].HostPath.Path != "/data/src" {
		t.Errorf("expected hostPath volume /data/src, got %+v", podSpec.Volumes)
	}
	container := podSpec.Containers[0]
	if container.WorkingDir != sourceMountPath {
		t.Errorf("workingDir = %q, want %q", container.WorkingDir, sourceMountPath)
	}
}

func TestBuildRayJobHeadOnlyWhenNoWorkers(t *testing.T) {
	spec := testSpec()
	spec.Workers.Max = 0

	rayJob := BuildRayJob(spec)

	if len(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs) != 0 {
		t.Errorf("expected no worker groups for head-only job, got %d", len(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs))
	}
}

func TestSpecMismatchDetectsImageChange(t *testing.T) {
	existing := BuildRayJob(testSpec())

	desired := BuildRayJob(testSpec())
	desired.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Image = "takenking9879/ray-train:2.52.0"

	mismatch := SpecMismatch(existing, desired)
	if mismatch == nil {
		t.Fatal("expected spec mismatch on image, got nil")
	}
	if mismatch.Field != "image" {
		t.Errorf("mismatch field = %q, want image", mismatch.Field)
	}
}

func TestBuildRayJobWorkerGroupResources(t *testing.T) {

	rayJob := BuildRayJob(testSpec())
	groups := rayJob.Spec.RayClusterSpec.WorkerGroupSpecs
	if len(groups) != 1 {
		t.Fatalf("expected 1 worker group, got %d", len(groups))
	}
	group := groups[0]
	if *group.MinReplicas != 1 || *group.MaxReplicas != 2 {
		t.Errorf("replicas min/max = %d/%d", *group.MinReplicas, *group.MaxReplicas)
	}

	limits := group.Template.Spec.Containers[0].Resources.Limits
	gpu, ok := limits[gpuResourceName]
	if !ok || gpu.Value() != 1 {
		t.Errorf("expected 1 GPU limit, got %+v", limits)
	}
	requests := group.Template.Spec.Containers[0].Resources.Requests
	if requests.Cpu().String() != "2" || requests.Memory().String() != "4Gi" {
		t.Errorf("unexpected worker requests: %+v", requests)
	}
}

func TestBuildRayJobSubmitterHasRequests(t *testing.T) {

	rayJob := BuildRayJob(testSpec())

	submitter := rayJob.Spec.SubmitterPodTemplate
	if submitter == nil {
		t.Fatal("expected a submitter pod template")
	}
	requests := submitter.Spec.Containers[0].Resources.Requests
	if requests.Cpu().String() != submitterCPU || requests.Memory().String() != submitterMemory {
		t.Errorf("submitter requests = %+v, want cpu=%s mem=%s", requests, submitterCPU, submitterMemory)
	}
}

func TestSpecMismatchNilWhenEqual(t *testing.T) {

	if mismatch := SpecMismatch(BuildRayJob(testSpec()), BuildRayJob(testSpec())); mismatch != nil {
		t.Errorf("expected nil mismatch for equal specs, got %v", mismatch)
	}
}

// A resubmit after a checkpoint ack carries a newer cursor and generation for
// the same RayJob: it adopts it. A user variable that changed is still a
// mismatch, and the error shows the user's env only.
func TestRayJobMismatchIgnoresResumeEnv(t *testing.T) {
	first := testSpec()
	first.Env = map[string]string{"FOO": "bar", "ZENTHROS_GENERATION": "1"}
	resumed := testSpec()
	resumed.Env = map[string]string{"FOO": "bar", "ZENTHROS_GENERATION": "2", "ZENTHROS_CHECKPOINT_URI": "s3://b/x",
		"ZENTHROS_CHECKPOINT_SEQ": "3", "ZENTHROS_CHECKPOINT_CURSORS": `{"train":{"seq":3,"uri":"s3://b/x"}}`,
		"ZENTHROS_REPORT_TOKEN": "t"}
	if mismatch := SpecMismatch(BuildRayJob(first), BuildRayJob(resumed)); mismatch != nil {
		t.Fatalf("resume env caused a mismatch: %v", mismatch)
	}
	changed := testSpec()
	changed.Env = map[string]string{"FOO": "baz", "ZENTHROS_GENERATION": "2"}
	mismatch := SpecMismatch(BuildRayJob(first), BuildRayJob(changed))
	if mismatch == nil || !strings.Contains(mismatch.Error(), "baz") || strings.Contains(mismatch.Error(), "ZENTHROS_GENERATION") {
		t.Fatalf("user env change: %v", mismatch)
	}
}

func TestBuildRayJobTranslatesDataReferences(t *testing.T) {
	spec := testSpec()
	spec.Artifacts = v1.Artifacts{
		Input:  "s3a://warehouse/processed/features",
		Output: "s3a://warehouse/processed/run-1",
	}

	rayJob := BuildRayJob(spec)
	env := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Env

	got := map[string]string{}
	for _, e := range env {
		got[e.Name] = e.Value
	}
	if got[backend.DataInputEnv] != spec.Artifacts.Input {
		t.Errorf("%s = %q, want %q", backend.DataInputEnv, got[backend.DataInputEnv], spec.Artifacts.Input)
	}
	if got[backend.DataOutputEnv] != spec.Artifacts.Output {
		t.Errorf("%s = %q, want %q", backend.DataOutputEnv, got[backend.DataOutputEnv], spec.Artifacts.Output)
	}
}

func TestBuildRayJobOmitsDataReferencesWhenAbsent(t *testing.T) {
	rayJob := BuildRayJob(testSpec())
	env := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Env
	for _, e := range env {
		if e.Name == backend.DataInputEnv || e.Name == backend.DataOutputEnv {
			t.Errorf("unexpected data reference env %+v", e)
		}
	}
}

func TestBuildRayJobMaterializesSecretAsEnvReference(t *testing.T) {
	spec := testSpec()
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeKubernetes, Namespace: spec.Namespace,
		Name: "api-token", Key: "token",
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "API_TOKEN"},
	}}
	job := BuildRayJob(spec)
	env := job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Env
	var found *corev1.EnvVar
	for index := range env {
		if env[index].Name == "API_TOKEN" {
			found = &env[index]
		}
	}
	if found == nil || found.ValueFrom == nil || found.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("secret env reference missing: %#v", env)
	}
	if found.Value != "" || found.ValueFrom.SecretKeyRef.Name != "api-token" || found.ValueFrom.SecretKeyRef.Key != "token" {
		t.Errorf("secret env reference = %#v", found)
	}
}

func TestBuildRayJobMaterializesSecretAs0600File(t *testing.T) {
	spec := testSpec()
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeKubernetes, Namespace: spec.Namespace,
		Name: "api-token", Key: "token",
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationFile, Path: "/run/secrets/api-token"},
	}}
	job := BuildRayJob(spec)
	pod := job.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec
	var secret *corev1.SecretVolumeSource
	var materialized *corev1.EmptyDirVolumeSource
	for _, volume := range pod.Volumes {
		if volume.Name == "zenthros-secret-source-0" {
			secret = volume.Secret
		}
		if volume.Name == "zenthros-secret-file-0" {
			materialized = volume.EmptyDir
		}
	}
	if secret == nil || materialized == nil {
		t.Fatalf("secret source/materialized volumes missing: %#v", pod.Volumes)
	}
	if secret.SecretName != "api-token" || secret.DefaultMode == nil || *secret.DefaultMode != 0400 {
		t.Errorf("secret volume = %#v", secret)
	}
	var mount corev1.VolumeMount
	for _, candidate := range pod.Containers[0].VolumeMounts {
		if candidate.Name == "zenthros-secret-file-0" {
			mount = candidate
		}
	}
	if mount.MountPath != "/run/secrets/api-token" || mount.SubPath != "value" || !mount.ReadOnly {
		t.Errorf("secret mount = %#v", mount)
	}
	container := pod.Containers[0]
	if container.SecurityContext == nil || container.SecurityContext.RunAsUser == nil || *container.SecurityContext.RunAsUser != secretRuntimeUID {
		t.Errorf("workload security context = %#v", container.SecurityContext)
	}
	if len(pod.InitContainers) != 1 {
		t.Fatalf("secret materializer init containers = %d, want 1", len(pod.InitContainers))
	}
	init := pod.InitContainers[0]
	if init.Image != secretInitImage || len(init.Args) != 1 || init.Args[0] != "cp /source/value /materialized/value && chmod 0600 /materialized/value && chown 1000:1000 /materialized/value" {
		t.Errorf("secret materializer = %#v", init)
	}
}

func TestSpecMismatchDetectsSecretSourceChange(t *testing.T) {
	spec := testSpec()
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeKubernetes, Namespace: spec.Namespace, Name: "token-a", Key: "value",
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "TOKEN"},
	}}
	existing := BuildRayJob(spec)
	spec.SecretSources[0].Name = "token-b"
	desired := BuildRayJob(spec)
	mismatch := SpecMismatch(existing, desired)
	if mismatch == nil || mismatch.Field != "secretSources" {
		t.Fatalf("SpecMismatch = %#v", mismatch)
	}
}

func TestSpecMismatchSameSecretSourceIsStable(t *testing.T) {
	spec := testSpec()
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeKubernetes, Namespace: spec.Namespace, Name: "token", Key: "value",
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "TOKEN"},
	}}
	first := BuildRayJob(spec)
	second := BuildRayJob(spec)
	if mismatch := SpecMismatch(first, second); mismatch != nil {
		t.Fatalf("identical secret bindings must reuse: %#v", mismatch)
	}
}

func gitSpec() jobspec.JobSpec {
	spec := testSpec()
	spec.Source = v1.Source{Git: "git@github.com:o/r.git"}
	return spec
}

func hasArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func mountAt(mounts []corev1.VolumeMount, name, path string) bool {
	for _, m := range mounts {
		if m.Name == name && m.MountPath == path {
			return true
		}
	}
	return false
}

func TestBuildRayJobPublicGitSource(t *testing.T) {

	rayJob := BuildRayJob(gitSpec())
	podSpec := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec

	if len(podSpec.InitContainers) != 1 {
		t.Fatalf("expected 1 initContainer, got %d", len(podSpec.InitContainers))
	}
	init := podSpec.InitContainers[0]
	if init.Name != gitSyncName || init.Image != gitSyncImage {
		t.Errorf("initContainer = %s/%s, want %s/%s", init.Name, init.Image, gitSyncName, gitSyncImage)
	}
	if !hasArg(init.Args, "--one-time") || !hasArg(init.Args, "--repo=git@github.com:o/r.git") {
		t.Errorf("missing required git-sync args: %+v", init.Args)
	}
	if hasArg(init.Args, "--ssh-key-file="+gitSecretMountPath+"/ssh") || hasArg(init.Args, "--add-user") {
		t.Errorf("public repo must not set ssh args: %+v", init.Args)
	}

	container := podSpec.Containers[0]
	if container.WorkingDir != gitLinkPath {
		t.Errorf("workingDir = %q, want %q", container.WorkingDir, gitLinkPath)
	}

	// git-sync runs non-root; the pod fsGroup lets it read the 0400 secret and
	// write the shared emptyDir.
	if podSpec.SecurityContext == nil || podSpec.SecurityContext.FSGroup == nil || *podSpec.SecurityContext.FSGroup != gitSyncUID {
		t.Errorf("expected pod fsGroup=%d for git source, got %+v", gitSyncUID, podSpec.SecurityContext)
	}

	// Shared emptyDir mounted at the same path on both containers; no secret volume.
	var sourceVolume *corev1.Volume
	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Name == sourceVolumeName {
			sourceVolume = &podSpec.Volumes[i]
		}
		if podSpec.Volumes[i].Secret != nil {
			t.Errorf("public repo must not mount a secret volume: %+v", podSpec.Volumes)
		}
	}
	if sourceVolume == nil || sourceVolume.EmptyDir == nil {
		t.Fatalf("expected emptyDir volume %q, got %+v", sourceVolumeName, podSpec.Volumes)
	}
	if !mountAt(init.VolumeMounts, sourceVolumeName, gitRootMountPath) {
		t.Errorf("initContainer missing source mount: %+v", init.VolumeMounts)
	}
	if !mountAt(container.VolumeMounts, sourceVolumeName, gitRootMountPath) {
		t.Errorf("ray container missing source mount: %+v", container.VolumeMounts)
	}

	if submitter := rayJob.Spec.SubmitterPodTemplate; submitter != nil && len(submitter.Spec.InitContainers) != 0 {
		t.Errorf("submitter must not run git-sync, got %d initContainers", len(submitter.Spec.InitContainers))
	}
}

func TestBuildRayJobPrivateGitSource(t *testing.T) {
	spec := gitSpec()
	spec.Source.GitSecretRef = "zenthros-git-ssh"

	rayJob := BuildRayJob(spec)
	podSpec := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec

	init := podSpec.InitContainers[0]
	for _, want := range []string{
		"--ssh-key-file=" + gitSecretMountPath + "/ssh",
		"--ssh-known-hosts-file=" + gitSecretMountPath + "/known_hosts",
		"--add-user",
	} {
		if !hasArg(init.Args, want) {
			t.Errorf("missing ssh arg %q in %+v", want, init.Args)
		}
	}

	var secretVol *corev1.Volume
	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Name == gitSecretVolumeName {
			secretVol = &podSpec.Volumes[i]
		}
	}
	if secretVol == nil || secretVol.Secret == nil {
		t.Fatalf("expected secret volume %q, got %+v", gitSecretVolumeName, podSpec.Volumes)
	}
	if secretVol.Secret.SecretName != "zenthros-git-ssh" {
		t.Errorf("secretName = %q, want zenthros-git-ssh", secretVol.Secret.SecretName)
	}
	if secretVol.Secret.DefaultMode == nil || *secretVol.Secret.DefaultMode != gitSecretDefaultMode {
		t.Errorf("defaultMode = %v, want 0400 (%d)", secretVol.Secret.DefaultMode, gitSecretDefaultMode)
	}

	var secretMount *corev1.VolumeMount
	for i := range init.VolumeMounts {
		if init.VolumeMounts[i].Name == gitSecretVolumeName {
			secretMount = &init.VolumeMounts[i]
		}
	}
	if secretMount == nil {
		t.Fatalf("initContainer missing secret mount: %+v", init.VolumeMounts)
	}
	if secretMount.MountPath != gitSecretMountPath || !secretMount.ReadOnly {
		t.Errorf("secret mount = %+v, want path=%s readOnly=true", *secretMount, gitSecretMountPath)
	}
}

func TestBuildRayJobGitRef(t *testing.T) {

	withRef := gitSpec()
	withRef.Source.GitRef = "master"
	initWithRef := BuildRayJob(withRef).Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.InitContainers[0]
	if !hasArg(initWithRef.Args, "--ref=master") {
		t.Errorf("expected --ref=master, got %+v", initWithRef.Args)
	}

	noRef := BuildRayJob(gitSpec()).Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.InitContainers[0]
	for _, a := range noRef.Args {
		if strings.HasPrefix(a, "--ref=") {
			t.Errorf("empty gitRef must not emit --ref, got %+v", noRef.Args)
		}
	}
}

func TestBuildRayJobGitSourceOnWorkers(t *testing.T) {

	rayJob := BuildRayJob(gitSpec())
	groups := rayJob.Spec.RayClusterSpec.WorkerGroupSpecs
	if len(groups) != 1 {
		t.Fatalf("expected 1 worker group, got %d", len(groups))
	}
	workerPod := groups[0].Template.Spec
	if len(workerPod.InitContainers) != 1 || workerPod.InitContainers[0].Name != gitSyncName {
		t.Errorf("worker pod missing git-sync initContainer: %+v", workerPod.InitContainers)
	}
}

func inlineSpec() jobspec.JobSpec {
	spec := testSpec()
	spec.Source = v1.Source{Inline: "dGVzdA=="}
	return spec
}

func TestBuildRayJobInlineSource(t *testing.T) {

	rayJob := BuildRayJob(inlineSpec())
	podSpec := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec

	if len(podSpec.InitContainers) != 1 {
		t.Fatalf("expected 1 initContainer, got %d", len(podSpec.InitContainers))
	}
	init := podSpec.InitContainers[0]
	if init.Name != inlineInitName || init.Image != inlineInitImage {
		t.Errorf("initContainer = %s/%s, want %s/%s", init.Name, init.Image, inlineInitName, inlineInitImage)
	}
	if !mountAt(init.VolumeMounts, inlineSecretVolumeName, inlineSecretMountPath) {
		t.Errorf("initContainer missing secret mount: %+v", init.VolumeMounts)
	}
	if !mountAt(init.VolumeMounts, sourceVolumeName, sourceMountPath) {
		t.Errorf("initContainer missing work mount: %+v", init.VolumeMounts)
	}

	container := podSpec.Containers[0]
	if container.WorkingDir != sourceMountPath {
		t.Errorf("workingDir = %q, want %q", container.WorkingDir, sourceMountPath)
	}

	var secretVol, emptyDirVol *corev1.Volume
	for i := range podSpec.Volumes {
		switch podSpec.Volumes[i].Name {
		case inlineSecretVolumeName:
			secretVol = &podSpec.Volumes[i]
		case sourceVolumeName:
			emptyDirVol = &podSpec.Volumes[i]
		}
	}
	if secretVol == nil || secretVol.Secret == nil {
		t.Fatalf("expected secret volume %q, got %+v", inlineSecretVolumeName, podSpec.Volumes)
	}
	wantSecret := InlineSecretNameFor(inlineSpec())
	if secretVol.Secret.SecretName != wantSecret {
		t.Errorf("secretName = %q, want %q", secretVol.Secret.SecretName, wantSecret)
	}
	if emptyDirVol == nil || emptyDirVol.EmptyDir == nil {
		t.Fatalf("expected emptyDir volume %q, got %+v", sourceVolumeName, podSpec.Volumes)
	}
}

func TestBuildRayJobInlineSourceOnWorkers(t *testing.T) {

	rayJob := BuildRayJob(inlineSpec())
	groups := rayJob.Spec.RayClusterSpec.WorkerGroupSpecs
	if len(groups) != 1 {
		t.Fatalf("expected 1 worker group, got %d", len(groups))
	}
	workerPod := groups[0].Template.Spec
	if len(workerPod.InitContainers) != 1 || workerPod.InitContainers[0].Name != inlineInitName {
		t.Errorf("worker pod missing inline initContainer: %+v", workerPod.InitContainers)
	}
}

func structuredSpec() jobspec.JobSpec {
	spec := testSpec()
	spec.Entrypoint = ""
	spec.Command = []string{"python", "train.py"}
	spec.Args = []string{"--message", "hello world"}
	return spec
}

func TestBuildRayJobStructuredCommand(t *testing.T) {

	rayJob := BuildRayJob(structuredSpec())

	// Ray runs the entrypoint through a shell, so every structured token lands
	// single-quoted: whitespace and shell metacharacters keep their literal meaning.
	want := `'python' 'train.py' '--message' 'hello world'`
	if rayJob.Spec.Entrypoint != want {
		t.Errorf("entrypoint = %q, want %q", rayJob.Spec.Entrypoint, want)
	}
}

// On a standing cluster the entrypoint goes over HTTP to one shell, so the
// quoted tokens are the entrypoint. A dedicated cluster's submitter adds a
// shell (entrypoint_test.go).
func TestBuildRayJobStructuredCommandQuotesShellMetachars(t *testing.T) {
	spec := structuredSpec()
	spec.RayCluster = &v1.RayClusterReference{Name: "standing", Namespace: "tenant-default"}
	spec.Command = []string{"bash", "-c"}
	spec.Args = []string{"echo $(whoami); rm -rf /tmp/$x", "it's quoted"}

	rayJob := BuildRayJob(spec)

	want := `'bash' '-c' 'echo $(whoami); rm -rf /tmp/$x' 'it'\''s quoted'`
	if rayJob.Spec.Entrypoint != want {
		t.Errorf("entrypoint = %q, want %q", rayJob.Spec.Entrypoint, want)
	}
}

func TestBuildRayJobWorkingDirOverridesSource(t *testing.T) {
	spec := testSpec() // hostPath source would default the container to /work
	spec.WorkingDir = "/data/run"

	rayJob := BuildRayJob(spec)
	container := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0]

	if container.WorkingDir != "/data/run" {
		t.Errorf("workingDir = %q, want explicit /data/run to win over the source mount", container.WorkingDir)
	}
}

func TestBuildRayJobWorkingDirWithoutSource(t *testing.T) {
	spec := testSpec()
	spec.Source = v1.Source{}
	spec.WorkingDir = "/scratch"

	rayJob := BuildRayJob(spec)
	container := rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0]

	if container.WorkingDir != "/scratch" {
		t.Errorf("workingDir = %q, want /scratch", container.WorkingDir)
	}
}

func TestSpecMismatchDetectsStructuredCommandChange(t *testing.T) {
	existing := BuildRayJob(structuredSpec())

	changed := structuredSpec()
	changed.Args = []string{"--message", "different"}
	desired := BuildRayJob(changed)

	mismatch := SpecMismatch(existing, desired)
	if mismatch == nil || mismatch.Field != "entrypoint" {
		t.Errorf("mismatch = %+v, want field=entrypoint for a changed structured command", mismatch)
	}
}

func TestBuildRayJobEnvIsSorted(t *testing.T) {
	env := BuildRayJob(testSpec()).Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Env
	wantOrder := []string{"BAZ", "FOO"}
	if len(env) != len(wantOrder) {
		t.Fatalf("env = %+v, want %v", env, wantOrder)
	}
	for i, want := range wantOrder {
		if env[i].Name != want {
			t.Errorf("env[%d] = %q, want %q (sorted order)", i, env[i].Name, want)
		}
	}
}
