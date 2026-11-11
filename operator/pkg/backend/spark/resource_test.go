// SPDX-License-Identifier: Apache-2.0

package spark

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

func testSpec() jobspec.JobSpec {
	return jobspec.JobSpec{
		ID: "uid-spark", Name: "spark-pi", Namespace: "tenant-default", Tenant: "default",
		Backend: backend.TypeK8s, Kind: v1.WorkloadKindSparkApplication,
		Image: "zenthros/spark-base:4.1.1", Env: map[string]string{"MODE": "test"},
		Spark: &v1.SparkSpec{
			MainApplicationFile: "local:///opt/spark/examples/jars/spark-examples.jar",
			MainClass:           "org.apache.spark.examples.SparkPi", Arguments: []string{"10"},
			SparkConf: map[string]string{"spark.sql.shuffle.partitions": "2"},
			Driver:    v1.SparkResourceSpec{CPU: "1", Memory: "512m"},
			Executor:  v1.SparkResourceSpec{CPU: "1", Memory: "512m"}, ExecutorCount: 2,
			RestartPolicy: v1.SparkRestartPolicy{Type: v1.SparkRestartOnFailure, MaxRestartAttempts: 3},
		},
	}
}

func TestSparkBuildHonorsWorkersShape(t *testing.T) {
	obj, err := Build(jobspec.JobSpec{
		ID:        "spark-gpu",
		Name:      "spark-gpu",
		Namespace: "tenant-acme",
		Kind:      v1.WorkloadKindSparkApplication,
		Image:     "spark:latest",
		Workers:   v1.WorkerGroup{GPUsPerNode: 1, Min: 2, Max: 2},
		Spark:     &v1.SparkSpec{MainApplicationFile: "local:///app.py"},
	})
	if err != nil {
		t.Fatal(err)
	}
	instances, _, _ := unstructured.NestedString(obj.Object, "spec", "sparkConf", "spark.executor.instances")
	conf, _, _ := unstructured.NestedMap(obj.Object, "spec", "sparkConf")
	joined := ""
	for k, v := range conf {
		if s, ok := v.(string); ok {
			joined += k + "=" + s + " "
		}
	}
	honorsCount := instances == "2"
	honorsGPU := strings.Contains(joined, "gpu") || strings.Contains(joined, "nvidia")
	if !honorsCount || !honorsGPU {
		t.Fatalf("Spark Build ignored workers.min=2 and gpusPerNode=1; instances=%q sparkConf=%v", instances, conf)
	}
}

func TestBuildTranslatesSparkIntent(t *testing.T) {
	application, err := Build(testSpec())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assertNestedString(t, application, "local:///opt/spark/examples/jars/spark-examples.jar", "spec", "jars")
	assertNestedString(t, application, "org.apache.spark.examples.SparkPi", "spec", "mainClass")
	assertNestedString(t, application, "4.1.1", "spec", "runtimeVersions", "sparkVersion")
	assertNestedString(t, application, "zenthros/spark-base:4.1.1", "spec", "sparkConf", "spark.kubernetes.container.image")
	assertNestedString(t, application, "2", "spec", "sparkConf", "spark.executor.instances")
	assertNestedString(t, application, "test", "spec", "sparkConf", "spark.kubernetes.driverEnv.MODE")
	assertNestedString(t, application, v1.SparkRestartOnFailure, "spec", "applicationTolerations", "restartConfig", "restartPolicy")
	assertNestedString(t, application, "Always", "spec", "applicationTolerations", "resourceRetainPolicy")
	if application.GetAnnotations()[SpecHashAnnotation] == "" {
		t.Fatal("spec hash annotation is empty")
	}
}

func TestBuildPythonUsesPyFiles(t *testing.T) {
	spec := testSpec()
	spec.Spark.MainApplicationFile = "local:///opt/spark/examples/src/main/python/pi.py"
	spec.Spark.MainClass = ""
	application, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assertNestedString(t, application, spec.Spark.MainApplicationFile, "spec", "pyFiles")
}

func TestBuildSourceDoesNotOverrideSparkWorkingDirectory(t *testing.T) {
	spec := testSpec()
	spec.Spark.MainApplicationFile = "local:///work/main.py"
	spec.Spark.MainClass = ""
	spec.Source = v1.Source{Inline: base64.StdEncoding.EncodeToString([]byte("bundle"))}

	application, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, role := range []string{"driverSpec", "executorSpec"} {
		if got := podSpecAt(t, application, role).Containers[0].WorkingDir; got != "" {
			t.Errorf("%s implicit workingDir = %q; want image default", role, got)
		}
	}

	spec.WorkingDir = "/workspace"
	application, err = Build(spec)
	if err != nil {
		t.Fatalf("Build with explicit workingDir: %v", err)
	}
	for _, role := range []string{"driverSpec", "executorSpec"} {
		if got := podSpecAt(t, application, role).Containers[0].WorkingDir; got != spec.WorkingDir {
			t.Errorf("%s explicit workingDir = %q; want %q", role, got, spec.WorkingDir)
		}
	}
}

func TestBuildAppliesEverySupportedSourceToDriverAndExecutor(t *testing.T) {
	tests := []struct {
		name   string
		source v1.Source
		check  func(*testing.T, corev1.PodSpec)
	}{
		{
			name: "git", source: v1.Source{Git: "https://example.com/repo.git", GitRef: "main"},
			check: func(t *testing.T, pod corev1.PodSpec) {
				if len(pod.InitContainers) != 1 || pod.InitContainers[0].Name != "git-sync" {
					t.Fatalf("git init containers = %#v", pod.InitContainers)
				}
				if pod.InitContainers[0].SecurityContext == nil || pod.InitContainers[0].SecurityContext.RunAsUser == nil || *pod.InitContainers[0].SecurityContext.RunAsUser != SparkRuntimeUID {
					t.Fatalf("git-sync uid = %#v; want %d so Spark operator fsGroup can write the clone", pod.InitContainers[0].SecurityContext, SparkRuntimeUID)
				}
				if pod.SecurityContext == nil || pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != SparkRuntimeUID {
					t.Fatalf("git fsGroup = %#v; want %d", pod.SecurityContext, SparkRuntimeUID)
				}
			},
		},
		{
			name: "inline", source: v1.Source{Inline: base64.StdEncoding.EncodeToString([]byte("bundle"))},
			check: func(t *testing.T, pod corev1.PodSpec) {
				if len(pod.InitContainers) != 1 || pod.InitContainers[0].Name != "inline-extract" {
					t.Fatalf("inline init containers = %#v", pod.InitContainers)
				}
			},
		},
		{
			name: "hostPath", source: v1.Source{HostPath: "/data/code"},
			check: func(t *testing.T, pod corev1.PodSpec) {
				if len(pod.Volumes) != 1 || pod.Volumes[0].HostPath == nil || pod.Volumes[0].HostPath.Path != "/data/code" {
					t.Fatalf("hostPath volumes = %#v", pod.Volumes)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := testSpec()
			spec.Source = test.source
			application, err := Build(spec)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			for _, role := range []string{"driverSpec", "executorSpec"} {
				t.Run(role, func(t *testing.T) { test.check(t, podSpecAt(t, application, role)) })
			}
		})
	}
}

func TestBuildAppliesSecretSourcesToDriverAndExecutor(t *testing.T) {
	spec := testSpec()
	spec.SecretSources = []v1.SecretSourceRef{
		{
			Type: v1.SecretSourceTypeKubernetes, Namespace: spec.Namespace, Name: "api-token", Key: "token",
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "API_TOKEN"},
		},
		{
			Type: v1.SecretSourceTypeKubernetes, Namespace: spec.Namespace, Name: "tls", Key: "cert",
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationFile, Path: "/run/tls/cert"},
		},
	}
	application, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, role := range []string{"driverSpec", "executorSpec"} {
		pod := podSpecAt(t, application, role)
		if len(pod.Containers[0].Env) != 1 || pod.Containers[0].Env[0].ValueFrom.SecretKeyRef.Name != "api-token" {
			t.Errorf("%s env = %#v", role, pod.Containers[0].Env)
		}
		if len(pod.InitContainers) != 1 || pod.Containers[0].SecurityContext == nil || pod.Containers[0].SecurityContext.RunAsUser == nil || *pod.Containers[0].SecurityContext.RunAsUser != SparkRuntimeUID {
			t.Errorf("%s file materialization = %#v", role, pod)
		}
		wantChown := "chown 185:185 /materialized/value"
		if len(pod.InitContainers[0].Args) != 1 || !strings.Contains(pod.InitContainers[0].Args[0], wantChown) {
			t.Errorf("%s secret chown = %#v; want %q", role, pod.InitContainers[0].Args, wantChown)
		}
	}
}

func podSpecAt(t *testing.T, application *unstructured.Unstructured, role string) corev1.PodSpec {
	t.Helper()
	raw, found, err := unstructured.NestedMap(application.Object, "spec", role, "podTemplateSpec")
	if err != nil || !found {
		t.Fatalf("%s pod template: found=%v err=%v", role, found, err)
	}
	var template corev1.PodTemplateSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &template); err != nil {
		t.Fatalf("decode %s pod template: %v", role, err)
	}
	return template.Spec
}

func TestSpecMismatchUsesCanonicalHash(t *testing.T) {
	existing, _ := Build(testSpec())
	desired, _ := Build(testSpec())
	if mismatch := SpecMismatch(existing, desired); mismatch != nil {
		t.Fatalf("identical specs mismatch: %v", mismatch)
	}
	changed := testSpec()
	changed.Spark.ExecutorCount = 4
	desired, _ = Build(changed)
	if mismatch := SpecMismatch(existing, desired); !errors.Is(mismatch, backend.ErrSpecMismatch) {
		t.Fatalf("changed spec mismatch = %v", mismatch)
	}
}

func TestBuildSelectsRuntimeImage(t *testing.T) {
	tests := []struct {
		name  string
		image string
	}{
		{name: "base runtime stays the default", image: "zenthros/spark-base:4.1.1"},
		{name: "data runtime is selected explicitly", image: "zenthros/spark-data:4.0.1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := testSpec()
			spec.Image = test.image
			application, err := Build(spec)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			assertNestedString(t, application, test.image, "spec", "sparkConf", "spark.kubernetes.container.image")
			for _, role := range []string{"driverSpec", "executorSpec"} {
				if got := podSpecAt(t, application, role).Containers[0].Image; got != test.image {
					t.Errorf("%s image = %q, want %q", role, got, test.image)
				}
			}
		})
	}
}

func TestBuildEnablesNativePrometheusMetrics(t *testing.T) {
	spec := testSpec()
	// Platform defaults must win so the scrape contract stays stable.
	spec.Spark.SparkConf["spark.ui.prometheus.enabled"] = "false"

	application, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assertNestedString(t, application, "true", "spec", "sparkConf", "spark.ui.prometheus.enabled")
	assertNestedString(t, application, "org.apache.spark.metrics.sink.PrometheusServlet",
		"spec", "sparkConf", "spark.metrics.conf.*.sink.prometheusServlet.class")
	assertNestedString(t, application, MetricsEndpointPath,
		"spec", "sparkConf", "spark.metrics.conf.*.sink.prometheusServlet.path")
}

func TestBuildAnnotatesPodsForMetricsScraping(t *testing.T) {
	application, err := Build(testSpec())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	wantPorts := map[string]string{"driverSpec": "4040", "executorSpec": "4041"}
	for role, port := range wantPorts {
		annotations := podTemplateAt(t, application, role).Annotations
		if annotations["prometheus.io/scrape"] != "true" || annotations["prometheus.io/path"] != MetricsEndpointPath || annotations["prometheus.io/port"] != port {
			t.Errorf("%s metrics annotations = %#v, want scrape=true path=%s port=%s", role, annotations, MetricsEndpointPath, port)
		}
	}
}

func TestBuildTranslatesDataReferences(t *testing.T) {
	spec := testSpec()
	spec.Artifacts = v1.Artifacts{
		Input:  "s3a://warehouse/processed/features",
		Output: "s3a://warehouse/processed/run-1",
	}
	application, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, envKey := range []string{"driverEnv", "executorEnv"} {
		assertNestedString(t, application, "s3a://warehouse/processed/features",
			"spec", "sparkConf", "spark.kubernetes."+envKey+"."+backend.DataInputEnv)
		assertNestedString(t, application, "s3a://warehouse/processed/run-1",
			"spec", "sparkConf", "spark.kubernetes."+envKey+"."+backend.DataOutputEnv)
	}
}

func TestBuildRejectsS3SourceSelection(t *testing.T) {
	spec := testSpec()
	spec.Source = v1.Source{S3: "s3://bucket/bundle.tar.gz"}
	_, err := Build(spec)
	if !errors.Is(err, backend.ErrAdmissionRejected) {
		t.Fatalf("expected admission rejection for s3 source, got %v", err)
	}
	if !strings.Contains(err.Error(), "s3 source bundles") {
		t.Fatalf("rejection detail missing: %v", err)
	}
}

func TestBuildPassesJobEnv(t *testing.T) {
	spec := testSpec()
	// Build must carry the job's env to driver and executor processes.
	spec.Env = map[string]string{"WAREHOUSE_TYPE": "s3", "WAREHOUSE_REF": "s3a://warehouse/data"}
	application, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assertNestedString(t, application, "s3a://warehouse/data", "spec", "sparkConf", "spark.kubernetes.driverEnv.WAREHOUSE_REF")
	assertNestedString(t, application, "s3", "spec", "sparkConf", "spark.kubernetes.executorEnv.WAREHOUSE_TYPE")
}

func podTemplateAt(t *testing.T, application *unstructured.Unstructured, role string) corev1.PodTemplateSpec {
	t.Helper()
	raw, found, err := unstructured.NestedMap(application.Object, "spec", role, "podTemplateSpec")
	if err != nil || !found {
		t.Fatalf("%s pod template: found=%v err=%v", role, found, err)
	}
	var template corev1.PodTemplateSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &template); err != nil {
		t.Fatalf("decode %s pod template: %v", role, err)
	}
	return template
}

func assertNestedString(t *testing.T, object *unstructured.Unstructured, want string, fields ...string) {
	t.Helper()
	got, found, err := unstructured.NestedString(object.Object, fields...)
	if err != nil || !found || got != want {
		t.Fatalf("%v = %q, found=%v, err=%v; want %q", fields, got, found, err, want)
	}
}
