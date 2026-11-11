// SPDX-License-Identifier: Apache-2.0

// Package spark translates Zenthros Spark intent into Apache Spark Operator resources.
package spark

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8spod"
	"github.com/zenthros/operator/pkg/jobspec"
)

const (
	APIVersion            = "spark.apache.org/v1"
	Kind                  = "SparkApplication"
	SparkVersion          = "4.1.1"
	SpecHashAnnotation    = "zenthros.ai/spec-hash"
	JobIDLabel            = "zenthros.ai/job-id"
	WorkloadKindLabel     = "zenthros.ai/workload-kind"
	ApplicationNameLabel  = "zenthros.ai/spark-application"
	DriverServiceAccount  = "spark"
	DriverContainerName   = "spark-kubernetes-driver"
	ExecutorContainerName = "spark-kubernetes-executor"
	SparkRuntimeUID       = int64(185)

	// MetricsEndpointPath is where the native Prometheus servlet serves driver
	// and executor metrics on the Spark UI port. Workload Prometheus scrapes it
	// in the execution environment; the control plane never proxies it.
	MetricsEndpointPath = "/metrics/prometheus"
	// ExecutorMetricsPath serves executor metrics aggregated by the driver UI.
	ExecutorMetricsPath = "/metrics/executors/prometheus"
	// DriverUIPort is the Spark driver UI port that hosts the metrics servlet.
	DriverUIPort = 4040
	// ExecutorUIPort is the Spark executor UI port.
	ExecutorUIPort = 4041
)

var GroupVersionKind = schema.FromAPIVersionAndKind(APIVersion, Kind)

// AddToScheme registers the unstructured SparkApplication types used by the client.
func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypeWithName(GroupVersionKind, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(GroupVersionKind.GroupVersion().WithKind(Kind+"List"), &unstructured.UnstructuredList{})
	metav1.AddToGroupVersion(scheme, GroupVersionKind.GroupVersion())
	return nil
}

// ValidateSourceSelection rejects source modes that are incompatible with the
// Spark runtime before submission. Called both by Build and controller-side so an
// incompatible selection fails fast (admission) instead of retrying as transient.
func ValidateSourceSelection(spec jobspec.JobSpec) error {
	if spec.Source.S3 != "" {
		return fmt.Errorf("%w: s3 source bundles are not a supported Spark runtime selection; use git, inline, or hostPath", backend.ErrAdmissionRejected)
	}
	return nil
}

// Build translates one validated Spark job into the pinned Apache operator v1 API.
// The job's env travels to driver/executor env together with the durable data
// references derived from the job artifacts. Raw data never crosses the
// controller: only these reference strings do.
func Build(spec jobspec.JobSpec) (*unstructured.Unstructured, error) {
	if err := jobspec.ValidateWorkload(spec); err != nil {
		return nil, fmt.Errorf("build SparkApplication: %w", err)
	}
	if err := ValidateSourceSelection(spec); err != nil {
		return nil, err
	}

	sparkConf := copyStringMap(spec.Spark.SparkConf)
	applyMetricsConfig(sparkConf)
	sparkConf["spark.kubernetes.container.image"] = spec.Image
	sparkConf["spark.kubernetes.authenticate.driver.serviceAccountName"] = DriverServiceAccount
	setResourceConf(sparkConf, "driver", spec.Spark.Driver)
	setResourceConf(sparkConf, "executor", spec.Spark.Executor)
	executorCount := spec.Spark.ExecutorCount
	// workers is the shape the user asked for. A leftover executorCount must not
	// hide replicas or leave a GPU request off the spark conf.
	if spec.Workers.Max > 0 {
		executorCount = spec.Workers.Max
	} else if spec.Workers.Min > 0 {
		executorCount = spec.Workers.Min
	}
	if executorCount == 0 {
		executorCount = 1
	}
	sparkConf["spark.executor.instances"] = strconv.FormatInt(int64(executorCount), 10)
	if spec.Workers.GPUsPerNode > 0 {
		sparkConf["spark.executor.resource.gpu.amount"] = strconv.FormatInt(int64(spec.Workers.GPUsPerNode), 10)
	}
	workloadEnv := backend.WithDataReferences(spec.Env, spec.Artifacts)
	for _, name := range sortedKeys(workloadEnv) {
		sparkConf["spark.kubernetes.driverEnv."+name] = workloadEnv[name]
		sparkConf["spark.kubernetes.executorEnv."+name] = workloadEnv[name]
	}

	nativeSpec := map[string]any{
		"driverArgs":      stringsToAny(spec.Spark.Arguments),
		"runtimeVersions": map[string]any{"sparkVersion": SparkVersion},
		"sparkConf":       stringMapToAny(sparkConf),
		"applicationTolerations": map[string]any{
			// Zenthros GC owns log retention and deletes the SparkApplication
			// when its retention window expires. Keep native resources until
			// that explicit cleanup so terminal driver logs remain readable.
			"resourceRetainPolicy": "Always",
			"restartConfig": map[string]any{
				"restartPolicy":      restartPolicy(spec.Spark.RestartPolicy.Type),
				"maxRestartAttempts": int64(spec.Spark.RestartPolicy.MaxRestartAttempts),
			},
		},
	}
	applicationField, err := applicationFileField(spec.Spark.MainApplicationFile, spec.Spark.MainClass)
	if err != nil {
		return nil, err
	}
	for key, value := range applicationField {
		nativeSpec[key] = value
	}
	driverTemplate, err := workloadPodTemplate(spec, DriverContainerName)
	if err != nil {
		return nil, fmt.Errorf("build SparkApplication driver template: %w", err)
	}
	executorTemplate, err := workloadPodTemplate(spec, ExecutorContainerName)
	if err != nil {
		return nil, fmt.Errorf("build SparkApplication executor template: %w", err)
	}
	nativeSpec["driverSpec"] = map[string]any{"podTemplateSpec": driverTemplate}
	nativeSpec["executorSpec"] = map[string]any{"podTemplateSpec": executorTemplate}

	hash, err := hashSpec(nativeSpec)
	if err != nil {
		return nil, fmt.Errorf("hash SparkApplication spec: %w", err)
	}
	labels := k8spod.WorkloadLabels(spec)
	labels[WorkloadKindLabel] = Kind
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": APIVersion,
		"kind":       Kind,
		"metadata": map[string]any{
			"name":        spec.Name,
			"namespace":   spec.Namespace,
			"labels":      stringMapToAny(labels),
			"annotations": map[string]any{SpecHashAnnotation: hash},
		},
		"spec": nativeSpec,
	}}
	if spec.OwnerUID != "" {
		obj.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: spec.OwnerAPIVersion,
			Kind:       spec.OwnerKind,
			Name:       spec.OwnerName,
			UID:        types.UID(spec.OwnerUID),
		}})
	}
	return obj, nil
}

func workloadPodTemplate(spec jobspec.JobSpec, containerName string) (map[string]any, error) {
	template := k8spod.WorkloadPodTemplate(spec,
		corev1.PodSpec{Containers: []corev1.Container{{Name: containerName, Image: spec.Image}}},
		k8spod.RuntimeUIDs{Source: SparkRuntimeUID, Secrets: SparkRuntimeUID})
	if spec.WorkingDir == "" {
		// Spark stages local application files into its current directory before
		// launching them. Using the shared source mount (/work) as that directory
		// makes local:///work/main.py a self-copy and Spark removes the source.
		// Keep the runtime image's working directory unless the user chose one.
		template.Spec.Containers[0].WorkingDir = ""
	}
	template.Labels[WorkloadKindLabel] = Kind
	template.Labels[ApplicationNameLabel] = spec.Name
	template.Annotations = metricsAnnotations(containerName)
	converted, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&template)
	if err != nil {
		return nil, err
	}
	return converted, nil
}

func applicationFileField(mainFile, mainClass string) (map[string]any, error) {
	extension := strings.ToLower(filepath.Ext(mainFile))
	if extension == ".py" {
		if mainClass != "" {
			return nil, fmt.Errorf("build SparkApplication: mainClass is not valid for a Python application")
		}
		return map[string]any{"pyFiles": mainFile}, nil
	}
	if mainClass == "" {
		return nil, fmt.Errorf("build SparkApplication: mainClass is required for non-Python applications")
	}
	return map[string]any{"jars": mainFile, "mainClass": mainClass}, nil
}

func restartPolicy(value string) string {
	if value == "" {
		return v1.SparkRestartNever
	}
	return value
}

func setResourceConf(conf map[string]string, role string, resources v1.SparkResourceSpec) {
	if resources.CPU != "" {
		conf["spark."+role+".cores"] = resources.CPU
	}
	if resources.Memory != "" {
		conf["spark."+role+".memory"] = resources.Memory
	}
}

// applyMetricsConfig enables the native Spark Prometheus servlet on driver and
// executors. The servlet serves on the Spark UI port inside the pod; scraping is
// done by the workload Prometheus in the execution environment, so metrics never
// traverse the control channel. Platform defaults override user values for these
// keys to keep the scrape contract stable.
func applyMetricsConfig(conf map[string]string) {
	conf["spark.ui.prometheus.enabled"] = "true"
	conf["spark.metrics.conf.*.sink.prometheusServlet.class"] = "org.apache.spark.metrics.sink.PrometheusServlet"
	conf["spark.metrics.conf.*.sink.prometheusServlet.path"] = MetricsEndpointPath
}

// metricsAnnotations marks a workload pod as a metrics source for environment-local
// scraping. Executor metrics are additionally reachable through the driver's
// executor servlet (ExecutorMetricsPath), which the PodMonitor uses.
func metricsAnnotations(containerName string) map[string]string {
	port := DriverUIPort
	if containerName == ExecutorContainerName {
		port = ExecutorUIPort
	}
	return map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/path":   MetricsEndpointPath,
		"prometheus.io/port":   strconv.Itoa(port),
	}
}

func hashSpec(spec map[string]any) (string, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// SpecMismatch reports an application of this job whose spec differs from
// what a resubmit would create; whose job it is, is k8s.EnsureObject's.
func SpecMismatch(existing, desired *unstructured.Unstructured) *backend.SpecMismatchError {
	desiredHash := desired.GetAnnotations()[SpecHashAnnotation]
	existingHash := existing.GetAnnotations()[SpecHashAnnotation]
	if existingHash == desiredHash && desiredHash != "" {
		return nil
	}
	if existingHash == "" {
		liveSpec, _, _ := unstructured.NestedMap(existing.Object, "spec")
		existingHash, _ = hashSpec(liveSpec)
	}
	if existingHash == desiredHash {
		return nil
	}
	return backend.NewSpecMismatchError("specHash", desiredHash, existingHash)
}

func copyStringMap(source map[string]string) map[string]string {
	out := make(map[string]string, len(source)+8)
	for key, value := range source {
		out[key] = value
	}
	return out
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func stringMapToAny(values map[string]string) map[string]any {
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func stringsToAny(values []string) []any {
	out := make([]any, len(values))
	for index, value := range values {
		out[index] = value
	}
	return out
}
