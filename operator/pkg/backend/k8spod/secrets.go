// SPDX-License-Identifier: Apache-2.0

package k8spod

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/secretsource"
)

const (
	secretProjectionMode = int32(0400)
	SecretRuntimeUID     = int64(1000)
	SecretInitImage      = "busybox:1.36.1"
	secretInitCPU        = "10m"
	secretInitMemory     = "16Mi"
)

// ApplySecretSources adds reference-backed env vars and read-only file mounts.
func ApplySecretSources(podSpec *corev1.PodSpec, refs []v1.SecretSourceRef) {
	ApplySecretSourcesAsUser(podSpec, refs, SecretRuntimeUID)
}

// ApplySecretSourcesAsUser adds SecretSources materialized for runtimeUID.
// Workload adapters with a pinned runtime identity should use this instead of
// forcing their image to run as the shared default UID.
func ApplySecretSourcesAsUser(podSpec *corev1.PodSpec, refs []v1.SecretSourceRef, runtimeUID int64) {
	if len(refs) == 0 || len(podSpec.Containers) == 0 {
		return
	}
	container := &podSpec.Containers[0]
	for index, ref := range refs {
		if ref.Type != v1.SecretSourceTypeKubernetes {
			continue
		}
		keySelector := &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name}, Key: ref.Key}
		if ref.Materialization.Mode == v1.SecretMaterializationEnv {
			container.Env = append(container.Env, corev1.EnvVar{
				Name: secretsource.EnvName(ref), ValueFrom: &corev1.EnvVarSource{SecretKeyRef: keySelector},
			})
			continue
		}
		sourceVolumeName := fmt.Sprintf("zenthros-secret-source-%d", index)
		materializedVolumeName := fmt.Sprintf("zenthros-secret-file-%d", index)
		targetPath := secretsource.FilePath(ref)
		podSpec.Volumes = append(podSpec.Volumes,
			corev1.Volume{Name: sourceVolumeName, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: ref.Name, DefaultMode: pointer(secretProjectionMode),
				Items: []corev1.KeyToPath{{Key: ref.Key, Path: "value", Mode: pointer(secretProjectionMode)}},
			}}},
			corev1.Volume{Name: materializedVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		)
		podSpec.InitContainers = append(podSpec.InitContainers, secretFileInitContainer(index, sourceVolumeName, materializedVolumeName, runtimeUID))
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name: materializedVolumeName, MountPath: targetPath, SubPath: "value", ReadOnly: true,
		})
		container.SecurityContext = secretRuntimeSecurityContext(container.SecurityContext, runtimeUID)
	}
}

func secretFileInitContainer(index int, sourceVolumeName, materializedVolumeName string, runtimeUID int64) corev1.Container {
	root := int64(0)
	nonRoot := false
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	return corev1.Container{
		Name: fmt.Sprintf("zenthros-secret-file-%d", index), Image: SecretInitImage,
		Command: []string{"/bin/sh", "-c"},
		Args: []string{fmt.Sprintf(
			"cp /source/value /materialized/value && chmod 0600 /materialized/value && chown %d:%d /materialized/value",
			runtimeUID, runtimeUID,
		)},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser: &root, RunAsNonRoot: &nonRoot, AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			ReadOnlyRootFilesystem: &readOnlyRootFilesystem,
			Capabilities:           &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"CHOWN"}},
		},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(secretInitCPU), corev1.ResourceMemory: resource.MustParse(secretInitMemory),
		}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: sourceVolumeName, MountPath: "/source", ReadOnly: true},
			{Name: materializedVolumeName, MountPath: "/materialized"},
		},
	}
}

func secretRuntimeSecurityContext(existing *corev1.SecurityContext, runtimeUID int64) *corev1.SecurityContext {
	if existing == nil {
		existing = &corev1.SecurityContext{}
	}
	nonRoot := true
	existing.RunAsUser = pointer(runtimeUID)
	existing.RunAsNonRoot = &nonRoot
	return existing
}

func pointer[T any](value T) *T {
	return &value
}
