// SPDX-License-Identifier: Apache-2.0

// Package k8spod builds destination-local Kubernetes pod fragments shared by
// native workload adapters.
package k8spod

import (
	"crypto/sha256"
	"encoding/hex"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

const (
	SourceVolumeName = "source"
	SourceMountPath  = "/work"

	GitSyncImage         = "registry.k8s.io/git-sync/git-sync:v4.4.0"
	GitSyncName          = "git-sync"
	GitRootMountPath     = "/git"
	GitLinkPath          = "/git/src"
	GitSecretMountPath   = "/etc/git-secret"
	GitSecretVolumeName  = "git-ssh"
	gitSyncCPU           = "100m"
	gitSyncMemory        = "128Mi"
	GitSyncUID           = int64(65533)
	GitSecretDefaultMode = int32(0400)

	InlineInitName         = "inline-extract"
	InlineInitImage        = "busybox:1.36"
	InlineSecretVolumeName = "inline-src"
	InlineSecretMountPath  = "/src"
	inlineInitCPU          = "100m"
	inlineInitMemory       = "64Mi"
)

// ApplySource adds one validated source mode to the first workload container.
func ApplySource(podSpec *corev1.PodSpec, spec jobspec.JobSpec) {
	ApplySourceAs(podSpec, spec, GitSyncUID)
}

// ApplySourceAs is ApplySource with an explicit uid/fsGroup for git-sync.
// Spark pins the driver to uid 185 and the native operator may overwrite the
// pod securityContext; git-sync must share that uid or it cannot write the
// source emptyDir and the driver pod fails immediately.
func ApplySourceAs(podSpec *corev1.PodSpec, spec jobspec.JobSpec, uid int64) {
	if len(podSpec.Containers) == 0 {
		return
	}
	container := &podSpec.Containers[0]
	switch {
	case spec.Source.HostPath != "":
		container.WorkingDir = SourceMountPath
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: SourceVolumeName, MountPath: SourceMountPath})
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name:         SourceVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: spec.Source.HostPath}},
		})
	case spec.Source.Git != "":
		container.WorkingDir = GitLinkPath
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: SourceVolumeName, MountPath: GitRootMountPath})
		podSpec.InitContainers = append(podSpec.InitContainers, gitSyncInitContainer(spec.Source, uid))
		podSpec.Volumes = append(podSpec.Volumes, gitSyncVolumes(spec.Source)...)
		podSpec.SecurityContext = &corev1.PodSecurityContext{FSGroup: &uid}
	case spec.Source.Inline != "":
		container.WorkingDir = SourceMountPath
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: SourceVolumeName, MountPath: SourceMountPath})
		podSpec.InitContainers = append(podSpec.InitContainers, inlineExtractInitContainer(spec))
		podSpec.Volumes = append(podSpec.Volumes, inlineVolumes(spec)...)
	}
}

func gitSyncInitContainer(source v1.Source, uid int64) corev1.Container {
	args := []string{"--repo=" + source.Git}
	if source.GitRef != "" {
		args = append(args, "--ref="+source.GitRef)
	}
	args = append(args, "--root="+GitRootMountPath, "--link="+GitLinkPath, "--one-time")
	mounts := []corev1.VolumeMount{{Name: SourceVolumeName, MountPath: GitRootMountPath}}
	if source.GitSecretRef != "" {
		args = append(args,
			"--ssh-key-file="+GitSecretMountPath+"/ssh",
			"--ssh-known-hosts-file="+GitSecretMountPath+"/known_hosts",
			"--add-user",
		)
		mounts = append(mounts, corev1.VolumeMount{Name: GitSecretVolumeName, MountPath: GitSecretMountPath, ReadOnly: true})
	}
	return corev1.Container{
		Name: GitSyncName, Image: GitSyncImage, Args: args,
		SecurityContext: &corev1.SecurityContext{RunAsUser: &uid},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(gitSyncCPU), corev1.ResourceMemory: resource.MustParse(gitSyncMemory),
		}},
		VolumeMounts: mounts,
	}
}

func gitSyncVolumes(source v1.Source) []corev1.Volume {
	volumes := []corev1.Volume{{Name: SourceVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
	if source.GitSecretRef != "" {
		mode := GitSecretDefaultMode
		volumes = append(volumes, corev1.Volume{
			Name: GitSecretVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: source.GitSecretRef, DefaultMode: &mode,
			}},
		})
	}
	return volumes
}

// InlineSecretJobLabel names the job on its inline Secrets, for cleanup:
// the Secret's own name carries the bundle's digest.
const InlineSecretJobLabel = "zenthros.ai/job-name"

// InlineSecretNameFor is the Secret holding spec's inline bundle, named by the
// bundle's digest: a later job of the same name with other code gets a Secret
// of its own instead of the one an earlier job left.
func InlineSecretNameFor(spec jobspec.JobSpec) string {
	sum := sha256.Sum256([]byte(spec.Source.Inline))
	return "zenthros-src-" + spec.Name + "-" + hex.EncodeToString(sum[:])[:10]
}

func inlineExtractInitContainer(spec jobspec.JobSpec) corev1.Container {
	return corev1.Container{
		Name: InlineInitName, Image: InlineInitImage,
		Command: []string{"/bin/sh", "-c", "tar xzf " + InlineSecretMountPath + "/bundle.tar.gz -C " + SourceMountPath},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(inlineInitCPU), corev1.ResourceMemory: resource.MustParse(inlineInitMemory),
		}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: InlineSecretVolumeName, MountPath: InlineSecretMountPath, ReadOnly: true},
			{Name: SourceVolumeName, MountPath: SourceMountPath},
		},
	}
}

func inlineVolumes(spec jobspec.JobSpec) []corev1.Volume {
	return []corev1.Volume{
		{Name: InlineSecretVolumeName, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: InlineSecretNameFor(spec)}}},
		{Name: SourceVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
}
