// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// localHome is HOME in a local job's pod: the person's uid means nothing to
// the image, and Python's caches want somewhere to go.
const localHome = "/tmp"

const localSharedVolume = "zenthros-shared"

// LocalCluster is what differs on a person's own cluster, with no hub: the
// chart gave the agent one namespace, which needs no making and no binding,
// and the SDK's store is a folder the nodes share.
type LocalCluster struct {
	// Folder is at the same path on every node and in every job's pod, so a
	// file:// URI means the same on this computer and in the job.
	Folder string
	// UID and GID are the person's, so that what a job writes there is theirs
	// to read and remove.
	UID, GID int64
}

// ParseLocalUser reads uid:gid.
func ParseLocalUser(user string) (uid, gid int64, err error) {
	first, second, ok := strings.Cut(user, ":")
	if ok {
		uid, err = strconv.ParseInt(first, 10, 64)
	}
	if ok && err == nil {
		gid, err = strconv.ParseInt(second, 10, 64)
	}
	if !ok || err != nil {
		return 0, 0, fmt.Errorf("the job's user %q is not uid:gid", user)
	}
	return uid, gid, nil
}

// UseLocal makes this the agent of no hub, whose grants are all in namespace.
func (e *Executor) UseLocal(local *LocalCluster, namespace string) {
	e.Local, e.Namespace = local, namespace
	e.discovery.namespace = namespace
}

func (l *LocalCluster) apply(pod *corev1.PodSpec) {
	kind := corev1.HostPathDirectory
	pod.Volumes = append(pod.Volumes, corev1.Volume{
		Name:         localSharedVolume,
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: l.Folder, Type: &kind}},
	})
	if pod.SecurityContext == nil {
		pod.SecurityContext = &corev1.PodSecurityContext{}
	}
	pod.SecurityContext.RunAsUser = &l.UID
	pod.SecurityContext.RunAsGroup = &l.GID
	pod.SecurityContext.FSGroup = &l.GID
	workload := &pod.Containers[0]
	workload.VolumeMounts = append(workload.VolumeMounts, corev1.VolumeMount{Name: localSharedVolume, MountPath: l.Folder})
	workload.Env = append(workload.Env, corev1.EnvVar{Name: "HOME", Value: localHome}, corev1.EnvVar{Name: "PYTHONUNBUFFERED", Value: "1"})
}
