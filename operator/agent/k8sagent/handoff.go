// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/pkg/backend/k8spod"
)

// The handoff of a retiring Node's pods (docs/interruptions.md, "Kubernetes
// nodes"). A retiring Node is cordoned at once, but its pods are evicted only
// when the rest of its pool is up: every live slot of the pool has a Ready
// Node, and the images of the pods to move are already on those Nodes. Then a
// pod goes from a Ready node to a Ready node that has its image, instead of
// waiting Pending for a machine to boot and an image to download. Waiting is
// bounded: eviction starts no later than the retire-by deadline less the
// pods' termination grace and HandoffMargin, so they still stop cleanly before
// the provider acts.
//
// Nothing here predicts how long a machine or an image takes: the agent
// watches them happen. A pool with nothing coming (deleted, revoked) has no
// live slot, so its Nodes drain at once.

const (
	// HandoffMargin is kept before the deadline on top of the pods' grace,
	// for the eviction calls, the kubelet and anything unforeseen.
	HandoffMargin = 10 * time.Second
	// MaxEvictGrace caps the termination grace the deadline makes room for:
	// a two-minute notice cannot honor a ten-minute grace anyway.
	MaxEvictGrace = 60 * time.Second
	// defaultGrace is Kubernetes' terminationGracePeriodSeconds default.
	defaultGrace = 30 * time.Second
	// maxPrepullTargets bounds how many Nodes one retiring Node prepulls on.
	maxPrepullTargets = 5

	// LabelPrepullFor marks a prepull pod with the retiring Node it serves.
	LabelPrepullFor = "infrastructure.zenthros.io/prepull-for"
	// prepullCommand does not exist in any image: the kubelet pulls the image,
	// then fails to start the container, so nothing of the image ever runs.
	prepullCommand = "/zenthros-prepull-noop"
)

// startErrors are the waiting reasons of a container that cannot start,
// which a prepull's container is made to be: after a pull failure or one of
// these, a prepull gets no further.
var startErrors = map[string]bool{
	"CreateContainerConfigError": true, "CreateContainerError": true, "RunContainerError": true,
}

// evictBy is when a retiring Node's pods are evicted even though its pool is
// not up: the deadline, less the longest grace among them (capped) and
// HandoffMargin.
func evictBy(retireBy time.Time, pods []*corev1.Pod) time.Time {
	grace := time.Duration(0)
	for _, pod := range pods {
		g := defaultGrace
		if pod.Spec.TerminationGracePeriodSeconds != nil {
			g = time.Duration(*pod.Spec.TerminationGracePeriodSeconds) * time.Second
		}
		grace = max(grace, g)
	}
	return retireBy.Add(-min(grace, MaxEvictGrace) - HandoffMargin)
}

// prepull makes sure the images of pods are on every target Node, through one
// prepull pod per pod and target that lacks one of them. It reports whether
// every prepull is over: pulled, or failed in a way waiting would not change
// (a pull error, a namespace that refuses the pod). A failed prepull never
// holds the handoff: the pod then pulls after it moves, as it would have.
func (r *NodeReconciler) prepull(ctx context.Context, retiring *corev1.Node, pods []*corev1.Pod, targets []*corev1.Node) (bool, error) {
	if len(targets) > maxPrepullTargets {
		targets = targets[:maxPrepullTargets]
	}
	done := true
	for _, target := range targets {
		present := nodeImages(target)
		for _, pod := range pods {
			missing := missingImages(pod, present)
			if len(missing) == 0 {
				continue
			}
			key := client.ObjectKey{Namespace: pod.Namespace, Name: prepullName(retiring.Name, target.Name, string(pod.UID))}
			existing := &corev1.Pod{}
			err := r.Client.Get(ctx, key, existing)
			if apierrors.IsNotFound(err) {
				created := prepullPod(key, retiring, target, pod, missing)
				if err := r.Client.Create(ctx, created); err != nil && !apierrors.IsAlreadyExists(err) {
					if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
						r.Logger.Info("cannot prepull; the pod will pull after it moves", "pod", pod.Namespace+"/"+pod.Name,
							"node", target.Name, "error", err.Error())
						continue
					}
					return false, err
				}
				done = false
				continue
			}
			if err != nil {
				return false, err
			}
			if !prepullOver(existing) {
				done = false
			}
		}
	}
	return done, nil
}

// prepullPod pulls missing on target, with what pulling them takes from the
// pod it serves (namespace, service account, pull secrets). It runs nothing.
func prepullPod(key client.ObjectKey, retiring, target *corev1.Node, pod *corev1.Pod, missing []string) *corev1.Pod {
	containers := make([]corev1.Container, 0, len(missing))
	for i, image := range missing {
		containers = append(containers, corev1.Container{
			Name: "image-" + strconv.Itoa(i), Image: image, Command: []string{prepullCommand},
			ImagePullPolicy: corev1.PullIfNotPresent,
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
				Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace,
			Labels:      map[string]string{LabelPrepullFor: retiring.Name},
			Annotations: map[string]string{"infrastructure.zenthros.io/prepull-target": target.Name, "infrastructure.zenthros.io/prepull-source": pod.Name},
		},
		Spec: corev1.PodSpec{
			NodeName:                      target.Name,
			RestartPolicy:                 corev1.RestartPolicyNever,
			ServiceAccountName:            pod.Spec.ServiceAccountName,
			AutomountServiceAccountToken:  ptr.To(false),
			ImagePullSecrets:              pod.Spec.ImagePullSecrets,
			Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			TerminationGracePeriodSeconds: ptr.To[int64](0),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65534),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: containers,
		},
	}
}

// prepullOver is whether every image of a prepull pod is pulled, or will not
// be.
func prepullOver(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}
	if len(pod.Status.ContainerStatuses) < len(pod.Spec.Containers) {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.ImageID != "" {
			continue
		}
		if w := status.State.Waiting; w != nil && (k8spod.PullFailed(w.Reason) || startErrors[w.Reason]) {
			continue
		}
		return false
	}
	return true
}

// deletePrepulls removes the prepull pods of Nodes that no longer need them:
// every one whose retiring Node is not in keep.
func (r *NodeReconciler) deletePrepulls(ctx context.Context, keep map[string]bool) error {
	pods := &corev1.PodList{}
	if err := r.Client.List(ctx, pods, client.HasLabels{LabelPrepullFor}); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if keep[pod.Labels[LabelPrepullFor]] {
			continue
		}
		if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func prepullName(retiring, target, podUID string) string {
	sum := sha256.Sum256([]byte(retiring + "/" + target + "/" + podUID))
	return "zenthros-prepull-" + hex.EncodeToString(sum[:])[:16]
}

// missingImages are the pod's images (init containers included) the Node does
// not report.
func missingImages(pod *corev1.Pod, present map[string]bool) []string {
	var missing []string
	seen := map[string]bool{}
	for _, list := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for _, container := range list {
			image := normalizeImage(container.Image)
			if container.Image == "" || seen[image] || present[image] {
				continue
			}
			seen[image] = true
			missing = append(missing, container.Image)
		}
	}
	return missing
}

func nodeImages(node *corev1.Node) map[string]bool {
	present := map[string]bool{}
	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			present[normalizeImage(name)] = true
		}
	}
	return present
}

// normalizeImage writes a reference as the container runtime reports it:
// docker.io/library/nginx:latest for nginx.
func normalizeImage(ref string) string {
	name, digest, hasDigest := strings.Cut(ref, "@")
	first, _, hasSlash := strings.Cut(name, "/")
	if !hasSlash || (!strings.ContainsAny(first, ".:") && first != "localhost") {
		if !hasSlash {
			name = "library/" + name
		}
		name = "docker.io/" + name
	}
	if hasDigest {
		// The runtime reports a pinned image by digest, without its tag.
		if slash, colon := strings.LastIndex(name, "/"), strings.LastIndex(name, ":"); colon > slash {
			name = name[:colon]
		}
		return name + "@" + digest
	}
	if last := name[strings.LastIndex(name, "/")+1:]; !strings.Contains(last, ":") {
		name += ":latest"
	}
	return name
}
