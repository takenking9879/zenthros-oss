// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend/k8serr"
	"github.com/zenthros/operator/pkg/backend/k8spod"
)

const (
	// LabelPrewarm names the Prewarm a DaemonSet or Job warms for.
	LabelPrewarm = "zenthros.ai/prewarm"
	// annotationPrewarmKey is the Prewarm generation a DaemonSet was built for.
	annotationPrewarmKey = "zenthros.ai/prewarm-key"
	// prewarmToolsDir is where the init container copies the tools image's
	// static busybox, so every image runs its sleep without a shell of its
	// own.
	prewarmToolsDir = "/zenthros-prewarm"
	// prewarmCacheDir is where an artifact Job mounts the claim.
	prewarmCacheDir = "/cache"
	// defaultToolsImage is a static busybox: its binary runs inside any
	// image. The default busybox variant is dynamically linked.
	defaultToolsImage = "busybox:1.36.1-musl"
)

// Prepare warms the cluster for a Prewarm and reports each item: images
// through a pre-pull DaemonSet on the nodes spec.kubernetes.nodeSelector
// names, artifacts through a Job that writes them to the claim
// spec.kubernetes.pvcName. It returns at once; every call reports progress.
// The kubelet's and the API server's words reach the Prewarm unchanged.
func (e *Executor) Prepare(ctx context.Context, req *agentpb.PrepareRequest) ([]*agentpb.PrepareItemStatus, error) {
	name := prewarmName(req.GetKey())
	if req.GetRelease() {
		return nil, e.releasePrewarm(ctx, name)
	}
	k := req.GetKubernetes()
	var images []*agentpb.PrepareItem
	out := make([]*agentpb.PrepareItemStatus, 0, len(req.GetItems()))
	byRef := map[string]*agentpb.PrepareItemStatus{}
	for _, it := range req.GetItems() {
		s := &agentpb.PrepareItemStatus{Ref: it.GetRef(), Phase: "Pending"}
		out = append(out, s)
		byRef[it.GetRef()] = s
		switch {
		case it.GetImage() != "":
			images = append(images, it)
		case it.GetArtifactUri() != "":
			e.prepareArtifact(ctx, name, k, it, s)
		case it.GetToolchainName() != "":
			s.Phase, s.Message = "Failed", "toolchains are not warmed on a Kubernetes backend; put them in the job's image"
		default:
			s.Phase, s.Message = "Failed", fmt.Sprintf("item %s has nothing to warm", it.GetRef())
		}
	}
	if len(images) > 0 {
		if err := e.prepareImages(ctx, name, req.GetKey(), k, images, byRef); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// prewarmName is a Prewarm's name from its key (name/generation).
func prewarmName(key string) string {
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		return key[:i]
	}
	return key
}

// prewarmObjectName is a DNS label for an object warming name: readable
// when it fits, a digest suffix keeps it unique when it does not.
func prewarmObjectName(parts ...string) string {
	base := "zenthros-prewarm-" + strings.Join(parts, "-")
	if len(base) <= 52 {
		return base
	}
	sum := sha256.Sum256([]byte(base))
	return base[:43] + "-" + hex.EncodeToString(sum[:])[:8]
}

// labelValue fits a Prewarm name in a label value.
func labelValue(name string) string {
	if len(name) <= 63 {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return name[:54] + "-" + hex.EncodeToString(sum[:])[:8]
}

func toolsImage(k *agentpb.PrepareKubernetes) string {
	if k.GetImage() != "" {
		return k.GetImage()
	}
	return defaultToolsImage
}

// PrewarmDaemonSet pulls every image on the nodes nodeSelector names. An
// init container copies the tools image's static busybox into a shared
// volume; each image is then its own container running that busybox's
// sleep, so an image with no shell warms too, and one image the node cannot
// pull does not hold back the others.
func PrewarmDaemonSet(namespace, name, key string, k *agentpb.PrepareKubernetes, images []string) *appsv1.DaemonSet {
	labels := map[string]string{LabelPrewarm: labelValue(name), "app.kubernetes.io/managed-by": "zenthros-agent"}
	tools := corev1.VolumeMount{Name: "tools", MountPath: prewarmToolsDir, ReadOnly: true}
	small := corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("1m"), corev1.ResourceMemory: resource.MustParse("8Mi")}}
	containers := make([]corev1.Container, 0, len(images))
	for i, image := range images {
		containers = append(containers, corev1.Container{
			Name: fmt.Sprintf("image-%d", i), Image: image, ImagePullPolicy: corev1.PullIfNotPresent,
			Command: []string{prewarmToolsDir + "/sleep", "2147483647"}, VolumeMounts: []corev1.VolumeMount{tools}, Resources: small,
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		})
	}
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: prewarmObjectName(name), Namespace: namespace, Labels: labels,
			Annotations: map[string]string{annotationPrewarmKey: key}},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{LabelPrewarm: labelValue(name)}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					NodeSelector: k.GetNodeSelector(),
					InitContainers: []corev1.Container{{
						Name: "tools", Image: toolsImage(k), ImagePullPolicy: corev1.PullIfNotPresent,
						Command:      []string{"sh", "-c", "cp /bin/busybox " + prewarmToolsDir + "/sleep"},
						VolumeMounts: []corev1.VolumeMount{{Name: "tools", MountPath: prewarmToolsDir}}, Resources: small,
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
					}},
					Containers:                    containers,
					Volumes:                       []corev1.Volume{{Name: "tools", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
					TerminationGracePeriodSeconds: ptrTo(int64(0)),
				},
			},
		},
	}
}

func ptrTo[T any](v T) *T { return &v }

// prepareImages creates or updates the Prewarm's DaemonSet and reports each
// image as imagePhase reads it.
func (e *Executor) prepareImages(ctx context.Context, name, key string, k *agentpb.PrepareKubernetes, items []*agentpb.PrepareItem, byRef map[string]*agentpb.PrepareItemStatus) error {
	images := make([]string, len(items))
	for i, it := range items {
		images[i] = it.GetImage()
	}
	want := PrewarmDaemonSet(e.Namespace, name, key, k, images)
	ds := &appsv1.DaemonSet{}
	switch err := e.client.Get(ctx, client.ObjectKeyFromObject(want), ds); {
	case apierrors.IsNotFound(err):
		if err := e.client.Create(ctx, want); err != nil {
			return k8serr.Classify(err)
		}
		ds = want
	case err != nil:
		return k8serr.Classify(err)
	case ds.Annotations[annotationPrewarmKey] != key:
		ds.Annotations = want.Annotations
		ds.Spec.Template = want.Spec.Template
		if err := e.client.Update(ctx, ds); err != nil {
			return k8serr.Classify(err)
		}
	}
	var pods corev1.PodList
	if err := e.client.List(ctx, &pods, client.InNamespace(e.Namespace), client.MatchingLabels{LabelPrewarm: labelValue(name)}); err != nil {
		return k8serr.Classify(err)
	}
	st := ds.Status
	// An API server sets generation 1 on create; until the DaemonSet controller
	// observed it, nothing is known about the nodes.
	settled := ds.Generation != 0 && st.ObservedGeneration == ds.Generation && st.UpdatedNumberScheduled == st.DesiredNumberScheduled
	for i, it := range items {
		s := byRef[it.GetRef()]
		s.Phase, s.Message = imagePhase(fmt.Sprintf("image-%d", i), it.GetImage(), pods.Items, settled, st.DesiredNumberScheduled, k)
		if s.Phase == "Ready" {
			e.noteWarm(it.GetImage())
		}
	}
	return nil
}

// noteWarm records an image every targeted node holds, for Declare.
func (e *Executor) noteWarm(image string) {
	e.warmMu.Lock()
	defer e.warmMu.Unlock()
	if e.warm == nil {
		e.warm = map[string]bool{}
	}
	e.warm[image] = true
}

// warmImages is what noteWarm recorded, sorted.
func (e *Executor) warmImages() []string {
	e.warmMu.Lock()
	defer e.warmMu.Unlock()
	out := make([]string, 0, len(e.warm))
	for image := range e.warm {
		out = append(out, image)
	}
	sort.Strings(out)
	return out
}

// imagePhase is one image across the DaemonSet's pods: Ready once its
// container runs on every scheduled pod; Failed, with the kubelet's reason
// and message, when a node cannot pull it or its container cannot run.
func imagePhase(container, image string, pods []corev1.Pod, settled bool, desired int32, k *agentpb.PrepareKubernetes) (string, string) {
	if settled && desired == 0 {
		return "Failed", "no node matches nodeSelector " + formatSelector(k.GetNodeSelector())
	}
	var failures []string
	done := int32(0)
	for i := range pods {
		pod := &pods[i]
		where := fmt.Sprintf("pod %s on node %s", pod.Name, pod.Spec.NodeName)
		for _, cs := range pod.Status.InitContainerStatuses {
			if w := cs.State.Waiting; w != nil && k8spod.PullFailed(w.Reason) {
				failures = append(failures, fmt.Sprintf("%s: tools image %s: %s: %s", where, cs.Image, w.Reason, w.Message))
			}
			if t := cs.LastTerminationState.Terminated; t != nil && t.ExitCode != 0 {
				failures = append(failures, fmt.Sprintf("%s: tools image exited %d: %s", where, t.ExitCode, strings.TrimSpace(t.Reason+": "+t.Message)))
			}
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != container {
				continue
			}
			switch {
			case cs.State.Running != nil:
				done++
			case cs.State.Waiting != nil && k8spod.PullFailed(cs.State.Waiting.Reason):
				failures = append(failures, fmt.Sprintf("%s: %s: %s", where, cs.State.Waiting.Reason, cs.State.Waiting.Message))
			case cs.LastTerminationState.Terminated != nil:
				t := cs.LastTerminationState.Terminated
				failures = append(failures, fmt.Sprintf("%s: %s exited %d: %s", where, image, t.ExitCode, strings.TrimSpace(t.Reason+": "+strings.TrimSpace(t.Message))))
			case cs.State.Waiting != nil && cs.State.Waiting.Reason != "ContainerCreating" && cs.State.Waiting.Reason != "PodInitializing":
				failures = append(failures, fmt.Sprintf("%s: %s: %s", where, cs.State.Waiting.Reason, cs.State.Waiting.Message))
			}
		}
	}
	switch {
	case len(failures) > 0:
		sort.Strings(failures)
		return "Failed", strings.Join(slices.Compact(failures), "; ")
	case settled && done >= desired:
		return "Ready", ""
	}
	return "Running", ""
}

func formatSelector(sel map[string]string) string {
	if len(sel) == 0 {
		return "{} (every node)"
	}
	keys := make([]string, 0, len(sel))
	for k := range sel {
		keys = append(keys, k+"="+sel[k])
	}
	sort.Strings(keys)
	return "{" + strings.Join(keys, ",") + "}"
}

// PrewarmArtifactJob writes one artifact to the claim: downloaded once by
// its digest, verified, and hard-linked at paths/<path>, so the name
// resolves wherever a job mounts the claim.
func PrewarmArtifactJob(namespace, name string, k *agentpb.PrepareKubernetes, it *agentpb.PrepareItem) *batchv1.Job {
	sum := sha256.Sum256([]byte(it.GetArtifactSha256() + "/" + it.GetArtifactPath()))
	labels := map[string]string{LabelPrewarm: labelValue(name), "app.kubernetes.io/managed-by": "zenthros-agent"}
	script := `set -e
blob="` + prewarmCacheDir + `/sha256/$SHA256"
if [ ! -f "$blob" ]; then
  mkdir -p "` + prewarmCacheDir + `/sha256"
  tmp="$blob.partial"
  wget -q -O "$tmp" "$URI"
  got=$(sha256sum "$tmp" | cut -d' ' -f1)
  if [ "$got" != "$SHA256" ]; then
    rm -f "$tmp"
    echo "sha256 mismatch for $SHOWN: got $got, want $SHA256" >&2
    exit 1
  fi
  mv "$tmp" "$blob"
fi
link="` + prewarmCacheDir + `/paths/$ARTIFACT_PATH"
mkdir -p "$(dirname "$link")"
ln -f "$blob" "$link"
`
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: prewarmObjectName(name, hex.EncodeToString(sum[:])[:10]), Namespace: namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptrTo(int32(1)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					NodeSelector:  k.GetNodeSelector(),
					Containers: []corev1.Container{{
						Name: "fetch", Image: toolsImage(k), ImagePullPolicy: corev1.PullIfNotPresent,
						Command: []string{"sh", "-c", script},
						Env: []corev1.EnvVar{{Name: "URI", Value: it.GetArtifactUri()}, {Name: "SHOWN", Value: redactURI(it.GetArtifactUri())},
							{Name: "SHA256", Value: it.GetArtifactSha256()}, {Name: "ARTIFACT_PATH", Value: it.GetArtifactPath()}},
						VolumeMounts:             []corev1.VolumeMount{{Name: "cache", MountPath: prewarmCacheDir}},
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
					}},
					Volumes: []corev1.Volume{{Name: "cache", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: k.GetPvcName()}}}},
				},
			},
		},
	}
}

// prepareArtifact creates the artifact's Job once and reports it: Ready when
// it completed, Failed with the container's own last words.
func (e *Executor) prepareArtifact(ctx context.Context, name string, k *agentpb.PrepareKubernetes, it *agentpb.PrepareItem, s *agentpb.PrepareItemStatus) {
	if k.GetPvcName() == "" {
		s.Phase, s.Message = "Failed", "artifacts on a Kubernetes backend need spec.kubernetes.pvcName, the claim they are written to"
		return
	}
	if strings.Contains(it.GetArtifactPath(), "..") {
		s.Phase, s.Message = "Failed", fmt.Sprintf("artifact path %q must be a relative path without ..", it.GetArtifactPath())
		return
	}
	want := PrewarmArtifactJob(e.Namespace, name, k, it)
	job := &batchv1.Job{}
	switch err := e.client.Get(ctx, client.ObjectKeyFromObject(want), job); {
	case apierrors.IsNotFound(err):
		if err := e.client.Create(ctx, want); err != nil {
			s.Phase, s.Message = "Failed", redactIn(k8serr.Classify(err).Error(), it.GetArtifactUri())
			return
		}
		s.Phase = "Running"
		return
	case err != nil:
		s.Phase, s.Message = "Failed", k8serr.Classify(err).Error()
		return
	}
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			s.Phase = "Ready"
			return
		case batchv1.JobFailed:
			s.Phase, s.Message = "Failed", redactIn(e.jobPodMessage(ctx, job, c), it.GetArtifactUri())
			return
		}
	}
	s.Phase = "Running"
}

// jobPodMessage is a failed Job's container termination message (its last
// log lines), or the Job condition's own message when no pod says more. Its
// pods are the Job's by its UID: a former Job of the name may have left some.
func (e *Executor) jobPodMessage(ctx context.Context, job *batchv1.Job, cond batchv1.JobCondition) string {
	var pods corev1.PodList
	if err := e.client.List(ctx, &pods, client.InNamespace(job.Namespace), client.MatchingLabels{batchv1.ControllerUidLabel: string(job.UID)}); err == nil {
		for _, pod := range pods.Items {
			for _, cs := range pod.Status.ContainerStatuses {
				if t := cs.State.Terminated; t != nil && t.ExitCode != 0 && strings.TrimSpace(t.Message) != "" {
					return strings.TrimSpace(t.Message)
				}
			}
		}
	}
	return strings.TrimSpace(cond.Reason + ": " + cond.Message)
}

// releasePrewarm removes the Prewarm's DaemonSet and Jobs. What they wrote
// stays: images in the nodes' stores, artifacts in the claim.
func (e *Executor) releasePrewarm(ctx context.Context, name string) error {
	sel := client.MatchingLabels{LabelPrewarm: labelValue(name)}
	background := client.PropagationPolicy(metav1.DeletePropagationBackground)
	var errs []error
	for _, obj := range []client.Object{&appsv1.DaemonSet{}, &batchv1.Job{}} {
		if err := e.client.DeleteAllOf(ctx, obj, client.InNamespace(e.Namespace), sel, background); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, k8serr.Classify(err))
		}
	}
	return errors.Join(errs...)
}

// redactURI is a URL without its query, which may hold a signature.
func redactURI(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<unparseable url>"
	}
	u.RawQuery, u.Fragment, u.User = "", "", nil
	return u.String()
}

// redactIn removes a presigned URL's query from a message.
func redactIn(msg, raw string) string {
	if raw == "" {
		return msg
	}
	return strings.ReplaceAll(msg, raw, redactURI(raw))
}
