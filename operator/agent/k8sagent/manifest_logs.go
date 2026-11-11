// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/pkg/backend"
)

// ownerDepth is how far up a pod's owners the agent looks for the object: a
// PyTorchJob owns its pods directly, a batch Job owns them, a Deployment owns
// them through a ReplicaSet, and a JobSet through a Job.
const ownerDepth = 3

// manifestPods are the object's pods, newest first: those the logSelector
// picks, or else those whose owner chain reaches the object within ownerDepth.
func (e *Executor) manifestPods(ctx context.Context, obj *unstructured.Unstructured, c manifestContract) ([]corev1.Pod, error) {
	opts := []client.ListOption{client.InNamespace(obj.GetNamespace())}
	if c.LogSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(c.LogSelector)
		if err != nil {
			return nil, fmt.Errorf("manifest.logSelector: %w", err)
		}
		opts = append(opts, client.MatchingLabelsSelector{Selector: selector})
	}
	var list corev1.PodList
	if err := e.client.List(ctx, &list, opts...); err != nil {
		return nil, fmt.Errorf("list pods of %s/%s: %w", obj.GetKind(), obj.GetName(), err)
	}
	pods := list.Items
	if c.LogSelector == nil {
		owned := ownerChain{e: e, ctx: ctx, namespace: obj.GetNamespace(), root: obj.GetUID(), seen: map[types.UID]bool{}}
		pods = slices.DeleteFunc(pods, func(p corev1.Pod) bool { return !owned.reaches(p.OwnerReferences, ownerDepth) })
	}
	slices.SortFunc(pods, func(a, b corev1.Pod) int { return b.CreationTimestamp.Compare(a.CreationTimestamp.Time) })
	return pods, nil
}

// ownerChain walks ownerReferences up to the root object, reading each owner
// once.
type ownerChain struct {
	e         *Executor
	ctx       context.Context
	namespace string
	root      types.UID
	seen      map[types.UID]bool
}

func (o ownerChain) reaches(refs []metav1.OwnerReference, depth int) bool {
	if depth <= 0 {
		return false
	}
	for _, r := range refs {
		if r.UID == o.root {
			return true
		}
	}
	for _, r := range refs {
		if reached, ok := o.seen[r.UID]; ok {
			if reached {
				return true
			}
			continue
		}
		o.seen[r.UID] = false
		gv, err := schema.ParseGroupVersion(r.APIVersion)
		if err != nil {
			continue
		}
		owner := &unstructured.Unstructured{}
		owner.SetGroupVersionKind(gv.WithKind(r.Kind))
		if err := o.e.client.Get(o.ctx, client.ObjectKey{Namespace: o.namespace, Name: r.Name}, owner); err != nil {
			continue
		}
		if o.reaches(owner.GetOwnerReferences(), depth-1) {
			o.seen[r.UID] = true
			return true
		}
	}
	return false
}

// manifestLogs streams every container of the object's pods, each line
// prefixed [pod/container] so a multi-replica job reads as one log.
func (e *Executor) manifestLogs(ctx context.Context, r ref, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	obj, err := e.getManifest(ctx, r)
	if err != nil {
		return nil, err
	}
	pods, err := e.manifestPods(ctx, obj, readContract(obj))
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("%s has no pods yet", r)
	}
	out := make(chan backend.LogChunk, 64)
	var wg sync.WaitGroup
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			opts := &corev1.PodLogOptions{Follow: follow, Container: container.Name}
			if tailLines >= 0 {
				tl := int64(tailLines)
				opts.TailLines = &tl
			}
			prefix := "[" + pod.Name + "/" + container.Name + "] "
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Each open waits for its own container to start: opened one
				// after another, eight pods still creating held every line
				// back for eight graces (06#7).
				stream, err := e.openLogStream(ctx, pod.Namespace, pod.Name, opts)
				if err != nil {
					// One container that has not started does not hide the others.
					e.logger.V(1).Info("skipping container log", "pod", pod.Name, "container", container.Name, "error", err.Error())
					return
				}
				defer stream.Close()
				scanner := backend.LogLines(stream)
				for scanner.Scan() {
					chunk := backend.LogChunk{Stream: "stdout", Data: []byte(prefix + scanner.Text() + "\n"), Timestamp: time.Now()}
					select {
					case out <- chunk:
					case <-ctx.Done():
						return
					}
				}
			}()
		}
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}
