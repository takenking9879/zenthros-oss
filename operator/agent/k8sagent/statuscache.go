// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"fmt"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/zenthros/operator/pkg/backend/spark"
)

// StatusCacheLabel is on every object and pod the agent makes (the job's
// ID): the status cache holds those and nothing else of the cluster's.
const StatusCacheLabel = "zenthros.ai/job-id"

// statusCacheSync bounds the wait for the cache's first list at start.
const statusCacheSync = time.Minute

// cachedKinds are the objects a status read asks for, by kind: Jobs and pods
// always, and each custom resource the cluster serves (C-11/12).
func cachedKinds() []client.Object {
	application := &unstructured.Unstructured{}
	application.SetGroupVersionKind(spark.GroupVersionKind)
	return []client.Object{&batchv1.Job{}, &corev1.Pod{}, &rayv1.RayJob{}, &rayv1.RayCluster{}, application}
}

// StartStatusCache watches the objects and pods the agent made, for status
// reads that ask no apiserver (Executor.StatusCache). A custom resource the
// cluster does not serve has no informer: its reads go to the apiserver. It
// runs until ctx ends. namespace, on the agent of no hub, is all it may watch:
// its Jobs and pods there.
func StartStatusCache(ctx context.Context, restCfg *rest.Config, scheme *runtime.Scheme, namespace string) (client.Reader, error) {
	made, err := labels.NewRequirement(StatusCacheLabel, selection.Exists, nil)
	if err != nil {
		return nil, err
	}
	selector := labels.NewSelector().Add(*made)
	kinds := cachedKinds()
	var namespaces map[string]cache.Config
	if namespace != "" {
		kinds = kinds[:2]
		namespaces = map[string]cache.Config{namespace: {}}
	}
	httpClient, err := rest.HTTPClientFor(restCfg)
	if err != nil {
		return nil, fmt.Errorf("status cache: %w", err)
	}
	mapper, err := apiutil.NewDynamicRESTMapper(restCfg, httpClient)
	if err != nil {
		return nil, fmt.Errorf("status cache: %w", err)
	}
	if kinds, err = servedKinds(kinds, scheme, mapper); err != nil {
		return nil, fmt.Errorf("status cache: %w", err)
	}
	byObject := map[client.Object]cache.ByObject{}
	for _, object := range kinds {
		byObject[object] = cache.ByObject{Label: selector}
	}
	c, err := cache.New(restCfg, cache.Options{
		HTTPClient:        httpClient,
		Mapper:            mapper,
		Scheme:            scheme,
		ByObject:          byObject,
		DefaultNamespaces: namespaces,
		// Only what the status path reads: anything else asked of this
		// cache is an error, never a new cluster-wide informer.
		ReaderFailOnMissingInformer: true,
	})
	if err != nil {
		return nil, fmt.Errorf("status cache: %w", err)
	}
	for _, object := range kinds {
		if _, err := c.GetInformer(ctx, object); err != nil {
			return nil, fmt.Errorf("status cache informer for %T: %w", object, err)
		}
	}
	go func() { _ = c.Start(ctx) }()
	syncCtx, cancel := context.WithTimeout(ctx, statusCacheSync)
	defer cancel()
	if !c.WaitForCacheSync(syncCtx) {
		return nil, fmt.Errorf("status cache did not sync within %s", statusCacheSync)
	}
	return c, nil
}

// servedKinds is the kinds the cluster serves. Asked for one it does not, the
// cache fails to start, whatever the others.
func servedKinds(kinds []client.Object, scheme *runtime.Scheme, mapper meta.RESTMapper) ([]client.Object, error) {
	served := make([]client.Object, 0, len(kinds))
	for _, object := range kinds {
		if _, err := apiutil.IsObjectNamespaced(object, scheme, mapper); meta.IsNoMatchError(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		served = append(served, object)
	}
	return served, nil
}
