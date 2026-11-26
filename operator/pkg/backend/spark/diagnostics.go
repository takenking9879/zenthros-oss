// SPDX-License-Identifier: Apache-2.0

package spark

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AgentHandlePrefix starts the handle an agent issues for a SparkApplication.
const AgentHandlePrefix = "spark:"

// NewApplication is an empty SparkApplication named namespace/name, to read
// into or delete.
func NewApplication(namespace, name string) *unstructured.Unstructured {
	application := &unstructured.Unstructured{}
	application.SetGroupVersionKind(GroupVersionKind)
	application.SetNamespace(namespace)
	application.SetName(name)
	return application
}

// ApplicationPods are the application's driver and executor pods, each
// carrying its job ID: an application of the same name deleted before has
// pods with the same application name (N12).
func ApplicationPods(ctx context.Context, cl client.Client, application *unstructured.Unstructured) []corev1.Pod {
	var pods corev1.PodList
	if err := cl.List(ctx, &pods,
		client.InNamespace(application.GetNamespace()),
		client.MatchingLabels{ApplicationNameLabel: application.GetName(), JobIDLabel: application.GetLabels()[JobIDLabel]},
	); err != nil {
		return nil
	}
	return pods.Items
}
