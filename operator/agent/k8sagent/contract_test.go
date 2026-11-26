// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/takenking9879/zenthros-oss/operator/agent/executortest"
	v1 "github.com/takenking9879/zenthros-oss/operator/api/v1"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend/k8s"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend/k8spod"
	"github.com/takenking9879/zenthros-oss/operator/pkg/jobspec"
)

// failedWith fails the Job as its controller does once its pod ended: the
// pod's workload container terminated with exit 137 for reason, the
// kubelet's word ("OOMKilled" when the kernel killed it for memory).
func failedWith(reason string) func(t *testing.T, e executortest.Executor, handle string) {
	return func(t *testing.T, e executortest.Executor, handle string) {
		ctx := context.Background()
		k := e.(*Executor)
		_, r, err := translatorForHandle(handle)
		if err != nil {
			t.Fatal(err)
		}
		var job batchv1.Job
		if err := k.client.Get(ctx, client.ObjectKey{Namespace: r.namespace, Name: r.name}, &job); err != nil {
			t.Fatal(err)
		}
		// The apiserver gives every Job its own selector.
		job.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"controller-uid": "c-1"}}
		if err := k.client.Update(ctx, &job); err != nil {
			t.Fatal(err)
		}
		exited := corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: reason}}
		labels := map[string]string{"controller-uid": "c-1", k8spod.LabelJobID: job.Labels[k8spod.LabelJobID]}
		if err := k.client.Create(ctx, workloadPod(r.name+"-0", labels, corev1.PodFailed, exited, k8s.WorkloadContainerName)); err != nil {
			t.Fatal(err)
		}
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
			Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}
		if err := k.client.Status().Update(ctx, &job); err != nil {
			t.Fatal(err)
		}
	}
}

func TestK8sagentKeepsTheExecutorContract(t *testing.T) {
	executortest.Contract(t, executortest.Harness{
		New: func(t *testing.T) executortest.Executor { return newTestExecutor(t) },
		Spec: jobspec.JobSpec{ID: "uid-contract", Name: "contract", Namespace: "tenant-default", Tenant: "default",
			Kind: v1.WorkloadKindJob, Image: "team/train:1", Command: []string{"sleep", "300"}},
		End: func(t *testing.T, e executortest.Executor, handle string) {
			k := e.(*Executor)
			_, r, err := translatorForHandle(handle)
			if err != nil {
				t.Fatal(err)
			}
			var job batchv1.Job
			if err := k.client.Get(context.Background(), client.ObjectKey{Namespace: r.namespace, Name: r.name}, &job); err != nil {
				t.Fatal(err)
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			if err := k.client.Status().Update(context.Background(), &job); err != nil {
				t.Fatal(err)
			}
		},
		Missing:         k8s.JobAgentHandle("tenant-default", "never-submitted"),
		KilledForMemory: failedWith("OOMKilled"),
		Killed:          failedWith("Error"),
		Confirms:        true,
	})
}
