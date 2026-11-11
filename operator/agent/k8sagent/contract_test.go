// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/agent/executortest"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend/k8s"
	"github.com/zenthros/operator/pkg/jobspec"
)

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
		Missing: k8s.JobAgentHandle("tenant-default", "never-submitted"),
	})
}
