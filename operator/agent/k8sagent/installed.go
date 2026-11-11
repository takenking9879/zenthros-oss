// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/owner"
)

// InstalledJobs lists the workloads of this backend of every fixed kind that
// have not ended, so an agent that restarts tracks the work that outlived it and
// its first heartbeat does not report an idle cluster. Another agent's in the
// same cluster are not its own to track or to stop.
func (e *Executor) InstalledJobs(ctx context.Context, backendID string) ([]backend.InstalledJob, error) {
	return e.workloads(ctx, backendID, false)
}

// EndedJobs lists the workloads of this backend of every fixed kind that ended:
// each keeps its pods and Secrets until its Cancel.
func (e *Executor) EndedJobs(ctx context.Context, backendID string) ([]backend.InstalledJob, error) {
	return e.workloads(ctx, backendID, true)
}

// workloads lists this backend's workloads that have ended, or not. A kind the
// cluster does not serve has none. A Manifest is of whatever kind its user
// wrote: the agent cannot list every kind, and the controller's Status reads it.
// A kind it could not list or read (custom RBAC, an outage) is in the error, and
// the others' workloads are still returned.
func (e *Executor) workloads(ctx context.Context, backendID string, ended bool) ([]backend.InstalledJob, error) {
	var jobs []backend.InstalledJob
	var errs []error
	for _, t := range translators {
		if t.resource.Empty() {
			continue
		}
		gvk, err := apiutil.GVKForObject(t.object(ref{}), e.client.Scheme())
		if err != nil {
			return nil, err
		}
		var installed metav1.PartialObjectMetadataList
		installed.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
		if err := e.client.List(ctx, &installed, client.MatchingLabels{owner.LabelBackendID: backendID}, client.InNamespace(e.discovery.namespace)); err != nil {
			if !meta.IsNoMatchError(err) {
				errs = append(errs, fmt.Errorf("list installed %s: %w", t.kind, err))
			}
			continue
		}
		for _, item := range installed.Items {
			r := ref{namespace: item.Namespace, name: item.Name}
			_, state, err := t.read(e, ctx, e.client, r)
			if err != nil {
				errs = append(errs, fmt.Errorf("read installed %s %s/%s: %w", t.kind, item.Namespace, item.Name, err))
				continue
			}
			if state.Phase.Terminal() != ended {
				continue
			}
			jobs = append(jobs, backend.InstalledJob{JobID: item.Labels[owner.LabelJobID], Handle: t.handle(item.Namespace, item.Name)})
		}
	}
	return jobs, errors.Join(errs...)
}
