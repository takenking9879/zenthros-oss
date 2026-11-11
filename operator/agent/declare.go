// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/capability"
)

// Declarer is an Executor that says which workload kinds it runs and on what.
// The agent advertises the declaration; the controller admits a job to this
// backend only when its kind is declared. An executor that is not a Declarer
// is advertised with no declaration, and the controller does not filter it.
type Declarer interface {
	Declare(ctx context.Context) (capability.Set, capability.Host)
}

// declaration is what the agent advertises now.
func (a *Agent) declaration(ctx context.Context) (capability.Set, capability.Host, string) {
	declarer, ok := a.executor.(Declarer)
	if !ok {
		return nil, capability.Host{}, ""
	}
	set, host := declarer.Declare(ctx)
	if a.cfg.JobGrace > 0 {
		host.JobGraceSeconds = int32((a.cfg.JobGrace + time.Second - 1) / time.Second)
	}
	return set, host, capability.Hash(set, host)
}

// admitKind refuses a kind the executor did not declare, naming what it runs.
// An executor without a declaration decides for itself.
func (a *Agent) admitKind(ctx context.Context, kind string) error {
	set, _, _ := a.declaration(ctx)
	if set == nil {
		return nil
	}
	if _, ok := set.Find(kind); ok {
		return nil
	}
	return fmt.Errorf("%w: backend %s runs %s; not %q", backend.ErrUnsupportedKind, a.cfg.BackendID, set.String(), kind)
}
