// SPDX-License-Identifier: Apache-2.0

package capability

import (
	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	v1 "github.com/zenthros/operator/api/v1"
)

// ForPool is what a VMPool's machines run, from its spec: the declaration
// their agents make once they are heard. A Container pool's machines run
// jobs as processes in the pool's image; a VM pool's run containerd only when
// its runtime installs it (the default, empty runtime does not). The
// Scheduler and the controller both read it from here: the Scheduler once
// assumed containerd on every VM pool and decided containerd jobs onto
// machines the controller then refused them on.
func ForPool(spec infrav1alpha1.VMPoolSpec) Set {
	if spec.MachineKind == infrav1alpha1.MachineKindContainer {
		return Container()
	}
	rt := spec.Runtime
	return VM(rt.Container.Engine == v1.RuntimeContainerd, rt.DefaultJobRuntime)
}
