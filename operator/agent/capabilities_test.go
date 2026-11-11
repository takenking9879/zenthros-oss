// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"testing"

	"github.com/zenthros/operator/agent/k8sagent"
	"github.com/zenthros/operator/agent/procagent"
)

// What each executor can do beyond Submit, Status, Cancel and Logs is
// written down here, not found out at run time: the agent type-asserts each
// optional interface and does nothing when it is missing, so a method lost
// in a refactor was a feature that silently stopped (the k8s agent's
// restart read 0 jobs: it had no InstalledJobs). A change to a row is a
// decision, with its reason beside it.
func TestExecutorCapabilities(t *testing.T) {
	type caps struct{ installed, ended, preempt, declare, image, prepare, batch bool }
	for name, tc := range map[string]struct {
		executor Executor
		want     caps
	}{
		"systemd": {SystemdExecutor{}, caps{installed: true, ended: true, preempt: true, declare: true, image: true, prepare: true, batch: true}},
		// Its jobs die with the agent (D9): there is nothing to find after a
		// restart. It runs the container's own image and warms nothing.
		"procagent": {&procagent.Executor{}, caps{ended: true, preempt: true, declare: true}},
		// A cluster is no machine with a notice; its Nodes' handoff is the
		// NodeReconciler's. Pods report the digest they pulled in Status.
		"k8sagent": {&k8sagent.Executor{}, caps{installed: true, ended: true, declare: true, prepare: true}},
	} {
		_, installed := tc.executor.(installedLister)
		_, ended := tc.executor.(endedLister)
		_, preempt := tc.executor.(preemptSignaler)
		_, declare := tc.executor.(Declarer)
		_, image := tc.executor.(imageResolver)
		_, prepare := tc.executor.(Preparer)
		_, batch := tc.executor.(batchStatusReader)
		if got := (caps{installed, ended, preempt, declare, image, prepare, batch}); got != tc.want {
			t.Errorf("%s can %+v, want %+v", name, got, tc.want)
		}
	}
}
