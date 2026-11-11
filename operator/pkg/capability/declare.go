// SPDX-License-Identifier: Apache-2.0

package capability

import v1 "github.com/zenthros/operator/api/v1"

// LifecycleOf is the lifecycle an executor declares for a kind it runs.
func LifecycleOf(kind string) string {
	if kind == v1.WorkloadKindRayCluster {
		return Service
	}
	return RunToCompletion
}

// VM is what the VM agent runs: a Job, as a process under systemd and in a
// container when the host has containerd. defaultRuntime is the one a job
// gets when it names none; one the host cannot run falls back to systemd.
func VM(containerd bool, defaultRuntime string) Set {
	runtimes := []string{RuntimeSystemd}
	if containerd {
		runtimes = append(runtimes, RuntimeContainerd)
	}
	if defaultRuntime != RuntimeContainerd || !containerd {
		defaultRuntime = RuntimeSystemd
	}
	return Set{{
		Kind:           v1.WorkloadKindJob,
		Lifecycle:      RunToCompletion,
		Runtimes:       runtimes,
		DefaultRuntime: defaultRuntime,
		Endpoints:      true,
	}}
}

// Container is what the container agent runs: a Job, as its own child
// process inside the container's image. There is no other runtime to choose.
func Container() Set {
	return Set{{
		Kind:           v1.WorkloadKindJob,
		Lifecycle:      RunToCompletion,
		Runtimes:       []string{RuntimeProcess},
		DefaultRuntime: RuntimeProcess,
		Endpoints:      true,
	}}
}

// Runs reports whether w runs runtime. workspace-containers is containerd
// with per-Workspace isolation, so a containerd declaration covers it.
func (w Workload) Runs(runtime string) bool {
	if runtime == v1.RuntimeWorkspaceContainers {
		runtime = RuntimeContainerd
	}
	for _, r := range w.Runtimes {
		if r == runtime {
			return true
		}
	}
	return false
}

// Kubernetes is the declaration for kinds the in-cluster agent found it can
// create.
func Kubernetes(kinds ...string) Set {
	set := make(Set, 0, len(kinds))
	for _, kind := range kinds {
		set = append(set, Workload{
			Kind:           kind,
			Lifecycle:      LifecycleOf(kind),
			Runtimes:       []string{RuntimeKubernetes},
			DefaultRuntime: RuntimeKubernetes,
			// The in-cluster agent reads a Manifest's endpointFrom; a Job
			// as a Service on Kubernetes is not translated yet.
			Endpoints: kind == v1.WorkloadKindManifest,
		})
	}
	return set.Normalize()
}
