// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"os/exec"
)

const (
	runtimeClassNone                = "none"
	runtimeClassContainerd          = "containerd"
	runtimeClassWorkspaceContainers = "workspace-containers"
)

// probeRuntimeClass reports whether containerd/ctr is available on this host.
// workspace-containers is the same observed class as containerd; the operator
// records intent for multi-Workspace separately on BackendAccess.
func probeRuntimeClass() string {
	if _, err := exec.LookPath("ctr"); err != nil {
		return runtimeClassNone
	}
	out, err := exec.Command("systemctl", "is-active", "containerd").Output()
	if err != nil {
		return runtimeClassNone
	}
	if string(out) != "active\n" && string(out) != "active" {
		return runtimeClassNone
	}
	return runtimeClassContainerd
}
