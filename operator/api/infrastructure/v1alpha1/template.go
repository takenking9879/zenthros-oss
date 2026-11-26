// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"strconv"
	"time"
)

// DefaultIdleTTL is how long a pool's machine past its minReady stays with
// no job when the pool names no idleTTL.
const DefaultIdleTTL = 10 * time.Minute

// IdleAfter is how long a machine of the pool past its minReady stays with
// no job before the capacity manager drains it: its idleTTL, or
// DefaultIdleTTL.
func (p *VMPool) IdleAfter() time.Duration {
	if p.Spec.Scaling.IdleTTL != nil {
		return p.Spec.Scaling.IdleTTL.Duration
	}
	return DefaultIdleTTL
}

// A template is a VMPool an administrator labels zenthros.ai/template=true:
// what a Workspace's provisioner may make machines from (zenthros.create).
// A Workspace's machines of it are of two kinds: pools of one that
// zenthros-api makes, labelled with their Workspace, name and template (the
// chart's admission policy holds it to those), and the slots the template's
// own pool makes for a job sent to the template.
const (
	LabelTemplate    = "zenthros.ai/template"
	LabelMachine     = "zenthros.ai/machine"
	LabelMachineName = "zenthros.ai/machine-name"
	// AnnotationWorkspaceLimit is how many machines of the template one
	// Workspace may have at once, of both kinds; 1 when absent.
	AnnotationWorkspaceLimit = "zenthros.ai/workspace-limit"
)

// IsTemplate reports a template: labelled so, its machines made by Zenthros
// (not attached ones), and no supplier of a cluster's nodes.
func (p *VMPool) IsTemplate() bool {
	return p.Labels[LabelTemplate] == "true" && p.Spec.Ownership != OwnershipExternal && p.Spec.Join == nil
}

// WorkspaceLimit is how many machines of a template one Workspace may have
// at once (AnnotationWorkspaceLimit), 1 when the annotation is absent or no
// count.
func (p *VMPool) WorkspaceLimit() int {
	if n, err := strconv.Atoi(p.Annotations[AnnotationWorkspaceLimit]); err == nil && n >= 0 {
		return n
	}
	return 1
}
