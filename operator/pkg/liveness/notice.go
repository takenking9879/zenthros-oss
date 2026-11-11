// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"time"

	"github.com/zenthros/operator/pkg/infrastructure/provider/traits"
)

// NoticeContainerSIGTERM is a container agent's SIGTERM: whoever runs the
// container is stopping it. A cloud's own notices are the cloud's
// (traits.DrainCap).
const NoticeContainerSIGTERM = "container-sigterm"

// NoticeLifetime is the provisioner's own notice: the machine reached its
// pool's spec.scaling.maxLifetime (CapacityRequest status.lifetimeEndedAt).
const NoticeLifetime = "machine-lifetime"

// How long an interrupted worker may still commit checkpoints after a notice
// that no cloud declares a cap for.
const (
	DefaultDrainCap = 25 * time.Second
	// ContainerDrainCap bounds a container's window. The agent sets the
	// notice's deadline from its own grace (ZENTHROS_PREEMPT_GRACE), which
	// is what whoever stops the container allows; this only caps it.
	ContainerDrainCap = 110 * time.Second
)

// Notice is a machine's interruption notice as its agent heard it.
type Notice struct {
	// Source is a cloud's (traits.NoticeAWSIMDS, ...) or
	// NoticeContainerSIGTERM.
	Source string
	// Action is what the provider will do (terminate, stop, hibernate, preempted).
	Action string
	// Deadline is when the provider acts; zero when it does not say.
	Deadline time.Time
}

// DrainUntil is the end of the worker's window to commit checkpoints: the
// provider's own deadline, never later than its source's cap from now: the
// cap its cloud declares (traits.DrainCap), a container's, or the default.
func DrainUntil(n Notice, now time.Time) time.Time {
	limit, declared := traits.DrainCap(n.Source)
	switch {
	case declared:
	case n.Source == NoticeContainerSIGTERM:
		limit = ContainerDrainCap
	default:
		limit = DefaultDrainCap
	}
	until := now.Add(limit)
	if !n.Deadline.IsZero() && n.Deadline.Before(until) {
		until = n.Deadline
	}
	if until.Before(now) {
		return now
	}
	return until
}
