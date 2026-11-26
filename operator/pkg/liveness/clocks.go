// SPDX-License-Identifier: Apache-2.0

// Package liveness holds the worker-lost silence clocks. One heartbeat send
// stream; two grace windows. Miss count is derived, not a third knob.
package liveness

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// DefaultHeartbeatInterval is how often the agent sends on the control stream.
	DefaultHeartbeatInterval = 15 * time.Second
	// DefaultJobGrace is the Allocated/running silence window before WORKER_LOST.
	DefaultJobGrace = 45 * time.Second
	// DefaultIdleGrace is the Ready-no-job silence window. Distinct from jobGrace.
	DefaultIdleGrace = 30 * time.Second
)

// MissCount is how many heartbeats fit in grace: ceil(grace / interval).
// It is derived from the two clocks; it is not independently configurable.
func MissCount(grace, interval time.Duration) int {
	if interval <= 0 || grace <= 0 {
		return 0
	}
	n := int((grace + interval - 1) / interval)
	if n < 1 {
		return 1
	}
	return n
}

// ResolveHeartbeatInterval uses the configured send interval, or the default.
func ResolveHeartbeatInterval(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return DefaultHeartbeatInterval
}

// ResolveJobGrace is the cluster default, then an optional job override.
// A job override shorter than 2×interval is raised to that floor. interval<=0
// skips the floor so tests can inject a short grace without a rebuild.
func ResolveJobGrace(override *metav1.Duration, clusterDefault, interval time.Duration) time.Duration {
	grace := clusterDefault
	if grace <= 0 {
		grace = DefaultJobGrace
	}
	if override == nil {
		return grace
	}
	grace = override.Duration
	if interval > 0 {
		min := 2 * interval
		if grace < min {
			return min
		}
	}
	return grace
}

// MaxBackendJobGraceBeats caps the job grace a backend may ask for, in
// heartbeats: a longer silence window only delays noticing that its jobs
// are lost.
const MaxBackendJobGraceBeats = 10

// ResolveJobGraceOn is ResolveJobGrace for a job on a backend whose link
// needs backendGrace (the grace its host declared, capability.Host.JobGrace;
// 0 for none). A slow or relayed link, a userspace tailnet node over the
// internet, can stall longer than the cluster's default: five heartbeats
// instead of three. A job override still wins. Otherwise the backend's grace
// only lengthens the cluster default, never shortens it, and is capped at
// MaxBackendJobGraceBeats heartbeats.
func ResolveJobGraceOn(override *metav1.Duration, backendGrace, clusterDefault, interval time.Duration) time.Duration {
	grace := ResolveJobGrace(override, clusterDefault, interval)
	if override != nil || backendGrace <= grace {
		return grace
	}
	return min(backendGrace, MaxBackendJobGraceBeats*ResolveHeartbeatInterval(interval))
}

// ResolveIdleGrace is the optional VMPool.spec.scaling.idleGrace, else the
// cluster default (Helm/env). Distinct from IdleTTL (healthy scale-down).
func ResolveIdleGrace(override *metav1.Duration, clusterDefault time.Duration) time.Duration {
	if override != nil && override.Duration > 0 {
		return override.Duration
	}
	if clusterDefault > 0 {
		return clusterDefault
	}
	return DefaultIdleGrace
}
