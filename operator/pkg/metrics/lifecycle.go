// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	v1 "github.com/zenthros/operator/api/v1"
)

const (
	// OwnershipManaged is a managed capacity slot — exclusive and durable.
	OwnershipManaged = "Managed"
)

var pendingDurationBuckets = []float64{0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600}

var (
	pendingSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_controller_pending_seconds",
		Help:    "Time a ZenthrosJob spent in Pending before leaving that phase.",
		Buckets: pendingDurationBuckets,
	})

	backendBusyTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_controller_backend_busy_total",
		Help: "Admissions refused because the target backend was BACKEND_BUSY.",
	})

	backendIdleSeconds = prometheus.NewDesc(
		"zenthros_controller_backend_idle_seconds",
		"Seconds an exclusive durable unit (dedicated VM, managed slot, or rayClusterRef) has been idle. Shared or ephemeral RayClusters are never emitted.",
		[]string{"backend_id"},
		nil,
	)
)

func lifecycleCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		pendingSeconds,
		backendBusyTotal,
	}
}

// ExclusiveIdleUnit is true only for a durable unit whose occupancy has a
// single meaning: a dedicated VM or container, a managed slot, or a rayClusterRef cluster
// (X8: one Zenthros job). Ephemeral per-job RayClusters and shared pools
// pass sharedPool=true and are never exclusive.
func ExclusiveIdleUnit(backendType, ownership string, sharedPool bool) bool {
	if sharedPool {
		return false
	}
	// A dedicated VM or container is one unit. So is a Kubernetes spoke
	// that is a rayClusterRef unit (sharedPool=false after X8); ephemeral
	// and shared RayClusters pass sharedPool.
	if backendType == v1.BackendKindVM || backendType == v1.BackendKindContainer || backendType == v1.BackendKindK8s {
		return true
	}
	return ownership == OwnershipManaged
}

// ObservePending records how long a job stayed in Pending.
func ObservePending(elapsed time.Duration) {
	if elapsed < 0 {
		return
	}
	pendingSeconds.Observe(elapsed.Seconds())
}

// RecordBackendBusy counts one BACKEND_BUSY admission refusal.
func RecordBackendBusy() {
	backendBusyTotal.Inc()
}

// BackendBusyValue is the current BACKEND_BUSY counter (tests).
func BackendBusyValue() float64 {
	return counterValue(backendBusyTotal)
}

// IdleSnapshot is the scrape-time view an IdleCollector needs.
type IdleSnapshot struct {
	ID          string
	Type        string
	Ownership   string
	RunningJobs int
	SharedPool  bool
}

// IdleCollector emits backend_idle_seconds only for exclusive durable units.
// Shared or ephemeral units are omitted so they cannot be mistaken for a
// free pool. Idle start is remembered in-process between scrapes.
type IdleCollector struct {
	snapshot  func() []IdleSnapshot
	mu        sync.Mutex
	idleSince map[string]time.Time
	now       func() time.Time
}

// NewIdleCollector builds a collector over snapshot.
func NewIdleCollector(snapshot func() []IdleSnapshot) *IdleCollector {
	return &IdleCollector{
		snapshot:  snapshot,
		idleSince: make(map[string]time.Time),
		now:       time.Now,
	}
}

// Describe emits the idle metric descriptor.
func (c *IdleCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- backendIdleSeconds
}

// Collect emits idle seconds for exclusive units that currently have no job.
func (c *IdleCollector) Collect(ch chan<- prometheus.Metric) {
	now := c.now()
	seen := make(map[string]bool)
	for _, backend := range c.snapshot() {
		seen[backend.ID] = true
		if !ExclusiveIdleUnit(backend.Type, backend.Ownership, backend.SharedPool) {
			c.forget(backend.ID)
			continue
		}
		if backend.RunningJobs > 0 {
			c.forget(backend.ID)
			continue
		}
		start := c.idleStart(backend.ID, now)
		ch <- prometheus.MustNewConstMetric(backendIdleSeconds, prometheus.GaugeValue, now.Sub(start).Seconds(), backend.ID)
	}
	c.dropUnseen(seen)
}

func (c *IdleCollector) idleStart(id string, now time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if start, ok := c.idleSince[id]; ok {
		return start
	}
	c.idleSince[id] = now
	return now
}

func (c *IdleCollector) forget(id string) {
	c.mu.Lock()
	delete(c.idleSince, id)
	c.mu.Unlock()
}

func (c *IdleCollector) dropUnseen(seen map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.idleSince {
		if !seen[id] {
			delete(c.idleSince, id)
		}
	}
}
