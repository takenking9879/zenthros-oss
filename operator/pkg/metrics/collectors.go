// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// BackendSnapshot is the registry view the backend collector needs: one entry
// per connected agent with its current health.
type BackendSnapshot struct {
	ID      string
	Healthy bool
}

// BackendCollector serves the fleet-at-a-glance gauges from a live registry
// snapshot at scrape time: zenthros_backend_health per backend and
// zenthros_connected_agents. Pull-based on purpose — health flips between
// heartbeats, so a stored gauge would go stale between registry events.
type BackendCollector struct {
	snapshot    func() []BackendSnapshot
	backendDesc *prometheus.Desc
	agentsDesc  *prometheus.Desc
}

// NewBackendCollector builds a collector over snapshot, which must return the
// current registered backends (wire it to agentreg.Registry.Snapshot).
func NewBackendCollector(snapshot func() []BackendSnapshot) *BackendCollector {
	return &BackendCollector{
		snapshot: snapshot,
		backendDesc: prometheus.NewDesc(
			"zenthros_backend_health",
			"Health of each registered backend (1 healthy, 0 unhealthy).",
			[]string{"backend"}, nil,
		),
		agentsDesc: prometheus.NewDesc(
			"zenthros_connected_agents",
			"Number of agents currently streaming to the controller.",
			nil, nil,
		),
	}
}

// Describe emits the metric descriptors.
func (c *BackendCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.backendDesc
	ch <- c.agentsDesc
}

// Collect snapshots the registry and emits one health gauge per backend.
func (c *BackendCollector) Collect(ch chan<- prometheus.Metric) {
	backends := c.snapshot()
	ch <- prometheus.MustNewConstMetric(c.agentsDesc, prometheus.GaugeValue, float64(len(backends)))
	for _, backend := range backends {
		health := 0.0
		if backend.Healthy {
			health = 1.0
		}
		ch <- prometheus.MustNewConstMetric(c.backendDesc, prometheus.GaugeValue, health, backend.ID)
	}
}
