// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// Provisioner operational metrics. Labels are low-cardinality:
// operation/result/kind/component only. Instance IDs and request UIDs stay
// in Events and audit annotations.
var (
	provisionerOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_provisioner_operations_total",
		Help: "Provisioner mutations and repair actions by operation and result.",
	}, []string{"operation", "result", "component"})

	provisionerOrphans = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "zenthros_provisioner_orphans",
		Help: "Current orphan observations by kind and phase.",
	}, []string{"kind", "phase"})

	provisionerCatalogAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_provisioner_catalog_age_seconds",
		Help: "Age of the canonical Catalog snapshot as observed by the Provisioner.",
	})
)

func provisionerCollectors() []prometheus.Collector {
	return []prometheus.Collector{provisionerOperations, provisionerOrphans, provisionerCatalogAge}
}

func IncProvisionerOperation(operation, result, component string) {
	provisionerOperations.WithLabelValues(operation, result, component).Inc()
}

func SetProvisionerOrphans(kind, phase string, value float64) {
	provisionerOrphans.WithLabelValues(kind, phase).Set(value)
}

func ResetProvisionerOrphans() {
	provisionerOrphans.Reset()
}

func SetProvisionerCatalogAge(seconds float64) {
	provisionerCatalogAge.Set(seconds)
}
