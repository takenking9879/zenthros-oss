// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Agent spool ingest results (A-19). persist_seconds / spool_insert_seconds stay out.
const (
	SpoolIngestAccepted        = "accepted"
	SpoolIngestDuplicate       = "duplicate"
	SpoolIngestInvalid         = "invalid"
	SpoolIngestPayloadTooLarge = "payload_too_large"
	SpoolIngestFull            = "spool_full"
)

var (
	agentSpoolPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_agent_report_spool_pending",
		Help: "Unacked ExecutionReports currently in the Agent local spool.",
	})

	agentSpoolPendingBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_agent_report_spool_pending_bytes",
		Help: "Payload bytes of unacked ExecutionReports in the Agent local spool.",
	})

	agentSpoolOldestAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_agent_report_spool_oldest_age_seconds",
		Help: "Age of the oldest unacked ExecutionReport in the Agent local spool.",
	})

	agentSpoolIngest = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_agent_report_spool_ingest_total",
		Help: "Local ExecutionReport ingest results (accepted, duplicate, invalid, payload_too_large, spool_full).",
	}, []string{"result"})

	agentReportDelivery = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "zenthros_agent_report_delivery_seconds",
		Help: "Time from spool ingest to Controller ACK for one ExecutionReport.",
		// 0.2: the ack objective (p99 under 200 ms) is a bucket edge.
		Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.25, 0.5, 1, 2.5, 5, 15, 30, 60, 120},
	})
)

func agentSpoolCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		agentSpoolPending,
		agentSpoolPendingBytes,
		agentSpoolOldestAge,
		agentSpoolIngest,
		agentReportDelivery,
	}
}

// SetAgentSpoolPending updates the backlog gauges after a spool mutation.
func SetAgentSpoolPending(count int, bytes int64, oldestAge time.Duration) {
	agentSpoolPending.Set(float64(count))
	agentSpoolPendingBytes.Set(float64(bytes))
	if count == 0 {
		agentSpoolOldestAge.Set(0)
		return
	}
	agentSpoolOldestAge.Set(oldestAge.Seconds())
}

// RecordAgentSpoolIngest counts one local ingest result.
func RecordAgentSpoolIngest(result string) {
	agentSpoolIngest.WithLabelValues(result).Inc()
}

// ObserveAgentReportDelivery records spool-ingest to Controller-ACK latency.
func ObserveAgentReportDelivery(elapsed time.Duration) {
	if elapsed < 0 {
		return
	}
	agentReportDelivery.Observe(elapsed.Seconds())
}
