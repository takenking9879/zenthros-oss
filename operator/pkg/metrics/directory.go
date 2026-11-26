// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Steps of a backend directory publish, for
// zenthros_backend_directory_publish_failures_total.
const (
	DirectoryEncode = "encode"
	DirectoryRead   = "read"
	DirectoryCreate = "create"
	DirectoryUpdate = "update"
)

var (
	directoryPublishFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_backend_directory_publish_failures_total",
		Help: "Backend directory publishes that failed, by step (encode, read, create, update).",
	}, []string{"step"})

	directoryPublished = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_backend_directory_last_published_timestamp_seconds",
		Help: "When the Controller last confirmed the backend directory current (written, or unchanged). The Scheduler ignores backends whose heartbeat in it is older than BackendMaxAge.",
	})

	directoryBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_backend_directory_bytes",
		Help: "Size of the last backend directory the Controller encoded, as stored (gzipped). A ConfigMap holds at most 1 MiB.",
	})
)

func directoryCollectors() []prometheus.Collector {
	return []prometheus.Collector{directoryPublishFailures, directoryPublished, directoryBytes}
}

// RecordDirectoryPublishFailure counts a backend directory publish that
// failed at step.
func RecordDirectoryPublishFailure(step string) {
	directoryPublishFailures.WithLabelValues(step).Inc()
}

// RecordDirectoryPublished records a backend directory confirmed current at
// at, encoded in size bytes.
func RecordDirectoryPublished(at time.Time, size int) {
	directoryPublished.Set(float64(at.Unix()))
	directoryBytes.Set(float64(size))
}
