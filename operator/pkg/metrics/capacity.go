// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Managed-capacity lifecycle durations. Every value is
// derived from a persisted timestamp pair at the moment the pair completes, so
// the histograms are aggregate timing evidence only: scheduling never consumes
// them and the CRD statuses remain the sole persisted timing model.
var capacityDurationBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 900, 1200, 1800}

var (
	capacityProvisioningDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_capacity_provisioning_duration_seconds",
		Help:    "Provider instance creation to first boot observation (provisioningAt -> bootingAt).",
		Buckets: capacityDurationBuckets,
	})

	capacityTimeToReady = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_capacity_time_to_ready_seconds",
		Help:    "Capacity request to enrolled-and-ready backend (requestedAt -> readyAt).",
		Buckets: capacityDurationBuckets,
	})

	capacityWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_capacity_wait_seconds",
		Help:    "Time a poolRef job waits from creation until its workload is submitted.",
		Buckets: capacityDurationBuckets,
	})

	capacityReservationHold = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_capacity_reservation_hold_seconds",
		Help:    "How long a workload holds its reserved machine (reservedAt -> releasedAt).",
		Buckets: capacityDurationBuckets,
	})

	capacityCleanupDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_capacity_cleanup_duration_seconds",
		Help:    "Termination start to provider termination completion (terminatingAt -> terminatedAt).",
		Buckets: capacityDurationBuckets,
	})

	capacityEndToEnd = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_capacity_end_to_end_seconds",
		Help:    "Full infrastructure attempt lifetime (requestedAt -> terminatedAt).",
		Buckets: capacityDurationBuckets,
	})
)

// capacityCollectors is the registration-order list appended to the controller
// event metrics.
func capacityCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		capacityProvisioningDuration,
		capacityTimeToReady,
		capacityWait,
		capacityReservationHold,
		capacityCleanupDuration,
		capacityEndToEnd,
	}
}

// RegisterProvisioner exposes the persisted infrastructure lifecycle timing
// metrics from the process that records those transitions.
func RegisterProvisioner(reg prometheus.Registerer) error {
	for _, collector := range capacityCollectors() {
		if err := reg.Register(collector); err != nil {
			return err
		}
	}
	for _, collector := range provisionerCollectors() {
		if err := reg.Register(collector); err != nil {
			return err
		}
	}
	return nil
}

// ObserveCapacityProvisioning records provisioningAt -> bootingAt.
func ObserveCapacityProvisioning(elapsed time.Duration) {
	capacityProvisioningDuration.Observe(elapsed.Seconds())
}

// ObserveCapacityTimeToReady records requestedAt -> readyAt.
func ObserveCapacityTimeToReady(elapsed time.Duration) {
	capacityTimeToReady.Observe(elapsed.Seconds())
}

// ObserveCapacityWait records the job-side wait from creation to submission.
func ObserveCapacityWait(elapsed time.Duration) {
	capacityWait.Observe(elapsed.Seconds())
}

// ObserveCapacityReservationHold records reservedAt -> releasedAt.
func ObserveCapacityReservationHold(elapsed time.Duration) {
	capacityReservationHold.Observe(elapsed.Seconds())
}

// ObserveCapacityCleanup records terminatingAt -> terminatedAt.
func ObserveCapacityCleanup(elapsed time.Duration) {
	capacityCleanupDuration.Observe(elapsed.Seconds())
}

// ObserveCapacityEndToEnd records requestedAt -> terminatedAt.
func ObserveCapacityEndToEnd(elapsed time.Duration) {
	capacityEndToEnd.Observe(elapsed.Seconds())
}
