// SPDX-License-Identifier: Apache-2.0

// Package metrics declares the Zenthros platform metrics and the
// helpers the controller and agent use to record them. Workload metrics (Ray,
// GPU, custom Python) are deliberately absent: they stay in the environment
// that runs the workload and are scraped there, never by the platform.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	v1 "github.com/zenthros/operator/api/v1"
)

// Submission outcomes for zenthros_submissions_total.
const (
	SubmitAccepted = "accepted"
	SubmitRejected = "rejected"
	SubmitFailed   = "failed"
)

// Enrollment outcomes for zenthros_enrollment_total.
const (
	EnrollSuccess = "success"
	EnrollFailed  = "failed"
)

// submissionLatencyBuckets spans an instant reconcile to minutes of backoff:
// a healthy submission lands well under a second, while a job retried under
// transient backpressure can take several minutes to reach a backend.
var submissionLatencyBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// controlStreamDurationBuckets spans a flapping reconnect (seconds) to a stable
// agent connection (days).
var controlStreamDurationBuckets = []float64{1, 5, 15, 30, 60, 300, 900, 3600, 21600, 86400}

var (
	submissionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_submissions_total",
		Help: "Backend submissions by target backend and outcome (accepted, rejected by policy/capacity, failed with error).",
	}, []string{"backend", "result"})

	submissionLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_submission_latency_seconds",
		Help:    "Time from ZenthrosJob creation to its first successful backend submission.",
		Buckets: submissionLatencyBuckets,
	})

	activeLogStreams = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_active_log_streams",
		Help: "Log streams currently proxied from agents to zctl.",
	})

	statusUpdatesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_status_updates_total",
		Help: "Agent-pushed status updates received, by reported phase.",
	}, []string{"phase"})

	reconcileDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_reconcile_duration_seconds",
		Help:    "Duration of one ZenthrosJob reconcile pass.",
		Buckets: prometheus.DefBuckets,
	})

	reconcileErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_reconcile_errors_total",
		Help: "Reconcile passes that returned an error.",
	})

	controlStreamDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_grpc_stream_duration_seconds",
		Help:    "Lifetime of an agent control stream, from accepted registration to close.",
		Buckets: controlStreamDurationBuckets,
	})

	enrollmentTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_enrollment_total",
		Help: "Agent enrollment (Enroll RPC) attempts by outcome.",
	}, []string{"result"})
)

// controllerCollectors is every event-style controller collector, in registration
// order. Fleet-state gauges (backend health, active jobs) and the job index's
// full scans (hubmetrics) are built at startup and passed to
// RegisterController alongside these.
func controllerCollectors() []prometheus.Collector {
	collectors := []prometheus.Collector{
		submissionsTotal,
		submissionLatency,
		statusUpdatesTotal,
		reconcileDuration,
		reconcileErrorsTotal,
		gatewayReplicasExcluded,
	}
	collectors = append(collectors, reportHubCollectors()...)
	collectors = append(collectors, lifecycleCollectors()...)
	collectors = append(collectors, directoryCollectors()...)
	collectors = append(collectors, grpcCollectors()...)
	return append(collectors, capacityCollectors()...)
}

// gatewayCollectors is every series incremented in the Gateway process.
// Channel, enrollment, stream lifetime, log-proxy gauges, and report ACKs
// live here so Prometheus scrapes the process that actually Inc()s them.
func gatewayCollectors() []prometheus.Collector {
	collectors := []prometheus.Collector{
		activeLogStreams,
		controlStreamDuration,
		enrollmentTotal,
		agentSessionsRefused,
		gatewayLogChannels,
		gatewayLogChannelsRefused,
		gatewayLogStreamsRefused,
		gatewayLogPullsBehind,
		gatewayLogShed,
		gatewayLogChannelOpen,
		gatewayLogBytes,
	}
	collectors = append(collectors, channelCollectors()...)
	collectors = append(collectors, grpcCollectors()...)
	return append(collectors, reportGatewayCollectors()...)
}

// RegisterGateway registers collectors that increment in the Gateway process.
func RegisterGateway(reg prometheus.Registerer, collectors ...prometheus.Collector) error {
	for _, collector := range append(gatewayCollectors(), collectors...) {
		if err := reg.Register(collector); err != nil {
			return err
		}
	}
	return nil
}

// RegisterController registers all controller metrics with reg — in production
// the manager's metrics registry, served on the --metrics-bind-address endpoint.
// Extra collectors (backend health, active jobs) are registered with the event
// metrics in one call so a partial registration cannot leave a half-visible set.
func RegisterController(reg prometheus.Registerer, collectors ...prometheus.Collector) error {
	for _, collector := range append(controllerCollectors(), collectors...) {
		if err := reg.Register(collector); err != nil {
			return err
		}
	}
	return nil
}

// RecordSubmission counts one backend submission attempt for a target backend.
func RecordSubmission(backendID, result string) {
	submissionsTotal.WithLabelValues(backendID, result).Inc()
}

// ObserveSubmissionLatency records the time from CRD creation to the first
// successful submission of a job.
func ObserveSubmissionLatency(elapsed time.Duration) {
	submissionLatency.Observe(elapsed.Seconds())
}

// LogStreamOpened marks the start of a proxied log stream.
func LogStreamOpened() { activeLogStreams.Inc() }

// LogStreamClosed marks the end of a proxied log stream.
func LogStreamClosed() { activeLogStreams.Dec() }

// RecordStatusUpdate counts an agent-pushed status update by phase.
func RecordStatusUpdate(phase string) {
	statusUpdatesTotal.WithLabelValues(knownPhase(phase)).Inc()
}

// knownPhase is phase as a label: it comes from a machine, and each new label
// is a series kept for good.
func knownPhase(phase string) string {
	switch v1.Phase(phase) {
	case v1.PhasePending, v1.PhaseSubmitted, v1.PhaseRunning, v1.PhaseSucceeded, v1.PhaseFailed, v1.PhaseUnschedulable, v1.PhaseCancelled:
		return phase
	}
	return "other"
}

// ObserveReconcileDuration records how long one reconcile pass took.
func ObserveReconcileDuration(elapsed time.Duration) {
	reconcileDuration.Observe(elapsed.Seconds())
}

// RecordReconcileError counts a reconcile pass that returned an error.
func RecordReconcileError() { reconcileErrorsTotal.Inc() }

// ObserveControlStreamDuration records the lifetime of a closed agent stream.
func ObserveControlStreamDuration(elapsed time.Duration) {
	controlStreamDuration.Observe(elapsed.Seconds())
}

// RecordEnrollment counts an Enroll RPC outcome.
func RecordEnrollment(result string) {
	enrollmentTotal.WithLabelValues(result).Inc()
}
