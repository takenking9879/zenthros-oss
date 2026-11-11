// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	v1 "github.com/zenthros/operator/api/v1"
)

// Agent job outcomes for zenthros_agent_jobs_total.
const (
	JobSucceeded = "succeeded"
	JobFailed    = "failed"
)

// Executor operations for zenthros_agent_executor_errors_total.
const (
	OpSubmit  = "submit"
	OpLog     = "log"
	OpCleanup = "cleanup"
)

// agentJobSetupBuckets spans a fast systemd unit start (sub-second) to a full
// RayCluster provisioning (minutes).
var agentJobSetupBuckets = []float64{0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}

// agentJobTeardownBuckets spans a unit stop to a RayJob cascade delete.
var agentJobTeardownBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

var (
	agentJobsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_agent_jobs_total",
		Help: "Jobs that reached a terminal phase on this agent, by result.",
	}, []string{"result"})

	agentJobSetup = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_agent_job_setup_seconds",
		Help:    "Time from Submit to the workload observed Running (provisioning overhead, not workload runtime).",
		Buckets: agentJobSetupBuckets,
	})

	agentJobTeardown = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_agent_job_teardown_seconds",
		Help:    "Time from a workload reaching a terminal phase to its resources being cleaned up.",
		Buckets: agentJobTeardownBuckets,
	})

	agentExecutorErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_agent_executor_errors_total",
		Help: "Executor failures by operation (submit, log, cleanup).",
	}, []string{"op"})

	agentReconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_agent_control_reconnects_total",
		Help: "Times the agent redialed the controller after the control stream dropped.",
	})

	agentControlStreamUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_agent_control_stream_up",
		Help: "Whether the control stream to the controller is currently established (1 up, 0 down).",
	})

	agentHeartbeatsSent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_agent_heartbeat_sent_total",
		Help: "Heartbeats successfully sent to the controller.",
	})

	agentLogStreamsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_agent_log_streams_active",
		Help: "Log streams this agent is currently serving to the controller.",
	})

	agentHeartbeatAck = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_agent_heartbeat_ack_seconds",
		Help:    "Time from sending a heartbeat on the control stream to reading its ack. Grows when the stream or the Gateway is congested.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
)

// agentCollectors is every process-wide agent collector, in registration order.
func agentCollectors() []prometheus.Collector {
	collectors := []prometheus.Collector{
		agentJobsTotal,
		agentJobSetup,
		agentJobTeardown,
		agentExecutorErrors,
		agentReconnectsTotal,
		agentControlStreamUp,
		agentHeartbeatsSent,
		agentLogStreamsActive,
		agentHeartbeatAck,
	}
	collectors = append(collectors, statusPollCollectors()...)
	return append(collectors, agentSpoolCollectors()...)
}

// NewAgentRegistry builds the registry the agent serves on its metrics
// endpoint: the platform agent metrics plus the standard Go/process collectors.
// agentType labels the active-jobs gauge (k8s or vm); activeJobCount reports
// the live job count at scrape time, so the gauge never drifts from the tracker.
func NewAgentRegistry(agentType string, activeJobCount func() float64) *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(agentCollectors()...)
	if agentType == v1.BackendKindK8s {
		registry.MustRegister(k8sAPICollectors()...)
	}
	registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        "zenthros_agent_jobs_active",
			Help:        "Jobs currently tracked by this agent.",
			ConstLabels: prometheus.Labels{"type": agentType},
		},
		activeJobCount,
	))
	return registry
}

// RecordAgentJobResult counts a job that reached a terminal phase.
func RecordAgentJobResult(result string) {
	agentJobsTotal.WithLabelValues(result).Inc()
}

// ObserveAgentJobSetup records the time from Submit to the workload Running.
func ObserveAgentJobSetup(elapsed time.Duration) {
	agentJobSetup.Observe(elapsed.Seconds())
}

// ObserveAgentJobTeardown records the time from a terminal phase to cleanup.
func ObserveAgentJobTeardown(elapsed time.Duration) {
	agentJobTeardown.Observe(elapsed.Seconds())
}

// RecordAgentExecutorError counts an executor failure by operation.
func RecordAgentExecutorError(op string) {
	agentExecutorErrors.WithLabelValues(op).Inc()
}

// RecordAgentReconnect counts a redial of the control channel.
func RecordAgentReconnect() { agentReconnectsTotal.Inc() }

// SetAgentControlStreamUp reflects whether the control stream is established.
func SetAgentControlStreamUp(up bool) {
	if up {
		agentControlStreamUp.Set(1)
		return
	}
	agentControlStreamUp.Set(0)
}

// RecordAgentHeartbeatSent counts a heartbeat delivered to the controller.
func RecordAgentHeartbeatSent() { agentHeartbeatsSent.Inc() }

// ObserveAgentHeartbeatAck records one heartbeat's send-to-ack time.
func ObserveAgentHeartbeatAck(elapsed time.Duration) { agentHeartbeatAck.Observe(elapsed.Seconds()) }

// AgentLogStreamOpened marks the start of a served log stream.
func AgentLogStreamOpened() { agentLogStreamsActive.Inc() }

// AgentLogStreamClosed marks the end of a served log stream.
func AgentLogStreamClosed() { agentLogStreamsActive.Dec() }
