// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// k8s API call results. No job-id label (A-22).
const (
	K8sAPIOk    = "ok"
	K8sAPIError = "error"
)

var (
	statusPollSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_agent_status_poll_seconds",
		Help:    "Duration of one StatusWatcher sweep over the agent's active jobs.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	})

	statusPollTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_agent_status_poll_total",
		Help: "StatusWatcher sweeps completed on this agent.",
	})

	statusPollErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_agent_status_poll_errors_total",
		Help: "Per-job Status() failures during a StatusWatcher sweep.",
	})

	k8sAPIRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_agent_k8s_api_requests_total",
		Help: "Kubernetes API calls this k8s-agent made during status polling, by result.",
	}, []string{"result"})

	k8sAPIRequestSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_agent_k8s_api_request_seconds",
		Help:    "Kubernetes API call duration on this k8s-agent during status polling.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	})
)

func statusPollCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		statusPollSeconds,
		statusPollTotal,
		statusPollErrors,
	}
}

func k8sAPICollectors() []prometheus.Collector {
	return []prometheus.Collector{
		k8sAPIRequests,
		k8sAPIRequestSeconds,
	}
}

// ObserveStatusPoll records one completed StatusWatcher sweep.
func ObserveStatusPoll(elapsed time.Duration, pollErrors int) {
	statusPollSeconds.Observe(elapsed.Seconds())
	statusPollTotal.Inc()
	if pollErrors > 0 {
		statusPollErrors.Add(float64(pollErrors))
	}
}

// RecordK8sAPI records one destination-cluster API call. Only the k8s agent
// registry scrapes these series; VM agents never register them.
func RecordK8sAPI(err error, elapsed time.Duration) {
	result := K8sAPIOk
	if err != nil {
		result = K8sAPIError
	}
	k8sAPIRequests.WithLabelValues(result).Inc()
	k8sAPIRequestSeconds.Observe(elapsed.Seconds())
}
