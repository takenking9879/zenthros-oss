// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// ExecutionReport processing outcomes (A-19). Received/bytes increment on
// decode, before validation, job lookup, or Postgres. Outcome is recorded
// after that path finishes — a rejected report still counted as received.
const (
	ReportAccepted         = "accepted"
	ReportRejected         = "rejected"
	ReportNotFound         = "not_found"
	ReportStoreUnavailable = "store_unavailable"
	ReportInternalError    = "internal_error"
)

// ExecutionReport ACK send results. Distinct from processing outcome: this
// is whether the Controller ACK left the stream.
const (
	ReportAckAccepted   = "accepted"
	ReportAckRejected   = "rejected"
	ReportAckRetryable  = "retryable"
	ReportAckSendFailed = "send_failed"
)

var reportHandleBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

var (
	executionReportsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_controller_execution_reports_received_total",
		Help: "ExecutionReports decoded on the control stream, before validation, lookup, or Postgres.",
	})

	executionReportBytesReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_controller_execution_report_bytes_received_total",
		Help: "Decoded ExecutionReport wire bytes received, before validation, lookup, or Postgres.",
	})

	executionReportsOutcome = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_controller_execution_reports_total",
		Help: "ExecutionReport processing outcomes (accepted, rejected, not_found, store_unavailable, internal_error).",
	}, []string{"outcome"})

	executionReportHandle = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_controller_execution_report_handle_seconds",
		Help:    "Time to handle one decoded ExecutionReport through validation, lookup, and store.",
		Buckets: reportHandleBuckets,
	})

	executionReportAck = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_controller_execution_report_ack_seconds",
		Help:    "Time to send the Controller ExecutionReport ACK on the control stream.",
		Buckets: reportHandleBuckets,
	})

	executionReportAcks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_controller_execution_report_acks_total",
		Help: "ExecutionReport ACKs sent on the control stream, by result.",
	}, []string{"result"})
)

func reportHubCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		executionReportsReceived,
		executionReportBytesReceived,
		executionReportsOutcome,
		executionReportHandle,
	}
}

func reportGatewayCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		executionReportAck,
		executionReportAcks,
	}
}

// RecordExecutionReportReceived counts one decoded report and its wire size.
// Call immediately after decode, before ValidateSnapshot / job lookup / Store.Put.
func RecordExecutionReportReceived(bytes int) {
	executionReportsReceived.Inc()
	if bytes > 0 {
		executionReportBytesReceived.Add(float64(bytes))
	}
}

// RecordExecutionReportOutcome counts the processing result of one report.
func RecordExecutionReportOutcome(outcome string) {
	executionReportsOutcome.WithLabelValues(outcome).Inc()
}

// ObserveExecutionReportHandle records handle duration (decode through store).
func ObserveExecutionReportHandle(elapsed time.Duration) {
	executionReportHandle.Observe(elapsed.Seconds())
}

// ObserveExecutionReportAck records the ACK send duration.
func ObserveExecutionReportAck(elapsed time.Duration) {
	executionReportAck.Observe(elapsed.Seconds())
}

// RecordExecutionReportAck counts one ACK send result.
func RecordExecutionReportAck(result string) {
	executionReportAcks.WithLabelValues(result).Inc()
}

// ExecutionReportsReceivedValue is the current received counter. Tests use it
// to prove increment-before-validate without exporting the collector.
func ExecutionReportsReceivedValue() float64 {
	return counterValue(executionReportsReceived)
}

// ExecutionReportOutcomeValue is the current outcome counter for one label.
func ExecutionReportOutcomeValue(outcome string) float64 {
	return counterValue(executionReportsOutcome.WithLabelValues(outcome))
}

func counterValue(c prometheus.Counter) float64 {
	var metric dto.Metric
	if err := c.Write(&metric); err != nil {
		return 0
	}
	return metric.GetCounter().GetValue()
}

// MemoryReportStore is the in-memory report store's size and what its byte
// budget let go, read at scrape time. A controller with a report database
// has neither.
func MemoryReportStore(bytes, dropped func() int64) []prometheus.Collector {
	return []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "zenthros_controller_execution_report_memory_bytes",
			Help: "About how many bytes of reports the in-memory store holds (no report database).",
		}, func() float64 { return float64(bytes()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "zenthros_controller_execution_reports_dropped_total",
			Help: "Reports the in-memory store let go to stay within its byte budget, the oldest intermediate ones first.",
		}, func() float64 { return float64(dropped()) }),
	}
}
