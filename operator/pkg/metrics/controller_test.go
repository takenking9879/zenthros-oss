// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRegisterControllerRegistersAllCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	if err := RegisterController(registry); err != nil {
		t.Fatalf("RegisterController: %v", err)
	}
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := []string{
		"zenthros_reconcile_errors_total",
		"zenthros_reconcile_duration_seconds",
		"zenthros_submission_latency_seconds",
	}
	got := make(map[string]bool)
	for _, family := range metricFamilies {
		got[family.GetName()] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("registered metrics missing %q (got %v)", name, got)
		}
	}
	for _, name := range []string{
		"zenthros_controller_agent_messages_received_total",
		"zenthros_active_log_streams",
		"zenthros_grpc_stream_duration_seconds",
		"zenthros_enrollment_total",
		"zenthros_controller_execution_report_acks_total",
	} {
		if got[name] {
			t.Errorf("%s must register on the Gateway, not the Controller", name)
		}
	}
}

func TestRegisterGatewayRegistersChannelAndAckSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	if err := RegisterGateway(registry); err != nil {
		t.Fatalf("RegisterGateway: %v", err)
	}
	RecordAgentMessageReceived(MsgHeartbeat, "gw-metrics")
	RecordExecutionReportAck(ReportAckAccepted)
	ObserveControlStreamDuration(time.Second)
	LogStreamOpened()
	t.Cleanup(LogStreamClosed)
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := make(map[string]bool)
	for _, family := range metricFamilies {
		got[family.GetName()] = true
	}
	for _, name := range []string{
		"zenthros_controller_agent_messages_received_total",
		"zenthros_controller_execution_report_ack_seconds",
		"zenthros_controller_execution_report_acks_total",
		"zenthros_active_log_streams",
		"zenthros_grpc_stream_duration_seconds",
	} {
		if !got[name] {
			t.Errorf("gateway registry missing %q (got %v)", name, got)
		}
	}
	if got["zenthros_reconcile_errors_total"] {
		t.Error("reconcile series must stay on the Controller")
	}
}

func TestRegisterControllerRejectsDoubleRegistration(t *testing.T) {
	registry := prometheus.NewRegistry()
	if err := RegisterController(registry); err != nil {
		t.Fatalf("first RegisterController: %v", err)
	}
	if err := RegisterController(registry); err == nil {
		t.Fatal("second RegisterController must fail on the same registry")
	}
}

func TestRecordSubmission(t *testing.T) {
	RecordSubmission("backend-metric-a", SubmitAccepted)
	RecordSubmission("backend-metric-a", SubmitRejected)
	RecordSubmission("backend-metric-a", SubmitRejected)
	RecordSubmission("backend-metric-b", SubmitFailed)

	if got := testutil.ToFloat64(submissionsTotal.WithLabelValues("backend-metric-a", SubmitAccepted)); got != 1 {
		t.Errorf("accepted count = %v, want 1", got)
	}
	if got := testutil.ToFloat64(submissionsTotal.WithLabelValues("backend-metric-a", SubmitRejected)); got != 2 {
		t.Errorf("rejected count = %v, want 2", got)
	}
	if got := testutil.ToFloat64(submissionsTotal.WithLabelValues("backend-metric-b", SubmitFailed)); got != 1 {
		t.Errorf("failed count = %v, want 1", got)
	}
}

func TestObserveSubmissionLatency(t *testing.T) {
	ObserveSubmissionLatency(2 * time.Second)
	count := testutil.CollectAndCount(submissionLatency)
	if count == 0 {
		t.Fatal("submission latency histogram emitted no samples")
	}
}

func TestLogStreamGauge(t *testing.T) {
	LogStreamOpened()
	LogStreamOpened()
	LogStreamClosed()
	if got := testutil.ToFloat64(activeLogStreams); got != 1 {
		t.Errorf("active log streams = %v, want 1", got)
	}
	LogStreamClosed()
}

func TestRecordStatusUpdate(t *testing.T) {
	RecordStatusUpdate("Running")
	RecordStatusUpdate("Succeeded")
	if got := testutil.ToFloat64(statusUpdatesTotal.WithLabelValues("Running")); got != 1 {
		t.Errorf("Running updates = %v, want 1", got)
	}
	if got := testutil.ToFloat64(statusUpdatesTotal.WithLabelValues("Succeeded")); got != 1 {
		t.Errorf("Succeeded updates = %v, want 1", got)
	}
}

func TestReconcileMetrics(t *testing.T) {
	ObserveReconcileDuration(10 * time.Millisecond)
	RecordReconcileError()
	RecordReconcileError()
	if got := testutil.ToFloat64(reconcileErrorsTotal); got != 2 {
		t.Errorf("reconcile errors = %v, want 2", got)
	}
}

func TestControlStreamDuration(t *testing.T) {
	ObserveControlStreamDuration(30 * time.Second)
	if count := testutil.CollectAndCount(controlStreamDuration); count == 0 {
		t.Fatal("stream duration histogram emitted no samples")
	}
}

func TestRecordEnrollment(t *testing.T) {
	RecordEnrollment(EnrollSuccess)
	RecordEnrollment(EnrollFailed)
	if got := testutil.ToFloat64(enrollmentTotal.WithLabelValues(EnrollSuccess)); got != 1 {
		t.Errorf("enrollment success = %v, want 1", got)
	}
	if got := testutil.ToFloat64(enrollmentTotal.WithLabelValues(EnrollFailed)); got != 1 {
		t.Errorf("enrollment failed = %v, want 1", got)
	}
}

// A phase is a machine's word: each new one was a series for good, and 16 MiB
// of label, before anything asked whose job it named.
func TestAPhaseNoOneNamedIsOneSeries(t *testing.T) {
	statusUpdatesTotal.Reset()
	for _, phase := range []string{"made-up-1", "made-up-2", "made-up-3"} {
		RecordStatusUpdate(phase)
	}
	RecordStatusUpdate("Running")
	if got := testutil.CollectAndCount(statusUpdatesTotal); got != 2 {
		t.Fatalf("%d series, want Running and one for all the rest", got)
	}
}
