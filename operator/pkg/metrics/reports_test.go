// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordExecutionReportReceivedBeforeOutcome(t *testing.T) {
	beforeReceived := testutil.ToFloat64(executionReportsReceived)
	beforeBytes := testutil.ToFloat64(executionReportBytesReceived)
	beforeRejected := testutil.ToFloat64(executionReportsOutcome.WithLabelValues(ReportRejected))

	RecordExecutionReportReceived(128)
	if got := testutil.ToFloat64(executionReportsReceived) - beforeReceived; got != 1 {
		t.Fatalf("received = %v, want +1", got)
	}
	if got := testutil.ToFloat64(executionReportBytesReceived) - beforeBytes; got != 128 {
		t.Fatalf("bytes = %v, want +128", got)
	}

	RecordExecutionReportOutcome(ReportRejected)
	if got := testutil.ToFloat64(executionReportsOutcome.WithLabelValues(ReportRejected)) - beforeRejected; got != 1 {
		t.Fatalf("rejected outcome = %v, want +1", got)
	}
	if got := testutil.ToFloat64(executionReportsReceived) - beforeReceived; got != 1 {
		t.Fatalf("rejected report must still count as received; received delta = %v", got)
	}
}

func TestExecutionReportHandleAndAckHistograms(t *testing.T) {
	ObserveExecutionReportHandle(12 * time.Millisecond)
	ObserveExecutionReportAck(3 * time.Millisecond)
	if testutil.CollectAndCount(executionReportHandle) == 0 {
		t.Fatal("handle histogram emitted no samples")
	}
	if testutil.CollectAndCount(executionReportAck) == 0 {
		t.Fatal("ack histogram emitted no samples")
	}
	RecordExecutionReportAck(ReportAckAccepted)
	if got := testutil.ToFloat64(executionReportAcks.WithLabelValues(ReportAckAccepted)); got < 1 {
		t.Fatalf("acks accepted = %v, want >= 1", got)
	}
}

func TestRegisterControllerIncludesReportSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	if err := RegisterController(registry); err != nil {
		t.Fatalf("RegisterController: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := make(map[string]bool)
	for _, family := range families {
		got[family.GetName()] = true
	}
	for _, name := range []string{
		"zenthros_controller_execution_reports_received_total",
		"zenthros_controller_execution_report_bytes_received_total",
		"zenthros_controller_execution_report_handle_seconds",
		"zenthros_controller_pending_seconds",
		"zenthros_controller_backend_busy_total",
	} {
		if !got[name] {
			t.Errorf("controller registry missing %q", name)
		}
	}
	if got["zenthros_controller_execution_report_persist_seconds"] {
		t.Error("persist_seconds must not be registered")
	}
}
