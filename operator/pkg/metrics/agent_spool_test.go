// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAgentSpoolPendingAndIngest(t *testing.T) {
	SetAgentSpoolPending(3, 4096, 12*time.Second)
	if got := testutil.ToFloat64(agentSpoolPending); got != 3 {
		t.Fatalf("pending = %v, want 3", got)
	}
	if got := testutil.ToFloat64(agentSpoolPendingBytes); got != 4096 {
		t.Fatalf("pending bytes = %v, want 4096", got)
	}
	if got := testutil.ToFloat64(agentSpoolOldestAge); got != 12 {
		t.Fatalf("oldest age = %v, want 12", got)
	}
	SetAgentSpoolPending(0, 0, time.Hour)
	if got := testutil.ToFloat64(agentSpoolOldestAge); got != 0 {
		t.Fatalf("empty spool oldest age = %v, want 0", got)
	}

	before := testutil.ToFloat64(agentSpoolIngest.WithLabelValues(SpoolIngestAccepted))
	RecordAgentSpoolIngest(SpoolIngestAccepted)
	if got := testutil.ToFloat64(agentSpoolIngest.WithLabelValues(SpoolIngestAccepted)) - before; got != 1 {
		t.Fatalf("ingest accepted = %v, want +1", got)
	}
	ObserveAgentReportDelivery(250 * time.Millisecond)
	if testutil.CollectAndCount(agentReportDelivery) == 0 {
		t.Fatal("delivery histogram emitted no samples")
	}
}

func TestAgentRegistryHasNoSpoolInsertSeconds(t *testing.T) {
	names := registryNames(t, NewAgentRegistry("vm", func() float64 { return 0 }))
	if names["zenthros_agent_report_spool_insert_seconds"] {
		t.Fatal("spool_insert_seconds must stay out")
	}
	if !names["zenthros_agent_report_spool_pending"] {
		t.Fatal("agent registry missing spool pending gauge")
	}
}
