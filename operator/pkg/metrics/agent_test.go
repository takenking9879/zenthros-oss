// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestNewAgentRegistryServesPlatformAndRuntimeMetrics(t *testing.T) {
	registry := NewAgentRegistry("vm", func() float64 { return 3 })

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := make(map[string]bool)
	for _, family := range families {
		got[family.GetName()] = true
	}

	for _, name := range []string{
		"zenthros_agent_jobs_active",
		"zenthros_agent_control_stream_up",
		"zenthros_agent_control_reconnects_total",
		"zenthros_agent_heartbeat_sent_total",
		"zenthros_agent_log_streams_active",
		"go_goroutines",
		"process_open_fds",
	} {
		if _, ok := got[name]; !ok {
			t.Errorf("agent registry missing %q", name)
		}
	}
}

func TestAgentJobsActiveGaugeReflectsCount(t *testing.T) {
	registry := NewAgentRegistry("k8s", func() float64 { return 7 })
	metric := gatherGauge(t, registry, "zenthros_agent_jobs_active")
	if metric.GetGauge().GetValue() != 7 {
		t.Errorf("jobs active = %v, want 7", metric.GetGauge().GetValue())
	}
	if metric.GetLabel()[0].GetValue() != "k8s" {
		t.Errorf("type label = %q, want k8s", metric.GetLabel()[0].GetValue())
	}
}

func TestRecordAgentJobResult(t *testing.T) {
	RecordAgentJobResult(JobSucceeded)
	RecordAgentJobResult(JobFailed)
	RecordAgentJobResult(JobFailed)
	if got := testutil.ToFloat64(agentJobsTotal.WithLabelValues(JobSucceeded)); got != 1 {
		t.Errorf("succeeded = %v, want 1", got)
	}
	if got := testutil.ToFloat64(agentJobsTotal.WithLabelValues(JobFailed)); got != 2 {
		t.Errorf("failed = %v, want 2", got)
	}
}

func TestAgentSetupAndTeardownHistograms(t *testing.T) {
	ObserveAgentJobSetup(4 * time.Second)
	ObserveAgentJobTeardown(500 * time.Millisecond)
	if count := testutil.CollectAndCount(agentJobSetup); count == 0 {
		t.Fatal("setup histogram emitted no samples")
	}
	if count := testutil.CollectAndCount(agentJobTeardown); count == 0 {
		t.Fatal("teardown histogram emitted no samples")
	}
}

func TestRecordAgentExecutorError(t *testing.T) {
	RecordAgentExecutorError(OpSubmit)
	RecordAgentExecutorError(OpLog)
	RecordAgentExecutorError(OpCleanup)
	for _, op := range []string{OpSubmit, OpLog, OpCleanup} {
		if got := testutil.ToFloat64(agentExecutorErrors.WithLabelValues(op)); got != 1 {
			t.Errorf("op %s errors = %v, want 1", op, got)
		}
	}
}

func TestAgentControlStreamMetrics(t *testing.T) {
	SetAgentControlStreamUp(true)
	if got := testutil.ToFloat64(agentControlStreamUp); got != 1 {
		t.Errorf("stream up = %v, want 1", got)
	}
	SetAgentControlStreamUp(false)
	if got := testutil.ToFloat64(agentControlStreamUp); got != 0 {
		t.Errorf("stream up = %v, want 0", got)
	}
	RecordAgentReconnect()
	if got := testutil.ToFloat64(agentReconnectsTotal); got != 1 {
		t.Errorf("reconnects = %v, want 1", got)
	}
}

func TestAgentHeartbeatAndLogStreamMetrics(t *testing.T) {
	RecordAgentHeartbeatSent()
	if got := testutil.ToFloat64(agentHeartbeatsSent); got != 1 {
		t.Errorf("heartbeats = %v, want 1", got)
	}
	AgentLogStreamOpened()
	AgentLogStreamOpened()
	AgentLogStreamClosed()
	if got := testutil.ToFloat64(agentLogStreamsActive); got != 1 {
		t.Errorf("active agent log streams = %v, want 1", got)
	}
	AgentLogStreamClosed()
}

// gatherGauge extracts a single metric family's first sample from a registry.
func gatherGauge(t *testing.T, registry interface {
	Gather() ([]*dto.MetricFamily, error)
}, name string) *dto.Metric {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family.GetMetric()[0]
		}
	}
	t.Fatalf("metric %q not found", name)
	return nil
}
