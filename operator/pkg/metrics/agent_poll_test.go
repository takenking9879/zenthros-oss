// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestObserveStatusPoll(t *testing.T) {
	beforeSweeps := testutil.ToFloat64(statusPollTotal)
	beforeErrors := testutil.ToFloat64(statusPollErrors)
	ObserveStatusPoll(8*time.Millisecond, 2)
	if got := testutil.ToFloat64(statusPollTotal) - beforeSweeps; got != 1 {
		t.Fatalf("sweeps = %v, want +1", got)
	}
	if got := testutil.ToFloat64(statusPollErrors) - beforeErrors; got != 2 {
		t.Fatalf("poll errors = %v, want +2", got)
	}
	if testutil.CollectAndCount(statusPollSeconds) == 0 {
		t.Fatal("status poll histogram emitted no samples")
	}
}

func TestRecordK8sAPI(t *testing.T) {
	beforeOK := testutil.ToFloat64(k8sAPIRequests.WithLabelValues(K8sAPIOk))
	beforeErr := testutil.ToFloat64(k8sAPIRequests.WithLabelValues(K8sAPIError))
	RecordK8sAPI(nil, 2*time.Millisecond)
	RecordK8sAPI(errors.New("get rayjob"), 4*time.Millisecond)
	if got := testutil.ToFloat64(k8sAPIRequests.WithLabelValues(K8sAPIOk)) - beforeOK; got != 1 {
		t.Fatalf("ok = %v, want +1", got)
	}
	if got := testutil.ToFloat64(k8sAPIRequests.WithLabelValues(K8sAPIError)) - beforeErr; got != 1 {
		t.Fatalf("error = %v, want +1", got)
	}
}

func TestNewAgentRegistryK8sAPIOnlyOnK8s(t *testing.T) {
	k8sNames := registryNames(t, NewAgentRegistry("k8s", func() float64 { return 0 }))
	vmNames := registryNames(t, NewAgentRegistry("vm", func() float64 { return 0 }))

	if !k8sNames["zenthros_agent_status_poll_seconds"] {
		t.Error("k8s registry missing status_poll")
	}
	if !k8sNames["zenthros_agent_k8s_api_request_seconds"] {
		t.Error("k8s registry missing k8s_api")
	}
	if !vmNames["zenthros_agent_status_poll_seconds"] {
		t.Error("vm registry missing status_poll")
	}
	if vmNames["zenthros_agent_k8s_api_requests_total"] {
		t.Error("vm registry must not expose k8s_api series")
	}
	if vmNames["zenthros_agent_k8s_api_request_seconds"] {
		t.Error("vm registry must not expose k8s_api duration")
	}
}

func TestAgentRegistryHasNoJobIDLabel(t *testing.T) {
	registry := NewAgentRegistry("k8s", func() float64 { return 1 })
	RecordK8sAPI(nil, time.Millisecond)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "zenthrosjob_id" {
					t.Fatalf("metric %s has forbidden zenthrosjob_id label", family.GetName())
				}
			}
		}
	}
}

func registryNames(t *testing.T, registry interface {
	Gather() ([]*dto.MetricFamily, error)
}) map[string]bool {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := make(map[string]bool, len(families))
	for _, family := range families {
		got[family.GetName()] = true
	}
	return got
}
