// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestBackendCollectorEmitsHealthAndConnected(t *testing.T) {
	collector := NewBackendCollector(func() []BackendSnapshot {
		return []BackendSnapshot{
			{ID: "cluster-b", Healthy: true},
			{ID: "vm-01", Healthy: false},
		}
	})

	want := `
# HELP zenthros_backend_health Health of each registered backend (1 healthy, 0 unhealthy).
# TYPE zenthros_backend_health gauge
zenthros_backend_health{backend="cluster-b"} 1
zenthros_backend_health{backend="vm-01"} 0
# HELP zenthros_connected_agents Number of agents currently streaming to the controller.
# TYPE zenthros_connected_agents gauge
zenthros_connected_agents 2
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(want)); err != nil {
		t.Errorf("backend collector output mismatch: %v", err)
	}
}

func TestBackendCollectorEmptyFleet(t *testing.T) {
	collector := NewBackendCollector(func() []BackendSnapshot { return nil })

	want := `
# HELP zenthros_connected_agents Number of agents currently streaming to the controller.
# TYPE zenthros_connected_agents gauge
zenthros_connected_agents 0
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(want)); err != nil {
		t.Errorf("empty fleet output mismatch: %v", err)
	}
}
