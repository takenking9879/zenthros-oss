// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestProvisionerOperationalMetricsHaveNoInstanceLabels(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(provisionerOperations); err != nil {
		t.Fatal(err)
	}
	IncProvisionerOperation("create", "ok", "provisioner")
	IncProvisionerOperation("orphan-detect", "quarantined", "provisioner")
	want := `
# HELP zenthros_provisioner_operations_total Provisioner mutations and repair actions by operation and result.
# TYPE zenthros_provisioner_operations_total counter
zenthros_provisioner_operations_total{component="provisioner",operation="create",result="ok"} 1
zenthros_provisioner_operations_total{component="provisioner",operation="orphan-detect",result="quarantined"} 1
`
	if err := testutil.CollectAndCompare(provisionerOperations, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
