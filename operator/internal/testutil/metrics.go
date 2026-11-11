// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// HistogramSamples is how many samples the histogram name holds among the
// collectors register adds. The collectors are process globals: a test reads
// it before and after what it does, and does not run in parallel.
func HistogramSamples(t *testing.T, register func(prometheus.Registerer) error, name string) uint64 {
	t.Helper()
	registry := prometheus.NewRegistry()
	if err := register(registry); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	t.Fatalf("no histogram %s", name)
	return 0
}
