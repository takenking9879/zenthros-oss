// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestAWSCatalogMetricsUseBoundedLabels(t *testing.T) {
	collector := NewAWSCatalogCollector()
	registry := prometheus.NewPedanticRegistry()
	if err := collector.Register(registry); err != nil {
		t.Fatal(err)
	}
	collector.ObserveRetry("DescribeInstanceTypes", "us-east-2")
	collector.ObserveRequest("DescribeInstanceTypes", "us-east-2", "error", "throttled", time.Second, 2)
	collector.ObservePartial("us-east-2", "prices")
	collector.ObservePlacementScore("us-east-2", false)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "zenthros_aws_catalog_requests_total" {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetName() == "instance_type" || label.GetName() == "error" {
						t.Fatalf("unbounded label leaked: %v", label)
					}
				}
			}
		}
	}
}
