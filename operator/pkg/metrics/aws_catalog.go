// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// AWSCatalogCollector exposes bounded AWS discovery request telemetry.
type AWSCatalogCollector struct {
	mu                 sync.Mutex
	requests           *prometheus.CounterVec
	retries            *prometheus.CounterVec
	latency            *prometheus.HistogramVec
	pages              *prometheus.CounterVec
	partial            *prometheus.CounterVec
	placementAvailable *prometheus.GaugeVec
}

func NewAWSCatalogCollector() *AWSCatalogCollector {
	return &AWSCatalogCollector{
		requests:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "zenthros_aws_catalog_requests_total", Help: "AWS catalog API requests by operation and result."}, []string{"operation", "region", "result", "error_class"}),
		retries:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "zenthros_aws_catalog_retries_total", Help: "AWS catalog API retries by operation and region."}, []string{"operation", "region"}),
		latency:            prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "zenthros_aws_catalog_request_duration_seconds", Help: "AWS catalog API request latency."}, []string{"operation", "region"}),
		pages:              prometheus.NewCounterVec(prometheus.CounterOpts{Name: "zenthros_aws_catalog_pages_total", Help: "AWS catalog API pages fetched."}, []string{"operation", "region"}),
		partial:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "zenthros_aws_catalog_partial_total", Help: "AWS catalog partial section results."}, []string{"region", "section"}),
		placementAvailable: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "zenthros_aws_catalog_placement_score_available", Help: "Whether the last Spot placement score request returned data."}, []string{"region"}),
	}
}

func (c *AWSCatalogCollector) Register(registry prometheus.Registerer) error {
	for _, collector := range []prometheus.Collector{c.requests, c.retries, c.latency, c.pages, c.partial, c.placementAvailable} {
		if err := registry.Register(collector); err != nil {
			return err
		}
	}
	return nil
}

func (c *AWSCatalogCollector) ObserveRetry(operation, region string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retries.WithLabelValues(operation, region).Inc()
}

func (c *AWSCatalogCollector) ObserveRequest(operation, region, result, errorClass string, elapsed time.Duration, pageCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests.WithLabelValues(operation, region, result, errorClass).Inc()
	c.latency.WithLabelValues(operation, region).Observe(elapsed.Seconds())
	c.pages.WithLabelValues(operation, region).Add(float64(pageCount))
}

func (c *AWSCatalogCollector) ObservePartial(region, section string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partial.WithLabelValues(region, section).Inc()
}

func (c *AWSCatalogCollector) ObservePlacementScore(region string, available bool) {
	value := 0.0
	if available {
		value = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.placementAvailable.WithLabelValues(region).Set(value)
}
