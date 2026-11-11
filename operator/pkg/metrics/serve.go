// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Serve exposes /metrics over plain HTTP on addr until ctx is cancelled, then
// shuts down gracefully. Scraping is unauthenticated on purpose: the endpoint
// binds inside the trust boundary (pod network on a cluster, loopback/local
// network on a VM) and carries no customer data — platform counters only.
func Serve(ctx context.Context, addr string, registry *prometheus.Registry, logger logr.Logger) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics server listen %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	logger.Info("metrics server listening", "addr", listener.Addr().String())

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("metrics server serve: %w", err)
	}
	return nil
}
