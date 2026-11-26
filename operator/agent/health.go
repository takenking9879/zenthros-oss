// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
)

// healthState tracks whether the agent currently holds a registered control stream
// to the controller. It is written by the reconnect loop (serveOnce) and read by the
// probe handlers, so it must be safe for concurrent use.
type healthState struct {
	connected atomic.Bool
}

func (h *healthState) setConnected(connected bool) { h.connected.Store(connected) }

func (h *healthState) isConnected() bool { return h.connected.Load() }

// healthServer serves the kubelet probes for the agent pod. /healthz (liveness)
// always succeeds while the process is up: a disconnect from the controller is a
// transient condition the reconnect loop handles, not a reason to restart the
// container. /readyz (readiness) reflects the control stream, so `kubectl get pods`
// shows the agent NotReady exactly when it cannot receive new jobs. Plain HTTP on
// purpose: the kubelet probes locally over the pod network, no TLS material needed.
type healthServer struct {
	state  *healthState
	logger logr.Logger
}

func newHealthServer(state *healthState, logger logr.Logger) *healthServer {
	return &healthServer{state: state, logger: logger.WithName("health")}
}

// serve listens on addr and blocks until ctx is cancelled, then shuts down
// gracefully. It never returns a non-nil error once the listener is up.
func (s *healthServer) serve(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("health server listen %s: %w", addr, err)
	}
	return s.serveListener(ctx, listener)
}

// serveListener serves probes on an already-bound listener. Split out from serve so
// tests can bind 127.0.0.1:0 and learn the chosen port.
func (s *healthServer) serveListener(ctx context.Context, listener net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.state.isConnected() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "ok")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "not connected to controller")
	})

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.logger.Info("health server listening", "addr", listener.Addr().String())

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("health server serve: %w", err)
	}
	return nil
}
