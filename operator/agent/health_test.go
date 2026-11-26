// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

func TestHealthServerProbes(t *testing.T) {
	state := &healthState{}
	server := newHealthServer(state, logr.Discard())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	baseURL := "http://" + listener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.serveListener(ctx, listener) }()
	waitForHTTP(t, baseURL+"/healthz")

	cases := []struct {
		name      string
		connected bool
		path      string
		wantCode  int
	}{
		{"healthz while disconnected", false, "/healthz", http.StatusOK},
		{"readyz while disconnected", false, "/readyz", http.StatusServiceUnavailable},
		{"healthz while connected", true, "/healthz", http.StatusOK},
		{"readyz while connected", true, "/readyz", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state.setConnected(tc.connected)
			if got := statusCode(t, baseURL+tc.path); got != tc.wantCode {
				t.Errorf("%s = %d, want %d", tc.path, got, tc.wantCode)
			}
		})
	}

	state.setConnected(false)
	if got := statusCode(t, baseURL+"/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("readyz after disconnect = %d, want %d", got, http.StatusServiceUnavailable)
	}
}

func TestHealthServerStopsOnContextCancel(t *testing.T) {
	server := newHealthServer(&healthState{}, logr.Discard())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.serveListener(ctx, listener) }()
	waitForHTTP(t, "http://"+listener.Addr().String()+"/healthz")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveListener returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("health server did not stop after context cancel")
	}
}

func waitForHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if statusCode(t, url) == http.StatusOK {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to serve", url)
}

func statusCode(t *testing.T, url string) int {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Logf("get %s: %v", url, err)
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}
