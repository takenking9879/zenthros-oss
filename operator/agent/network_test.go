// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/go-logr/logr"
)

// With a dialer (a userspace tailnet node), both connections to the
// controller go through it, with the address as written: the control
// channel and the log channel alike. And there is no interface to wait for.
func TestControllerConnectionsGoThroughTheDialer(t *testing.T) {
	const controller = "zenthros-gw.tail0000.ts.net:9443"
	var mu sync.Mutex
	var dialed []string
	dial := func(_ context.Context, addr string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, addr)
		mu.Unlock()
		return nil, errors.New("fake tailnet: no route")
	}
	cfg := Config{BackendID: "c-1", BackendType: "container", ControllerAddr: controller,
		Network: NetworkTailscaleUserspace, Dial: dial}
	if cfg.tailscaleWait() != 0 {
		t.Fatalf("a userspace node waits %s for a tailnet interface it never has", cfg.tailscaleWait())
	}
	a := NewWithExecutor(cfg, SystemdExecutor{}, logr.Discard())
	through := func(name string, connect func() error) {
		t.Helper()
		mu.Lock()
		dialed = nil
		mu.Unlock()
		if err := connect(); err == nil {
			t.Fatalf("%s through a node that routes nowhere connected", name)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(dialed) == 0 || dialed[0] != controller {
			t.Fatalf("%s dialed %v, want %q through the dialer, unresolved", name, dialed, controller)
		}
	}
	through("control channel", func() error { return a.serveOnce(context.Background(), &tls.Config{}) })
	a.sessionEpoch.Store(1)
	through("log channel", func() error { return a.serveLogChannelOnce(context.Background(), &tls.Config{}) })
}
