// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
)

// The agent dials the hub's whole address list (the gateway's port, then
// 443 through Traefik), each address in order on every reconnection, so a
// laptop that moves to a network that closes 9443 reconnects on 443
// (bootstrap.DialTarget's tests run that against real servers). One address
// is dialed as it was.
func TestTheAgentDialsTheHubsWholeAddressList(t *testing.T) {
	list := &Agent{cfg: Config{ControllerAddr: "hub.example:9443,hub.example:443"}}
	if got := list.gatewayTarget(); got != "zenthros-hub:///hub.example:9443,hub.example:443" {
		t.Fatalf("a list is dialed as %q", got)
	}
	if n := len(list.gatewayDialOpts(&tls.Config{})); n != 4 {
		t.Fatalf("a list's dial has %d options, want its authority besides the usual three", n)
	}
	// A hub whose certificate names the gateway (join.tlsServerName): the TLS
	// carries that name and gRPC refuses a second authority beside it.
	if n := len(list.gatewayDialOpts(&tls.Config{ServerName: "zenthros-controller"})); n != 3 {
		t.Fatalf("a list to a named certificate has %d options, want the usual three", n)
	}
	one := &Agent{cfg: Config{ControllerAddr: "hub.example:9443"}}
	if got := one.gatewayTarget(); got != "hub.example:9443" || len(one.gatewayDialOpts(&tls.Config{})) != 3 {
		t.Fatalf("one address is dialed as %q", got)
	}
	own := &Agent{cfg: Config{ControllerAddr: "hub.example:9443", Dial: func(context.Context, string) (net.Conn, error) { return nil, nil }}}
	if got := own.gatewayTarget(); got != "passthrough:///hub.example:9443" {
		t.Fatalf("one address through the agent's own dialer is %q", got)
	}
}
