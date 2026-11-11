// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/agent"
	"github.com/zenthros/operator/pkg/tsnode"
)

// fakeNode records what goes through it and refuses every connection.
type fakeNode struct {
	mu     sync.Mutex
	events *[]string
	closed bool
}

func (n *fakeNode) Dial(_ context.Context, addr string) (net.Conn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	*n.events = append(*n.events, "dial "+addr)
	return nil, errors.New("fake tailnet: no route")
}

func (n *fakeNode) Close() error { n.closed = true; return nil }

// A userspace tailnet container joins the tailnet first and enrolls through
// it: the enrollment dial reaches the node with the controller's name as
// written, unresolved, since a MagicDNS name resolves only on the tailnet.
// The node is named after the backend, keeps its state under the container's
// state directory, and takes the auth key out of the environment.
func TestUserspaceTailnetCarriesEnrollment(t *testing.T) {
	state := t.TempDir()
	t.Setenv("ZENTHROS_AGENT_MODE", agentModeContainer)
	t.Setenv("ZENTHROS_STATE_DIR", state)
	t.Setenv("ZENTHROS_TAILSCALE_STATE_DIR", "")
	t.Setenv("ZENTHROS_JOIN_TOKEN", "join-token")
	t.Setenv("ZENTHROS_CA_CERT_HASH", "sha256:"+strings.Repeat("0", 64))
	t.Setenv("ZENTHROS_ENROLL_TIMEOUT", "1ms")
	t.Setenv(EnvTailscaleAuthKey, "tskey-auth-x")
	const controller = "zenthros-gw.tail0000.ts.net:9443"
	var events []string
	var got tsnode.Config
	node := &fakeNode{events: &events}
	up := func(_ context.Context, cfg tsnode.Config) (tailnetNode, error) {
		got = cfg
		events = append(events, "up")
		return node, nil
	}
	cfg := agent.Config{ControllerAddr: controller, BackendID: "fake-c-1", BackendType: "container",
		Network: agent.NetworkTailscaleUserspace}
	_, err := prepareToConnect(context.Background(), &cfg, up, logr.Discard())
	if err == nil {
		t.Fatal("enrollment through a node that routes nowhere succeeded")
	}
	if len(events) < 2 || events[0] != "up" || events[1] != "dial "+controller {
		t.Fatalf("events = %v, want the node up, then the enrollment dialing %s through it", events, controller)
	}
	if !node.closed {
		t.Error("a failed enrollment left the node running")
	}
	if got.Hostname != "fake-c-1" || got.AuthKey != "tskey-auth-x" || got.Dir != filepath.Join(state, "tailscale") {
		t.Errorf("node config = %+v", got)
	}
	if _, set := os.LookupEnv(EnvTailscaleAuthKey); set {
		t.Error("the auth key is still in the agent's environment, for its jobs to inherit")
	}
}

// Without the tailnet mode no node starts and nothing is dialed through one.
func TestNoTailnetNodeUnlessAsked(t *testing.T) {
	t.Setenv("ZENTHROS_AGENT_MODE", agentModeVM)
	up := func(context.Context, tsnode.Config) (tailnetNode, error) {
		t.Fatal("a node started for the direct network")
		return nil, nil
	}
	cfg := agent.Config{ControllerAddr: "gw:9443", BackendID: "b", Network: agent.NetworkDirect}
	node, err := prepareToConnect(context.Background(), &cfg, up, logr.Discard())
	if err != nil || node == nil || cfg.Dial != nil {
		t.Fatalf("node %v err %v dial set %v", node, err, cfg.Dial != nil)
	}
}

// The node is named after the backend; without a backend ID there is no
// name to give it, and the agent says so instead of joining as "agent".
func TestUserspaceTailnetNeedsABackendID(t *testing.T) {
	t.Setenv("ZENTHROS_AGENT_MODE", agentModeVM)
	cfg := agent.Config{ControllerAddr: "gw:9443", Network: agent.NetworkTailscaleUserspace}
	_, err := prepareToConnect(context.Background(), &cfg, func(context.Context, tsnode.Config) (tailnetNode, error) {
		t.Fatal("joined without a name")
		return nil, nil
	}, logr.Discard())
	if err == nil || !strings.Contains(err.Error(), "ZENTHROS_BACKEND_ID") {
		t.Fatalf("err = %v", err)
	}
}

// The mode is accepted wherever ZENTHROS_NETWORK is read; a container still
// defaults to direct.
func TestEnvNetworkAcceptsUserspaceTailnet(t *testing.T) {
	t.Setenv("ZENTHROS_NETWORK", agent.NetworkTailscaleUserspace)
	if got := envNetwork(agentModeContainer); got != agent.NetworkTailscaleUserspace {
		t.Fatalf("network = %q", got)
	}
}
