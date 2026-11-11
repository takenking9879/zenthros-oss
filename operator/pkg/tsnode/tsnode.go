// SPDX-License-Identifier: Apache-2.0

// Package tsnode puts a process on a tailnet without a TUN device, root or
// tailscaled: a userspace Tailscale node (tsnet) inside the process itself.
// It is how an agent in an unprivileged container reaches a controller that
// is only on the tailnet (ZENTHROS_NETWORK=tailscale-userspace,
// docs/design/container-agent.md). The node only dials out: nothing listens
// on it, so a job in the same container gains no tailnet address to reach.
package tsnode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"tailscale.com/logtail"
	"tailscale.com/tsnet"
)

// Config is the node's identity and where it keeps its state.
type Config struct {
	// Dir holds the node's key and state. A restarted process with the same
	// Dir is the same node and needs no auth key. Required.
	Dir string
	// Hostname is the device name on the tailnet: the backend ID, so the
	// controller can find the device by name to remove it. Required.
	Hostname string
	// AuthKey joins a node that Dir does not hold yet. A one-time key is
	// enough: it is spent on the first join, and later starts use Dir.
	AuthKey string
	// ControlURL is the coordination server; empty is Tailscale's.
	ControlURL string
	// Logf receives the node's status lines (it is up, it needs a key).
	// Nil drops them.
	Logf func(format string, args ...any)
}

// Node is a running userspace Tailscale node.
type Node struct {
	srv *tsnet.Server
}

// Up starts the node and waits until it is on the tailnet. The node is
// ephemeral: the tailnet removes its device once it has been offline a while,
// so a container that is gone leaves no device behind.
func Up(ctx context.Context, cfg Config) (*Node, error) {
	if cfg.Dir == "" || cfg.Hostname == "" {
		return nil, errors.New("tsnode: a state directory and a hostname are required")
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("tsnode: state directory: %w", err)
	}
	if cfg.AuthKey == "" && !HasState(cfg.Dir) {
		return nil, fmt.Errorf("tsnode: %s holds no node and no auth key was given", cfg.Dir)
	}
	// The node's own logs stay here; nothing is uploaded to Tailscale.
	logtail.Disable()
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	srv := &tsnet.Server{
		Dir:        cfg.Dir,
		Hostname:   cfg.Hostname,
		AuthKey:    cfg.AuthKey,
		ControlURL: cfg.ControlURL,
		Ephemeral:  true,
		UserLogf:   logf,
	}
	if _, err := srv.Up(ctx); err != nil {
		_ = srv.Close()
		return nil, fmt.Errorf("tsnode: join the tailnet as %s: %w", cfg.Hostname, err)
	}
	return &Node{srv: srv}, nil
}

// HasState reports whether dir already holds a node, so a restart needs no
// auth key.
func HasState(dir string) bool {
	info, err := os.Stat(stateFile(dir))
	return err == nil && info.Size() > 0
}

// stateFile is where tsnet keeps the node (its FileStore).
func stateFile(dir string) string {
	return filepath.Join(dir, "tailscaled.state")
}

// Dial opens a TCP connection through the tailnet. addr may be a tailnet IP
// or a MagicDNS name.
func (n *Node) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return n.srv.Dial(ctx, "tcp", addr)
}

// IPv4 is the node's tailnet address, or "".
func (n *Node) IPv4() string {
	ip4, _ := n.srv.TailscaleIPs()
	if !ip4.IsValid() {
		return ""
	}
	return ip4.String()
}

// Close stops the node. It does not log out: a restarted process with the
// same state directory is the same device, with no new key needed.
func (n *Node) Close() error {
	return n.srv.Close()
}
