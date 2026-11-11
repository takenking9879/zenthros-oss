// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/agent"
	"github.com/zenthros/operator/internal/envconf"
	"github.com/zenthros/operator/pkg/bootstrap"
	"github.com/zenthros/operator/pkg/tsnode"
)

// EnvTailscaleAuthKey joins a userspace tailnet node the first time; the
// agent drops it from its environment once read, so no job inherits it.
const EnvTailscaleAuthKey = "ZENTHROS_TAILSCALE_AUTHKEY"

// tailnetNode is what the agent uses of a running node.
type tailnetNode interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
	io.Closer
}

// upTailnetFunc starts a node; tsnode.Up in production.
type upTailnetFunc func(context.Context, tsnode.Config) (tailnetNode, error)

func upTailnet(ctx context.Context, cfg tsnode.Config) (tailnetNode, error) {
	return tsnode.Up(ctx, cfg)
}

// tailnetStateDir keeps the node beside the agent's other state: a container's
// state directory, or ZENTHROS_TAILSCALE_STATE_DIR.
func tailnetStateDir() string {
	if dir := os.Getenv("ZENTHROS_TAILSCALE_STATE_DIR"); dir != "" {
		return dir
	}
	if envconf.Or("ZENTHROS_AGENT_MODE", agentModeVM) == agentModeContainer {
		return containerPath("tailscale")
	}
	return "/var/lib/zenthros/tailscale"
}

// joinUserspaceTailnet puts the agent on the tailnet as its backend ID and
// sends every connection to the controller (enrollment included) through
// the node. It returns once the node is up, so what follows can dial.
func joinUserspaceTailnet(ctx context.Context, cfg *agent.Config, up upTailnetFunc, logger logr.Logger) (tailnetNode, error) {
	if cfg.BackendID == "" {
		return nil, fmt.Errorf("ZENTHROS_NETWORK=%s needs ZENTHROS_BACKEND_ID: it is the node's name on the tailnet", agent.NetworkTailscaleUserspace)
	}
	authKey := os.Getenv(EnvTailscaleAuthKey)
	_ = os.Unsetenv(EnvTailscaleAuthKey)
	ctx, cancel := context.WithTimeout(ctx, envconf.Duration("ZENTHROS_ENROLL_TIMEOUT", defaultEnrollTimeout))
	defer cancel()
	log := logger.WithName("tailnet")
	node, err := up(ctx, tsnode.Config{
		Dir:        filepath.Clean(tailnetStateDir()),
		Hostname:   cfg.BackendID,
		AuthKey:    authKey,
		ControlURL: os.Getenv("ZENTHROS_TAILSCALE_CONTROL_URL"),
		Logf:       func(format string, args ...any) { log.Info(fmt.Sprintf(format, args...)) },
	})
	if err != nil {
		return nil, err
	}
	cfg.Dial = node.Dial
	logger.Info("on the tailnet (userspace); the controller is reached through it", "hostname", cfg.BackendID, "controllerAddr", cfg.ControllerAddr)
	return node, nil
}

// prepareToConnect readies what the agent connects with, in this order: a
// container's state paths, the tailnet (a userspace node), then a
// container's enrollment, which goes through that node. The returned closer
// stops the node; without one it does nothing.
func prepareToConnect(ctx context.Context, cfg *agent.Config, up upTailnetFunc, logger logr.Logger) (io.Closer, error) {
	container := envconf.Or("ZENTHROS_AGENT_MODE", agentModeVM) == agentModeContainer
	if container {
		applyContainerDefaults(cfg)
	}
	var node io.Closer = noNode{}
	if cfg.Network == agent.NetworkTailscaleUserspace {
		joined, err := joinUserspaceTailnet(ctx, cfg, up, logger)
		if err != nil {
			return nil, fmt.Errorf("join the tailnet: %w", err)
		}
		node = joined
	}
	if container {
		if err := enrollIfNeeded(ctx, *cfg, logger); err != nil {
			_ = node.Close()
			return nil, err
		}
		if cfg.BackendID == "" {
			// A short join names no backend: the hub enrolled the token's
			// own, and the certificate says which. So it does after every
			// restart, with no variable to keep.
			pemBytes, err := os.ReadFile(cfg.TLS.CertFile)
			if err == nil {
				cfg.BackendID, err = bootstrap.CertCommonName(pemBytes)
			}
			if err != nil {
				_ = node.Close()
				return nil, fmt.Errorf("the backend this container is: %w", err)
			}
		}
	}
	return node, nil
}

// noNode is the closer of an agent with no tailnet node.
type noNode struct{}

func (noNode) Close() error { return nil }
