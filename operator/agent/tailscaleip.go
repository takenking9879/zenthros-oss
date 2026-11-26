// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"net"
	"time"
)

// Agent network modes (Config.Network, ZENTHROS_NETWORK).
const (
	NetworkTailscale = "tailscale"
	NetworkDirect    = "direct"
	// NetworkTailscaleUserspace is a Tailscale node inside the agent itself
	// (pkg/tsnode): the tailnet for a container with no TUN device and no
	// root. Config.Dial carries every connection to the controller.
	NetworkTailscaleUserspace = "tailscale-userspace"
)

// tailscaleIPWaitPoll is how often waitForTailscaleIP re-checks for the interface.
const tailscaleIPWaitPoll = 500 * time.Millisecond

// tailscaleIPWaitMax bounds the wait so an agent with no Tailscale interface (e.g. a
// standalone VM dialing over plain network) does not block registration indefinitely;
// it then reports an empty address and the controller falls back to the peer IP.
const tailscaleIPWaitMax = 30 * time.Second

// waitForTailscaleIP returns the agent's Tailscale IPv4 address, waiting until the
// tailscale interface is assigned one. The tailscale sidecar brings up tailscale0 a
// moment after the agent container starts; advertising before it is ready would report
// an empty address (and the controller would fall back to an unreliable peer IP). The
// wait ends early on context cancellation (shutdown) or after `wait`.
func waitForTailscaleIP(ctx context.Context, wait time.Duration) string {
	if ip := tailscaleIP(); ip != "" {
		return ip
	}
	if wait <= 0 {
		return ""
	}
	deadline := time.After(wait)
	for {
		select {
		case <-ctx.Done():
			return ""
		case <-deadline:
			return ""
		case <-time.After(tailscaleIPWaitPoll):
			if ip := tailscaleIP(); ip != "" {
				return ip
			}
		}
	}
}

// tailscaleIP returns the agent's Tailscale IPv4 address by scanning the network
// interfaces for an address in the Tailscale CGNAT range (100.64.0.0/10). The agent
// reports this in its Advertise so the controller can dial its log server directly;
// the gRPC peer address is not reliable because the control connection can be relayed
// or host-NATted to an address that does not reach the agent. Empty when no Tailscale
// interface is present.
func tailscaleIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if ip4 := ipNet.IP.To4(); ip4 != nil && inTailscaleRange(ip4) {
				return ip4.String()
			}
		}
	}
	return ""
}

// inTailscaleRange reports whether ip is in 100.64.0.0/10 (first octet 100, second
// 64-127), the CGNAT block Tailscale assigns to tailnet nodes.
func inTailscaleRange(ip net.IP) bool {
	return len(ip) == 4 && ip[0] == 100 && ip[1] >= 64 && ip[1] <= 127
}
