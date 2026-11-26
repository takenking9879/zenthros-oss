// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestWaitForTailscaleIPRespectsShortWait(t *testing.T) {
	start := time.Now()
	_ = waitForTailscaleIP(context.Background(), time.Millisecond)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("short wait took %s; want a single probe then return", elapsed)
	}
}

func TestInTailscaleRange(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"100.64.0.1", true},      // start of 100.64.0.0/10
		{"100.127.255.254", true}, // end of 100.64.0.0/10
		{"100.88.193.42", true},   // a real tailnet address
		{"100.63.255.255", false}, // just below the range
		{"100.128.0.0", false},    // just above the range
		{"10.42.0.49", false},     // pod network
		{"192.168.1.1", false},    // private LAN
	}
	for _, c := range cases {
		if got := inTailscaleRange(net.ParseIP(c.ip).To4()); got != c.want {
			t.Errorf("inTailscaleRange(%s) = %v, want %v", c.ip, got, c.want)
		}
	}
}

// Found in the local E2E: an agent without Tailscale registered 30s after
// start, and again 30s after every reconnect, most of the 45s job grace.
func TestDirectAgentDoesNotWaitForTailscale(t *testing.T) {
	if got := (Config{Network: NetworkDirect}).tailscaleWait(); got != 0 {
		t.Fatalf("direct agent waits %s for Tailscale", got)
	}
	if got := (Config{}).tailscaleWait(); got != tailscaleIPWaitMax {
		t.Fatalf("default agent waits %s, want %s", got, tailscaleIPWaitMax)
	}
}
