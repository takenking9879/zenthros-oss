// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"
)

const tailscaleDialInterval = 200 * time.Millisecond

func tailscaleUp(ctx context.Context, authKey, hostname string) error {
	if err := ensureTailscaleInstalled(ctx); err != nil {
		return fmt.Errorf("install tailscale: %w", err)
	}
	args := []string{"up", "--authkey=" + authKey, "--hostname=" + hostname, "--reset"}
	if err := runCmd(ctx, "sudo", append([]string{"tailscale"}, args...)...); err != nil {
		return fmt.Errorf("tailscale up: %w", err)
	}
	return nil
}

// deviceHostname is the tailnet name of a machine joining as backendID: the
// ID itself, since tailnet cleanup finds a backend's devices by name
// (tailnet.OwnerOf). A short join learns its ID only once enrolled: until
// then the device has the OS's name, and rename says to rename it after.
func deviceHostname(backendID string, osHostname func() (string, error)) (name string, rename bool, err error) {
	if backendID != "" {
		return backendID, false, nil
	}
	host, err := osHostname()
	if err != nil {
		return "", false, fmt.Errorf("resolve hostname for --ts-hostname: %w", err)
	}
	return host, true, nil
}

// tailscaleSetHostname renames this machine's tailnet device.
func tailscaleSetHostname(ctx context.Context, hostname string) error {
	if err := runCmd(ctx, "sudo", "tailscale", "set", "--hostname="+hostname); err != nil {
		return fmt.Errorf("name the tailnet device after the backend: %w", err)
	}
	return nil
}

// waitForHost dials the --controller host:port. It does not assume 9443.
func waitForHost(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("controller address %q must be host:port: %w", addr, err)
	}
	dialAddr := net.JoinHostPort(host, port)
	for {
		conn, err := net.DialTimeout("tcp", dialAddr, tailscaleDialInterval)
		if err == nil {
			conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out reaching %s", dialAddr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tailscaleDialInterval):
		}
	}
}

func runCmd(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("%s %v: exit %d", name, shownArgs(args), exitErr.ExitCode())
		}
		return err
	}
	return nil
}

// secretFlags carry a credential as their value: an error that shows a
// command names the flag, never the value, which would land in the console
// or a cloud-init log (19#1).
var secretFlags = []string{"--authkey=", "--auth-key="}

// shownArgs is args as an error may show them.
func shownArgs(args []string) []string {
	shown := slices.Clone(args)
	for i, arg := range shown {
		for _, flag := range secretFlags {
			if strings.HasPrefix(arg, flag) {
				shown[i] = flag + "<redacted>"
			}
		}
	}
	return shown
}
