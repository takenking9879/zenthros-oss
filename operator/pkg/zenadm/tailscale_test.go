// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"strings"
	"testing"
)

// A joined VM's tailnet device is named after its backend: with the OS's
// name (ubuntu), cleanup for a backend named ubuntu took it for its own (N1).
func TestAJoinedDeviceIsNamedAfterItsBackend(t *testing.T) {
	osName := func() (string, error) { return "ubuntu", nil }
	if name, rename, err := deviceHostname("gpu-7", osName); err != nil || name != "gpu-7" || rename {
		t.Fatalf("with an ID: %q rename=%v err=%v, want gpu-7 and no rename", name, rename, err)
	}
	if name, rename, err := deviceHostname("", osName); err != nil || name != "ubuntu" || !rename {
		t.Fatalf("short join: %q rename=%v err=%v, want the OS name until enrolled, then a rename", name, rename, err)
	}
}

// A command that fails shows its flags, never a credential's value: the
// error goes to the console and to cloud-init's log (19#1).
func TestAFailedCommandNeverShowsItsAuthKey(t *testing.T) {
	err := runCmd(context.Background(), "false", "tailscale", "up", "--authkey=tskey-auth-secret", "--hostname=vm-1")
	if err == nil {
		t.Fatal("false succeeded")
	}
	if strings.Contains(err.Error(), "tskey-auth-secret") || !strings.Contains(err.Error(), "--authkey=<redacted>") || !strings.Contains(err.Error(), "--hostname=vm-1") {
		t.Fatalf("error %q: want the key redacted and the other flags shown", err)
	}
}
