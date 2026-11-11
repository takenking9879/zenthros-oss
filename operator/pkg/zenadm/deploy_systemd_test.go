// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/zenthros/operator/agent"
	"github.com/zenthros/operator/pkg/systemd"
)

func TestRenderAgentUnitMarksManualJoinExternal(t *testing.T) {
	unit := renderAgentUnit(JoinParams{
		BackendID:      "vm-manual",
		BackendType:    "vm",
		ControllerAddr: "controller:9443",
		StateDir:       "/var/lib/zenthros",
	}, "/usr/local/bin/zenthros-agent")
	if !strings.Contains(unit, "Environment=ZENTHROS_BACKEND_OWNERSHIP=External") {
		t.Fatalf("manual unit does not pin External ownership:\n%s", unit)
	}
	if !strings.Contains(unit, "Environment=ZENTHROS_HEARTBEAT_INTERVAL=15s") {
		t.Fatalf("manual unit does not set heartbeat interval:\n%s", unit)
	}
	if !strings.Contains(unit, "CPUQuota=50%") {
		t.Fatalf("agent unit must cap at 0.5 CPU, not 1:\n%s", unit)
	}
	if strings.Contains(unit, "CPUQuota=100%") {
		t.Fatalf("agent CPUQuota must be 50 percent of one CPU:\n%s", unit)
	}
}

func TestRenderAgentUnitUsesExplicitTLSServerName(t *testing.T) {
	unit := renderAgentUnit(JoinParams{
		BackendID:      "vm-managed",
		BackendType:    "vm",
		ControllerAddr: "controller.tailnet.example:9443",
		TLSServerName:  "zenthros-controller",
		StateDir:       "/var/lib/zenthros",
	}, "/usr/local/bin/zenthros-agent")
	if !strings.Contains(unit, "Environment=ZENTHROS_TLS_SERVER_NAME=zenthros-controller") {
		t.Fatalf("unit does not use explicit TLS server name:\n%s", unit)
	}
}

// The agent's network mode comes from --network, not from whether zenadm
// set up Tailscale: managed bootstrap brings Tailscale up itself and joins
// without --tailscale, and its agent must still be a Tailscale agent. Only an
// explicit --network direct skips the Tailscale wait on every (re)connect.
func TestRenderAgentUnitSetsNetworkFromJoin(t *testing.T) {
	for _, tc := range []struct {
		network string
		want    string
	}{
		{"", "Environment=ZENTHROS_NETWORK=tailscale"},
		{NetworkTailscale, "Environment=ZENTHROS_NETWORK=tailscale"},
		{NetworkDirect, "Environment=ZENTHROS_NETWORK=direct"},
	} {
		unit := renderAgentUnit(JoinParams{
			BackendID:      "vm-1",
			BackendType:    "vm",
			ControllerAddr: "controller:9443",
			StateDir:       "/var/lib/zenthros",
			Network:        tc.network,
		}, "/usr/local/bin/agent")
		if !strings.Contains(unit, tc.want) {
			t.Fatalf("network=%q: unit lacks %q:\n%s", tc.network, tc.want, unit)
		}
	}
}

func TestJoinRejectsUnknownOrContradictoryNetwork(t *testing.T) {
	base := JoinParams{ControllerAddr: "c:9443", Token: "t", CACertHash: "sha256:x", BackendID: "vm-1", DeployMode: DeploySystemd}
	for _, tc := range []struct {
		name string
		edit func(*JoinParams)
		ok   bool
	}{
		{"default", func(*JoinParams) {}, true},
		{"direct", func(p *JoinParams) { p.Network = NetworkDirect }, true},
		{"unknown", func(p *JoinParams) { p.Network = "wireguard" }, false},
		{"direct with tailscale", func(p *JoinParams) { p.Network = NetworkDirect; p.Tailscale = true; p.TSAuthKey = "k" }, false},
		// No address in it: the wait for the controller indexed an empty
		// list and panicked (code review 2026-11-08).
		{"a controller list with no address", func(p *JoinParams) { p.ControllerAddr = " , " }, false},
		{"a controller list", func(p *JoinParams) { p.ControllerAddr = "c:9443,c:443" }, true},
	} {
		p := base
		tc.edit(&p)
		if err := p.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

// A detached agent exits agent.ExitDetached and systemd must not start it
// again: the hub would refuse every run (lab, 2026-10-18).
func TestAgentUnitDoesNotRestartADetachedAgent(t *testing.T) {
	want := "RestartPreventExitStatus=" + strconv.Itoa(agent.ExitDetached)
	unit := renderAgentUnit(JoinParams{BackendID: "vm-a", BackendType: "vm", ControllerAddr: "controller:9443", StateDir: "/var/lib/zenthros"},
		"/usr/local/bin/zenthros-agent")
	if !strings.Contains(unit, "Restart=always") || !strings.Contains(unit, want) {
		t.Fatalf("the agent unit lacks %s:\n%s", want, unit)
	}
}

// The folders the owner opens reach the agent as its HostRootsEnv, quoted:
// a folder may hold a space. Each is bound into the agent's view too, as its
// jobs see it, writable: under ProtectHome=tmpfs a folder in /home was not
// there for it, and a job's hostPath in it failed to resolve (code review
// 2026-11-08); read-only, it could not link a job's file secret in a working
// directory there. None, no line.
func TestJoinOpensOnlyTheFoldersItIsGiven(t *testing.T) {
	p := JoinParams{BackendID: "vm-1", BackendType: "vm", ControllerAddr: "hub:9443", StateDir: "/var/lib/zenthros",
		Folders: []string{"/home/u/data", "/srv/my set 100%"}}
	unit := renderAgentUnit(p, "/usr/local/bin/zenthros-agent")
	for _, want := range []string{"Environment=\"ZENTHROS_HOSTPATH_ROOTS=/home/u/data:/srv/my set 100%%\"\n",
		"BindPaths=\"-/home/u/data\"\n", "BindPaths=\"-/srv/my set 100%%\"\n"} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit does not open the folders (%s):\n%s", want, unit)
		}
	}
	p.Folders = nil
	if unit := renderAgentUnit(p, "/usr/local/bin/zenthros-agent"); strings.Contains(unit, "HOSTPATH_ROOTS") || strings.Contains(unit, "BindPaths") {
		t.Fatalf("a join with no folder opens one:\n%s", unit)
	}
	for _, bad := range []string{"data", "/", "/data/", "/a:/b", `/a"b`, `/a\b`, "/a\nb", "/a/../b"} {
		if ValidFolder(bad) == nil {
			t.Errorf("ValidFolder(%q) took it", bad)
		}
	}
	for _, good := range []string{"/data", "/srv/my set", "/mnt/disk-2/runs"} {
		if err := ValidFolder(good); err != nil {
			t.Errorf("ValidFolder(%q): %v", good, err)
		}
	}
	p.Folders = []string{"/"}
	p.Token, p.CACertHash, p.DeployMode = "t", "sha256:x", DeploySystemd
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "whole machine") {
		t.Fatalf("join --folder / validated: %v", err)
	}
}

// The agent stays root, held to what it does: no home of anyone's, /usr
// and /boot read-only, no kernel modules or logs. /etc stays writable:
// useradd makes a Workspace's job user there. Its /tmp is the machine's: with
// a private one, a job's working directory there and the file secret linked
// in it were the agent's alone, and the job failed 200/CHDIR (full suite of
// 2026-11-08). What it hides behind ProtectHome=tmpfs is systemd.AgentHomes.
func TestTheAgentsUnitHoldsIt(t *testing.T) {
	unit := renderAgentUnit(JoinParams{BackendID: "vm-1", BackendType: "vm", ControllerAddr: "hub:9443", StateDir: "/var/lib/zenthros",
		Folders: []string{"/home/u/my data"}}, "/usr/local/bin/zenthros-agent")
	for _, want := range []string{"ProtectSystem=yes\n", "ProtectHome=tmpfs\n", "ProtectKernelModules=yes\n",
		"ProtectKernelLogs=yes\n", "ProtectControlGroups=yes\n", "RestrictRealtime=yes\n", "LockPersonality=yes\n"} {
		if !strings.Contains(unit, want) {
			t.Errorf("agent unit has no %q:\n%s", want, unit)
		}
	}
	for _, never := range []string{"ProtectSystem=full", "ProtectSystem=strict", "User=", "NoNewPrivileges"} {
		if strings.Contains(unit, never) {
			t.Errorf("agent unit has %q: it could not make job users or sudo:\n%s", never, unit)
		}
	}
	for _, never := range []string{"PrivateTmp", "BindReadOnlyPaths", "ProtectHome=yes", "ProtectHome=read-only"} {
		if strings.Contains(unit, never) {
			t.Errorf("agent unit has %q: what it makes for a job would not be where the job looks:\n%s", never, unit)
		}
	}
	if !slices.Equal(systemd.AgentHomes, []string{"/home", "/root", "/run/user"}) {
		t.Errorf("systemd.AgentHomes = %v, want what ProtectHome=tmpfs hides: /home, /root, /run/user", systemd.AgentHomes)
	}
	analyze, err := exec.LookPath("systemd-analyze")
	if err != nil {
		t.Skip("systemd-analyze not installed")
	}
	path := filepath.Join(t.TempDir(), agentUnitName)
	if err := os.WriteFile(path, []byte(unit), 0o644); err != nil {
		t.Fatal(err)
	}
	out, _ := exec.Command(analyze, "verify", "--man=no", path).CombinedOutput()
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, path+":") {
			t.Errorf("systemd-analyze verify: %s", line)
		}
	}
}
