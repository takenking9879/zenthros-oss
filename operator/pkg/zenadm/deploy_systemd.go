// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zenthros/operator/pkg/systemd"
)

const agentUnitName = "zenthros-agent.service"

func deploySystemd(p JoinParams) error {
	agentBin, err := resolveAgentBinary()
	if err != nil {
		return err
	}
	unit := renderAgentUnit(p, agentBin)

	tmp, err := os.CreateTemp("", "zenthros-agent-unit-")
	if err != nil {
		return fmt.Errorf("create temp unit file: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(unit); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp unit file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp unit file: %w", err)
	}

	ctx := context.Background()
	unitPath := "/etc/systemd/system/" + agentUnitName
	if err := runCmd(ctx, "sudo", "cp", tmp.Name(), unitPath); err != nil {
		return fmt.Errorf("install unit %s: %w", unitPath, err)
	}
	if err := runCmd(ctx, "sudo", "systemctl", "daemon-reload"); err != nil {
		return fmt.Errorf("daemon-reload: %w", err)
	}
	if err := runCmd(ctx, "sudo", "systemctl", "enable", "--now", agentUnitName); err != nil {
		return fmt.Errorf("start %s: %w", agentUnitName, err)
	}
	return nil
}

func resolveAgentBinary() (string, error) {
	if path := os.Getenv("ZENTHROS_AGENT_BINARY"); path != "" {
		return filepath.Abs(path)
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve agent binary (set ZENTHROS_AGENT_BINARY): %w", err)
	}
	dir := filepath.Dir(exe)
	// install-agent.sh puts zenthros-agent beside zenadm; a build tree, agent.
	for _, name := range []string{"zenthros-agent", "agent"} {
		candidate := filepath.Join(dir, name)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("agent binary not found next to zenadm (set ZENTHROS_AGENT_BINARY)")
}

// agentConfinement holds the agent, root, to what it does: it writes job
// units and their users (/etc), the machine's state (/var/lib/zenthros),
// toolchains (/opt/zenthros) and job networks, and powers the machine off.
// It reads no home (an empty one of its own, ProtectHome=tmpfs,
// systemd.AgentHomes), and /usr and /boot are read-only (ProtectSystem=yes:
// =full makes /etc read-only, where useradd writes). Its /tmp is the
// machine's: a job's working directory there, and the file secret the agent
// links in it, must be where the job, started in the machine's own /tmp,
// looks (PrivateTmp made it CHDIR-fail, full suite of 2026-11-08). Not a user
// of its own (systemctl --user): a job running as the agent's user could
// read its mTLS key.
const agentConfinement = `ProtectSystem=yes
ProtectHome=tmpfs
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
RestrictRealtime=yes
LockPersonality=yes`

// foldersLine opens the machine's folders to jobs (systemd.HostRootsEnv), a
// quoted assignment as a folder may hold a space, or no line. Each is also
// bound into the agent's own view as its jobs see it, writable: its /home is
// an empty one (agentConfinement), and there it resolves a job's hostPath
// before the job or containerd mounts it, and links a file secret in a job's
// working directory. "-": a folder not made yet does not stop the agent; a
// job naming it is refused as one not there.
func foldersLine(folders []string) string {
	if len(folders) == 0 {
		return ""
	}
	line := `Environment="` + systemd.HostRootsEnv + "=" + strings.ReplaceAll(strings.Join(folders, ":"), "%", "%%") + "\"\n"
	for _, folder := range folders {
		line += `BindPaths="-` + strings.ReplaceAll(folder, "%", "%%") + "\"\n"
	}
	return line
}

func renderAgentUnit(p JoinParams, agentBin string) string {
	agentMode := "vm"
	if p.BackendType == "k8s" {
		agentMode = "k8s"
	}
	ownership := p.Ownership
	if ownership == "" {
		ownership = "External"
	}
	network := p.Network
	if network == "" {
		network = NetworkTailscale
	}
	tlsServerName := p.TLSServerName
	if tlsServerName == "" {
		tlsServerName = hostOf(p.ControllerAddr)
	}
	return `[Unit]
Description=Zenthros agent (backend ` + p.BackendID + `)
After=network-online.target tailscaled.service
Wants=network-online.target

[Service]
Type=simple
Environment=ZENTHROS_CONTROLLER_ADDR=` + p.ControllerAddr + `
Environment=ZENTHROS_BACKEND_ID=` + p.BackendID + `
Environment=ZENTHROS_BACKEND_TYPE=` + p.BackendType + `
Environment=ZENTHROS_BACKEND_OWNERSHIP=` + ownership + `
Environment=ZENTHROS_AGENT_MODE=` + agentMode + `
Environment=ZENTHROS_NETWORK=` + network + `
Environment=ZENTHROS_TLS_CERT=` + filepath.Join(p.StateDir, "agent.crt") + `
Environment=ZENTHROS_TLS_KEY=` + filepath.Join(p.StateDir, "agent.key") + `
Environment=ZENTHROS_TLS_CA=` + filepath.Join(p.StateDir, "ca.crt") + `
Environment=ZENTHROS_TLS_SERVER_NAME=` + tlsServerName + `
Environment=ZENTHROS_SYSTEMD_SCOPE=system
Environment=ZENTHROS_USE_SUDO=true
Environment=ZENTHROS_HEARTBEAT_INTERVAL=15s
Environment=ZENTHROS_AGENT_HEALTH_ADDR=` + agentHealthAddr + `
` + foldersLine(p.Folders) + `ExecStart=` + agentBin + `
Restart=always
RestartSec=5
RestartPreventExitStatus=78
CPUAccounting=yes
CPUQuota=50%
` + agentConfinement + `

[Install]
WantedBy=multi-user.target
`
}
