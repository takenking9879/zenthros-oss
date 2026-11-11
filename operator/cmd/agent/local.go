// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/zenthros/operator/agent"
	"github.com/zenthros/operator/internal/envconf"
)

const (
	// envLocalSocket says where a local agent listens, for itself and for
	// `zenthros-agent local`.
	envLocalSocket = "ZENTHROS_LOCAL_SOCKET"
	// envLocalIdleExit is how long a local agent in a container stays with
	// no work: past it the agent exits and the container stops, to be
	// started again by the next command. "0" never exits.
	envLocalIdleExit     = "ZENTHROS_LOCAL_IDLE_EXIT"
	defaultLocalIdleExit = 30 * time.Minute
)

// localSocket is the command socket: under the state directory, in a
// directory of its own only its owner reaches.
func localSocket() string {
	return envconf.Or(envLocalSocket, filepath.Join(containerStateDir(), "local", "agent.sock"))
}

// localStateDir is what a local agent keeps for the controller it does not
// have: the acked checkpoints.
func localStateDir(string) string {
	return filepath.Join(containerStateDir(), "local")
}

// applyLocalDefaults leaves a local agent on no network of its own: no
// probes, no metrics, no notices from a cloud. Its reports go to its spool
// and nowhere else.
func applyLocalDefaults(cfg *agent.Config) {
	if os.Getenv("ZENTHROS_AGENT_MODE") == agentModeContainer {
		applyContainerDefaults(cfg)
	}
	cfg.BackendID = agent.LocalWorkspace
	cfg.HealthAddr, cfg.MetricsAddr = "", ""
	cfg.SkipMetadataNotices = true
}
