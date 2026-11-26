// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"os"
	"path/filepath"
)

// A job's log is a file its unit writes itself (StandardOutput=append:): the
// job's stdout and stderr as they come, through no other process. The agent
// reads it (agent/logfile) and frees its oldest part past a cap. journald is
// not in the way: it cost 40-56 mCPU to keep 145 KB/s, and dropped lines past
// ~333 a second (P10).

// LogSuffix ends the name of every job unit's log file: <unit>.log.
const LogSuffix = ".log"

// logDir is where the job units' logs are, as a shell path expression. Only
// root reads a system scope's.
func logDir(cfg Config) string {
	if cfg.Scope == ScopeUser {
		return "$HOME/.local/share/zenthros/logs"
	}
	return "/var/lib/zenthros/logs"
}

// LogPath is a unit's log file, as a shell path expression.
func LogPath(cfg Config, name string) string {
	return logDir(cfg) + "/" + name + LogSuffix
}

// unitLogPath is LogPath as the unit file names it: systemd expands %h, not
// $HOME.
func unitLogPath(cfg Config, name string) string {
	if cfg.Scope == ScopeUser {
		return "%h/.local/share/zenthros/logs/" + name + LogSuffix
	}
	return LogPath(cfg, name)
}

// LogDir is the directory of the job units' logs, for a reader on this host.
func (c Config) LogDir() (string, error) {
	if c.Scope == ScopeUser {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, ".local/share/zenthros/logs"), nil
	}
	return logDir(c), nil
}
