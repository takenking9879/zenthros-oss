// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/jobspec"
)

// The unit writes its log itself, to a file the agent reads: no journald on
// the way (P10), and no systemd lines in the job's log.
func TestTheUnitWritesItsLogToAFile(t *testing.T) {
	for _, tt := range []struct {
		cfg  Config
		path string
	}{
		{Config{Scope: ScopeSystem}, "/var/lib/zenthros/logs/zenthros-job-1.service.log"},
		{Config{Scope: ScopeUser}, "%h/.local/share/zenthros/logs/zenthros-job-1.service.log"},
	} {
		unit := renderUnit(jobspec.JobSpec{ID: "job-1", Name: "j", Command: []string{"/bin/true"}}, nil, tt.cfg, "", "", nil)
		for _, want := range []string{"StandardOutput=append:" + tt.path + "\n", "StandardError=append:" + tt.path + "\n"} {
			if !strings.Contains(unit, want) {
				t.Errorf("%s unit lacks %q:\n%s", tt.cfg.Scope, want, unit)
			}
		}
	}
}

func TestUnitTerminal(t *testing.T) {
	tests := []struct {
		name       string
		showOutput string
		want       bool
	}{
		{"running", "ActiveState=active\nSubState=running\nResult=success\n", false},
		{"succeeded", "ActiveState=inactive\nResult=success\n", true},
		{"failed", "ActiveState=failed\nResult=exit-code\n", true},
		{"awaiting-first-start", "ActiveState=inactive\nResult=\n", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unitEnded(ParseShow(tt.showOutput)); got != tt.want {
				t.Errorf("unitEnded() = %v, want %v", got, tt.want)
			}
		})
	}
}

// The log file is made before the unit starts, in both starts (a new unit
// and an installed one that never ran): systemd fails a start whose log
// directory is missing, and would make the file 0644.
func TestTheLogIsMadeBeforeTheStart(t *testing.T) {
	spec := jobspec.JobSpec{ID: "uid-log", Name: "j", Command: []string{"/bin/true"}}
	for _, installed := range []bool{false, true} {
		cfg := Config{Scope: ScopeSystem, UseSudo: true}
		runner := &fakeRunner{}
		if installed {
			runner.unitContent = RenderUnit(spec, nil, cfg, "")
		}
		if _, err := NewManager(cfg, runner, logr.Discard()).Install(context.Background(), spec, nil); err != nil {
			t.Fatalf("Install: %v", err)
		}
		made, started := -1, -1
		for i, cmd := range runner.commands {
			if made < 0 && strings.Contains(cmd, "umask 077 && : >>") && strings.Contains(cmd, LogPath(cfg, "zenthros-uid-log.service")) {
				made = i
			}
			if started < 0 && strings.Contains(cmd, "enable --now") {
				started = i
			}
		}
		if made < 0 || started < 0 || made > started {
			t.Fatalf("installed=%v: the log made at %d, the unit started at %d:\n%s", installed, made, started, strings.Join(runner.commands, "\n"))
		}
	}
}
