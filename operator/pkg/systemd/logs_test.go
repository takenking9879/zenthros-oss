// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"testing"
)

func TestLogStreamerBuildArgs(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		unit      string
		follow    bool
		tailLines int
		wantArgs  []string
	}{
		{
			name:      "system scope follow with tail",
			cfg:       Config{Scope: ScopeSystem, UseSudo: false},
			unit:      "zenthros-abc.service",
			follow:    true,
			tailLines: 100,
			wantArgs:  []string{"-u", "zenthros-abc.service", "--output=cat", "--no-pager", "-f", "-n", "100"},
		},
		{
			name:      "user scope no follow all lines",
			cfg:       Config{Scope: ScopeUser, UseSudo: false},
			unit:      "zenthros-xyz.service",
			follow:    false,
			tailLines: -1,
			wantArgs:  []string{"-u", "zenthros-xyz.service", "--output=cat", "--no-pager", "--user"},
		},
		{
			name:      "system scope no follow zero tail",
			cfg:       Config{Scope: ScopeSystem, UseSudo: true},
			unit:      "test.service",
			follow:    false,
			tailLines: 0,
			wantArgs:  []string{"-u", "test.service", "--output=cat", "--no-pager", "-n", "0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewLogStreamer(tt.cfg, nil)
			got := s.buildArgs(tt.unit, tt.follow, tt.tailLines)
			if len(got) != len(tt.wantArgs) {
				t.Fatalf("buildArgs() = %v, want %v", got, tt.wantArgs)
			}
			for i, arg := range got {
				if arg != tt.wantArgs[i] {
					t.Errorf("buildArgs()[%d] = %q, want %q", i, arg, tt.wantArgs[i])
				}
			}
		})
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
