// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/base64"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/agent/awssecret"
	"github.com/zenthros/operator/agent/executortest"
	"github.com/zenthros/operator/pkg/systemd"
)

// fakeSystemd is a machine's systemd as the executor drives it: unit files
// written and removed, units enabled and disabled, and what show reports.
type fakeSystemd struct {
	mu     sync.Mutex
	files  map[string]string
	active map[string]bool
}

var (
	unitWrite = regexp.MustCompile(`printf '%s' '([A-Za-z0-9+/=]+)'.*"(/etc/systemd/system/[^"]+)"`)
	unitPath  = regexp.MustCompile(`"(/etc/systemd/system/[^"]+\.service)"`)
)

func (s *fakeSystemd) Run(_ context.Context, cmd string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unit := func(verb string) string {
		_, rest, _ := strings.Cut(cmd, verb+" ")
		return strings.Fields(rest)[0]
	}
	switch {
	case unitWrite.MatchString(cmd):
		m := unitWrite.FindStringSubmatch(cmd)
		body, _ := base64.StdEncoding.DecodeString(m[1])
		s.files[m[2]] = string(body)
	case strings.Contains(cmd, "cat ") && unitPath.MatchString(cmd):
		body, ok := s.files[unitPath.FindStringSubmatch(cmd)[1]]
		if !ok {
			return "", errors.New("cat: No such file or directory")
		}
		return body, nil
	case strings.Contains(cmd, "rm -f ") && unitPath.MatchString(cmd):
		delete(s.files, unitPath.FindStringSubmatch(cmd)[1])
	case strings.Contains(cmd, "enable --now"):
		s.active[unit("enable --now")] = true
	case strings.Contains(cmd, "disable --now"):
		delete(s.active, unit("disable --now"))
	case strings.Contains(cmd, "systemctl show"):
		name := unit("show")
		if _, ok := s.files["/etc/systemd/system/"+name]; !ok {
			return "LoadState=not-found\nActiveState=inactive\nSubState=dead\n", nil
		}
		if s.active[name] {
			return "LoadState=loaded\nActiveState=active\nSubState=running\n", nil
		}
		return "LoadState=loaded\nActiveState=inactive\nSubState=dead\n", nil
	case strings.Contains(cmd, "test -e"):
		return "", errors.New("test: no such file")
	case strings.Contains(cmd, "ls -1 "):
		var units []string
		for path := range s.files {
			units = append(units, filepath.Base(path))
		}
		return strings.Join(units, "\n"), nil
	}
	return "", nil
}

// end is the unit's process ending on its own: installed, and not active.
func (s *fakeSystemd) end(unit string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active, unit)
}

func TestSystemdKeepsTheExecutorContract(t *testing.T) {
	var machine *fakeSystemd
	executortest.Contract(t, executortest.Harness{
		New: func(t *testing.T) executortest.Executor {
			machine = &fakeSystemd{files: map[string]string{}, active: map[string]bool{}}
			cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
			return SystemdExecutor{Manager: systemd.NewManager(cfg, machine, logr.Discard()), Secrets: awssecret.Config{Root: t.TempDir()}}
		},
		Spec:    vmTestSpec(),
		End:     func(_ *testing.T, _ executortest.Executor, handle string) { machine.end(handle) },
		Missing: systemd.UnitName("never-submitted"),
	})
}
