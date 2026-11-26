// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"strings"
	"testing"
	"time"

	"github.com/takenking9879/zenthros-oss/operator/pkg/jobspec"
)

// A container job's unit removes the job's own network when it stops,
// after recording how it ended: the run script's net-down mode. A plain
// systemd job has no such line.
func TestContainerUnitRemovesItsNetworkOnStop(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := jobspec.JobSpec{ID: "job-1", Name: "j", Entrypoint: "true"}
	unit := RenderUnitWithWrapper(spec, nil, cfg, "", "/var/lib/zenthros/jobs/job-1/"+ContainerRunScript)
	record := strings.Index(unit, "ExecStopPost=-+/bin/sh")
	down := strings.Index(unit, `ExecStopPost=-"/var/lib/zenthros/jobs/job-1/ctr-run" `+ContainerNetDownArg)
	if record < 0 || down < 0 || down < record {
		t.Fatalf("record at %d, net-down at %d, want net-down after:\n%s", record, down, unit)
	}
	if plain := RenderUnit(spec, nil, cfg, ""); strings.Contains(plain, ContainerNetDownArg) {
		t.Fatalf("plain unit:\n%s", plain)
	}
}

// A container job's unit stops its task itself, before ctr is signalled,
// within a stop that fits the controller's 30 s Cancel: systemd's own
// SIGTERM reached only ctr and its SIGKILL, 90 s later, never the task, so a
// job that ignored SIGTERM ran on after its timeout (38). A host job's unit
// is systemd's to stop: no such lines.
func TestContainerUnitStopsItsTaskItself(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := jobspec.JobSpec{ID: "job-1", Name: "j", Entrypoint: "true"}
	unit := RenderUnitWithWrapper(spec, nil, cfg, "", "/var/lib/zenthros/jobs/job-1/"+ContainerRunScript)
	for _, line := range []string{
		`ExecStop="/var/lib/zenthros/jobs/job-1/ctr-run" ` + ContainerStopArg + "\n",
		"TimeoutStopSec=" + containerStopTimeout + "\n",
	} {
		if !strings.Contains(unit, line) {
			t.Fatalf("container unit without %q:\n%s", line, unit)
		}
	}
	if timeout, err := time.ParseDuration(containerStopTimeout); err != nil || timeout >= 30*time.Second {
		t.Fatalf("TimeoutStopSec=%s does not fit a 30 s Cancel", containerStopTimeout)
	}
	if plain := RenderUnit(spec, nil, cfg, ""); strings.Contains(plain, "ExecStop=") || strings.Contains(plain, "TimeoutStopSec") {
		t.Fatalf("plain unit:\n%s", plain)
	}
}

// The secrets wrapper lives on tmpfs (/dev/shm or /run), which a container
// host mounts noexec: started directly it is systemd's 203/EXEC (found on a
// Floci machine, 2026-10-16). The shell reads it instead, so it never needs
// to be executable where it lives; the job's argv follows it unchanged.
func TestTheSecretsWrapperIsReadByTheShellNotExecuted(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := jobspec.JobSpec{ID: "job-1", Name: "j", Command: []string{"python3", "-c", "print(1)"}}
	wrapper := "/dev/shm/zenthros/secrets/job-1/secret-env-wrapper"
	unit := RenderUnitWithWrapper(spec, nil, cfg, "", wrapper)
	var start string
	for _, line := range strings.Split(unit, "\n") {
		if strings.HasPrefix(line, "ExecStart=") {
			start = strings.TrimPrefix(line, "ExecStart=")
		}
	}
	plain := execStartCommand(spec)
	if start != `/bin/sh "`+wrapper+`" `+plain {
		t.Fatalf("ExecStart=%s\nwant /bin/sh %q %s", start, wrapper, plain)
	}
	// The container run script is the unit's own, made executable where
	// it lives: it stays the program.
	ctr := RenderUnitWithWrapper(spec, nil, cfg, "", "/var/lib/zenthros/jobs/job-1/"+ContainerRunScript)
	if !strings.Contains(ctr, `ExecStart="/var/lib/zenthros/jobs/job-1/ctr-run" `+plain+"\n") {
		t.Fatalf("container unit:\n%s", ctr)
	}
}
