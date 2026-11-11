// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/shellword"
)

func hostJob() jobspec.JobSpec {
	spec := gitSpec()
	spec.Workspace = "team-a"
	spec.Env = map[string]string{"TOKEN": "s3cr3t value"}
	return spec
}

// envFileFromInstall is the environment file an Install wrote.
func envFileFromInstall(t *testing.T, commands []string) string {
	t.Helper()
	for _, cmd := range commands {
		if !strings.Contains(cmd, ".service.env") {
			continue
		}
		_, rest, ok := strings.Cut(cmd, "printf '%s' '")
		if !ok {
			continue
		}
		payload, _, _ := strings.Cut(rest, "'")
		raw, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			t.Fatalf("decode env file: %v", err)
		}
		return string(raw)
	}
	t.Fatalf("Install wrote no environment file; ran %v", commands)
	return ""
}

func unitLines(unit string) map[string][]string {
	lines := map[string][]string{}
	for _, line := range strings.Split(unit, "\n") {
		if key, value, ok := strings.Cut(line, "="); ok && !strings.HasPrefix(line, "#") {
			lines[key] = append(lines[key], value)
		}
	}
	return lines
}

func TestAWorkspacesJobUserIsItsNamesHash(t *testing.T) {
	a, b := JobUser("team-a"), JobUser("team-b")
	if !regexp.MustCompile(`^zj-[0-9a-f]{8}$`).MatchString(a) || a == b || a != JobUser("team-a") {
		t.Fatalf("JobUser(team-a) = %q, JobUser(team-b) = %q: want zj- and 8 hex, one per Workspace", a, b)
	}
	// useradd takes at most 32 characters, and systemd's User= the same.
	if len(a) > 32 {
		t.Fatalf("%q is longer than a user name may be", a)
	}
}

// A host job runs as its Workspace's user, with nothing to raise and the
// machine read-only but its own directories, its Workspace's environments,
// the machine's /tmp and /var/tmp, and the folders the owner opened.
func TestAHostJobRunsAsItsWorkspacesUserAndWritesOnlyItsOwn(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: true, HostRoots: []string{"/data", "/srv/my set"}}
	m := NewManager(cfg, runner, logr.Discard())
	spec := hostJob()
	if _, err := m.Install(context.Background(), spec, spec.Env); err != nil {
		t.Fatalf("Install: %v", err)
	}
	unit := unitBodyFromInstall(t, runner.commands)
	got := unitLines(unit)
	user := JobUser("team-a")
	want := map[string]string{
		"User": user, "Group": user, "NoNewPrivileges": "yes", "CapabilityBoundingSet": "", "AmbientCapabilities": "",
		"ProtectSystem": "strict", "ProtectHome": "read-only", "ProtectProc": "invisible",
		"RestrictSUIDSGID": "yes", "RestrictNamespaces": "yes", "ProtectKernelTunables": "yes",
		"ProtectKernelModules": "yes", "ProtectKernelLogs": "yes", "ProtectControlGroups": "yes",
	}
	for key, value := range want {
		if len(got[key]) != 1 || got[key][0] != value {
			t.Errorf("%s = %q, want %q", key, got[key], value)
		}
	}
	// PrivateDevices hides /dev/nvidia*; PrivateTmp would take a file://
	// checkpoint in /tmp away with the unit, before the next attempt reads it.
	for _, never := range []string{"PrivateDevices", "PrivateTmp"} {
		if _, ok := got[never]; ok {
			t.Errorf("a job unit with %s:\n%s", never, unit)
		}
	}
	work := IsolatedWorkDir("team-a", spec.ID, cfg)
	home := HomeDir("team-a", spec.ID, cfg)
	wantPaths := `"` + home + `" "` + work + `" "` + EnvsDir("team-a") + `" "/tmp" "/var/tmp" -"/data" -"/srv/my set"`
	if len(got["ReadWritePaths"]) != 1 || got["ReadWritePaths"][0] != wantPaths {
		t.Errorf("ReadWritePaths = %q, want %q", got["ReadWritePaths"], wantPaths)
	}
	if got["WorkingDirectory"][0] != work {
		t.Errorf("WorkingDirectory = %q, want the work directory %s", got["WorkingDirectory"], work)
	}
	// The result is recorded as root, outside the job's read-only machine.
	if !strings.HasPrefix(got["ExecStopPost"][0], "-+/bin/sh") {
		t.Errorf("the result record does not run as root: ExecStopPost=%s", got["ExecStopPost"][0])
	}
	// Its environment is in root's file, not in the unit anyone reads.
	if _, ok := got["Environment"]; ok || strings.Contains(unit, "s3cr3t") {
		t.Errorf("the unit carries the job's environment:\n%s", unit)
	}
	if got["EnvironmentFile"][0] != EnvFilePath(UnitName(spec.ID)) {
		t.Errorf("EnvironmentFile = %q", got["EnvironmentFile"])
	}
	env, err := systemdEnvFile(envFileFromInstall(t, runner.commands))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"TOKEN": "s3cr3t value", "HOME": home, jobspec.EnvEnvsDir: HostEnvsDir("team-a"), "XDG_CACHE_HOME": InstallCacheDir("team-a"),
	} {
		if env[name] != value {
			t.Errorf("environment %s = %q, want %q", name, env[name], value)
		}
	}
}

// A job's own HOME and cache are its to set.
func TestAHostJobKeepsTheHomeItSet(t *testing.T) {
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeSystem}, runner, logr.Discard())
	spec := hostJob()
	spec.Source = v1.Source{}
	env := map[string]string{"HOME": "/work/me", "XDG_CACHE_HOME": "/work/cache"}
	if _, err := m.Install(context.Background(), spec, env); err != nil {
		t.Fatalf("Install: %v", err)
	}
	got, err := systemdEnvFile(envFileFromInstall(t, runner.commands))
	if err != nil {
		t.Fatal(err)
	}
	if got["HOME"] != "/work/me" || got["XDG_CACHE_HOME"] != "/work/cache" {
		t.Fatalf("environment %q: the job's own HOME and cache were replaced", got)
	}
	// With nothing to run in, it runs in its home.
	if dir := unitLines(unitBodyFromInstall(t, runner.commands))["WorkingDirectory"]; len(dir) != 1 || dir[0] != HomeDir("team-a", spec.ID, Config{Scope: ScopeSystem}) {
		t.Fatalf("WorkingDirectory = %q, want its home", dir)
	}
}

// Order: the user exists before anything is given to it; the source is in
// place, as root, before its directory is given; all of it before the unit
// starts. The job directory itself stays root's: a job owning it could plant
// a link at the retain marker, which the agent writes as root.
func TestAJobsDirectoriesAreGivenAfterItsSourceAndBeforeItStarts(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	m := NewManager(cfg, runner, logr.Discard())
	spec := hostJob()
	spec.RetainWorkdir = true
	if _, err := m.Install(context.Background(), spec, spec.Env); err != nil {
		t.Fatalf("Install: %v", err)
	}
	at := func(substr string) int {
		for i, cmd := range runner.commands {
			if strings.Contains(cmd, substr) {
				return i
			}
		}
		t.Fatalf("no command with %q; ran %v", substr, runner.commands)
		return -1
	}
	user, clone := at("useradd"), at("git clone")
	work := at("chown -hR " + shellword.Quote(JobUser("team-a")+":"+JobUser("team-a")))
	envFile, start := at(".service.env"), at("enable --now")
	if !(user < work && clone < work && work < envFile && envFile < start) {
		t.Fatalf("order: useradd %d, clone %d, chown of the work directory %d, environment %d, start %d; want useradd and clone before the chown, all before the start",
			user, clone, work, envFile, start)
	}
	jobDir := IsolatedJobDir("team-a", spec.ID, cfg)
	for _, cmd := range runner.commands {
		if strings.Contains(cmd, "chown") && (strings.Contains(cmd, `'`+jobDir+`' `) || strings.HasSuffix(cmd, `'`+jobDir+`'`)) {
			t.Fatalf("the job directory itself was given to the job: %s", cmd)
		}
	}
	// The environments' directory is given, not what is in it.
	for _, cmd := range runner.commands {
		if strings.Contains(cmd, "chown -hR") && strings.Contains(cmd, EnvsDir("team-a")) {
			t.Fatalf("the Workspace's environments are walked on every job: %s", cmd)
		}
	}
	if !strings.Contains(runner.commands[at(".service.env")], "umask 077") || !strings.Contains(runner.commands[at(".service.env")], "mkdir -p -m 0700") {
		t.Fatalf("the environment file is written where a job may read it: %s", runner.commands[at(".service.env")])
	}
}

// A container's unit runs ctr as root; its process is held by the container.
// A user manager's unit is its user's: it keeps its Environment= lines.
func TestOnlyAHostJobOnTheSystemManagerIsHeldByItsUnit(t *testing.T) {
	spec := hostJob()
	ctr := renderUnit(spec, spec.Env, Config{Scope: ScopeSystem}, "", "/var/lib/zenthros/jobs/j/"+ContainerRunScript, nil)
	if got := unitLines(ctr); got["User"] != nil || got["Environment"] != nil || got["EnvironmentFile"] == nil {
		t.Errorf("container unit: User %q, Environment %q, EnvironmentFile %q", got["User"], got["Environment"], got["EnvironmentFile"])
	}
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeSystem}, runner, logr.Discard())
	if _, err := m.InstallWithWrapper(context.Background(), spec, spec.Env, "/var/lib/zenthros/workspaces/team-a/jobs/"+spec.ID+"/"+ContainerRunScript); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if unitLines(unitBodyFromInstall(t, runner.commands))["User"] != nil {
		t.Errorf("a container's unit runs as the job's user")
	}
	// Its work directory and environments are still the job's: its
	// process is that user in the container.
	if !runner.ran("chown -hR " + shellword.Quote(JobUser("team-a")+":"+JobUser("team-a")) + " '" + IsolatedWorkDir("team-a", spec.ID, Config{Scope: ScopeSystem})) {
		t.Errorf("the container job's work directory stays root's; ran %v", runner.commands)
	}
	user := renderUnit(spec, spec.Env, Config{Scope: ScopeUser}, "", "", nil)
	if got := unitLines(user); got["User"] != nil || got["EnvironmentFile"] != nil || len(got["Environment"]) != 1 {
		t.Errorf("user unit: User %q, EnvironmentFile %q, Environment %q", got["User"], got["EnvironmentFile"], got["Environment"])
	}
}

func TestUninstallRemovesTheEnvironmentFile(t *testing.T) {
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())
	if err := m.Uninstall(context.Background(), "zenthros-uid-123.service"); err != nil {
		t.Fatal(err)
	}
	if !runner.ran(`rm -f "/etc/systemd/system/zenthros-uid-123.service" "` + EnvFilePath("zenthros-uid-123.service") + `"`) {
		t.Fatalf("the environment file stays after its unit; ran %v", runner.commands)
	}
}

// A hostPath is one of the folders the machine's owner opened, resolved:
// the unit runs in what was checked, not in a link swapped in later.
func TestAHostPathIsOneOfTheOpenFolders(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data")
	outside := filepath.Join(root, "outside")
	for _, dir := range []string{filepath.Join(data, "set"), outside} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(data, "link")); err != nil {
		t.Fatal(err)
	}
	install := func(roots []string, hostPath string) (string, error) {
		runner := &fakeRunner{}
		m := NewManager(Config{Scope: ScopeSystem, HostRoots: roots}, runner, logr.Discard())
		spec := hostJob()
		spec.Source = v1.Source{HostPath: hostPath}
		if _, err := m.Install(context.Background(), spec, nil); err != nil {
			return "", err
		}
		return unitLines(unitBodyFromInstall(t, runner.commands))["WorkingDirectory"][0], nil
	}
	if _, err := install(nil, filepath.Join(data, "set")); !errors.Is(err, backend.ErrRejected) {
		t.Errorf("a hostPath on a machine that opened no folder: %v, want ErrRejected", err)
	}
	if _, err := install([]string{data}, filepath.Join(data, "link")); !errors.Is(err, backend.ErrRejected) {
		t.Errorf("a link out of the open folder: %v, want ErrRejected", err)
	}
	dir, err := install([]string{data}, filepath.Join(data, "set"))
	if err != nil || dir != filepath.Join(data, "set") {
		t.Errorf("a hostPath in the open folder: WorkingDirectory %q, %v", dir, err)
	}
}

// The agent sees the machine as its jobs do everywhere but the homes it is
// given an empty one of (ProtectHome=tmpfs), where only the open folders are
// bound in: the machine's /tmp is its jobs' (no PrivateTmp).
func TestTheAgentSeesWhatItsJobsSeeButTheHomes(t *testing.T) {
	folders := []string{"/home/u/data", "/srv/sets"}
	for path, want := range map[string]bool{
		"/tmp/work/conf/token":     true,
		"/var/tmp/x":               true,
		"/srv/proj":                true,
		"/var/lib/zenthros/jobs/a": true,
		"/home/u/data/run/token":   true,
		"/home/u/data":             true,
		"/home/u/data/../notes":    false,
		"/home/u/notes/token":      false,
		"/home/u/database":         false,
		"/root/.aws/credentials":   false,
		"/run/user/1000/token":     false,
		"/home":                    false,
	} {
		if got := AgentSees(path, folders); got != want {
			t.Errorf("AgentSees(%q) = %v, want %v", path, got, want)
		}
	}
}

// A job's ID and Workspace name its unit's directories and the paths it may
// write: a spec that names a path there, from a hub that is not what it
// says, is refused before anything is made of it.
func TestAJobIDOrWorkspaceThatIsAPathIsRefused(t *testing.T) {
	for _, spec := range []jobspec.JobSpec{
		{ID: ""}, {ID: ".."}, {ID: "../../etc"}, {ID: "a/b"}, {ID: "x y"},
		{ID: "uid-1", Workspace: "../etc"}, {ID: "uid-1", Workspace: `team" "/etc`}, {ID: "uid-1", Workspace: "Team_A"},
	} {
		if err := ValidateUnitInputs(spec, nil); !errors.Is(err, backend.ErrInvalid) {
			t.Errorf("id %q workspace %q: %v, want ErrInvalid", spec.ID, spec.Workspace, err)
		}
	}
	for _, spec := range []jobspec.JobSpec{{ID: "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0-a2", Workspace: "team-a"}, {ID: "uid-1"}} {
		if err := ValidateUnitInputs(spec, nil); err != nil {
			t.Errorf("id %q workspace %q refused: %v", spec.ID, spec.Workspace, err)
		}
	}
}

// slowUsers counts how many useradd run at once.
type slowUsers struct {
	fakeRunner
	running, most atomic.Int32
}

func (r *slowUsers) Run(ctx context.Context, cmd string) (string, error) {
	if strings.Contains(cmd, "useradd") {
		now := r.running.Add(1)
		for {
			most := r.most.Load()
			if now <= most || r.most.CompareAndSwap(most, now) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		r.running.Add(-1)
	}
	return r.fakeRunner.Run(ctx, cmd)
}

// useradd locks the user database and refuses a second one at once: the
// first jobs of two Workspaces make their users one after the other.
func TestJobUsersAreMadeOneAtATime(t *testing.T) {
	runner := &slowUsers{}
	m := NewManager(Config{Scope: ScopeSystem}, runner, logr.Discard())
	var wg sync.WaitGroup
	for _, ws := range []string{"a", "b", "c", "d"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.EnsureJobUser(context.Background(), ws); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if runner.most.Load() != 1 {
		t.Fatalf("%d useradd at once, want 1", runner.most.Load())
	}
}

// systemd itself takes a confined unit: every directive is one it knows,
// with a value it reads. Skipped where systemd-analyze is not installed.
func TestAConfinedUnitPassesSystemdAnalyzeVerify(t *testing.T) {
	analyze, err := exec.LookPath("systemd-analyze")
	if err != nil {
		t.Skip("systemd-analyze not installed")
	}
	spec := hostJob()
	cfg := Config{Scope: ScopeSystem, HostRoots: []string{"/data", "/srv/100% set"}}
	jail := &confinement{user: JobUser("team-a"), home: "/tmp", envs: "/tmp", folders: cfg.HostRoots, writable: []string{"/tmp", "/var/tmp"}}
	dir := t.TempDir()
	path := filepath.Join(dir, unitName(spec.ID))
	if err := os.WriteFile(path, []byte(renderUnit(spec, nil, cfg, "/tmp", "", jail)), 0o644); err != nil {
		t.Fatal(err)
	}
	out, _ := exec.Command(analyze, "verify", "--man=no", path).CombinedOutput()
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, path+":") || strings.Contains(line, "fatal error") || strings.Contains(line, "bad unit file setting") ||
			strings.Contains(line, "Unknown key") || strings.Contains(line, "Failed to parse") {
			t.Errorf("systemd-analyze verify: %s", line)
		}
	}
}
