// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/capability"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/runtimegate"
)

func newTestExecutor(t *testing.T, image string) *Executor {
	t.Helper()
	dir := t.TempDir()
	t.Setenv(runtimegate.EnvReadyFile, filepath.Join(dir, "ready"))
	t.Setenv(runtimegate.EnvPendingFile, filepath.Join(dir, "pending"))
	e, err := New(Config{
		StateDir:  filepath.Join(dir, "state"),
		Image:     image,
		KillGrace: 2 * time.Second,
		BaseEnv:   []string{"PATH=" + os.Getenv("PATH"), "IMAGE_VAR=from-image", "ZENTHROS_JOIN_TOKEN=secret-token"},
		Logger:    logr.Discard(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func shellJob(id, script string) jobspec.JobSpec {
	return jobspec.JobSpec{ID: id, Kind: v1.WorkloadKindJob, Command: []string{"sh", "-c", script}}
}

// waitTerminal waits for a job to end. Its bound is no latency check: a
// full -race run of every package starved a `true` past 10 s (twice, in two
// tests); the wait returns as soon as the job ends.
func waitTerminal(t *testing.T, e *Executor, handle string) backend.JobState {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		state, err := e.Status(context.Background(), handle)
		if err != nil {
			t.Fatalf("Status(%s): %v", handle, err)
		}
		if state.Phase.Terminal() {
			return state
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish", handle)
	return backend.JobState{}
}

// The container agent must not depend on the VM executor's world: a
// container has no systemd, no containerd and no instance metadata.
func TestNoVMDependencies(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "github.com/zenthros/operator/agent/procagent").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	for _, forbidden := range []string{
		"github.com/zenthros/operator/pkg/systemd",
		"github.com/zenthros/operator/pkg/containerd",
		"github.com/zenthros/operator/agent/awssecret",
		"github.com/zenthros/operator/pkg/cloudmeta",
	} {
		for _, line := range strings.Split(string(out), "\n") {
			if line == forbidden {
				t.Errorf("procagent imports %s", forbidden)
			}
		}
	}
}

func TestJobRunsToSuccessAndFailureVerbatim(t *testing.T) {
	e := newTestExecutor(t, "")
	ok, err := e.Submit(context.Background(), shellJob("ok", "echo hello"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if state := waitTerminal(t, e, ok); state.Phase != v1.PhaseSucceeded {
		t.Fatalf("ok: %+v", state)
	}
	bad, err := e.Submit(context.Background(), shellJob("bad", "echo boom >&2; exit 7"), nil)
	if err != nil {
		t.Fatal(err)
	}
	state := waitTerminal(t, e, bad)
	if state.Phase != v1.PhaseFailed || state.Reasons[0].Code != v1.ReasonProcessFailed {
		t.Fatalf("bad: %+v", state)
	}
	if !strings.Contains(state.Message, "code 7") || !strings.Contains(state.Message, "boom") {
		t.Fatalf("message %q lacks the exit code or the log tail", state.Message)
	}
}

// The result is on disk before the job reads as finished, so Status says
// the same after the agent restarts with its state directory intact.
func TestResultIsWrittenBeforeTheJobReadsFinished(t *testing.T) {
	e := newTestExecutor(t, "")
	handle, err := e.Submit(context.Background(), shellJob("r", "exit 3"), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, e, handle)
	if _, err := readResult(e.jobDir(handle)); err != nil {
		t.Fatalf("job finished but its result is not on disk: %v", err)
	}
	restarted, err := New(e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	state, err := restarted.Status(context.Background(), handle)
	if err != nil || state.Phase != v1.PhaseFailed || !strings.Contains(state.Message, "code 3") {
		t.Fatalf("after restart: %+v, %v", state, err)
	}
}

// A job that was running when its agent went away is gone: a container's
// jobs are the agent's children. Status says not found, which makes the
// controller submit it again instead of waiting out the lease.
func TestJobLostWithTheAgentIsNotFound(t *testing.T) {
	e := newTestExecutor(t, "")
	left := leftByAnEarlierAgent(t, e, "lost", "sleep 30")
	restarted, err := New(e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = restarted.Status(context.Background(), "lost")
	if !errors.Is(err, backend.ErrNotFound) || !strings.Contains(err.Error(), "earlier agent session") {
		t.Fatalf("Status of a job the agent lost = %v, want not found naming the restart", err)
	}
	if err := left.Wait(); err == nil {
		t.Fatal("the job the earlier agent left still ran to its end")
	}
	// The resubmit starts it fresh in the same container.
	if _, err := restarted.Submit(context.Background(), shellJob("lost", "sleep 30"), nil); err != nil {
		t.Fatalf("resubmit after restart: %v", err)
	}
	restarted.Close(context.Background())
}

func TestSubmitIsIdempotentByAttempt(t *testing.T) {
	e := newTestExecutor(t, "")
	defer e.Close(context.Background())
	spec := shellJob("job", "sleep 30")
	first, err := e.Submit(context.Background(), spec, map[string]string{"A": "1"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := e.Submit(context.Background(), spec, map[string]string{"A": "1"})
	if err != nil || again != first {
		t.Fatalf("resubmit = %q, %v; want the same handle %q", again, err, first)
	}
	if _, err := e.Submit(context.Background(), spec, map[string]string{"A": "2"}); !errors.Is(err, backend.ErrSpecMismatch) {
		t.Fatalf("changed spec: %v, want spec mismatch", err)
	}
	spec.Attempt = 1
	retry, err := e.Submit(context.Background(), spec, map[string]string{"A": "1"})
	if err != nil || retry != "job-a1" {
		t.Fatalf("attempt 1 = %q, %v; want its own handle job-a1", retry, err)
	}
}

// running reports whether pid still runs: it exists and is not a zombie
// waiting for its parent to reap it.
func running(pid int) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	// /proc/<pid>/stat: "pid (comm) state ...".
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	stat := string(raw)
	state := strings.TrimSpace(stat[strings.LastIndex(stat, ")")+1:])
	return !strings.HasPrefix(state, "Z") && !strings.HasPrefix(state, "X")
}

// Cancel ends every process the job started, not only its leader.
func TestCancelKillsTheProcessGroup(t *testing.T) {
	e := newTestExecutor(t, "")
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	handle, err := e.Submit(context.Background(),
		shellJob("grp", "trap '' TERM; sleep 60 & echo $! > "+pidFile+"; wait"), nil)
	if err != nil {
		t.Fatal(err)
	}
	var child int
	deadline := time.Now().Add(5 * time.Second)
	for child == 0 && time.Now().Before(deadline) {
		if raw, err := os.ReadFile(pidFile); err == nil && len(strings.TrimSpace(string(raw))) > 0 {
			_, _ = fmt.Sscan(strings.TrimSpace(string(raw)), &child)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if child == 0 {
		t.Fatal("the job never started its child")
	}
	if err := e.Cancel(context.Background(), handle, owner.Ref{}); err != nil {
		t.Fatal(err)
	}
	// Cancel returns once the job's leader is reaped and the whole group was
	// sent SIGKILL. The kernel ends the child on its own time, and until its
	// new parent reaps it the pid still answers: it is gone, or a zombie,
	// within moments (under a loaded machine, not at once: 4 of 48 runs).
	deadline = time.Now().Add(10 * time.Second)
	for running(child) {
		if time.Now().After(deadline) {
			t.Fatalf("child %d outlived Cancel", child)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := e.Cancel(context.Background(), handle, owner.Ref{}); err != nil {
		t.Fatalf("second Cancel: %v", err)
	}
	entries, err := os.ReadDir(e.jobDir(handle))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != logFile {
		t.Fatalf("Cancel left %v in the job directory, want only its log", entries)
	}
}

// What a job leaves behind ends with it, and the Cancel the GC sends minutes
// later signals nothing: the group's number may be another job's leader by
// then.
func TestAnEndedJobsGroupIsNeverSignalledAgain(t *testing.T) {
	e := newTestExecutor(t, "")
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	handle, err := e.Submit(context.Background(), shellJob("left", "sleep 60 & echo $! > "+pidFile), nil)
	if err != nil {
		t.Fatal(err)
	}
	if state := waitTerminal(t, e, handle); state.Phase != v1.PhaseSucceeded {
		t.Fatalf("phase %s: %s", state.Phase, state.Message)
	}
	var child int
	if raw, err := os.ReadFile(pidFile); err != nil {
		t.Fatal(err)
	} else if _, err := fmt.Sscan(strings.TrimSpace(string(raw)), &child); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for running(child) {
		if time.Now().After(deadline) {
			t.Fatalf("child %d outlived its job", child)
		}
		time.Sleep(5 * time.Millisecond)
	}
	var signalled []int
	defer func(kill func(int, syscall.Signal) error) { killGroup = kill }(killGroup)
	killGroup = func(pid int, sig syscall.Signal) error {
		signalled = append(signalled, pid)
		return nil
	}
	if err := e.Cancel(context.Background(), handle, owner.Ref{}); err != nil {
		t.Fatal(err)
	}
	if len(signalled) != 0 {
		t.Fatalf("Cancel of an ended job signalled %v", signalled)
	}
}

// A Managed slot's release cancels a job that already ended; its log is
// still read afterwards (zctl logs), while Status no longer knows it and
// its files and work directory are gone. Found by TestContainerPoolManaged.
func TestCancelOfAnEndedJobKeepsItsLog(t *testing.T) {
	e := newTestExecutor(t, "")
	handle, err := e.Submit(context.Background(), shellJob("done", "echo E2E_KEPT; mkdir -p work && echo x > work/f"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if state := waitTerminal(t, e, handle); state.Phase != v1.PhaseSucceeded {
		t.Fatalf("phase %s: %s", state.Phase, state.Message)
	}
	if err := e.Cancel(context.Background(), handle, owner.Ref{}); err != nil {
		t.Fatal(err)
	}
	ch, err := e.Logs(context.Background(), handle, false, -1)
	if err != nil {
		t.Fatalf("Logs after Cancel: %v", err)
	}
	var out strings.Builder
	for chunk := range ch {
		out.Write(chunk.Data)
	}
	if !strings.Contains(out.String(), "E2E_KEPT") {
		t.Fatalf("log after Cancel = %q", out.String())
	}
	if _, err := e.Status(context.Background(), handle); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Status after Cancel = %v, want not found", err)
	}
	for _, name := range []string{resultFile, specFile, workDirName} {
		if _, err := os.Stat(filepath.Join(e.jobDir(handle), name)); !os.IsNotExist(err) {
			t.Fatalf("%s survived Cancel: %v", name, err)
		}
	}
}

// Released jobs' logs are capped: the oldest go once there are more than
// KeptLogs. A directory holding more than its log (a finished job not yet
// released) is never pruned, however old.
func TestReleasedLogsAreCapped(t *testing.T) {
	e := newTestExecutor(t, "")
	e.cfg.KeptLogs = 2
	base := time.Now().Add(-time.Hour)
	write := func(handle string, age int, files ...string) {
		t.Helper()
		dir := e.jobDir(handle)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, name := range append([]string{logFile}, files...) {
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, []byte(handle), 0o600); err != nil {
				t.Fatal(err)
			}
			at := base.Add(time.Duration(age) * time.Minute)
			if err := os.Chtimes(path, at, at); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("oldest", 0)
	write("unreleased", 1, resultFile)
	write("older", 2)
	write("newer", 3)
	write("released-now", 4, specFile)
	if err := e.Cancel(context.Background(), "released-now", owner.Ref{}); err != nil {
		t.Fatal(err)
	}
	for handle, want := range map[string]bool{"oldest": false, "older": false, "unreleased": true, "newer": true, "released-now": true} {
		_, err := os.Stat(filepath.Join(e.jobDir(handle), logFile))
		if got := err == nil; got != want {
			t.Errorf("%s kept = %v, want %v", handle, got, want)
		}
	}
}

// The job inherits the image's environment, never the agent's settings.
func TestJobSeesTheImageEnvNotTheAgentToken(t *testing.T) {
	e := newTestExecutor(t, "")
	out := filepath.Join(t.TempDir(), "env")
	handle, err := e.Submit(context.Background(), shellJob("env", "env > "+out),
		map[string]string{"ZENTHROS_JOB_ID": "env", "USER_VAR": "x"})
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, e, handle)
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	env := string(raw)
	for _, want := range []string{"IMAGE_VAR=from-image", "USER_VAR=x", "ZENTHROS_JOB_ID=env", "ZENTHROS_ATTEMPT=0"} {
		if !strings.Contains(env, want) {
			t.Errorf("job env lacks %s", want)
		}
	}
	if strings.Contains(env, "secret-token") {
		t.Error("the agent's join token reached the job")
	}
}

func TestImageAndRuntimeRefusals(t *testing.T) {
	const running = "python:3.12-slim-bookworm"
	e := newTestExecutor(t, running)
	cases := map[string]struct {
		spec jobspec.JobSpec
		want error
		text string
	}{
		"other image":  {spec: jobspec.JobSpec{ID: "a", Kind: v1.WorkloadKindJob, Image: "python:3.13-slim-bookworm", Command: []string{"true"}}, want: backend.ErrUnsupportedBackend, text: running},
		"systemd":      {spec: jobspec.JobSpec{ID: "b", Kind: v1.WorkloadKindJob, Runtime: v1.RuntimeSystemd, Command: []string{"true"}}, want: backend.ErrUnsupportedBackend, text: "not systemd"},
		"rayjob":       {spec: jobspec.JobSpec{ID: "c", Kind: v1.WorkloadKindRayJob}, want: backend.ErrUnsupportedKind},
		"missing exe":  {spec: jobspec.JobSpec{ID: "d", Kind: v1.WorkloadKindJob, Command: []string{"no-such-program-here"}}, want: backend.ErrInvalid, text: "not on the job's PATH"},
		"host secrets": {spec: jobspec.JobSpec{ID: "e", Kind: v1.WorkloadKindJob, Command: []string{"true"}, SecretSources: []v1.SecretSourceRef{{Type: v1.SecretSourceTypeAWSSecretsManager}}}, want: backend.ErrSecretSource},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := e.Submit(context.Background(), tc.spec, nil)
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("Submit = %v, want %v containing %q", err, tc.want, tc.text)
			}
		})
	}
	// The same image however spelled runs.
	handle, err := e.Submit(context.Background(), jobspec.JobSpec{ID: "same", Kind: v1.WorkloadKindJob,
		Image: "docker.io/library/python:3.12-slim-bookworm", Command: []string{"true"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, e, handle)
}

// A container that did not say its image cannot check one a job names.
func TestUnknownImageRefusesAJobThatNamesOne(t *testing.T) {
	e := newTestExecutor(t, "")
	_, err := e.Submit(context.Background(), jobspec.JobSpec{ID: "x", Kind: v1.WorkloadKindJob, Image: "alpine", Command: []string{"true"}}, nil)
	if !errors.Is(err, backend.ErrUnsupportedBackend) || !strings.Contains(err.Error(), EnvContainerImage) {
		t.Fatalf("Submit = %v", err)
	}
}

// While the environment is prepared, work is refused as busy, not failed.
func TestPendingEnvironmentIsBusy(t *testing.T) {
	e := newTestExecutor(t, "")
	if err := runtimegate.MarkPending(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(context.Background(), shellJob("busy", "true"), nil); !errors.Is(err, backend.ErrBackendBusy) {
		t.Fatalf("Submit while pending = %v, want busy", err)
	}
}

func TestPreemptSignalReachesTheJob(t *testing.T) {
	e := newTestExecutor(t, "")
	dir := t.TempDir()
	marker, trapped := filepath.Join(dir, "got-usr1"), filepath.Join(dir, "trapped")
	spec := shellJob("pre", "trap 'touch "+marker+"; exit 0' USR1; touch "+trapped+"; while true; do sleep 0.05; done")
	spec.PreemptSignal = "SIGUSR1"
	handle, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A signal before the trap is set ends the shell, not the trap.
	for wait := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(trapped); err == nil {
			break
		}
		if time.Now().After(wait) {
			t.Fatal("the job never set its trap")
		}
	}
	sent, err := e.SignalPreempt(context.Background(), handle)
	if err != nil || !sent {
		t.Fatalf("SignalPreempt = %v, %v", sent, err)
	}
	if state := waitTerminal(t, e, handle); state.Phase != v1.PhaseSucceeded {
		t.Fatalf("job did not exit on its signal: %+v", state)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("the job's trap never ran")
	}
}

func TestEntrypointNeedsAShellCommandDoesNot(t *testing.T) {
	spec := jobspec.JobSpec{Entrypoint: "echo hi"}
	argv, err := commandLine(spec, "/usr/bin:/bin")
	if err != nil || (argv[0] != "/bin/bash" && argv[0] != "/bin/sh") {
		t.Fatalf("entrypoint argv = %v, %v", argv, err)
	}
	// A command is run as it is: no shell between the agent and it.
	argv, err = commandLine(jobspec.JobSpec{Command: []string{"true"}, Args: []string{"x"}}, "/usr/bin:/bin")
	if err != nil || strings.HasSuffix(argv[0], "sh") || argv[len(argv)-1] != "x" {
		t.Fatalf("command argv = %v, %v", argv, err)
	}
}

func TestLogsTailAndFollowUntilTheJobEnds(t *testing.T) {
	e := newTestExecutor(t, "")
	handle, err := e.Submit(context.Background(), shellJob("logs", "for i in 1 2 3 4 5; do echo line$i; sleep 0.05; done"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := e.Logs(context.Background(), handle, true, -1)
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for chunk := range ch { // closes when the job ends
		all.Write(chunk.Data)
	}
	if got := all.String(); !strings.Contains(got, "line1") || !strings.Contains(got, "line5") {
		t.Fatalf("followed log = %q", got)
	}
	ch, err = e.Logs(context.Background(), handle, false, 2)
	if err != nil {
		t.Fatal(err)
	}
	var last strings.Builder
	for chunk := range ch {
		last.Write(chunk.Data)
	}
	if got := last.String(); got != "line4\nline5\n" {
		t.Fatalf("tail 2 = %q", got)
	}
}

func TestDeclaresProcessInItsImage(t *testing.T) {
	e := newTestExecutor(t, "python:3.12-slim-bookworm")
	set, host := e.Declare(context.Background())
	w, ok := set.Find(v1.WorkloadKindJob)
	if !ok || w.DefaultRuntime != capability.RuntimeProcess || len(set) != 1 {
		t.Fatalf("declaration = %+v", set)
	}
	if host.ContainerImage != "python:3.12-slim-bookworm" {
		t.Fatalf("host image = %q", host.ContainerImage)
	}
}

// A start the machine had no room for (its pid limit with MaxJobs running,
// a script still open for writing while another job forks, no file or
// memory left) is the machine busy, and the controller requeues it. Read
// as Invalid, it failed the job for good. A program that is not there is
// the job's.
func TestAStartWithNoRoomIsBusyNotInvalid(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EAGAIN, syscall.ETXTBSY, syscall.EMFILE, syscall.ENFILE, syscall.ENOMEM} {
		err := startFailed("python", &os.PathError{Op: "fork/exec", Path: "/usr/bin/python", Err: errno})
		if !errors.Is(err, backend.ErrBackendBusy) || errors.Is(err, backend.ErrInvalid) {
			t.Errorf("%v: %v is not busy", errno, err)
		}
	}
	for _, err := range []error{
		&exec.Error{Name: "nosuch", Err: exec.ErrNotFound},
		&os.PathError{Op: "fork/exec", Path: "/work/run.sh", Err: syscall.EACCES},
	} {
		if got := startFailed("nosuch", err); !errors.Is(got, backend.ErrInvalid) {
			t.Errorf("%v is not invalid", got)
		}
	}
}

// A GPU that does not start is not offered from then on: the rest of the
// container still is.
func TestAGPUThatDoesNotStartIsNoLongerDeclared(t *testing.T) {
	e := newTestExecutor(t, "runpod/pytorch:2.8.0")
	e.host = func(context.Context) capability.Host {
		return capability.Host{CPU: 8, GPUVendor: "nvidia", GPUModel: "NVIDIA H100 80GB HBM3", GPUCount: 1}
	}
	if _, host := e.Declare(context.Background()); host.GPUCount != 1 {
		t.Fatalf("before: %+v", host)
	}
	e.WithoutGPU()
	_, host := e.Declare(context.Background())
	if host.GPUCount != 0 || host.GPUModel != "" || host.GPUVendor != "" || host.CPU != 8 {
		t.Fatalf("after WithoutGPU: %+v", host)
	}
}
