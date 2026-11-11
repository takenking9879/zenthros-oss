// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/zenthros/operator/agent"
)

// attachedTestAgent stands in for the agent proper under an attach
// supervisor (TestMain, ZENTHROS_TEST_ATTACH): it says what it was handed
// and what its environ shows, then behaves as ZENTHROS_TEST_ATTACH says.
//   - "run": registers, leaves an orphan, and runs until SIGTERM.
//   - "crash-once": its first run registers and dies; later runs are "run".
//   - "fail": exits 1 at once, every run.
//   - "detached": registers, then the hub detaches it: exits ExitDetached.
func attachedTestAgent() int {
	if err := applyHandoff(); err != nil {
		fmt.Println("CHILD_ERROR", err)
		return 3
	}
	fmt.Println("CHILD_PID", os.Getpid())
	fmt.Println("CHILD_TOKEN", os.Getenv("ZENTHROS_JOIN_TOKEN"))
	fmt.Println("CHILD_AUTHKEY", os.Getenv(EnvTailscaleAuthKey))
	fmt.Println("CHILD_ENVIRON", environHolds(handoffMarker))
	fmt.Println("CHILD_CMDLINE", cmdlineHoldsPID(os.Getpid(), handoffMarker))
	fmt.Println("SUPERVISOR_CMDLINE", cmdlineHoldsPID(os.Getppid(), handoffMarker))
	var config []string
	for _, name := range []string{"ZENTHROS_AGENT_MODE", "ZENTHROS_CONTROLLER_ADDR", "ZENTHROS_BACKEND_ID",
		"ZENTHROS_CA_CERT_HASH", "ZENTHROS_NETWORK", "ZENTHROS_TLS_SERVER_NAME", "ZENTHROS_CONTAINER_IMAGE", "ZENTHROS_JOB_GRACE"} {
		config = append(config, name+"="+os.Getenv(name))
	}
	fmt.Println("CHILD_CONFIG", strings.Join(config, " "))
	// attach, still waiting for this run to register: the supervisor's
	// parent.
	if attach := parentOf(os.Getppid()); attach > 1 {
		fmt.Println("ATTACH_ENVIRON", environHoldsPID(attach, handoffMarker))
		fmt.Println("ATTACH_CMDLINE", cmdlineHoldsPID(attach, handoffMarker))
	}
	mode := os.Getenv("ZENTHROS_TEST_ATTACH")
	if mode == "fail" {
		return 1
	}
	if first := containerPath("test-first-run"); mode == "crash-once" && !exists(first) {
		_ = os.WriteFile(first, nil, 0o600)
		fmt.Println(attachRegistered)
		time.Sleep(200 * time.Millisecond)
		return 1
	}
	if mode == "detached" {
		fmt.Println(attachRegistered)
		time.Sleep(200 * time.Millisecond)
		return agent.ExitDetached
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	// A job's child outlives its parent: orphaned to the supervisor.
	_ = exec.Command("sh", "-c", "sleep 0.2 &").Run()
	fmt.Println(attachRegistered)
	<-sigs
	fmt.Println("CHILD_TERM")
	return 0
}

// attachEnv is the environment a shell in a running container has after
// pasting the attach command: the join env, with the token.
func attachEnv(t *testing.T, mode string) (state string, env []string) {
	t.Helper()
	if _, err := os.Stat("/proc/self/environ"); err != nil {
		t.Skip("no /proc")
	}
	state = t.TempDir()
	return state, append(os.Environ(),
		"ZENTHROS_TEST_ATTACH="+mode,
		"ZENTHROS_AGENT_MODE=container",
		"ZENTHROS_STATE_DIR="+state,
		"ZENTHROS_JOIN_TOKEN=token-"+handoffMarker,
		envAttachRestartDelay+"=50ms",
	)
}

// attachCmd runs `zenthros-agent attach args...` as the shell would. It
// gives up after a minute: an attach that re-execs itself forever would
// hang the test.
func attachCmd(env []string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], append([]string{"attach"}, args...)...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func readAttachLog(state string) string {
	raw, _ := os.ReadFile(filepath.Join(state, attachLogName))
	return string(raw)
}

func supervisorPID(t *testing.T, state string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(state, attachPIDName))
	pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || convErr != nil {
		t.Fatalf("no supervisor pid: %v %v\n%s", err, convErr, readAttachLog(state))
	}
	return pid
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil && !zombie(pid) }

func zombie(pid int) bool {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	rest := string(raw[strings.LastIndexByte(string(raw), ')')+2:])
	return strings.HasPrefix(rest, "Z")
}

func waitFor(t *testing.T, what string, within time.Duration, ok func() bool, detail func() string) {
	t.Helper()
	for deadline := time.Now().Add(within); !ok(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s\n%s", what, within, detail())
		}
	}
}

// stopAttach ends whatever supervisor a test left running.
func stopAttach(t *testing.T, state string) {
	t.Cleanup(func() {
		if raw, err := os.ReadFile(filepath.Join(state, attachPIDName)); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 1 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
}

// `zenthros-agent attach` in a running container's shell returns once the
// agent registered, and leaves it running detached: in its own session
// (a closed terminal's SIGHUP does not reach it), under a supervisor that
// adopts what the jobs orphan. The token reaches the agent, and no environ
// a job can read holds it: not attach's while it waits, not the
// supervisor's, not the agent's. Run again,
// it starts nothing new.
func TestAttachDetachesAndHandsOffTheToken(t *testing.T) {
	state, env := attachEnv(t, "run")
	stopAttach(t, state)
	out, err := attachCmd(env)
	if err != nil {
		t.Fatalf("attach: %v\n%s\n%s", err, out, readAttachLog(state))
	}
	if !strings.Contains(out, "attached") {
		t.Fatalf("attach said:\n%s", out)
	}
	pid := supervisorPID(t, state)
	if !alive(pid) {
		t.Fatalf("the supervisor %d is not running after attach returned\n%s", pid, readAttachLog(state))
	}
	if sid, _ := unix.Getsid(pid); sid != pid {
		t.Errorf("supervisor session %d, want its own (%d)", sid, pid)
	}
	if got := environHoldsPID(pid, handoffMarker); got != "clean" {
		t.Errorf("the supervisor's environ %s", got)
	}
	log := readAttachLog(state)
	for _, want := range []string{"CHILD_TOKEN token-" + handoffMarker, "CHILD_ENVIRON clean", "ATTACH_ENVIRON clean"} {
		if !strings.Contains(log, want) {
			t.Errorf("missing %q in the attach log:\n%s", want, log)
		}
	}
	waitFor(t, "orphans reaped", 3*time.Second, func() bool { return len(zombieChildren(t, pid)) == 0 },
		func() string { return fmt.Sprint(zombieChildren(t, pid)) })

	again, err := attachCmd(env)
	if err != nil || !strings.Contains(again, "already attached") {
		t.Fatalf("second attach: %v\n%s", err, again)
	}
	if got := supervisorPID(t, state); got != pid {
		t.Fatalf("second attach replaced supervisor %d with %d", pid, got)
	}
}

// parentOf is pid's parent, from /proc.
func parentOf(pid int) int {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw[strings.LastIndexByte(string(raw), ')')+2:]))
	if len(fields) < 2 {
		return 0
	}
	ppid, _ := strconv.Atoi(fields[1])
	return ppid
}

// cmdlineHoldsPID says whether pid's arguments hold marker: every process
// in the container can read them, its jobs included.
func cmdlineHoldsPID(pid int, marker string) string {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return "unreadable: " + err.Error()
	}
	if strings.Contains(string(raw), marker) {
		return "leaks"
	}
	return "clean"
}

// environHoldsPID is environHolds for another process.
func environHoldsPID(pid int, marker string) string {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		return "unreadable: " + err.Error()
	}
	if strings.Contains(string(raw), marker) {
		return "leaks"
	}
	return "clean"
}

// Nobody restarts an attached agent but its supervisor: an agent that dies
// is started again, with the token handed to it again (it has no
// certificate yet when it dies before enrolling).
func TestAttachRelaunchesADeadAgent(t *testing.T) {
	state, env := attachEnv(t, "crash-once")
	stopAttach(t, state)
	if out, err := attachCmd(env); err != nil {
		t.Fatalf("attach: %v\n%s", err, out)
	}
	waitFor(t, "the agent relaunched", 5*time.Second, func() bool {
		return strings.Count(readAttachLog(state), attachRegistered) >= 2
	}, func() string { return readAttachLog(state) })
	if n := strings.Count(readAttachLog(state), "CHILD_TOKEN token-"+handoffMarker); n < 2 {
		t.Fatalf("the relaunched agent got no token (%d runs had it):\n%s", n, readAttachLog(state))
	}
}

// `attach --stop` is the stop a provider would send PID 1: SIGTERM to the
// agent, which tells its jobs and drains. The supervisor does not start it
// again, and attach is free to run anew.
func TestAttachStopDrainsAndDoesNotRelaunch(t *testing.T) {
	state, env := attachEnv(t, "run")
	stopAttach(t, state)
	if out, err := attachCmd(env); err != nil {
		t.Fatalf("attach: %v\n%s", err, out)
	}
	pid := supervisorPID(t, state)
	out, err := attachCmd(env, "--stop")
	if err != nil || !strings.Contains(out, "stopped") {
		t.Fatalf("attach --stop: %v\n%s\n%s", err, out, readAttachLog(state))
	}
	if alive(pid) {
		t.Fatalf("supervisor %d still runs after --stop", pid)
	}
	log := readAttachLog(state)
	if !strings.Contains(log, "CHILD_TERM") {
		t.Errorf("the agent heard no SIGTERM:\n%s", log)
	}
	if n := strings.Count(log, attachRegistered); n != 1 {
		t.Errorf("the agent ran %d times, want once:\n%s", n, log)
	}
	if out, err := attachCmd(env, "--stop"); err != nil || !strings.Contains(out, "not attached") {
		t.Errorf("--stop with nothing attached: %v\n%s", err, out)
	}
}

// A supervisor killed outright takes its agent with it: two agents with one
// certificate would fight over the backend after the next attach.
func TestAttachAgentDiesWithItsSupervisor(t *testing.T) {
	state, env := attachEnv(t, "run")
	stopAttach(t, state)
	if out, err := attachCmd(env); err != nil {
		t.Fatalf("attach: %v\n%s", err, out)
	}
	pid := supervisorPID(t, state)
	var agent int
	for _, line := range strings.Split(readAttachLog(state), "\n") {
		if rest, ok := strings.CutPrefix(line, "CHILD_PID "); ok {
			agent, _ = strconv.Atoi(rest)
		}
	}
	if agent == 0 || !alive(agent) {
		t.Fatalf("no running agent in the log:\n%s", readAttachLog(state))
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	waitFor(t, "the agent gone with its supervisor", 5*time.Second, func() bool { return !alive(agent) },
		func() string { return readAttachLog(state) })
}

// An agent that cannot stay up (a spent token, a wrong address) is not
// started forever: after a few short runs the supervisor gives up, says so,
// and attach reports the failure instead of "attached".
func TestAttachGivesUpOnAnAgentThatCannotStart(t *testing.T) {
	state, env := attachEnv(t, "fail")
	stopAttach(t, state)
	out, err := attachCmd(env)
	if err == nil {
		t.Fatalf("attach succeeded with an agent that cannot start:\n%s", out)
	}
	if n := strings.Count(readAttachLog(state), "CHILD_PID"); n != attachMaxQuickExits {
		t.Errorf("the agent ran %d times, want %d:\n%s", n, attachMaxQuickExits, readAttachLog(state))
	}
	if !strings.Contains(out, "gave up") {
		t.Errorf("attach did not say why:\n%s", out)
	}
}

// An agent the hub detached (zenthros delete) exits ExitDetached: the
// supervisor ends with it instead of starting it again, which the hub would
// only refuse (lab, 2026-10-18: it kept retrying a revoked machine).
func TestAttachDoesNotRelaunchADetachedAgent(t *testing.T) {
	state, env := attachEnv(t, "detached")
	stopAttach(t, state)
	// It may end before attach looks: attach then says it did not stay up.
	_, _ = attachCmd(env)
	waitFor(t, "the supervisor ended with its detached agent", 5*time.Second, func() bool {
		return strings.Contains(readAttachLog(state), "exit 78")
	}, func() string { return readAttachLog(state) })
	// Past several restart delays (50 ms): no second run.
	time.Sleep(500 * time.Millisecond)
	if n := strings.Count(readAttachLog(state), "CHILD_PID"); n != 1 {
		t.Fatalf("the detached agent ran %d times, want once:\n%s", n, readAttachLog(state))
	}
}

// attach is the container agent's: without its mode it would start a VM
// agent that wants systemd.
func TestAttachNeedsContainerMode(t *testing.T) {
	_, env := attachEnv(t, "run")
	env = append(env, "ZENTHROS_AGENT_MODE=vm")
	out, err := attachCmd(env)
	if err == nil || !strings.Contains(out, "ZENTHROS_AGENT_MODE=container") {
		t.Fatalf("attach in vm mode: %v\n%s", err, out)
	}
}

// The attach log is capped: an agent attached for weeks must not fill the
// container's disk. It rotates once, keeping one older file.
func TestAttachLogRotates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, attachLogName)
	l, err := openRotatingLog(path, 100, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		fmt.Fprintf(l, "line %02d\n", i)
	}
	cur, _ := os.ReadFile(path)
	old, _ := os.ReadFile(path + ".1")
	if len(cur) > 100 || len(old) > 100 || len(old) == 0 {
		t.Fatalf("sizes %d and %d, want both at most 100 and an older file", len(cur), len(old))
	}
	if !strings.HasSuffix(string(cur), "line 29\n") {
		t.Fatalf("the newest lines are not in the current file:\n%s", cur)
	}
}

// The command `zenadm token create --attach` prints passes the join as
// flags, as `zenadm join` does on a VM: no variable to set. The agent gets
// every value; the token and the tailnet key reach it over the handoff
// pipe and stay out of every process's arguments and environ: attach's
// while it waits, the supervisor's and the agent's. A job could read any of
// them for as long as the agent runs.
func TestAttachTakesTheJoinFlagsAndHidesThem(t *testing.T) {
	if _, err := os.Stat("/proc/self/cmdline"); err != nil {
		t.Skip("no /proc")
	}
	state := t.TempDir()
	stopAttach(t, state)
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "ZENTHROS_") {
			env = append(env, kv)
		}
	}
	env = append(env, "ZENTHROS_TEST_ATTACH=run", "ZENTHROS_STATE_DIR="+state, envAttachRestartDelay+"=50ms")
	out, err := attachCmd(env,
		"--controller", "gw.tail1.ts.net:9443",
		"--token", "token-"+handoffMarker,
		"--ca-cert-hash", "sha256:00ff",
		"--backend-id", "ct-0a1b2c3d",
		"--tailscale", "--ts-authkey", "tskey-"+handoffMarker,
		"--image", "runpod/base:0.7.0",
		"--job-grace", "1m15s")
	if err != nil {
		t.Fatalf("attach: %v\n%s\n%s", err, out, readAttachLog(state))
	}
	log := readAttachLog(state)
	for _, want := range []string{
		"CHILD_TOKEN token-" + handoffMarker,
		"CHILD_AUTHKEY tskey-" + handoffMarker,
		"CHILD_CONFIG ZENTHROS_AGENT_MODE=container ZENTHROS_CONTROLLER_ADDR=gw.tail1.ts.net:9443 ZENTHROS_BACKEND_ID=ct-0a1b2c3d " +
			"ZENTHROS_CA_CERT_HASH=sha256:00ff ZENTHROS_NETWORK=tailscale-userspace ZENTHROS_TLS_SERVER_NAME=zenthros-controller " +
			"ZENTHROS_CONTAINER_IMAGE=runpod/base:0.7.0 ZENTHROS_JOB_GRACE=1m15s",
		"CHILD_ENVIRON clean", "CHILD_CMDLINE clean",
		"SUPERVISOR_CMDLINE clean",
		"ATTACH_ENVIRON clean", "ATTACH_CMDLINE clean",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("missing %q in the attach log:\n%s", want, log)
		}
	}
	if got := environHoldsPID(supervisorPID(t, state), handoffMarker); got != "clean" {
		t.Errorf("the supervisor's environ %s", got)
	}
}
