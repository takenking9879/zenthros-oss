// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/zenthros/operator/agent/procagent"
	"github.com/zenthros/operator/internal/envconf"
	"github.com/zenthros/operator/pkg/bootstrap"
	"github.com/zenthros/operator/pkg/infrastructure/provider/containerhosts"
)

// Attach mode: the agent joins a container that is already running, such
// as a pod someone opened a shell in, instead of being its entrypoint
// (docs/design/container-agent.md, "Attach to a running container"). It
// detaches from the shell and runs under a supervisor that does what PID 1
// does in a container the agent starts: the secrets leave every environ a
// job can read, orphans are reaped, and signals reach the agent. What PID 1
// alone gets it cannot: a provider stops a container by signalling its
// PID 1, so an attached agent hears no stop, and its jobs end as lost
// workers and run again elsewhere. And a restart of the container brings
// back its own command, not the agent.
const (
	attachLogName = "attach.log"
	attachPIDName = "attach.pid"
	attachLock    = "attach.lock"
	// attachRegistered is what the agent logs once it is registered; attach
	// returns when its log says it.
	attachRegistered = "registered with controller"
	// maxAttachLog caps the log; one older file is kept.
	maxAttachLog = 16 << 20
	// envAttachRestartDelay is the first wait before an exited agent is
	// started again; it doubles to attachMaxRestartDelay.
	envAttachRestartDelay   = "ZENTHROS_ATTACH_RESTART_DELAY"
	defaultAttachRestart    = 2 * time.Second
	attachMaxRestartDelay   = time.Minute
	defaultAttachWait       = 3 * time.Minute
	attachStopMargin        = 30 * time.Second
	attachSupervisorFlag    = "--supervise"
	attachSupervisorMessage = "zenthros-agent attach: "
	// defaultTailnetTLSName is the controller certificate's name on a
	// tailnet, where the address the agent dials is not one of its names.
	defaultTailnetTLSName = "zenthros-controller"
)

// attachHealthyRun is how long an agent run lasts before its exit counts as
// a crash to recover from, not a failure to start. attachMaxQuickExits
// such failures in a row stop the supervisor: a refused token or a wrong
// address will not get better by retrying.
const (
	attachHealthyRun    = time.Minute
	attachMaxQuickExits = 5
)

// runAttach is `zenthros-agent attach`; argv[0] is the agent's executable.
func runAttach(argv []string) int {
	fs := flag.NewFlagSet("attach", flag.ContinueOnError)
	stop := fs.Bool("stop", false, "stop the attached agent: its jobs get the interruption notice and ZENTHROS_PREEMPT_GRACE to checkpoint")
	supervise := fs.Bool(strings.TrimPrefix(attachSupervisorFlag, "--"), false, "internal: run as the detached supervisor")
	wait := fs.Duration("wait", defaultAttachWait, "how long attach waits for the agent to register before it returns")
	join := attachJoinFlags(fs)
	args := argv[2:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		// `zenadm join HUB --token zt1_...`: the hub's address first, as on
		// a VM. The flag package stops at the first argument not a flag.
		*join.values["ZENTHROS_CONTROLLER_ADDR"], args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	self, rest := argv[0], fs.Args()
	switch {
	case *stop:
		return stopAttached(os.Stdout)
	case *supervise:
		return superviseAttached(self, rest)
	}
	if err := join.apply(); err != nil {
		fmt.Fprintln(os.Stdout, attachSupervisorMessage+err.Error())
		return 2
	}
	// What re-execs without the secrets: the join is in the environment
	// now, the secrets on their way to the handoff pipe, and no argument
	// holds them.
	argv = append([]string{self, "attach", "--wait", wait.String()}, rest...)
	return attach(argv, self, rest, *wait, os.Stdout)
}

// attachJoin is the join as flags, named as `zenadm join` names them on a
// VM. Each becomes the variable the agent reads; the token and the tailnet
// key are handed off, as from the environment.
type attachJoin struct {
	values    map[string]*string
	tailscale *bool
}

// attachJoinVars are the flags and the variables they set.
var attachJoinVars = []struct{ flag, env, usage string }{
	{"controller", "ZENTHROS_CONTROLLER_ADDR", "controller address host:port"},
	{"token", "ZENTHROS_JOIN_TOKEN", "one-time bootstrap token"},
	{"ca-cert-hash", "ZENTHROS_CA_CERT_HASH", "pin sha256:<hex> of the controller CA"},
	{"backend-id", "ZENTHROS_BACKEND_ID", "backend id this container joins as"},
	{"ts-authkey", EnvTailscaleAuthKey, "one-time tailnet auth key (with --tailscale)"},
	{"tls-server-name", "ZENTHROS_TLS_SERVER_NAME", "controller certificate name (with --tailscale: zenthros-controller)"},
	{"image", procagent.EnvContainerImage, "the image this container runs; a job naming another is refused"},
	{"job-grace", containerhosts.EnvJobGrace, "how long the container's link may go unheard while it runs a job"},
}

func attachJoinFlags(fs *flag.FlagSet) *attachJoin {
	j := &attachJoin{values: map[string]*string{}}
	for _, v := range attachJoinVars {
		j.values[v.env] = fs.String(v.flag, "", v.usage)
	}
	j.tailscale = fs.Bool("tailscale", false, "reach the controller from a userspace tailnet node")
	return j
}

// apply sets the flags given in the environment, with container mode.
func (j *attachJoin) apply() error {
	for _, v := range attachJoinVars {
		if value := *j.values[v.env]; value != "" {
			if err := os.Setenv(v.env, value); err != nil {
				return err
			}
		}
	}
	defaults := map[string]string{"ZENTHROS_AGENT_MODE": agentModeContainer}
	if *j.tailscale {
		defaults["ZENTHROS_NETWORK"] = bootstrap.NetworkTailscaleUserspace
		defaults["ZENTHROS_TLS_SERVER_NAME"] = defaultTailnetTLSName
	}
	for name, value := range defaults {
		if os.Getenv(name) == "" {
			if err := os.Setenv(name, value); err != nil {
				return err
			}
		}
	}
	return nil
}

// attach starts the supervisor detached and waits for the agent to
// register. Its environ holds the secrets until scrubInit re-execs it
// without them, before it waits: a job may run in the meantime.
func attach(argv []string, self string, rest []string, wait time.Duration, out io.Writer) int {
	if os.Getenv("ZENTHROS_AGENT_MODE") != agentModeContainer {
		fmt.Fprintln(out, attachSupervisorMessage+"attach runs the container agent: set ZENTHROS_AGENT_MODE=container")
		return 2
	}
	env, secrets, err := scrubInit(argv)
	if err != nil {
		fmt.Fprintf(out, attachSupervisorMessage+"%v\n", err)
	}
	state := containerStateDir()
	if err := os.MkdirAll(state, 0o700); err != nil {
		fmt.Fprintf(out, attachSupervisorMessage+"%v\n", err)
		return 1
	}
	if pid, held := attachedPID(state); held {
		fmt.Fprintf(out, attachSupervisorMessage+"already attached (pid %d); log %s\n", pid, filepath.Join(state, attachLogName))
		return 0
	}
	logPath := filepath.Join(state, attachLogName)
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		fmt.Fprintf(out, attachSupervisorMessage+"%v\n", err)
		return 1
	}
	defer log.Close()
	from, _ := log.Seek(0, io.SeekEnd)
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		fmt.Fprintf(out, attachSupervisorMessage+"%v\n", err)
		return 1
	}
	defer devnull.Close()
	files := []*os.File{devnull, log, log}
	if len(secrets) > 0 {
		pipe, err := handoffPipe(secrets)
		if err != nil {
			fmt.Fprintf(out, attachSupervisorMessage+"%v\n", err)
			return 1
		}
		defer pipe.Close()
		files = append(files, pipe)
		env = append(env, envHandoffFD+"=3")
	}
	supervisor, err := os.StartProcess(self, append([]string{self, "attach", attachSupervisorFlag}, rest...), &os.ProcAttr{
		Env:   env,
		Files: files,
		// Its own session: the shell's terminal closing does not stop it.
		Sys: &syscall.SysProcAttr{Setsid: true},
	})
	if err != nil {
		fmt.Fprintf(out, attachSupervisorMessage+"start the supervisor: %v\n", err)
		return 1
	}
	for deadline := time.Now().Add(wait); ; time.Sleep(250 * time.Millisecond) {
		since := logSince(logPath, from)
		var ws syscall.WaitStatus
		if pid, _ := syscall.Wait4(supervisor.Pid, &ws, syscall.WNOHANG, nil); pid == supervisor.Pid {
			since = logSince(logPath, from)
			if strings.Contains(since, "already attached") {
				fmt.Fprint(out, lastLines(since, 1))
				return 0
			}
			fmt.Fprintf(out, attachSupervisorMessage+"the agent did not stay up; the end of %s:\n%s\n", logPath, lastLines(since, 20))
			return 1
		}
		if strings.Contains(since, attachRegistered) {
			fmt.Fprintf(out, attachSupervisorMessage+"attached as backend %s (supervisor pid %d)\n  log:  %s\n  stop: %s attach --stop\n",
				attachedBackend(), supervisor.Pid, logPath, self)
			return 0
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(out, attachSupervisorMessage+"the agent has not registered in %s; it keeps trying in the background (supervisor pid %d). The end of %s:\n%s\n",
				wait, supervisor.Pid, logPath, lastLines(since, 20))
			return 1
		}
	}
}

// attachedBackend is the backend the container attached as: the one it was
// told, or, after a short join, the one its certificate names.
func attachedBackend() string {
	if id := os.Getenv("ZENTHROS_BACKEND_ID"); id != "" {
		return id
	}
	if pemBytes, err := os.ReadFile(containerPath("agent.crt")); err == nil {
		if id, err := bootstrap.CertCommonName(pemBytes); err == nil {
			return id
		}
	}
	return "(see the log)"
}

// logSince is what the log gained past offset.
func logSince(path string, offset int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return ""
	}
	raw, _ := io.ReadAll(f)
	return string(raw)
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n") + "\n"
}

// superviseAttached is the detached supervisor: one per state directory,
// held by a lock, which ends with it however it ends.
func superviseAttached(self string, rest []string) int {
	secrets, handoffErr := receiveHandoff()
	state := containerStateDir()
	logPath := filepath.Join(state, attachLogName)
	log, err := openRotatingLog(logPath, maxAttachLog, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, attachSupervisorMessage+"%v\n", err)
		return 1
	}
	logf := func(format string, args ...any) {
		fmt.Fprintf(log, time.Now().UTC().Format(time.RFC3339)+" "+attachSupervisorMessage+format+"\n", args...)
	}
	if handoffErr != nil {
		logf("%v", handoffErr)
	}
	lock, err := lockAttach(state)
	if errors.Is(err, unix.EWOULDBLOCK) {
		pid, _ := attachedPID(state)
		logf("already attached (pid %d)", pid)
		return 0
	}
	if err != nil {
		logf("%v", err)
		return 1
	}
	defer lock.Close()
	pidPath := filepath.Join(state, attachPIDName)
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		logf("%v", err)
		return 1
	}
	defer os.Remove(pidPath)
	logf("supervising the agent (pid %d). It is not the container's init: a stop of the container reaches no agent, so its jobs end as lost workers and run again elsewhere", os.Getpid())
	// One pipe carries every run's output into the capped log.
	r, w, err := os.Pipe()
	if err != nil {
		logf("%v", err)
		return 1
	}
	copied := make(chan struct{})
	go func() {
		_, _ = io.Copy(log, r)
		close(copied)
	}()
	code := supervise(supervision{
		argv:         append([]string{self}, rest...),
		env:          os.Environ(),
		secrets:      secrets,
		stdout:       w,
		relaunch:     true,
		restartDelay: envconf.Duration(envAttachRestartDelay, defaultAttachRestart),
		logf:         logf,
	})
	_ = w.Close()
	select {
	case <-copied:
	case <-time.After(time.Second): // an orphan holds the pipe
	}
	logf("stopped (exit %d)", code)
	return code
}

// lockAttach takes the state directory's attach lock, or fails with
// EWOULDBLOCK when a supervisor holds it. The descriptor is close-on-exec,
// so no child keeps it after the supervisor is gone.
func lockAttach(state string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(state, attachLock), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// attachedPID is the running supervisor's pid, if one holds the lock.
func attachedPID(state string) (int, bool) {
	lock, err := lockAttach(state)
	if err == nil {
		lock.Close()
		return 0, false
	}
	if !errors.Is(err, unix.EWOULDBLOCK) {
		return 0, false
	}
	raw, _ := os.ReadFile(filepath.Join(state, attachPIDName))
	pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	return pid, true
}

// stopAttached sends the supervisor SIGTERM, which it forwards to the agent,
// and waits for the supervisor to end: the agent's grace, and a margin.
func stopAttached(out io.Writer) int {
	state := containerStateDir()
	pid, held := attachedPID(state)
	if !held {
		fmt.Fprintln(out, attachSupervisorMessage+"not attached")
		return 0
	}
	if pid <= 1 {
		fmt.Fprintf(out, attachSupervisorMessage+"a supervisor holds %s but its pid is unknown\n", filepath.Join(state, attachLock))
		return 1
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		fmt.Fprintf(out, attachSupervisorMessage+"signal supervisor %d: %v\n", pid, err)
		return 1
	}
	within := envconf.Duration("ZENTHROS_PREEMPT_GRACE", defaultPreemptGrace) + attachStopMargin
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if _, held := attachedPID(state); !held {
			fmt.Fprintln(out, attachSupervisorMessage+"stopped")
			return 0
		}
	}
	fmt.Fprintf(out, attachSupervisorMessage+"supervisor %d is still stopping after %s\n", pid, within)
	return 1
}

// rotatingLog is a file capped at max bytes: past it, the file becomes
// path.1 (replacing an older one) and a new one starts. As the supervisor's
// log it is also its stdout and stderr, so a panic lands there too.
type rotatingLog struct {
	mu       sync.Mutex
	path     string
	max      int64
	f        *os.File
	size     int64
	asStdout bool
}

func openRotatingLog(path string, max int64, asStdout bool) (*rotatingLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	l := &rotatingLog{path: path, max: max, f: f, size: info.Size(), asStdout: asStdout}
	if asStdout {
		l.redirect()
	}
	return l, nil
}

func (l *rotatingLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	written := 0
	for len(p) > 0 {
		if l.size >= l.max {
			l.rotate()
		}
		// Cut at a line end that fits, so a line is not split across files
		// unless it alone is longer than the cap.
		chunk := p
		if room := l.max - l.size; int64(len(chunk)) > room {
			chunk = p[:room]
			if i := bytes.LastIndexByte(chunk, '\n'); i >= 0 {
				chunk = p[:i+1]
			} else if l.size > 0 {
				l.rotate()
				continue
			}
		}
		n, err := l.f.Write(chunk)
		l.size += int64(n)
		written += n
		if err != nil {
			return written, err
		}
		p = p[n:]
	}
	return written, nil
}

// rotate moves the file aside and starts a new one; if it cannot, it keeps
// writing past the cap.
func (l *rotatingLog) rotate() {
	if err := os.Rename(l.path, l.path+".1"); err != nil {
		l.size = 0
		return
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0o600)
	if err != nil {
		l.size = 0
		return
	}
	l.f.Close()
	l.f, l.size = f, 0
	if l.asStdout {
		l.redirect()
	}
}

func (l *rotatingLog) redirect() {
	_ = unix.Dup2(int(l.f.Fd()), 1)
	_ = unix.Dup2(int(l.f.Fd()), 2)
}
