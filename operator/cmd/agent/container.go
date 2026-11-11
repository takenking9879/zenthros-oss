// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"

	"github.com/zenthros/operator/agent"
	"github.com/zenthros/operator/agent/procagent"
	"github.com/zenthros/operator/internal/envconf"
	"github.com/zenthros/operator/pkg/bootstrap"
	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/infrastructure/provider/containerhosts"
	"github.com/zenthros/operator/pkg/liveness"
	"github.com/zenthros/operator/pkg/pyenv"
	"github.com/zenthros/operator/pkg/runtimegate"
	"github.com/zenthros/operator/pkg/urlredact"
)

// The container mode: the agent runs inside one unprivileged container and
// its jobs are its child processes (docs/design/container-agent.md). All its
// state lives under ZENTHROS_STATE_DIR, the one place the contract says it
// may write.
const (
	agentModeContainer = "container"

	defaultContainerStateDir = "/tmp/zenthros"
	// defaultPreemptGrace is how long a SIGTERM'd container keeps going for
	// its jobs to checkpoint: under the 10s a container runtime commonly
	// allows before SIGKILL. Whoever starts the container sets the real one.
	defaultPreemptGrace = 8 * time.Second
	// reportFlush is how long the agent stays after its last job ended on a
	// SIGTERM, for the job's final reports to reach the controller.
	reportFlush = 2 * time.Second
	// defaultEnrollTimeout bounds how long a starting container retries a
	// controller it cannot reach yet.
	defaultEnrollTimeout = 5 * time.Minute
)

func containerStateDir() string {
	return envconf.Or("ZENTHROS_STATE_DIR", defaultContainerStateDir)
}

// containerPath is a file under the state directory.
func containerPath(parts ...string) string {
	return filepath.Join(append([]string{containerStateDir()}, parts...)...)
}

// applyContainerDefaults points everything the agent writes at the state
// directory, unless the environment already says where.
func applyContainerDefaults(cfg *agent.Config) {
	if cfg.TLS.CertFile == "" {
		cfg.TLS.CertFile = containerPath("agent.crt")
	}
	if cfg.TLS.KeyFile == "" {
		cfg.TLS.KeyFile = containerPath("agent.key")
	}
	if cfg.TLS.CAFile == "" {
		cfg.TLS.CAFile = containerPath("ca.crt")
	}
	if cfg.TLS.ServerName == "" {
		cfg.TLS.ServerName = bootstrap.HostOf(cfg.ControllerAddr)
	}
	// A container asks no cloud metadata service: its notice is SIGTERM.
	cfg.SkipMetadataNotices = true
	for name, value := range map[string]string{
		runtimegate.EnvReadyFile:   containerPath("runtime-ready"),
		runtimegate.EnvPendingFile: containerPath("runtime-pending"),
	} {
		if os.Getenv(name) == "" {
			_ = os.Setenv(name, value)
		}
	}
}

func newContainerExecutor(logger logr.Logger, toolchain *toolchainEnv) (*procagent.Executor, error) {
	self, _ := os.Executable()
	return procagent.New(procagent.Config{
		StateDir:   containerStateDir(),
		Image:      os.Getenv(procagent.EnvContainerImage),
		AgentBin:   self,
		KillGrace:  envconf.Duration("ZENTHROS_KILL_GRACE", 0),
		PathPrefix: toolchain.pathPrefix,
		ExtraEnv:   toolchain.extraEnv,
		Logger:     logger.WithName("procagent"),
	})
}

// containerIdentity is the container's id, as whoever started it says:
// ZENTHROS_CONTAINER_ID, or the variable ZENTHROS_CONTAINER_ID_ENV names
// (a provider's own, such as its pod id). The agent knows no provider names.
func containerIdentity() string {
	if id := os.Getenv("ZENTHROS_CONTAINER_ID"); id != "" {
		return id
	}
	if name := os.Getenv(containerhosts.EnvContainerIDName); name != "" {
		return os.Getenv(name)
	}
	return ""
}

// enrollIfNeeded gets the agent its certificate when the state directory has
// none: a new container, or one whose state did not survive a restart. With
// a certificate there, a restarted container just reconnects. Without one
// and without a usable token, the container cannot come back: that is a lost
// backend, and the error says why.
func enrollIfNeeded(ctx context.Context, cfg agent.Config, logger logr.Logger) error {
	if exists(cfg.TLS.CertFile) && exists(cfg.TLS.KeyFile) && exists(cfg.TLS.CAFile) {
		return nil
	}
	token := os.Getenv("ZENTHROS_JOIN_TOKEN")
	if token == "" {
		return fmt.Errorf("no agent certificate in %s and no ZENTHROS_JOIN_TOKEN to enroll with", containerStateDir())
	}
	params := bootstrap.JoinParams{
		ControllerAddr: cfg.ControllerAddr,
		TLSServerName:  cfg.TLS.ServerName,
		Token:          token,
		CACertHash:     os.Getenv("ZENTHROS_CA_CERT_HASH"),
		BackendID:      cfg.BackendID,
		BackendType:    cfg.BackendType,
		Dial:           cfg.Dial,
	}
	if id := containerIdentity(); id != "" {
		params.IdentityDocument = []byte(id)
	}
	deadline := time.Now().Add(envconf.Duration("ZENTHROS_ENROLL_TIMEOUT", defaultEnrollTimeout))
	backoff := time.Second
	for {
		cred, err := bootstrap.Join(ctx, params)
		if err == nil {
			if err := cred.Write(containerStateDir()); err != nil {
				return err
			}
			logger.Info("enrolled", "backendID", cfg.BackendID, "stateDir", containerStateDir())
			return nil
		}
		if enrollRefused(err) || time.Now().After(deadline) {
			return fmt.Errorf("enroll backend %s: %w", cfg.BackendID, err)
		}
		logger.Info("controller not reachable yet; retrying enrollment", "error", err.Error(), "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 15*time.Second {
			backoff *= 2
		}
	}
}

// enrollRefused is the controller saying no (a used, expired or wrong
// token), which a retry cannot change, as opposed to not answering or a
// check it could not make (fault.Retryable).
func enrollRefused(err error) bool {
	return !fault.Retryable(err)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// containerSignalContext is the agent's context in a container. SIGTERM is
// the interruption notice: the agent tells its jobs (interrupt), then stops
// when they have ended (plus a moment for their last reports) or when the
// grace runs out. SIGINT, or a second SIGTERM, stops it at once.
func containerSignalContext(sigs <-chan os.Signal, interrupt func(cloudmeta.Notice), idle func() bool, grace time.Duration, logger logr.Logger) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		sig := <-sigs
		if sig != syscall.SIGTERM || grace <= 0 {
			return
		}
		deadline := time.Now().Add(grace)
		logger.Info("SIGTERM: notifying jobs before stopping", "grace", grace)
		interrupt(cloudmeta.Notice{Source: liveness.NoticeContainerSIGTERM, Action: "terminate", Deadline: deadline})
		var idleSince time.Time
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for time.Now().Before(deadline) {
			select {
			case <-sigs:
				return
			case <-tick.C:
			}
			if !idle() {
				idleSince = time.Time{}
				continue
			}
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if time.Since(idleSince) >= reportFlush {
				return
			}
		}
	}()
	return ctx
}

// runInit is the container's init when the agent starts as PID 1: it runs
// the agent as its child, forwards signals to it, and reaps every process
// that ends up orphaned to it (a job's children whose parent went first),
// so the container never fills with zombies. The agent itself is not PID 1,
// so its own waits never race the reaper. It exits as the agent did: the
// container's restart policy, not init, starts it again.
func runInit(argv, env, secrets []string) int {
	return supervise(supervision{argv: argv, env: env, secrets: secrets, stdout: os.Stdout})
}

// supervision is how supervise runs the agent.
type supervision struct {
	argv, env []string
	// secrets reach each run over a handoff pipe, never its environ.
	secrets []string
	// stdout is the agent's stdout and stderr.
	stdout *os.File
	// relaunch starts the agent again when it exits on its own: an attached
	// agent has no container restart behind it. A run that ends after a
	// stop signal, or with status 0, is not started again, nor is one that
	// failed attachMaxQuickExits times in a row, each in under
	// attachHealthyRun.
	relaunch     bool
	restartDelay time.Duration
	logf         func(format string, args ...any)
}

// stopSignals end the supervision once the agent exits; the others are
// only passed on.
var stopSignals = []os.Signal{syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT}

// supervise runs the agent as a child subreaper: what its jobs orphan is
// reparented here and reaped on SIGCHLD. Signals are forwarded to the
// agent. It returns the agent's exit status (128+n for signal n).
func supervise(s supervision) int {
	logf := s.logf
	if logf == nil {
		logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "zenthros-agent init: "+format+"\n", args...)
		}
	}
	// Pdeathsig belongs to the thread that started the child: keep it.
	runtime.LockOSThread()
	_ = unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
	// Both before the first start, so no exit or signal goes unseen.
	children := make(chan os.Signal, 1)
	signal.Notify(children, syscall.SIGCHLD)
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, append([]os.Signal{syscall.SIGUSR1, syscall.SIGUSR2}, stopSignals...)...)
	program, err := exec.LookPath(s.argv[0])
	if err != nil {
		logf("%v", err)
		return 1
	}
	stdout := s.stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	var child *os.Process
	var started time.Time
	start := func() error {
		files := []*os.File{os.Stdin, stdout, stdout}
		env := s.env
		if len(s.secrets) > 0 {
			// The agent proper reads what init took out of the environment
			// from descriptor 3; its environ holds none of it.
			pipe, err := handoffPipe(s.secrets)
			if err != nil {
				return err
			}
			defer pipe.Close()
			files = append(files, pipe)
			env = append(append([]string(nil), env...), envHandoffFD+"=3")
		}
		p, err := os.StartProcess(program, s.argv, &os.ProcAttr{Env: env, Files: files,
			// An agent outliving its supervisor would hold the backend
			// against the next one started with the same certificate.
			Sys: &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}})
		if err != nil {
			return fmt.Errorf("start agent: %w", err)
		}
		child, started = p, time.Now()
		return nil
	}
	if err := start(); err != nil {
		logf("%v", err)
		return 1
	}
	stopping, quick, delay := false, 0, s.restartDelay
	var retry <-chan time.Time
	for {
		select {
		case sig := <-sigs:
			if child != nil {
				_ = child.Signal(sig)
			}
			if slices.Contains(stopSignals, sig) {
				stopping = true
				if child == nil { // waiting to start it again
					return 0
				}
			}
		case <-retry:
			retry = nil
			if err := start(); err != nil {
				logf("%v", err)
				return 1
			}
		case <-children:
			for {
				var ws syscall.WaitStatus
				pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				if pid <= 0 || err != nil {
					break // none left that ended
				}
				if child == nil || pid != child.Pid {
					continue // an orphan, reaped
				}
				child = nil
				code := ws.ExitStatus()
				if ws.Signaled() {
					code = 128 + int(ws.Signal())
				}
				if !s.relaunch || stopping || code == 0 || code == agent.ExitDetached {
					// Detached: the hub refuses it now, so a new run would
					// only fail again.
					return code
				}
				ran := time.Since(started)
				if ran >= attachHealthyRun {
					quick, delay = 0, s.restartDelay
				}
				if quick++; quick >= attachMaxQuickExits {
					logf("the agent exited %d times in a row within %s of starting (last: status %d); gave up", quick, attachHealthyRun, code)
					return code
				}
				logf("the agent exited with status %d after %s; starting it again in %s", code, ran.Round(time.Millisecond), delay)
				retry = time.After(delay)
				delay = min(2*delay, attachMaxRestartDelay)
			}
		}
	}
}

// containerToolchain is the container's prepared Python, shared by the
// executor (the jobs' env) and runAgent (which prepares it).
var containerToolchain = &toolchainEnv{}

// toolchainEnv is the prepared Python environment every job gets: empty until
// the environment is ready, which is also before any job can be submitted.
type toolchainEnv struct {
	mu  sync.RWMutex
	env pyenv.Env
}

func (t *toolchainEnv) set(env pyenv.Env) {
	t.mu.Lock()
	t.env = env
	t.mu.Unlock()
}

func (t *toolchainEnv) pathPrefix() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.env.PathPrefix
}

func (t *toolchainEnv) extraEnv() map[string]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.env.Vars
}

// toolchainSteps are what prepareToolchains does, replaceable in tests.
type toolchainSteps struct {
	prepare     func(context.Context, pyenv.Toolchain) (pyenv.Env, error)
	markPending func() error
	markReady   func() error
	markFailed  func(error) error
}

// prepareToolchains completes the image's Python (ZENTHROS_TOOLCHAINS) while
// the agent is already connected. The gate is closed before it returns, so
// the agent's first heartbeat already says it takes no work; the job env is
// published before the gate opens, so the first job sees the venv. The
// channel carries the outcome once: an error means the container cannot run
// the jobs it was made for.
func prepareToolchains(ctx context.Context, raw string, steps toolchainSteps, holder *toolchainEnv, logger logr.Logger) (<-chan error, error) {
	toolchains, err := pyenv.Parse(raw)
	if err != nil || len(toolchains) == 0 {
		return nil, err
	}
	if err := steps.markPending(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		started := time.Now()
		logger.Info("preparing the python environment; taking no work until it is ready", "packages", toolchains[0].Packages)
		env, err := steps.prepare(ctx, toolchains[0])
		if err == nil {
			holder.set(env)
			err = steps.markReady()
		}
		if err == nil {
			logger.Info("python environment ready", "took", time.Since(started).Round(time.Millisecond))
		} else if failed := steps.markFailed(err); failed != nil {
			logger.Error(failed, "could not record why the python environment failed")
		}
		done <- err
	}()
	return done, nil
}

// afterPrepare is what an agent does with the outcome of preparing its Python.
// Connected, it stays: the gate stays closed, and its heartbeat says why to
// whoever waits for the machine, which a restart would only repeat. With no
// hub to tell (local mode), it stops, and its exit says it.
func afterPrepare(err error, local bool, stop func(error), logger logr.Logger) {
	if err == nil {
		return
	}
	logger.Error(err, "the python environment cannot be prepared; taking no work")
	if local {
		stop(fmt.Errorf("prepare the python environment: %w", err))
	}
}

// markToolchainFailed records why, without what a URL's credentials would say.
func markToolchainFailed(err error) error {
	return runtimegate.MarkFailed(urlredact.Error(err).Error())
}

// preparePython is the real prepare step: uv through the pin chain the agent
// came with, or the image's own uv when no artifact base is set.
func preparePython(logger logr.Logger) func(context.Context, pyenv.Toolchain) (pyenv.Env, error) {
	return func(ctx context.Context, tc pyenv.Toolchain) (pyenv.Env, error) {
		uv, err := exec.LookPath("uv")
		urls := pyenv.ParseArtifactURLs(os.Getenv(pyenv.EnvArtifactURLs))
		if base := os.Getenv(pyenv.EnvInstallBase); base != "" || len(urls) > 0 || err != nil {
			uv, err = pyenv.FetchUV(ctx, pyenv.Fetch{
				Base:         base,
				ArtifactURLs: urls,
				SumsSHA256:   os.Getenv(pyenv.EnvSumsSHA256),
				Arch:         runtime.GOARCH,
				Dir:          containerPath("bin"),
			})
			if err != nil {
				return pyenv.Env{}, err
			}
		}
		return pyenv.Prepare(ctx, pyenv.Config{
			Root:   containerPath("python"),
			Python: os.Getenv("ZENTHROS_PYTHON"),
			UV:     uv,
			Logger: logger.WithName("pyenv"),
		}, tc)
	}
}
