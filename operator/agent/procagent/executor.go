// SPDX-License-Identifier: Apache-2.0

// Package procagent is the container agent's executor: each job is a child
// process of the agent, in the container's own image. It assumes only what
// docs/design/container-agent.md lists: a writable state directory, signals,
// and the image as it is. It never runs a shell of its own, never mounts,
// and never needs root, systemd or a container runtime; it does not import
// the VM executor's packages (pkg/systemd, pkg/containerd).
package procagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/zenthros/operator/agent/agepayload"
	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/capability"
	"github.com/zenthros/operator/pkg/containerfacts"
	"github.com/zenthros/operator/pkg/jobsource"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/jobspec/fingerprint"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/runtimegate"
	"github.com/zenthros/operator/pkg/secretsource"
)

// EnvContainerImage is the image the container runs, as whoever started it
// says. Jobs on this container run in it and no other.
const EnvContainerImage = "ZENTHROS_CONTAINER_IMAGE"

// defaultKeptLogs is how many released jobs' logs a container keeps.
const defaultKeptLogs = 20

// defaultMaxLogBytes caps one job's log: twenty kept logs stay well inside
// the 10 GB overlay of a RunPod or Vast.ai container, where the agent's
// certificate lives too.
const defaultMaxLogBytes = 128 << 20

// logDrainWait is how long a finished job's last output may take to reach
// its log before the result is recorded.
const logDrainWait = 2 * time.Second

// defaultKillGrace is how long a cancelled job has between SIGTERM and SIGKILL.
const defaultKillGrace = 10 * time.Second

// Files under each job's directory.
const (
	logFile        = "log"
	resultFile     = "result.json"
	specFile       = "spec.json"
	secretsDirName = "secrets"
	workDirName    = "work"
	// retainFile marks a job that keeps its work directory past Cancel.
	retainFile = "retain"
)

// envJobDir names, in every process a job starts, the job's directory: a
// restarted agent finds the processes an earlier session left by it, and
// only its own, whose directories are under its state directory.
const envJobDir = "ZENTHROS_PROCAGENT_JOB_DIR"

// Config is what the executor needs from the agent.
type Config struct {
	// StateDir holds every job's directory (jobs/<handle>). Required.
	StateDir string
	// Image is the container's own image (ZENTHROS_CONTAINER_IMAGE). Empty
	// means the container did not say, and a job naming an image is refused:
	// it cannot be checked against what runs here.
	Image string
	// Age decrypts encrypted-payload secret sources.
	Age agepayload.Config
	// AgentBin is this agent's executable; a job reaches the job protocol
	// with `$ZENTHROS_AGENT_BIN job ...`. Empty leaves the variable out.
	AgentBin string
	// KillGrace is the time between SIGTERM and SIGKILL on Cancel. Zero is 10s.
	KillGrace time.Duration
	// KeptLogs is how many released jobs' logs stay; older ones go, so a
	// long-lived container's state dir does not fill with them. Zero is 20.
	KeptLogs int
	// MaxLogBytes caps each job's log; past it the log says it was cut and
	// the rest of the output is dropped. Zero is 128 MiB.
	MaxLogBytes int64
	// BaseEnv is the environment a job inherits: the container's, which
	// carries the image's PATH, CUDA and library settings. Nil reads it from
	// the agent's own environment. The agent's ZENTHROS_* settings (its join
	// token among them) are never passed on.
	BaseEnv []string
	// PathPrefix returns directories put in front of the job's PATH (a
	// prepared Python environment). Nil adds none.
	PathPrefix func() []string
	// ExtraEnv returns variables every job gets (VIRTUAL_ENV). Nil adds none.
	ExtraEnv func() map[string]string
	Logger   logr.Logger
}

// Executor runs jobs as child processes. It is safe for concurrent use.
type Executor struct {
	cfg  Config
	host func(context.Context) capability.Host

	mu   sync.Mutex
	jobs map[string]*job
	// starting holds the handles whose process is being prepared (source
	// fetched, secrets written) outside mu.
	starting map[string]*startSlot
}

// startSlot is a handle reserved while its job is prepared.
type startSlot struct {
	hash      string
	ready     chan struct{} // closed when the start ends, either way
	cancelled bool          // Cancel came during the start
}

type job struct {
	handle string
	dir    string
	pgid   int
	signal syscall.Signal // preempt signal; 0 sends none
	start  time.Time
	done   chan struct{} // closed once result.json is written
	// res is how the process ended, set before done closes. Status reads
	// it when result.json could not be written (a full disk): the job
	// still ends.
	res *result
}

// result is a finished process, persisted before the job reads as finished.
type result struct {
	ExitCode   int       `json:"exitCode"`
	Signal     string    `json:"signal,omitempty"`
	Error      string    `json:"error,omitempty"`
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`
}

// New builds an executor. The state directory must be writable.
func New(cfg Config) (*Executor, error) {
	if cfg.StateDir == "" {
		return nil, errors.New("procagent: a state directory is required")
	}
	if err := os.MkdirAll(filepath.Join(cfg.StateDir, "jobs"), 0o700); err != nil {
		return nil, fmt.Errorf("procagent: state directory is not writable: %w", err)
	}
	if cfg.KillGrace <= 0 {
		cfg.KillGrace = defaultKillGrace
	}
	if cfg.KeptLogs <= 0 {
		cfg.KeptLogs = defaultKeptLogs
	}
	if cfg.MaxLogBytes <= 0 {
		cfg.MaxLogBytes = defaultMaxLogBytes
	}
	if cfg.BaseEnv == nil {
		cfg.BaseEnv = os.Environ()
	}
	// A container's jobs never outlive its agent (Close); those of an agent
	// that died do not outlive the next one either.
	killOrphans(filepath.Join(cfg.StateDir, "jobs"), cfg.Logger)
	e := &Executor{cfg: cfg, jobs: map[string]*job{}, starting: map[string]*startSlot{}}
	facts := sync.OnceValue(func() capability.Host {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return containerfacts.Read(ctx, containerfacts.Sources{})
	})
	e.host = func(context.Context) capability.Host { return facts() }
	return e, nil
}

// Declare says what this container runs: a Job, as a process, in its image.
func (e *Executor) Declare(ctx context.Context) (capability.Set, capability.Host) {
	host := e.host(ctx)
	host.ContainerImage = e.cfg.Image
	host.SecretStores = capability.SecretStoresContainer()
	return capability.Container(), host
}

func (e *Executor) jobDir(handle string) string {
	return filepath.Join(e.cfg.StateDir, "jobs", handle)
}

// Submit starts the job's process. A resubmit with the same spec adopts the
// running or finished process; with a changed spec it is a spec mismatch.
func (e *Executor) Submit(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (string, error) {
	spec, env = jobspec.AttemptScoped(spec, env)
	if spec.Kind != v1.WorkloadKindJob {
		return "", fmt.Errorf("%w: %q is not run in a container; it runs %s", backend.ErrUnsupportedKind, spec.Kind, v1.WorkloadKindJob)
	}
	if spec.Runtime != "" && spec.Runtime != v1.RuntimeProcess {
		return "", fmt.Errorf("%w: a container runs Job under %s; not %s", backend.ErrUnsupportedBackend, v1.RuntimeProcess, spec.Runtime)
	}
	if err := e.checkImage(spec.Image); err != nil {
		return "", err
	}
	if !runtimegate.Ready() {
		return "", fmt.Errorf("%w: the container's environment is still being prepared", backend.ErrBackendBusy)
	}
	if e.cfg.AgentBin != "" {
		env = withEnv(env, jobspec.EnvAgentBin, e.cfg.AgentBin)
	}
	handle := spec.ID
	hash := specHash(spec, env)

	e.mu.Lock()
	for {
		slot, ok := e.starting[handle]
		if !ok {
			break
		}
		if slot.hash != hash {
			e.mu.Unlock()
			return "", fmt.Errorf("%w: job %s is already starting here with a different spec", backend.ErrSpecMismatch, handle)
		}
		// The same job is being started: its outcome is this one's.
		e.mu.Unlock()
		<-slot.ready
		e.mu.Lock()
	}
	if j, ok := e.jobs[handle]; ok {
		e.mu.Unlock()
		if recorded, _ := readSpecHash(j.dir); recorded != hash {
			return "", fmt.Errorf("%w: job %s is already running here with a different spec", backend.ErrSpecMismatch, handle)
		}
		return handle, nil
	}
	dir := e.jobDir(handle)
	slot := &startSlot{hash: hash, ready: make(chan struct{})}
	e.starting[handle] = slot
	e.mu.Unlock()
	j, err := e.prepare(ctx, spec, env, dir, hash)
	e.mu.Lock()
	delete(e.starting, handle)
	cancelled := slot.cancelled
	if err == nil && j != nil && !cancelled {
		e.jobs[handle] = j
	}
	e.mu.Unlock()
	close(slot.ready)
	if err != nil {
		return "", err
	}
	if j != nil && cancelled {
		// Cancelled while it started: it does not run.
		e.stop(ctx, j)
		_ = e.Cancel(ctx, handle, owner.Ref{})
	}
	return handle, nil
}

// prepare runs outside mu: fetching the source (a git clone) may take
// minutes, and Status and Cancel of every other job wait for none of it. A
// nil job with no error is one that already finished here.
func (e *Executor) prepare(ctx context.Context, spec jobspec.JobSpec, env map[string]string, dir, hash string) (*job, error) {
	handle := spec.ID
	if _, err := readResult(dir); err == nil {
		// It finished, and this agent outlived it or was restarted with the
		// state directory intact: Status reports how it ended.
		if recorded, _ := readSpecHash(dir); recorded != hash {
			return nil, fmt.Errorf("%w: job %s already ran here with a different spec", backend.ErrSpecMismatch, handle)
		}
		return nil, nil
	}
	// Anything else in the directory is from a process that died with an
	// earlier agent session: it is not this run's.
	if err := os.RemoveAll(dir); err != nil {
		return nil, fmt.Errorf("clear job directory: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create job directory: %w", err)
	}
	j, err := e.start(ctx, spec, env, dir, hash)
	if err != nil {
		if cleanup := os.RemoveAll(dir); cleanup != nil {
			err = errors.Join(err, fmt.Errorf("and its directory, secrets included, stayed: %w", cleanup))
		}
		return nil, err
	}
	return j, nil
}

// checkImage refuses a job naming an image other than the container's.
func (e *Executor) checkImage(image string) error {
	if strings.TrimSpace(image) == "" {
		return nil
	}
	if e.cfg.Image == "" {
		return fmt.Errorf("%w: this container did not say which image it runs (%s), so a job naming %s cannot be checked against it",
			backend.ErrUnsupportedBackend, EnvContainerImage, image)
	}
	if !(capability.Host{ContainerImage: e.cfg.Image}).RunsImage(image) {
		return fmt.Errorf("%w: this container runs image %s; jobs on it run in that image, not %s",
			backend.ErrUnsupportedBackend, e.cfg.Image, image)
	}
	return nil
}

func (e *Executor) start(ctx context.Context, spec jobspec.JobSpec, env map[string]string, dir, hash string) (*job, error) {
	workDir, err := jobsource.Prepare(ctx, spec, filepath.Join(dir, workDirName))
	if err != nil {
		if errors.Is(err, jobsource.ErrUnsupported) {
			return nil, backend.Classified(backend.ErrInvalid, err)
		}
		return nil, err
	}
	if spec.WorkingDir != "" {
		workDir = spec.WorkingDir
	}
	if workDir == "" {
		// The image's root may be read-only; the job gets a directory it can write.
		workDir = filepath.Join(dir, workDirName)
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			return nil, err
		}
	}
	secretEnv, err := e.materializeSecrets(spec, dir, workDir)
	if err != nil {
		return nil, err
	}
	jobEnv := e.environment(env, secretEnv)
	argv, err := commandLine(spec, lookupEnv(jobEnv, "PATH"))
	if err != nil {
		return nil, err
	}
	signal, err := preemptSignal(spec.PreemptSignal)
	if err != nil {
		return nil, backend.Classified(backend.ErrInvalid, err)
	}
	if err := writeFile(filepath.Join(dir, specFile), []byte(hash)); err != nil {
		return nil, err
	}
	if spec.RetainWorkdir {
		// On disk, as the unit file holds it on a VM: a Cancel after an agent
		// restart, or a second one, still reads it (07#2).
		if err := writeFile(filepath.Join(dir, retainFile), nil); err != nil {
			return nil, err
		}
	}
	logOut, err := os.OpenFile(filepath.Join(dir, logFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open job log: %w", err)
	}
	// The job writes into a pipe the executor drains into the log, up to
	// MaxLogBytes. The pipe is an *os.File, so cmd.Wait does not wait
	// for a process the job left behind that still holds it.
	pipeOut, pipeIn, err := os.Pipe()
	if err != nil {
		logOut.Close()
		return nil, fmt.Errorf("open job log pipe: %w", err)
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		defer logOut.Close()
		defer pipeOut.Close()
		copyCapped(logOut, pipeOut, e.cfg.MaxLogBytes)
	}()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(jobEnv, envJobDir+"="+dir)
	cmd.Dir = workDir
	cmd.Stdout = pipeIn
	cmd.Stderr = pipeIn
	// Its own process group: Cancel and the preempt signal reach every
	// process the job started, and nothing else.
	// Pdeathsig ends the leader with the agent (D9, 07#5); killOrphans
	// ends the rest when the agent comes back. The signal is sent when the
	// thread that started the job exits, so the start runs on a locked
	// thread, which the runtime keeps after it is unlocked.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	started := time.Now()
	runtime.LockOSThread()
	err = cmd.Start()
	runtime.UnlockOSThread()
	pipeIn.Close() // the job holds its own copy
	if err != nil {
		<-drained
		return nil, startFailed(argv[0], err)
	}
	j := &job{
		handle: spec.ID,
		dir:    dir,
		pgid:   cmd.Process.Pid,
		signal: signal,
		start:  started,
		done:   make(chan struct{}),
	}
	go e.wait(j, cmd, drained)
	return j, nil
}

// wait records how the process ended, then marks the job finished. The
// result is on disk before Status can read the job as finished, so a
// finished job reads the same after an agent restart as before it.
func (e *Executor) wait(j *job, cmd *exec.Cmd, drained <-chan struct{}) {
	err := cmd.Wait()
	// What the job left behind ends with it, now that its group still
	// names it: a Cancel minutes later (the GC cancels every finished job)
	// signalled the group's number after the kernel could have given it to
	// another job's leader. stop does not signal
	// an ended job.
	_ = killGroup(-j.pgid, syscall.SIGKILL)
	// The last lines reach the log before the job reads as finished; a
	// process the job left behind does not hold it up past logDrainWait.
	timer := time.NewTimer(logDrainWait)
	select {
	case <-drained:
	case <-timer.C:
	}
	timer.Stop()
	res := result{ExitCode: cmd.ProcessState.ExitCode(), StartedAt: j.start, FinishedAt: time.Now()}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		res.Signal = status.Signal().String()
	}
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		res.Error = err.Error()
	}
	if raw, marshalErr := json.Marshal(res); marshalErr == nil {
		if writeErr := writeFile(filepath.Join(j.dir, resultFile), raw); writeErr != nil {
			e.cfg.Logger.Error(writeErr, "could not record how the job ended", "handle", j.handle)
		}
	}
	j.res = &res
	close(j.done)
}

// Status reports the job in Zenthros phases. A job this container does not
// know is not found: on a container that means it ended with an earlier
// agent session (the container restarted), and the controller submits it
// again.
func (e *Executor) Status(_ context.Context, handle string) (backend.JobState, error) {
	e.mu.Lock()
	j, ok := e.jobs[handle]
	_, starting := e.starting[handle]
	e.mu.Unlock()
	if starting {
		return backend.JobState{Phase: v1.PhasePending}, nil
	}
	if ok {
		select {
		case <-j.done:
		default:
			return backend.JobState{Phase: v1.PhaseRunning, StartTime: timePtr(j.start)}, nil
		}
	}
	dir := e.jobDir(handle)
	res, err := readResult(dir)
	if err != nil && ok && j.res != nil {
		res, err = *j.res, nil
	}
	if err != nil {
		return backend.JobState{}, backend.Classified(backend.ErrNotFound,
			fmt.Errorf("job %s is not running in this container: it never started here, or it ended with an earlier agent session", handle))
	}
	return stateOf(res, tail(filepath.Join(dir, logFile), 3)), nil
}

func stateOf(res result, lastLines string) backend.JobState {
	state := backend.JobState{StartTime: timePtr(res.StartedAt), CompletionTime: timePtr(res.FinishedAt)}
	if res.Signal == "" && res.Error == "" {
		code := int32(res.ExitCode)
		state.ExitCode = &code
	}
	if res.ExitCode == 0 && res.Signal == "" && res.Error == "" {
		state.Phase = v1.PhaseSucceeded
		state.Message = "process exited successfully"
		return state
	}
	detail := fmt.Sprintf("process exited with code %d", res.ExitCode)
	switch {
	case res.Error != "":
		detail = res.Error
	case res.Signal != "":
		detail = "process was killed by signal " + res.Signal
	}
	state.Phase = v1.PhaseFailed
	state.Reasons = []v1.Reason{{Code: v1.ReasonProcessFailed, Detail: detail}}
	state.Message = detail
	if lastLines != "" {
		state.Message += "; last log: " + lastLines
	}
	return state
}

// Cancel stops the job's process group (SIGTERM, then SIGKILL after the
// grace) and removes its directory unless the job asked to retain it. The
// log stays: a Managed slot's release cancels a job that already ended, and
// its logs are still read afterwards, as a VM's journal outlives the unit.
// The result goes, so Status no longer knows the job.
// Idempotent: a job that is not here is not an error.
func (e *Executor) Cancel(ctx context.Context, handle string, _ owner.Ref) error {
	e.mu.Lock()
	j, ok := e.jobs[handle]
	delete(e.jobs, handle)
	if slot, starting := e.starting[handle]; starting {
		// Submit stops it once its start ends.
		slot.cancelled = true
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()
	dir := e.jobDir(handle)
	if ok {
		e.stop(ctx, j)
	}
	if err := e.cleanupSecrets(dir); err != nil {
		return fmt.Errorf("remove the secrets of job %s: %w", handle, err)
	}
	if _, err := os.Stat(filepath.Join(dir, retainFile)); err == nil {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read job directory: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() == logFile {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("remove job directory: %w", err)
		}
	}
	e.pruneLogs()
	return nil
}

// EndedJobs lists the jobs that ended and whose directory still holds more than
// their log, secrets included, until their Cancel takes it, those of an earlier
// agent session too. A container is one backend's; a handle is its job's ID.
func (e *Executor) EndedJobs(_ context.Context, _ string) ([]backend.InstalledJob, error) {
	root := filepath.Join(e.cfg.StateDir, "jobs")
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the jobs' directories: %w", err)
	}
	var jobs []backend.InstalledJob
	var errs []error
	for _, entry := range entries {
		handle := entry.Name()
		files, err := os.ReadDir(filepath.Join(root, handle))
		if err != nil {
			errs = append(errs, fmt.Errorf("read job directory: %w", err))
			continue
		}
		released := len(files) == 0 || (len(files) == 1 && files[0].Name() == logFile)
		if !released && !e.runs(handle) {
			jobs = append(jobs, backend.InstalledJob{JobID: handle, Handle: handle})
		}
	}
	return jobs, errors.Join(errs...)
}

// runs is whether the job of handle is starting or running now.
func (e *Executor) runs(handle string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, starting := e.starting[handle]; starting {
		return true
	}
	j, known := e.jobs[handle]
	if !known {
		return false
	}
	select {
	case <-j.done:
		return false
	default:
		return true
	}
}

// pruneLogs removes the oldest released jobs' directories beyond KeptLogs.
// A released job's directory holds its log and nothing else; a running,
// finished-but-unreleased or retained job's holds more and is never pruned.
func (e *Executor) pruneLogs() {
	root := filepath.Join(e.cfg.StateDir, "jobs")
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	type kept struct {
		dir     string
		modTime time.Time
	}
	var logs []kept
	for _, entry := range entries {
		dir := filepath.Join(root, entry.Name())
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 1 || files[0].Name() != logFile {
			continue
		}
		info, err := files[0].Info()
		if err != nil {
			continue
		}
		logs = append(logs, kept{dir: dir, modTime: info.ModTime()})
	}
	if len(logs) <= e.cfg.KeptLogs {
		return
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i].modTime.Before(logs[j].modTime) })
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, l := range logs[:len(logs)-e.cfg.KeptLogs] {
		if _, running := e.jobs[filepath.Base(l.dir)]; running {
			continue // resubmitted under the same handle since it was listed
		}
		if _, starting := e.starting[filepath.Base(l.dir)]; starting {
			continue
		}
		_ = os.RemoveAll(l.dir)
	}
}

// stop ends the process group: SIGTERM, then SIGKILL once the grace ends.
func (e *Executor) stop(ctx context.Context, j *job) {
	select {
	case <-j.done:
		// Its group was killed when it ended; the number may be another's.
		return
	default:
	}
	_ = killGroup(-j.pgid, syscall.SIGTERM)
	timer := time.NewTimer(e.cfg.KillGrace)
	defer timer.Stop()
	select {
	case <-j.done:
	case <-timer.C:
	case <-ctx.Done():
	}
	// Every process the job started, including ones that outlived its leader.
	_ = killGroup(-j.pgid, syscall.SIGKILL)
	<-j.done
}

// killOrphans kills every process whose job directory is under jobsRoot:
// what an earlier session of this agent started and left running. A
// process's environment says whose job it is, never its number, which the
// kernel gives again; another agent's jobs, under another state directory,
// are not touched. A process that is gone or not readable is not one.
func killOrphans(jobsRoot string, logger logr.Logger) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		logger.Error(err, "could not look for the processes an earlier agent left")
		return
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		environ, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "environ"))
		if err != nil {
			continue
		}
		for _, kv := range bytes.Split(environ, []byte{0}) {
			dir, ok := bytes.CutPrefix(kv, []byte(envJobDir+"="))
			if ok && filepath.Dir(string(dir)) == jobsRoot {
				logger.Info("killing a process an earlier agent session's job left", "pid", pid, "job_dir", string(dir))
				_ = syscall.Kill(pid, syscall.SIGKILL)
				break
			}
		}
	}
}

// killGroup signals a process group; a test watches it.
var killGroup = syscall.Kill

// Close stops every running job, as when the agent shuts down: a container's
// jobs never outlive its agent.
func (e *Executor) Close(ctx context.Context) {
	e.mu.Lock()
	jobs := make([]*job, 0, len(e.jobs))
	for _, j := range e.jobs {
		jobs = append(jobs, j)
	}
	e.mu.Unlock()
	var wg sync.WaitGroup
	for _, j := range jobs {
		select {
		case <-j.done:
			continue
		default:
		}
		wg.Add(1)
		go func(j *job) {
			defer wg.Done()
			e.stop(ctx, j)
		}(j)
	}
	wg.Wait()
}

// SignalPreempt sends the job its preempt signal after the notice file is
// written. It reports whether a signal was sent.
func (e *Executor) SignalPreempt(_ context.Context, handle string) (bool, error) {
	e.mu.Lock()
	j, ok := e.jobs[handle]
	e.mu.Unlock()
	if !ok || j.signal == 0 {
		return false, nil
	}
	select {
	case <-j.done:
		return false, nil
	default:
	}
	if err := syscall.Kill(-j.pgid, j.signal); err != nil {
		return false, fmt.Errorf("signal job %s: %w", handle, err)
	}
	return true, nil
}

// environment is the job's environment: the container's, without the
// agent's own settings, then the prepared paths, then what the controller
// and the agent set, then the job's secrets.
func (e *Executor) environment(env, secretEnv map[string]string) []string {
	merged := map[string]string{}
	for _, kv := range e.cfg.BaseEnv {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || strings.HasPrefix(name, "ZENTHROS_") {
			continue
		}
		merged[name] = value
	}
	if e.cfg.ExtraEnv != nil {
		for name, value := range e.cfg.ExtraEnv() {
			merged[name] = value
		}
	}
	if e.cfg.PathPrefix != nil {
		if dirs := e.cfg.PathPrefix(); len(dirs) > 0 {
			path := strings.Join(dirs, string(os.PathListSeparator))
			if current := merged["PATH"]; current != "" {
				path += string(os.PathListSeparator) + current
			}
			merged["PATH"] = path
		}
	}
	for name, value := range env {
		merged[name] = value
	}
	for name, value := range secretEnv {
		merged[name] = value
	}
	if merged["PATH"] == "" {
		merged["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	}
	out := make([]string, 0, len(merged))
	for name, value := range merged {
		out = append(out, name+"="+value)
	}
	sort.Strings(out)
	return out
}

// materializeSecrets decrypts the job's encrypted payloads here: env values
// go to the process environment, in memory, and file values under the job's
// own directory (<dir>/secrets, inside ZENTHROS_STATE_DIR, never the image's
// read-only root). A file without an explicit path is read as
// $ZENTHROS_SECRETS_DIR/<name>/<key>; an explicit path must be under the
// job's directory or working directory, where it is linked. On error
// nothing is left. A type this container cannot resolve refuses the job
// before it runs.
func (e *Executor) materializeSecrets(spec jobspec.JobSpec, dir, workDir string) (map[string]string, error) {
	for _, ref := range spec.SecretSources {
		if ref.Type != v1.SecretSourceTypeEncryptedPayload {
			return nil, backend.NewSecretSourceError(v1.ReasonUnsupportedSecretSource,
				fmt.Sprintf("secret source type %q is not resolved in a container", ref.Type))
		}
	}
	bindings, err := agepayload.Bindings(spec, e.cfg.Age)
	if err != nil {
		return nil, err
	}
	env := map[string]string{}
	var files []secretfile.Binding
	defaults := false
	for _, b := range bindings {
		if b.Ref.Materialization.Mode == v1.SecretMaterializationFile {
			files = append(files, b)
			defaults = defaults || secretsource.AtDefaultPlace(b.Ref)
			continue
		}
		env[secretsource.EnvName(b.Ref)] = b.Value
	}
	result, err := secretfile.Apply(dir, secretsDirName, files)
	if err != nil {
		return nil, err
	}
	if err := secretfile.Link(result, []string{dir, workDir}); err != nil {
		// prepare removes the job's directory, its secrets with it.
		return nil, backend.Classified(backend.ErrInvalid, err)
	}
	if defaults {
		env[secretfile.EnvSecretsDir] = secretfile.SecretsDir(dir, secretsDirName)
	}
	return env, nil
}

func (e *Executor) cleanupSecrets(dir string) error {
	return secretfile.CleanupRoot(dir, secretsDirName)
}

// startFailed classifies a job that did not start. A machine with no room
// for one more process (its pid limit, a script another job's fork still
// holds open, no file or memory left) is busy, and the controller requeues
// the job; anything else is the job's command.
func startFailed(name string, err error) error {
	err = fmt.Errorf("start %s: %w", name, err)
	for _, full := range []syscall.Errno{syscall.EAGAIN, syscall.ETXTBSY, syscall.EMFILE, syscall.ENFILE, syscall.ENOMEM} {
		if errors.Is(err, full) {
			return backend.Classified(backend.ErrBackendBusy, err)
		}
	}
	return backend.Classified(backend.ErrInvalid, err)
}

// commandLine is the argv the job runs. A structured command runs as it is,
// with no shell. An entrypoint is a shell string, so it needs a shell
// in the image: bash when there is one (as on a machine), else sh.
func commandLine(spec jobspec.JobSpec, path string) ([]string, error) {
	if len(spec.Command) > 0 {
		program, err := lookPath(spec.Command[0], path)
		if err != nil {
			return nil, backend.Classified(backend.ErrInvalid, err)
		}
		argv := append([]string{program}, spec.Command[1:]...)
		return append(argv, spec.Args...), nil
	}
	if strings.TrimSpace(spec.Entrypoint) == "" {
		return nil, backend.Classified(backend.ErrInvalid, errors.New("the job has neither a command nor an entrypoint"))
	}
	for _, shell := range []string{"/bin/bash", "/bin/sh"} {
		if isExecutable(shell) {
			return []string{shell, "-c", spec.Entrypoint}, nil
		}
	}
	return nil, backend.Classified(backend.ErrInvalid,
		errors.New("entrypoint is a shell string and this image has no /bin/bash or /bin/sh; give the job a command instead"))
}

// lookPath finds name on the job's PATH, not the agent's.
func lookPath(name, path string) (string, error) {
	if strings.Contains(name, "/") {
		if isExecutable(name) {
			return name, nil
		}
		return "", fmt.Errorf("command %s is not an executable file in this image", name)
	}
	for _, dir := range filepath.SplitList(path) {
		if dir == "" {
			continue
		}
		candidate := filepath.Join(dir, name)
		if isExecutable(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("command %s is not on the job's PATH (%s) in this image", name, path)
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

var preemptSignals = map[string]syscall.Signal{
	"SIGTERM": syscall.SIGTERM, "SIGINT": syscall.SIGINT,
	"SIGUSR1": syscall.SIGUSR1, "SIGUSR2": syscall.SIGUSR2,
}

func preemptSignal(name string) (syscall.Signal, error) {
	if name == "" {
		return 0, nil
	}
	if sig, ok := preemptSignals[name]; ok {
		return sig, nil
	}
	return 0, fmt.Errorf("preempt signal %q is not one of SIGTERM, SIGINT, SIGUSR1, SIGUSR2", name)
}

// specHash fingerprints what defines a run, so a resubmit adopts the same
// process and a changed one is a spec mismatch.
func specHash(spec jobspec.JobSpec, env map[string]string) string {
	return fingerprint.Of(spec, env)
}

func readSpecHash(dir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, specFile))
	return string(raw), err
}

func readResult(dir string) (result, error) {
	var res result
	raw, err := os.ReadFile(filepath.Join(dir, resultFile))
	if err != nil {
		return res, err
	}
	return res, json.Unmarshal(raw, &res)
}

// writeFile replaces path atomically: a reader sees the old file or the new
// one, never half of it.
func writeFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func withEnv(env map[string]string, name, value string) map[string]string {
	out := make(map[string]string, len(env)+1)
	for k, v := range env {
		out[k] = v
	}
	out[name] = value
	return out
}

func lookupEnv(env []string, name string) string {
	for _, kv := range env {
		if k, v, ok := strings.Cut(kv, "="); ok && k == name {
			return v
		}
	}
	return ""
}

func timePtr(t time.Time) *metav1.Time {
	if t.IsZero() {
		return nil
	}
	mt := metav1.NewTime(t)
	return &mt
}

// SetAge sets the identity encrypted payloads are decrypted with.
func (e *Executor) SetAge(age agepayload.Config) {
	e.cfg.Age = age
}

// Running is how many jobs have a live process.
func (e *Executor) Running() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := len(e.starting)
	for _, j := range e.jobs {
		if !finished(j) {
			n++
		}
	}
	return n
}
