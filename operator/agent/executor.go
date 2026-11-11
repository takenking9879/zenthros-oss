// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/zenthros/operator/agent/agepayload"
	"github.com/zenthros/operator/agent/awssecret"
	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/capability"
	ctd "github.com/zenthros/operator/pkg/containerd"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/prewarm/cache"
	"github.com/zenthros/operator/pkg/secretsource"
	"github.com/zenthros/operator/pkg/shellword"
	"github.com/zenthros/operator/pkg/systemd"
)

// Executor runs jobs on the agent's local backend. It is the seam that lets the same
// Agent run on a VM (systemd units) or inside a Kubernetes cluster (native resources)
// without changing the control-channel handlers: each implementation maps its native
// lifecycle onto Submit/Cancel/Status and reports a backend.JobState (architecture §3).
type Executor interface {
	// Submit creates and starts the job's native resource, returning an opaque,
	// backend-native handle that is echoed back on Status/Cancel.
	Submit(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (handle string, err error)

	// Cancel stops and removes the job's resource. Idempotent: an absent resource is
	// not an error. job is whose it is (the CancelRequest's ID): an executor
	// whose handles name what users chose (Kubernetes objects) deletes only
	// what carries it.
	Cancel(ctx context.Context, handle string, job owner.Ref) error

	// Status returns the job's observed state in Zenthros terms, enriching a failure
	// with implementation-specific detail (e.g. the systemd journal tail).
	Status(ctx context.Context, handle string) (backend.JobState, error)

	// Logs returns a channel of log chunks for the job. If follow is true, the
	// channel stays open until the job terminates or ctx is cancelled. tailLines
	// limits initial output (-1 = all).
	Logs(ctx context.Context, handle string, follow bool, tailLines int) (<-chan backend.LogChunk, error)
}

// SystemdExecutor adapts the systemd Manager to the Executor interface: jobs run as
// per-job units and a failed unit's recent journal is folded into the status message.
// It is the VM execution path.
type SystemdExecutor struct {
	Manager *systemd.Manager
	Secrets awssecret.Config
	Age     agepayload.Config
	// Registry logs image pulls in to registries that need the machine's
	// identity (Amazon ECR); nil pulls every image anonymously.
	Registry ctd.RegistryAuth
	// ReportSocketDir is the directory holding the agent's report socket and
	// nothing else; a container job sees it read-only at the same path.
	// Empty gives container jobs no report socket.
	ReportSocketDir string
	// Cache is the machine's warm cache (Prewarm); nil disables it.
	Cache *cache.Manager
	// AgentBin is this agent's executable, which a job runs as
	// `$ZENTHROS_AGENT_BIN job ...`; empty leaves the variable out.
	AgentBin string
	// UV is the machine's uv (HostUV), bound read-only into every container
	// job at ContainerUV; empty binds none.
	UV string
	// Images removes the job images no job uses; nil removes none.
	Images *ImageGC
	// UserNS runs container jobs in a user namespace of their Workspace's:
	// the machine can (containerd.UserNamespaces).
	UserNS bool
}

// HostUV is where install-agent.sh puts the pinned uv of a machine with
// containerd (install_uv_tool), the static build, which runs in an image of
// any libc.
const HostUV = "/var/lib/zenthros/tools/uv"

// ContainerUV is where a container job finds it: first on an image's PATH,
// where the SDK looks (shutil.which), so a function's packages install with
// uv in an image that has none.
const ContainerUV = "/usr/local/bin/uv"

// ContainerAgentBin is where a container job finds the agent binary: the
// host's, bound read-only.
const ContainerAgentBin = "/usr/local/bin/zenthros-agent"

// ContainerEnvsDir is where a container job sees its Workspace's function
// environments (systemd.EnvsDir), bound read-write: they outlive the job.
// Under the job's HOME=/tmp every job built them again: a function's first
// 30 s on containerd (lab, 2026-10-18).
const ContainerEnvsDir = systemd.EnvsRoot

// agentBinEnv is where the job finds the agent binary: the host path, or
// inside a container the path it is bound at.
func (e SystemdExecutor) agentBinEnv(env map[string]string, container bool) map[string]string {
	if e.AgentBin == "" {
		return env
	}
	if container {
		return withEnv(env, jobspec.EnvAgentBin, ContainerAgentBin)
	}
	return withEnv(env, jobspec.EnvAgentBin, e.AgentBin)
}

// Submit installs and starts the job's unit. A unit with the same spec that is
// running, or ran to an end it recorded, is reused; one that never ran is
// started. A resubmit with a changed spec is surfaced as
// backend.ErrSpecMismatch for the controller.
func (e SystemdExecutor) Submit(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (string, error) {
	spec, env = attemptScoped(spec, env)
	if e.Images != nil {
		// The image its container job held (installCtrWrapper) is held from
		// now on while its unit runs.
		defer e.Images.Started(systemd.UnitName(spec.ID))
	}
	// Before any path is made of them.
	if err := systemd.ValidateUnitInputs(spec, env); err != nil {
		return "", err
	}
	if spec.Kind != v1.WorkloadKindJob {
		return "", fmt.Errorf("%w: %q is not run by the VM executor; it runs %s", backend.ErrUnsupportedKind, spec.Kind, v1.WorkloadKindJob)
	}
	container, err := containerRuntime(spec.Runtime)
	if err != nil {
		return "", err
	}
	img := strings.TrimSpace(spec.Image)
	if container && img == "" {
		return "", backend.Classified(backend.ErrInvalid, fmt.Errorf("runtime %s requires workload.template.image", spec.Runtime))
	}
	if !guestRuntimeReady() {
		// Connected but not ready for work yet: the controller backs off and
		// retries, exactly as for a full machine.
		return "", fmt.Errorf("%w: guest runtime is still installing", backend.ErrBackendBusy)
	}
	// The env is complete before it is matched: the unit's hash covers
	// everything its env holds, so a resubmit must hash the same env.
	if e.Cache != nil {
		env = withEnv(env, jobspec.EnvCacheDir, e.Cache.PathsDir())
	}
	env = e.agentBinEnv(env, container)
	secretRoot := e.secretRoot()
	if hasDefaultFileSecret(spec) {
		dir := secretsource.DefaultFileRoot
		if !container {
			dir = secretfile.SecretsDir(secretRoot, spec.ID)
		}
		env = withEnv(env, secretfile.EnvSecretsDir, dir)
	}
	if name, ok, err := e.Manager.MatchingUnit(ctx, spec, env); err != nil {
		if errors.Is(err, systemd.ErrSpecMismatch) {
			return "", fmt.Errorf("%w: %w", backend.ErrSpecMismatch, err)
		}
		return "", err
	} else if ok {
		adopt, adoptErr := e.Manager.Adoptable(ctx, name)
		if adoptErr != nil {
			return "", adoptErr
		}
		if adopt {
			return name, nil
		}
	}
	materialized, err := applyDestinationSecrets(ctx, spec, secretRoot, e.Secrets, e.Age)
	if err != nil {
		return "", err
	}
	wrapper := materialized.WrapperPath
	if materialized.Root != "" && e.Manager.Config().Scope == systemd.ScopeSystem {
		// The job reads them as its user, on the host or in its container.
		if _, err := e.Manager.GiveToJobUser(ctx, spec.Workspace, secretfile.Dir(materialized.Root, spec.ID)); err != nil {
			return "", withoutSecrets(err, materialized.Root, spec.ID)
		}
	}
	if !container {
		// The job sees the host: an explicit file path must be under its
		// own directories, where it is linked to the file in the root.
		cfg := e.Manager.Config()
		for _, m := range materialized.Mounts {
			if m.Explicit && !systemd.AgentSees(m.Target, cfg.HostRoots) {
				return "", withoutSecrets(backend.Classified(backend.ErrInvalid, fmt.Errorf(
					"file secret path %q is in a home folder this machine has not opened to jobs: open it (zenadm join --folder) or omit path and read $%s",
					m.Target, secretfile.EnvSecretsDir)), materialized.Root, spec.ID)
			}
		}
		dirs := []string{os.ExpandEnv(systemd.IsolatedJobDir(spec.Workspace, spec.ID, cfg)), spec.WorkingDir}
		if linkErr := secretfile.Link(materialized, dirs); linkErr != nil {
			return "", withoutSecrets(backend.Classified(backend.ErrInvalid, linkErr), materialized.Root, spec.ID)
		}
	}
	if container {
		path, wrapErr := e.installCtrWrapper(ctx, spec, env, wrapper, img, materialized.Mounts)
		if wrapErr != nil {
			return "", withoutSecrets(wrapErr, materialized.Root, spec.ID)
		}
		wrapper = path
	}
	name, err := e.Manager.InstallWithWrapper(ctx, spec, env, wrapper)
	if err != nil {
		if errors.Is(err, systemd.ErrSpecMismatch) {
			err = fmt.Errorf("%w: %w", backend.ErrSpecMismatch, err)
		}
		if e.unitMayRun(ctx, spec.ID) {
			return "", err
		}
		return "", withoutSecrets(err, materialized.Root, spec.ID)
	}
	if container && e.Images != nil {
		e.Images.Kick()
	}
	return name, nil
}

// unitMayRun reports a job unit that runs, or may: an install that failed
// may have queued its start (a dbus timeout, the submit's context ending
// mid systemctl), and a resubmit adopts that unit without writing its
// secret files again. Its secrets stay for it; Cancel removes them after
// the unit. A unit whose state cannot be read may run too.
func (e SystemdExecutor) unitMayRun(ctx context.Context, jobID string) bool {
	ctx, cancel := fault.Cleanup(ctx)
	defer cancel()
	active, err := e.Manager.UnitActive(ctx, systemd.UnitName(jobID))
	return err != nil || active
}

// InstalledJobs lists the job units still starting or running on this
// machine with the job each runs for, so an agent that restarts keeps
// watching (and counting) the work that outlived it. A finished unit (one
// kept by Retain, say) is not work: the controller's Status reads it.
func (e SystemdExecutor) InstalledJobs(ctx context.Context, _ string) ([]InstalledJob, error) {
	units, err := e.Manager.JobUnits(ctx)
	if err != nil {
		return nil, err
	}
	jobs := make([]InstalledJob, 0, len(units))
	for _, unit := range units {
		if active, err := e.Manager.UnitActive(ctx, unit); err != nil || !active {
			continue
		}
		jobs = append(jobs, InstalledJob{JobID: systemd.JobOfUnit(unit), Handle: unit})
	}
	return jobs, nil
}

// EndedJobs lists the job units that ended and are still installed: each keeps
// its work directory and secrets until its Cancel. A machine is one backend's.
// A unit whose state could not be read is in the error, and the others are
// still returned.
func (e SystemdExecutor) EndedJobs(ctx context.Context, _ string) ([]InstalledJob, error) {
	units, err := e.Manager.JobUnits(ctx)
	if err != nil {
		return nil, err
	}
	var jobs []InstalledJob
	var errs []error
	for _, unit := range units {
		active, err := e.Manager.UnitActive(ctx, unit)
		if err != nil {
			errs = append(errs, fmt.Errorf("read unit %s: %w", unit, err))
			continue
		}
		if !active {
			jobs = append(jobs, InstalledJob{JobID: systemd.JobOfUnit(unit), Handle: unit})
		}
	}
	return jobs, errors.Join(errs...)
}

// EnvAttempt tells the process which run it is: 0 first, n on the n-th
// re-run after a failure.
const EnvAttempt = jobspec.EnvAttempt

// attemptScoped gives a re-run its own unit, work directory and container
// (jobspec.AttemptScoped).
func attemptScoped(spec jobspec.JobSpec, env map[string]string) (jobspec.JobSpec, map[string]string) {
	return jobspec.AttemptScoped(spec, env)
}

// Cancel stops and removes the job's unit, then removes its work directory unless
// the job carries the retain-workdir GC flag. The flag is read off the unit file
// (falling back to the marker inside the job dir when a prior Cancel already
// removed the unit), so the decision survives agent restarts and repeat Cancels.
// Destination secrets are removed only after uninstall returns, so a unit that
// is still running does not lose its credentials. Journal entries are not
// purged: journald cannot prune per-unit, and its own retention bounds disk
// usage once the unit stops producing entries.
func (e SystemdExecutor) Cancel(ctx context.Context, handle string, _ owner.Ref) error {
	retain := e.Manager.RetainWorkdir(ctx, handle)
	if err := e.Manager.Uninstall(ctx, handle); err != nil {
		return err
	}
	jobID := systemd.JobIDFromUnit(handle)
	if jobID != "" {
		if err := cleanupDestinationSecrets(e.secretRoot(), jobID); err != nil {
			return fmt.Errorf("remove the secrets of job %s: %w", jobID, err)
		}
	}
	if retain {
		return nil
	}
	if jobID != "" {
		return e.Manager.RemoveWorkDir(ctx, jobID)
	}
	return nil
}

// Status maps the unit's systemd state to a JobState, appending the journal tail on
// failure.
func (e SystemdExecutor) Status(ctx context.Context, handle string) (backend.JobState, error) {
	props, err := e.Manager.Status(ctx, handle)
	if err != nil {
		return backend.JobState{}, err
	}
	return e.stateOf(ctx, handle, props)
}

// StatusAll is Status for every handle, their units read in one `systemctl
// show`; a batch that fails is read one unit at a time.
func (e SystemdExecutor) StatusAll(ctx context.Context, handles []string) ([]backend.JobState, []error) {
	states, errs := make([]backend.JobState, len(handles)), make([]error, len(handles))
	all, err := e.Manager.StatusAll(ctx, handles)
	for i, handle := range handles {
		if err != nil {
			states[i], errs[i] = e.Status(ctx, handle)
			continue
		}
		states[i], errs[i] = e.stateOf(ctx, handle, all[i])
	}
	return states, errs
}

func (e SystemdExecutor) stateOf(ctx context.Context, handle string, props map[string]string) (backend.JobState, error) {
	if systemd.UnitMissing(props) {
		return backend.JobState{}, backend.Classified(backend.ErrNotFound, fmt.Errorf("unit %s is not installed", handle))
	}
	state := systemd.MapState(props)
	if props["ActiveState"] == "inactive" {
		var err error
		if state, err = e.Manager.FinishedState(ctx, handle, state); err != nil {
			return backend.JobState{}, err
		}
	}
	if state.Phase == v1.PhaseFailed {
		if tail := e.Manager.JournalTail(ctx, handle, 3); tail != "" {
			state.Message += "; last log: " + tail
		}
	}
	return state, nil
}

// Logs streams journalctl output for the unit. The channel closes when the journal
// ends (non-follow) or ctx is cancelled. On follow, the stream also closes once the
// unit reaches a terminal state (the Manager's TerminalWatch, shared by every stream).
func (e SystemdExecutor) Logs(ctx context.Context, handle string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	streamer := systemd.NewWatchedLogStreamer(e.Manager.Config(), e.Manager.TerminalWatch())
	return streamer.Stream(ctx, handle, follow, tailLines)
}

// containerRuntime reports whether the runtime the controller resolved runs
// the job in a container. The executor never infers it from the host: an
// empty runtime is systemd, as the controller resolves it when the backend
// declared nothing.
func containerRuntime(runtime string) (bool, error) {
	switch runtime {
	case "", v1.RuntimeSystemd:
		return false, nil
	case v1.RuntimeContainerd, v1.RuntimeWorkspaceContainers:
		return true, nil
	default:
		return false, fmt.Errorf("%w: runtime %q is not run by the VM executor; it runs %s, %s",
			backend.ErrUnsupportedBackend, runtime, v1.RuntimeSystemd, v1.RuntimeContainerd)
	}
}

// stateDir holds the installer's markers; tests point it elsewhere.
var stateDir = "/var/lib/zenthros"

// declaredDefaultRuntime is the runtime a job gets when it names none, as the
// installer recorded it in runtime-default; systemd before the installer
// wrote it. It feeds the declaration only; Submit follows the runtime the
// controller resolved.
func declaredDefaultRuntime() string {
	if b, err := os.ReadFile(filepath.Join(stateDir, "runtime-default")); err == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			return v
		}
	}
	return v1.RuntimeSystemd
}

func (e SystemdExecutor) installCtrWrapper(ctx context.Context, spec jobspec.JobSpec, env map[string]string, innerWrapper, image string, secrets []secretfile.Mount) (string, error) {
	cfg := e.Manager.Config()
	merged := map[string]string{}
	for k, v := range env {
		merged[k] = v
	}
	if _, ok := merged["HOME"]; !ok {
		merged["HOME"] = "/tmp"
	}
	if _, ok := merged["PYTHONDONTWRITEBYTECODE"]; !ok {
		merged["PYTHONDONTWRITEBYTECODE"] = "1"
	}
	if _, ok := merged[jobspec.EnvEnvsDir]; !ok {
		merged[jobspec.EnvEnvsDir] = ContainerEnvsDir
	}
	// The installers' cache, as a host job's: in the Workspace's
	// environments, so the next job's packages come from it.
	if _, ok := merged["XDG_CACHE_HOME"]; !ok {
		merged["XDG_CACHE_HOME"] = systemd.InstallCacheIn(ContainerEnvsDir)
	}
	envs := systemd.EnvsDir(spec.Workspace)
	opts := ctd.RunOptsFromSpec(spec, cfg, merged, innerWrapper)
	if cfg.Scope == systemd.ScopeSystem {
		opts.User = systemd.JobUser(spec.Workspace)
	}
	opts.UserNS = e.UserNS
	opts.Image = ctd.NormalizeImageRef(image)
	opts.ExtraMounts = append(opts.ExtraMounts, ctd.BindMount{Source: envs, Dest: ContainerEnvsDir})
	if e.Cache != nil {
		// Artifacts are links into the cache, so the whole cache is bound,
		// read-only, at the path the links name.
		opts.ExtraMounts = append(opts.ExtraMounts, ctd.BindMount{Source: e.Cache.Root, Dest: e.Cache.Root, ReadOnly: true})
	}
	if e.AgentBin != "" {
		opts.ExtraMounts = append(opts.ExtraMounts, ctd.BindMount{Source: e.AgentBin, Dest: ContainerAgentBin, ReadOnly: true})
	}
	if e.UV != "" {
		opts.ExtraMounts = append(opts.ExtraMounts, ctd.BindMount{Source: e.UV, Dest: ContainerUV, ReadOnly: true})
	}
	// File secrets: read-only, each at the path the job asked for, inside
	// the container only; the host keeps them in the job's secret root.
	for _, m := range secrets {
		opts.ExtraMounts = append(opts.ExtraMounts, ctd.BindMount{Source: m.Source, Dest: m.Target, ReadOnly: true})
	}
	opts.ReportSocketDir = e.ReportSocketDir
	sudo := cfg.SudoPrefix()
	runner := e.Manager.Runner()
	dir := systemd.IsolatedJobDir(spec.Workspace, spec.ID, cfg)
	if err := opts.ConfineHostMounts(dir, cfg.HostRoots, resolveExisting); err != nil {
		return "", err
	}
	// The job's own directories, the ones it writes; the Manager gives it
	// its work directory once the source is in it, and its environments.
	leaves := []string{opts.TmpDir, opts.RunDir}
	for _, leaf := range []string{opts.WorkDir, opts.CwdSource} {
		if leaf != "" {
			leaves = append(leaves, leaf)
		}
	}
	quoted := make([]string, 0, len(leaves)+1)
	for _, d := range append([]string{dir}, leaves...) {
		quoted = append(quoted, shellword.Quote(d))
	}
	if _, err := runner.Run(ctx, sudo+"mkdir -p "+strings.Join(quoted, " ")); err != nil {
		return "", fmt.Errorf("create containerd job dir: %w", err)
	}
	if opts.User != "" {
		if _, err := e.Manager.GiveToJobUser(ctx, spec.Workspace, leaves...); err != nil {
			return "", err
		}
	}
	write := func(path, body, mode string) error {
		enc := base64.StdEncoding.EncodeToString([]byte(body))
		cmd := fmt.Sprintf(`printf '%%s' '%s' | base64 -d | %stee "%s" >/dev/null && %schmod %s "%s"`,
			enc, sudo, path, sudo, mode, path)
		_, err := runner.Run(ctx, cmd)
		return err
	}
	if err := write(opts.EnvFile, ctd.FormatEnvFile(merged), "0600"); err != nil {
		return "", fmt.Errorf("write ctr env: %w", err)
	}
	scriptPath := ctd.RunScriptPathFor(spec.Workspace, spec.ID, cfg)
	if err := write(scriptPath, ctd.RenderRunScript(opts), "0755"); err != nil {
		return "", fmt.Errorf("write ctr-run: %w", err)
	}
	// Held before the pull: a sweep keeps the image a job named while its
	// submit is under way and while its unit runs (Submit's Started).
	if e.Images != nil {
		e.Images.Hold(systemd.UnitName(spec.ID), opts.Image)
	}
	ok, err := ctd.ImageExists(ctx, runner, sudo, opts.Image)
	if err != nil {
		return "", err
	}
	if ok {
		_, _ = runner.Run(ctx, imageEvent("job_image_cache_hit", opts.Image, sudo))
	} else {
		_, _ = runner.Run(ctx, imageEvent("job_image_pull_start", opts.Image, sudo))
		if _, err := ctd.EnsureImageWith(ctx, runner, sudo, opts.Image, e.Registry); err != nil {
			return "", err
		}
		_, _ = runner.Run(ctx, imageEvent("job_image_pull_end", opts.Image, sudo))
	}
	if e.Cache != nil {
		e.Cache.NoteImage(opts.Image)
	}
	return scriptPath, nil
}

// imageEvent writes one ZENTHROS_EVENT line to the console. The image comes
// from the job spec, so it is passed as data, never as shell text.
func imageEvent(event, image, sudo string) string {
	return fmt.Sprintf(`printf 'ZENTHROS_EVENT event=%s image=%%s ts=%%s\n' %s "$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)" | %stee /dev/console >/dev/null`,
		event, shellword.Quote(image), sudo)
}

// resolveExisting resolves symlinks in the longest existing prefix of path,
// so a hostPath cannot leave its root through a link planted by an earlier job.
func resolveExisting(path string) (string, error) {
	rest := ""
	for dir := path; ; dir = filepath.Dir(dir) {
		resolved, err := filepath.EvalSymlinks(dir)
		if err == nil {
			return filepath.Join(resolved, rest), nil
		}
		if !errors.Is(err, fs.ErrNotExist) || dir == "/" {
			return "", err
		}
		rest = filepath.Join(filepath.Base(dir), rest)
	}
}

// withoutSecrets removes the secret files a failed submit wrote. A removal
// that fails is part of what the submit returns (07#4): the files are not
// left on the machine unknown to anyone.
func withoutSecrets(err error, root, jobID string) error {
	if cleanup := cleanupDestinationSecrets(root, jobID); cleanup != nil {
		return errors.Join(err, fmt.Errorf("and the job's secret files stayed under %s: %w", root, cleanup))
	}
	return err
}

// secretRoot is where the job's secret files go: the configured root, the
// age payloads', or the default tmpfs one.
func (e SystemdExecutor) secretRoot() string {
	for _, root := range []string{e.Secrets.Root, e.Age.Root} {
		if root != "" {
			return root
		}
	}
	return secretfile.DefaultRoot()
}

// Declare is what this host runs: a Job as a process under systemd, and in a
// container when containerd is active. The runtime is probed on every call so
// a host that finishes installing containerd declares it on the next heartbeat.
// The secret stores are the ones a VM resolves where it runs.
func (e SystemdExecutor) Declare(context.Context) (capability.Set, capability.Host) {
	host := hostFacts()
	host.SecretStores = capability.SecretStoresVM(host.Provider)
	if e.Cache != nil {
		host.CachedImages, host.CachedArtifacts = e.Cache.Summary()
	}
	return capability.VM(probeRuntimeClass() == runtimeClassContainerd, declaredDefaultRuntime()), host
}

// Prepare warms this machine's cache for a Prewarm, in the background.
func (e SystemdExecutor) Prepare(_ context.Context, req *agentpb.PrepareRequest) ([]*agentpb.PrepareItemStatus, error) {
	if e.Cache == nil {
		return nil, fmt.Errorf("%w: this machine's agent runs without a cache (ZENTHROS_CACHE_DIR)", backend.ErrUnsupportedBackend)
	}
	if req.GetRelease() {
		e.Cache.Release(req.GetKey())
		return nil, nil
	}
	return prepareStatuses(e.Cache.Prepare(req.GetKey(), cacheItems(req), int(req.GetConcurrency()))), nil
}

// SignalPreempt sends the unit the signal its job asked for on an
// interruption notice, if any.
func (e SystemdExecutor) SignalPreempt(ctx context.Context, handle string) (bool, error) {
	return e.Manager.SignalPreempt(ctx, handle)
}

// ResolvedImage is a container job's image pinned by the digest Submit
// pulled, read back from containerd. "" for a host job, or when containerd
// cannot say: the controller then keeps sending the tag.
func (e SystemdExecutor) ResolvedImage(ctx context.Context, spec jobspec.JobSpec) string {
	if container, err := containerRuntime(spec.Runtime); err != nil || !container {
		return ""
	}
	cfg := e.Manager.Config()
	ref, err := ctd.ImageDigest(ctx, e.Manager.Runner(), cfg.SudoPrefix(), spec.Image)
	if err != nil {
		return ""
	}
	return ref
}
