// SPDX-License-Identifier: Apache-2.0

// Package agent is the Zenthros agent that runs jobs on a target backend (a VM,
// a container or, through k8sagent, a Kubernetes cluster). It dials out to the
// Gateway and advertises a backend id over a gRPC + mTLS control stream, then
// executes the Submit/Cancel/Status commands it receives (per-job systemd
// units on a VM). Jobs live on the host, not in the agent, which a restarted
// agent finds again; what it keeps on disk is the report spool, until the
// controller acks each report. Kubernetes-mode agents resolve Kubernetes SecretSources
// locally and decrypt encrypted-payload ciphertext with a destination-only age
// identity. VM agents reject k8s-secret before submission, resolve
// aws-secrets-manager with the worker instance profile, and decrypt
// encrypted-payload ciphertext locally.
//
// The command handlers are transport-agnostic (they operate on a minimal stream
// interface), so the same Agent both runs standalone on a VM (dialing the controller
// outbound) and runs in-process inside the controller for the agent transport.
package agent

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/bootstrap"
	"github.com/zenthros/operator/pkg/capability/capwire"
	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/liveness"
	"github.com/zenthros/operator/pkg/metrics"
	"github.com/zenthros/operator/pkg/reportspool"
	"github.com/zenthros/operator/pkg/systemd"
	"github.com/zenthros/operator/pkg/tlsconfig"
)

// reconnectBackoff is the base delay before redialing after the control stream
// drops. The controller is the source of commands; the agent's only job when
// disconnected is to reconnect, while systemd keeps the jobs running.
const reconnectBackoff = 5 * time.Second

// reconnectCeilingFactor bounds the redial delay at 6 × the base: 30 s by
// default. A machine whose gateway restarted is back within it, and the
// controller judges a silent machine only a job grace (45 s) after the
// gateways answer again (liveness.Tracker.Anchor): the ceiling must stay
// below the grace, or a restart would lose live machines.
const reconnectCeilingFactor = 6

// reconnectStable is how long a control stream stays up to count as a
// session that held: the next drop starts the backoff over.
const reconnectStable = 30 * time.Second

// A Gateway that refuses for lack of room is not a failure to back off
// from: another replica may have room, and each dial lands on either. The
// agent redials within reconnectFullMin..reconnectFullMax, reconnectFullPatience
// times since a session last held. That spans a replica coming back (about
// 15 s, scale lab, 2026-10-10) and stays under the Gateway's Register
// limit per certificate; past it, with no room anywhere, a refusal backs off
// like any failure.
const (
	reconnectFullMin      = time.Second
	reconnectFullMax      = 3 * time.Second
	reconnectFullPatience = 12
)

// reconnectSchedule spaces redials: exponential from base up to ceiling,
// each delay drawn uniformly from zero to the current bound (full jitter).
// Every agent redialed 5 s after its stream dropped, so after a Gateway
// restart the whole fleet came back in the same second, each Register
// pulling the Hub, the Fence and a LogChannel with it.
type reconnectSchedule struct {
	base, ceiling time.Duration
	bound         time.Duration
	// fulls counts the quick retries spent since a session last held.
	fulls int
	// draw returns a number in [0, n); nil uses math/rand/v2.
	draw func(n int64) int64
}

// next is the delay before the next dial, after a session that lasted
// lasted. full says the dial ended in a Gateway's refusal for lack of room
// (liveness.RefusedFull).
func (r *reconnectSchedule) next(lasted time.Duration, full bool) time.Duration {
	draw := r.draw
	if draw == nil {
		draw = rand.Int64N
	}
	if lasted >= reconnectStable {
		r.bound, r.fulls = 0, 0
	}
	if full && r.fulls < reconnectFullPatience {
		r.fulls++
		return reconnectFullMin + time.Duration(draw(int64(reconnectFullMax-reconnectFullMin)+1))
	}
	if r.bound == 0 {
		r.bound = r.base
	} else {
		r.bound = min(2*r.bound, max(r.ceiling, r.base))
	}
	return time.Duration(draw(int64(r.bound) + 1))
}

// Keepalive probes the controller so a half-open TCP connection (e.g. the controller
// pod restarted or the tailnet path broke without a FIN) is detected within
// Time+Timeout instead of leaving Recv blocked forever. Without this the agent sat on
// a zombie stream and never reconnected (architecture §3 resilience).
//
// Time must exceed the server's enforcement MinTime (25s) to avoid ENHANCE_YOUR_CALM
// GoAways. Over Tailscale, packet jitter can make pings appear closer together than
// the actual interval, so we keep a generous margin.
var keepaliveParams = keepalive.ClientParameters{
	Time:                30 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: true,
}

// slotsChanged heartbeats now: the hub knows how many jobs a machine runs
// from its last heartbeat only, and refuses a job to one it believes full
// (BACKEND_BUSY). Waiting for the next beat kept a machine whose job had
// just ended closed to the next job for up to a whole interval, 15 s
// (scale lab, 2026-10-10).
func (a *Agent) slotsChanged() {
	if beat := a.beat.Load(); beat != nil {
		beat.send()
	}
}

// Logger is the logging dependency the agent needs.
type Logger = logr.Logger

// controlStream is the slice of a gRPC Register stream the agent uses: it receives
// commands (RegisterResponse) and sends responses (RegisterRequest). Both the real
// client stream and in-process test doubles satisfy it.
type controlStream interface {
	Send(*agentpb.RegisterRequest) error
	Recv() (*agentpb.RegisterResponse, error)
}

// Config is everything the agent needs to identify itself and manage units. The
// standalone binary also sets ControllerAddr and TLS to reach the controller.
type Config struct {
	// ControllerAddr is the controller's gRPC endpoint, "host:port" (standalone only).
	ControllerAddr string
	// BackendID is the unique backend id this agent fronts (spec.target.backend).
	BackendID string
	// BackendType is the implementation type, e.g. "vm".
	BackendType string
	// Ownership is External or Managed. Empty defaults to External.
	Ownership infrav1alpha1.BackendOwnership
	// SaveCertificate keeps a renewed key and certificate where TLS's files
	// come from, in place of writing the files. A Kubernetes agent's files
	// are its Secret, mounted read-only: the renewal goes into the Secret,
	// and the kubelet updates the files and the next start reads it. Nil
	// writes the files.
	SaveCertificate func(ctx context.Context, keyPEM, certPEM []byte) error
	// PowerOff powers the machine off. Nil on an agent that cannot (a
	// container, a simulator), which then ignores the hub's ask. Only a
	// Managed machine's is ever called (powerOff).
	PowerOff func(ctx context.Context) error
	// Version is the agent build version, reported on register.
	Version string
	// TLS holds the agent's mTLS client material (standalone only).
	TLS tlsconfig.ClientConfig
	// HealthAddr is the address the agent serves the kubelet probes (/healthz,
	// /readyz) on, e.g. ":8081". Empty disables the health server (in-process/test
	// agents).
	HealthAddr string
	// MetricsAddr is the address the agent serves its Prometheus /metrics endpoint
	// on, e.g. ":8080". Empty disables the metrics server (in-process/test agents).
	MetricsAddr string
	// MetricsPublic allows MetricsAddr to bind all interfaces. Default rewrites
	// ":8080" / "0.0.0.0:8080" to loopback.
	MetricsPublic bool
	// ReconnectBackoff overrides the default delay before redialing after a stream
	// drop. Zero uses the production default (5s). Tests set a short value.
	ReconnectBackoff time.Duration
	// TailscaleWait bounds how long Register polls for a Tailscale IP before
	// advertising an empty address (the controller then falls back to the peer IP).
	// Zero uses the production default (30s). Tests set a tiny value so in-process
	// agents do not block on an interface they will never have.
	TailscaleWait time.Duration
	// Network is how the agent reaches the controller: NetworkTailscale (the
	// default) or NetworkDirect. A direct agent never waits for a Tailscale
	// interface, which would otherwise hold back every connect and reconnect
	// by TailscaleWait and eat into the controller's heartbeat grace.
	Network string
	// Dial, when set, opens every connection to the controller (the control
	// and log channels): a userspace tailnet node's dialer. Nil dials the
	// host network.
	Dial func(ctx context.Context, addr string) (net.Conn, error)
	// MaxJobs is the concurrent-job capacity this agent advertises in its heartbeat.
	// The controller refuses new submissions once running jobs reach it (BACKEND_BUSY).
	// 0 means unlimited. Dedicated VM agents default to 1; Kubernetes agents
	// default to unlimited so ephemeral RayJobs can run in parallel.
	MaxJobs int
	// MaxMessageBytes bounds a single gRPC message on the control and log channels.
	// It must clear the largest inline source the controller may ship, once base64
	// and JSON overhead are added. 0 uses the production default (16 MiB).
	MaxMessageBytes int
	// MaxLogBytes is the per-request StreamLogs byte cap at this Agent. When a
	// request would exceed it, the stream ends with a truncated signal and leftover
	// stays on the pod/unit. 0 uses the production default (8 MiB).
	MaxLogBytes int
	// MaxLogBytesHourly is the Agent-wide StreamLogs budget per hour, distinct
	// from MaxLogBytes. 0 uses the production default (50 MiB).
	MaxLogBytesHourly int
	// LogBatchWindow is how long the log lines that follow a sent message
	// wait to travel together (logbatch.go). Zero uses the window the hub
	// names when it accepts the session, or the production default (1s)
	// when it names none; negative joins nothing: a message per line.
	LogBatchWindow time.Duration
	// AgeRecipients are destination-only public keys advertised on Register.
	AgeRecipients []string
	// ReportListen is the local ingest bind address. A path starting with
	// "/" or containing ".sock" is a Unix socket (VM). Otherwise it is a TCP
	// address such as ":9445" (Kubernetes). Empty disables ingest.
	ReportListen string
	// ReportEndpoint is the workload-facing URL injected into jobs
	// (unix:///path or http://service:port). Empty skips injection.
	ReportEndpoint string
	// PreemptDir is where the agent writes each job's interruption notice
	// (<dir>/<jobID>.json, ZENTHROS_PREEMPT_FILE). Empty disables it: a
	// Kubernetes agent does not act on its node's notice.
	PreemptDir string

	// ServiceDir is where the agent keeps each Service job's ports, so a
	// restarted agent publishes their endpoints again. Empty keeps them in
	// memory only.
	ServiceDir string
	// ReportSpoolPath is the SQLite WAL database used as the durable local
	// spool. Empty disables ExecutionReport.
	ReportSpoolPath string
	// ReportTokenKey makes report tokens HMAC(key, job id), stable across
	// restarts and spool loss (reportkey). Empty keeps random, spooled tokens.
	ReportTokenKey []byte
	// HeartbeatInterval is how often this agent sends on the one control
	// stream. Zero uses 15s.
	HeartbeatInterval time.Duration
	// JobGrace is the silence window this agent's link needs while it runs
	// a job, as its provider set it (ZENTHROS_JOB_GRACE): a slow or relayed
	// link stalls longer than the cluster's default. It is declared with
	// the host facts; the control plane only lengthens its default with it.
	// Zero declares none.
	JobGrace time.Duration
	// SkipMetadataNotices stops polling cloud metadata services for an
	// interruption notice. A container asks none: its notice is the SIGTERM
	// it receives, pushed with Interrupt.
	SkipMetadataNotices bool
}

// defaultMaxMessageBytes bounds gRPC messages when Config.MaxMessageBytes is unset.
// Mirrors controller.DefaultLimits().MaxMessageBytes so both ends agree.
const defaultMaxMessageBytes = 16 << 20

// defaultMaxLogBytes is the per-request StreamLogs cap when Config.MaxLogBytes is
// unset. Generous for a peek via zctl; leftover stays on the pod/unit (Q-13).
const defaultMaxLogBytes = 8 << 20

func (c Config) maxMessageBytes() int {
	if c.MaxMessageBytes > 0 {
		return c.MaxMessageBytes
	}
	return defaultMaxMessageBytes
}

func (c Config) maxLogBytes() int {
	if c.MaxLogBytes > 0 {
		return c.MaxLogBytes
	}
	return defaultMaxLogBytes
}

func (c Config) maxLogBytesHourly() int {
	if c.MaxLogBytesHourly > 0 {
		return c.MaxLogBytesHourly
	}
	return defaultHourlyLogBytes
}

func (c Config) tailscaleWait() time.Duration {
	// Only a kernel tailnet device has an interface to wait for; a userspace
	// node's address is not on one, and nothing dials the agent through it.
	if c.Network == NetworkDirect || c.Network == NetworkTailscaleUserspace {
		return 0
	}
	if c.TailscaleWait > 0 {
		return c.TailscaleWait
	}
	return tailscaleIPWaitMax
}

func (c Config) ageRecipients() []string {
	return append([]string(nil), c.AgeRecipients...)
}

func (c Config) reconnectBackoff() time.Duration {
	if c.ReconnectBackoff > 0 {
		return c.ReconnectBackoff
	}
	return reconnectBackoff
}

func (c Config) logBatchWindow() time.Duration {
	switch {
	case c.LogBatchWindow > 0:
		return c.LogBatchWindow
	case c.LogBatchWindow < 0:
		return 0
	}
	return defaultLogBatchWindow
}

// logBatchWindow is the window this agent batches log lines in: its own
// option when it was given one, else the one the hub named when it accepted
// this session (the installation's), else the production default.
func (a *Agent) logBatchWindow() time.Duration {
	if a.cfg.LogBatchWindow == 0 {
		switch ms := a.hubLogBatchWindowMs.Load(); {
		case ms > 0:
			return time.Duration(ms) * time.Millisecond
		case ms < 0:
			return 0
		}
	}
	return a.cfg.logBatchWindow()
}

func (c Config) heartbeatInterval() time.Duration {
	return liveness.ResolveHeartbeatInterval(c.HeartbeatInterval)
}

// ValidateForRun fails fast on a standalone configuration that cannot work.
func (c Config) ValidateForRun() error {
	if c.ControllerAddr == "" {
		return errors.New("agent: controller address is required")
	}
	if c.BackendID == "" {
		return errors.New("agent: backend id is required")
	}
	if c.BackendType == "" {
		return errors.New("agent: backend type is required")
	}
	if c.Ownership != "" && c.Ownership != infrav1alpha1.OwnershipExternal && c.Ownership != infrav1alpha1.OwnershipManaged {
		return fmt.Errorf("agent: ownership must be %s or %s", infrav1alpha1.OwnershipExternal, infrav1alpha1.OwnershipManaged)
	}
	return c.TLS.Validate()
}

// Agent executes jobs received over a control stream via an Executor. The executor
// selects the local backend (systemd units on a VM, native resources in a cluster).
type Agent struct {
	cfg          Config
	executor     Executor
	tracker      *jobTracker
	health       *healthState
	logger       Logger
	spool        *reportspool.Spool
	forwarder    *reportForwarder
	checkpoints  *checkpointWaiter
	children     *childWaiter
	hourlyLogs   *hourlyBudget
	sessionEpoch atomic.Int64
	// The LogChannel opens only when the gateway asks for it (logAsked)
	// and stays closed while nobody reads logs. logAcceptedEpoch is the
	// session the gateway last accepted, whose asks count;
	// logSessionChanged wakes a wait for an ask when the session changes.
	logAcceptedEpoch atomic.Int64
	// hubLogBatchWindowMs is the log batching window the hub named when it
	// accepted this session (RegisterResponse.log_batch_window_ms); zero
	// when it named none.
	hubLogBatchWindowMs atomic.Int64
	// poweringOff is set while the machine is being powered off at the hub's
	// ask, so that the ask repeated does not pull the switch twice.
	poweringOff       atomic.Bool
	logAsked          chan struct{}
	logSessionChanged chan struct{}
	// logChannelOnce, when set, replaces serveLogChannelOnce (a test counts
	// the channels the loop opens).
	logChannelOnce func(context.Context, *tls.Config) error
	// advertisedHash is the capability hash the gateway last received.
	advertisedHash atomic.Value
	// beat is the control stream's heartbeat, nil between streams
	// (slotsChanged).
	beat atomic.Pointer[HeartbeatSender]
	// interrupted is a notice pushed to the agent (Interrupt), and
	// interruptCh closes when it arrives.
	interrupted   atomic.Pointer[preemptionNotice]
	interruptCh   chan struct{}
	interruptOnce sync.Once
	// noticed is the jobs the notice was delivered to. Every control stream
	// raises the notice again; a job hears it once.
	noticedMu sync.Mutex
	noticed   map[string]bool
	// services and endpoints publish where a Service job answers.
	services  *services
	endpoints *endpointResolver
	// commit takes a job's checkpoint commit where no controller does (local
	// mode); nil sends it to the controller.
	commit func(context.Context, *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error)
	// spawnRefused is why a job may spawn no children here; empty asks the
	// controller.
	spawnRefused string
}

// New builds an Agent that runs jobs as systemd units via the given manager. It is the
// VM path; in-cluster agents use NewWithExecutor with a Kubernetes executor.
func New(cfg Config, manager *systemd.Manager, logger Logger) *Agent {
	return NewWithExecutor(cfg, SystemdExecutor{Manager: manager}, logger)
}

// NewWithExecutor builds an Agent around an arbitrary Executor, allowing the same
// control-channel logic to drive different local backends.
func NewWithExecutor(cfg Config, executor Executor, logger Logger) *Agent {
	a := &Agent{
		cfg:               cfg,
		executor:          executor,
		tracker:           newJobTracker(),
		health:            &healthState{},
		logger:            logger.WithName("agent"),
		hourlyLogs:        newHourlyBudget(cfg.maxLogBytesHourly()),
		interruptCh:       make(chan struct{}),
		logAsked:          make(chan struct{}, 1),
		logSessionChanged: make(chan struct{}, 1),
		services:          newServices(cfg.ServiceDir),
		endpoints:         newEndpointResolver(),
	}
	a.children = newChildWaiter()
	if cfg.ReportSpoolPath != "" {
		spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: cfg.ReportSpoolPath, TokenKey: cfg.ReportTokenKey})
		if err != nil {
			logger.Error(err, "failed to open execution-report spool; reports disabled")
		} else {
			a.spool = spool
			a.forwarder = newReportForwarder(spool, logger)
			a.checkpoints = newCheckpointWaiter(spool, logger)
		}
	}
	return a
}

// Run dials the controller and serves the control stream, reconnecting on failure
// until the context is cancelled. It never returns nil before ctx is done. This is
// the standalone entrypoint.
func (a *Agent) Run(ctx context.Context) error {
	// First, before any connection: the first heartbeat must count the work
	// that outlived the previous agent process.
	a.rebuildTracker(ctx)
	// A renewal the previous process died in the middle of is finished (or
	// dropped) before the pair is read.
	if a.cfg.TLS.CertFile != "" && a.cfg.TLS.KeyFile != "" {
		if err := a.certRenewer().recoverPending(); err != nil {
			a.logger.Error(err, "could not finish an interrupted certificate renewal")
		}
	}
	tlsCfg, err := a.cfg.TLS.Config()
	if err != nil {
		return err
	}
	go a.serveLogChannel(ctx, tlsCfg)
	a.serveHealth(ctx)
	a.serveMetrics(ctx)
	a.serveReports(ctx)
	if a.forwarder != nil {
		go a.forwarder.Run(ctx)
	}
	if a.checkpoints != nil {
		go a.checkpoints.Run(ctx)
	}
	base := a.cfg.reconnectBackoff()
	schedule := reconnectSchedule{base: base, ceiling: reconnectCeilingFactor * base}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		started := time.Now()
		err := a.serveOnce(ctx, tlsCfg)
		if stop := a.detached(ctx, err); stop != nil {
			return stop
		}
		backoff := schedule.next(time.Since(started), liveness.RefusedFull(err))
		if err != nil {
			a.logger.Error(err, "control stream ended, reconnecting", "backoff", backoff)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		// The next serveOnce iteration is a redial, not the initial connect.
		metrics.RecordAgentReconnect()
	}
}

// ErrDetached ends Run when the hub detached this machine (liveness.Detached).
var ErrDetached = errors.New("detached from the hub")

// ExitDetached is the agent's exit status then: its service manager does not
// start it again (the systemd unit's RestartPreventExitStatus, the attached
// container's supervisor).
const ExitDetached = 78

// detached is Run's end when the stream ended because the hub detached this
// machine: its jobs stopped, ErrDetached. Nil for any other end, which Run
// redials after.
func (a *Agent) detached(ctx context.Context, err error) error {
	if !liveness.RefusedDetached(err) {
		return nil
	}
	a.stopDetached(ctx)
	return fmt.Errorf("%w: %w", ErrDetached, err)
}

// stopDetached stops every job this machine runs and removes what the ones
// that ended left: the hub removed its access, so none can report and no Cancel
// comes for them, and the person's machine keeps no workload of the hub's, nor
// a job's secrets. A Cancel that failed is asked again until it goes through
// (or the agent is told to stop): exiting with it, the job ran on with no one
// to report to and no agent left to stop it (05#1).
func (a *Agent) stopDetached(ctx context.Context) {
	jobs := a.heldJobs(ctx)
	a.logger.Info("this machine was detached from the hub: stopping its jobs, then exiting", "jobs", len(jobs))
	for wait := detachedRetry; ; wait = min(2*wait, detachedRetryCeiling) {
		var running []activeJob
		for _, job := range jobs {
			resp := a.Cancel(ctx, &agentpb.CancelRequest{BackendId: a.cfg.BackendID, Id: job.jobID, HandleName: job.handleName})
			if resp.GetError() != "" {
				a.logger.Info("could not stop a job of the detached machine; asking again", "job_id", job.jobID, "error", resp.GetError(), "in", wait.String())
				running = append(running, job)
			}
		}
		if len(running) == 0 {
			return
		}
		jobs = running
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// heldJobs is all a detached machine holds: the jobs tracked, and the ones that
// ended. What could not be listed is logged and not waited for: an agent that
// may not list a kind has none of it, and one that restarts is refused again
// and lists again.
func (a *Agent) heldJobs(ctx context.Context) []activeJob {
	jobs := a.tracker.Snapshot()
	lister, ok := a.executor.(endedLister)
	if !ok {
		return jobs
	}
	ended, err := lister.EndedJobs(ctx, a.cfg.BackendID)
	if err != nil {
		a.logger.Error(err, "could not list some jobs that ended on the detached machine")
	}
	for _, job := range ended {
		jobs = append(jobs, activeJob{jobID: job.JobID, handleName: job.Handle})
	}
	return jobs
}

// detachedRetry and detachedRetryCeiling pace a detached machine's Cancels
// that failed.
var (
	detachedRetry        = time.Second
	detachedRetryCeiling = 30 * time.Second
)

// rebuildTracker tracks the workloads that outlived a previous agent process
// before the first heartbeat, so the control plane never reads an idle
// machine that is running a job. Best-effort: what could not be listed is
// left to fill from Status calls, and what was is tracked (the tracker only
// adds).
func (a *Agent) rebuildTracker(ctx context.Context) {
	lister, ok := a.executor.(installedLister)
	if !ok {
		return
	}
	jobs, err := lister.InstalledJobs(ctx, a.cfg.BackendID)
	if err != nil {
		a.logger.Error(err, "could not list some installed jobs; tracking them as the control plane asks")
	}
	if added := a.tracker.rebuild(jobs); added > 0 {
		a.logger.Info("tracking jobs installed before this agent started", "count", added)
	}
}

// serveMetrics starts the Prometheus endpoint exposing the agent's platform
// metrics. It is independent of the control stream and runs until ctx
// is cancelled. Disabled when no MetricsAddr is configured (in-process/test agents).
func (a *Agent) serveMetrics(ctx context.Context) {
	if a.cfg.MetricsAddr == "" {
		return
	}
	registry := metrics.NewAgentRegistry(a.cfg.BackendType, func() float64 {
		return float64(a.tracker.Count())
	})
	go func() {
		addr := metrics.RestrictBindAddress(a.cfg.MetricsAddr, a.cfg.MetricsPublic)
		if err := metrics.Serve(ctx, addr, registry, a.logger); err != nil && ctx.Err() == nil {
			a.logger.Error(err, "metrics server exited")
		}
	}()
}

// serveHealth starts the kubelet probe server (/healthz, /readyz). It is independent
// of the control stream and runs until ctx is cancelled. Disabled when no HealthAddr
// is configured (in-process/test agents).
func (a *Agent) serveHealth(ctx context.Context) {
	if a.cfg.HealthAddr == "" {
		return
	}
	server := newHealthServer(a.health, a.logger)
	go func() {
		if err := server.serve(ctx, a.cfg.HealthAddr); err != nil && ctx.Err() == nil {
			a.logger.Error(err, "health server exited")
		}
	}()
}

// serveOnce opens one connection, registers, and serves commands until the stream
// drops or the context is cancelled. The client connection is tied to a child context
// that is cancelled on return, so a blocked Recv (e.g. a zombie stream keepalive has
// not yet torn down) is unblocked and the outer Run loop can reconnect deterministically.
func (a *Agent) serveOnce(ctx context.Context, tlsCfg *tls.Config) error {
	// The stream lives past ctx by the agent's goodbye (depart): cancelling
	// its context resets it, which the gateway cannot tell from a broken
	// connection. Until ServeStream takes over, ctx still ends it.
	connCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	unlink := context.AfterFunc(ctx, cancel)

	conn, err := grpc.NewClient(a.gatewayTarget(), a.gatewayDialOpts(tlsCfg)...)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", a.cfg.ControllerAddr, err)
	}
	defer conn.Close()

	stream, err := agentpb.NewAgentClient(conn).Register(connCtx)
	if err != nil {
		return fmt.Errorf("open register stream: %w", err)
	}
	if err := a.Register(connCtx, stream); err != nil {
		return err
	}
	a.health.setConnected(true)
	defer a.health.setConnected(false)
	metrics.SetAgentControlStreamUp(true)
	defer metrics.SetAgentControlStreamUp(false)
	a.logger.Info("registered with controller", "backendID", a.cfg.BackendID, "addr", a.cfg.ControllerAddr)
	if a.cfg.TLS.CertFile != "" {
		client := agentpb.NewAgentClient(conn)
		go a.certRenewer().keepRenewed(connCtx, func(ctx context.Context, csrDER []byte) ([]byte, error) {
			resp, err := client.Renew(ctx, &agentpb.RenewRequest{Csr: csrDER})
			if err != nil {
				return nil, err
			}
			return resp.GetClientCert(), nil
		}, certCheckInterval)
	}
	if !unlink() {
		// Stopped while registering: there is nothing to say goodbye on.
		return ctx.Err()
	}
	return a.ServeStream(ctx, stream)
}

// ServeStream answers controller commands until the stream ends or ctx is cancelled.
// A goroutine drains Recv so that ctx cancellation terminates the loop even while it
// is blocked waiting for the next command; keepalive failures surface here as Recv
// errors, which Run turns into a reconnect. The StatusWatcher and HeartbeatSender
// run as child goroutines and are stopped when the stream-scoped context is cancelled.
func (a *Agent) ServeStream(ctx context.Context, stream controlStream) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sender := newStreamSender(stream)
	if a.forwarder != nil {
		a.forwarder.setSender(sender)
		defer a.forwarder.setSender(nil)
	}
	if a.checkpoints != nil {
		a.checkpoints.setSender(sender)
		defer a.checkpoints.setSender(nil)
	}
	if a.children != nil {
		a.children.setSender(sender)
		defer a.children.setSender(nil)
	}
	watcher := newStatusWatcher(a.executor, a.tracker, sender, a.cfg.BackendID, a.logger)
	watcher.decorate = a.withEndpoints
	if a.checkpoints != nil {
		watcher.heldCommits = a.checkpoints.holds
	}
	heartbeat := newHeartbeatSender(a.tracker, sender, a.cfg.BackendID, a.cfg.MaxJobs, a.cfg.ageRecipients(), a.logger)
	heartbeat.interval = a.cfg.heartbeatInterval()
	heartbeat.declare = a.declaration
	heartbeat.advertised = &a.advertisedHash
	a.beat.Store(heartbeat)
	defer a.beat.Store(nil)
	// The job's end is on the stream before the heartbeat that frees its
	// slot: the hub never sees a free slot under a job it still has running.
	watcher.finished = func(handle string) {
		a.services.Delete(handle)
		a.slotsChanged()
	}
	notices := newNoticeWatcher(a.logger)
	if a.cfg.SkipMetadataNotices {
		notices.sources = nil
		notices.rebalances = nil
	}
	notices.pushed = a.interrupted.Load
	notices.wake = a.interruptCh
	heartbeat.preempted = notices.Notice
	heartbeat.rebalance = notices.Rebalance
	// The job hears first: its file, then its signal, then the heartbeat
	// that ends its lease and starts the replacement.
	notices.deliver = func(notice preemptionNotice) {
		a.deliverNotice(streamCtx, notice)
		if a.forwarder != nil {
			a.forwarder.Wake()
		}
	}
	notices.onNotice = func(preemptionNotice) { heartbeat.send() }
	go watcher.Run(streamCtx)
	go notices.Run(streamCtx)
	go heartbeat.Run(streamCtx)

	// The reader outlives streamCtx until ServeStream returns: a stopping
	// agent still reads its gateway's answer to its goodbye (depart), and
	// one goroutine alone ever reads the stream.
	served := make(chan struct{})
	defer close(served)
	recv := make(chan recvResult, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			select {
			case recv <- recvResult{msg, err}:
			case <-served:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	lanes := a.startLanes(streamCtx, sender.Send)
	defer lanes.wait()
	defer cancel()
	for {
		select {
		case <-streamCtx.Done():
			if ctx.Err() != nil {
				a.depart(sender, recv)
			}
			return streamCtx.Err()
		case err := <-lanes.failed():
			return err
		case r := <-recv:
			if errors.Is(r.err, io.EOF) {
				return nil
			}
			if r.err != nil {
				return fmt.Errorf("recv command: %w", r.err)
			}
			if r.msg.GetHeartbeatAck() != nil {
				heartbeat.acked()
			}
			if r.msg.GetOpenLogChannel() != nil {
				a.askLogChannel()
				continue
			}
			lanes.route(streamCtx, r.msg)
		}
	}
}

// recvResult is one Recv of the control stream.
type recvResult struct {
	msg *agentpb.RegisterResponse
	err error
}

// departWait bounds how long a stopping agent waits for its gateway to end
// the stream after it closed its side.
var departWait = 2 * time.Second

// depart is a stopping agent's goodbye: it closes its side of the stream, so
// its gateway reads the end as the machine stopping (io.EOF) and not as a
// connection that broke, and waits a moment for the gateway to end the
// stream in turn. The controller then never counts this machine's silence
// as the path to the others failing.
func (a *Agent) depart(sender *streamSender, recv <-chan recvResult) {
	if !sender.closeSend() {
		return
	}
	timeout := time.NewTimer(departWait)
	defer timeout.Stop()
	for {
		select {
		case r := <-recv:
			if r.err != nil {
				return
			}
		case <-timeout.C:
			a.logger.Info("the gateway did not end the stream after the agent's goodbye", "waited", departWait.String())
			return
		}
	}
}

// Register advertises the agent over the stream and waits for admission. It reports
// the agent's Tailscale IP (waiting briefly for the interface to come up) so the
// controller can dial the log server directly.
func (a *Agent) Register(ctx context.Context, stream controlStream) error {
	// The session's epoch is the clock, and only goes forward: a clock
	// stepped back gave the reconnect an epoch below the session the gateway
	// still held, refused as stale until that stream was reaped (09#6).
	epoch := max(time.Now().UnixNano(), a.sessionEpoch.Load()+1)
	capabilities, host, capHash := a.declaration(ctx)
	a.advertisedHash.Store(capHash)
	advertise := &agentpb.RegisterRequest{Advertise: &agentpb.Advertise{
		BackendId:      a.cfg.BackendID,
		BackendType:    a.cfg.BackendType,
		AgentVersion:   a.cfg.Version,
		AgentAddr:      waitForTailscaleIP(ctx, a.cfg.tailscaleWait()),
		Ownership:      string(infrav1alpha1.NormalizeOwnership(a.cfg.Ownership)),
		AgeRecipients:  a.cfg.ageRecipients(),
		SessionEpoch:   epoch,
		Capabilities:   capwire.ToProto(capabilities),
		Host:           capwire.HostToProto(host),
		CapabilityHash: capHash,
		WireProtocol:   envelope.Protocol(),
	}}
	a.sessionEpoch.Store(epoch)
	if err := stream.Send(advertise); err != nil {
		return fmt.Errorf("send advertise: %w", err)
	}
	ack, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv register ack: %w", err)
	}
	if !ack.GetAccepted() {
		// The Gateway ends the stream after a refused ack; the status says
		// why, by its reason (a full Gateway), not by the ack's text.
		_, end := stream.Recv()
		return fmt.Errorf("controller rejected registration: %s: %w", ack.GetMessage(), end)
	}
	a.hubLogBatchWindowMs.Store(ack.GetLogBatchWindowMs())
	a.logSessionAccepted(epoch)
	return nil
}

// gatewayTarget is the controller address as a gRPC target. Through
// Config.Dial it is passed unresolved: the dialer's network resolves it.
// gatewayTarget is the hub's address list as gRPC dials it: each address in
// order, again on every reconnection (bootstrap.DialTarget).
func (a *Agent) gatewayTarget() string {
	return bootstrap.DialTarget(a.cfg.ControllerAddr, a.cfg.Dial != nil)
}

// DialGateway opens another connection to the gateway with the agent's own
// mTLS identity, for a service beside the control stream (a Kubernetes
// backend's node supply). Its own TCP keeps it off the control stream's
// HTTP/2 connection.
func (a *Agent) DialGateway() (*grpc.ClientConn, error) {
	tlsCfg, err := a.cfg.TLS.Config()
	if err != nil {
		return nil, err
	}
	return grpc.NewClient(a.gatewayTarget(), a.gatewayDialOpts(tlsCfg)...)
}

func (a *Agent) gatewayDialOpts(tlsCfg *tls.Config) []grpc.DialOption {
	maxMsg := a.cfg.maxMessageBytes()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsg), grpc.MaxCallRecvMsgSize(maxMsg)),
	}
	if a.cfg.Dial != nil {
		opts = append(opts, grpc.WithContextDialer(a.cfg.Dial))
	}
	return append(opts, bootstrap.DialOptions(a.cfg.ControllerAddr, tlsCfg.ServerName)...)
}

// Interrupt hands the agent an interruption notice it did not poll for: a
// container's SIGTERM. The agent does what it does for a metadata notice:
// each job's notice file, then its preempt signal, then the heartbeat that
// ends the lease. Only the first notice counts.
func (a *Agent) Interrupt(notice cloudmeta.Notice) {
	notice.Preempted = true
	if a.interrupted.CompareAndSwap(nil, &notice) {
		a.interruptOnce.Do(func() { close(a.interruptCh) })
	}
}
