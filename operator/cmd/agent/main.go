// SPDX-License-Identifier: Apache-2.0

// Command agent runs the Zenthros gateway on a target backend. It dials the
// controller outbound over gRPC + mTLS and executes jobs via the configured
// executor (systemd units on a VM, native resources in a K8s cluster).
//
// Enrollment is handled by zenadm (kubeadm equivalent); the agent binary is
// purely the long-running daemon (kubelet equivalent).
//
// All configuration is resolved once at startup (architecture §17, §20).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	uzap "go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/zenthros/operator/agent"
	"github.com/zenthros/operator/agent/agepayload"
	"github.com/zenthros/operator/agent/k8sagent"
	"github.com/zenthros/operator/agent/procagent"
	"github.com/zenthros/operator/agent/reportkey"
	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	"github.com/zenthros/operator/internal/buildinfo"
	"github.com/zenthros/operator/internal/envconf"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	externalgrpcpb "github.com/zenthros/operator/internal/grpc/gen/externalgrpc"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/spark"
	ctd "github.com/zenthros/operator/pkg/containerd"
	"github.com/zenthros/operator/pkg/containerd/imagegc"
	"github.com/zenthros/operator/pkg/containerfacts"
	"github.com/zenthros/operator/pkg/infrastructure/provider/containerhosts"
	"github.com/zenthros/operator/pkg/jobcli"
	"github.com/zenthros/operator/pkg/podsecurity"
	"github.com/zenthros/operator/pkg/prewarm/cache"
	"github.com/zenthros/operator/pkg/pyenv"
	"github.com/zenthros/operator/pkg/runtimegate"
	"github.com/zenthros/operator/pkg/systemd"
	"github.com/zenthros/operator/pkg/tlsconfig"
)

const (
	agentModeVM  = "vm"
	agentModeK8s = "k8s"
)

func main() {
	// `zenthros-agent job ...` runs inside a job, not as the daemon: the
	// report, checkpoint and notice protocol for programs in any language.
	if len(os.Args) > 1 && os.Args[1] == "job" {
		os.Exit((&jobcli.CLI{}).Run(context.Background(), os.Args[2:]))
	}
	// `zenthros-agent attach` joins a container that is already running:
	// the agent detaches from the shell it was started in, under a
	// supervisor that does what PID 1 does below.
	if len(os.Args) > 1 && os.Args[1] == "attach" {
		self, err := os.Executable()
		if err != nil {
			self = os.Args[0]
		}
		os.Exit(runAttach(append([]string{self}, os.Args[1:]...)))
	}
	// `zenthros-agent local ...` is the client of a local agent's socket
	// (agent/local.go): the SDK runs it through docker exec or kubectl exec.
	if len(os.Args) > 1 && os.Args[1] == "local" {
		os.Exit(agent.RunLocalCLI(context.Background(), os.Args[2:], localSocket(), os.Stdin, os.Stdout, os.Stderr))
	}
	// A container whose entrypoint is the agent makes it PID 1: it becomes
	// the container's init and runs the agent proper as its child.
	if os.Getpid() == 1 && os.Getenv("ZENTHROS_AGENT_MODE") == agentModeContainer {
		self, err := os.Executable()
		if err != nil {
			self = os.Args[0]
		}
		argv := append([]string{self}, os.Args[1:]...)
		env, secrets, err := scrubInit(argv)
		if err != nil {
			fmt.Fprintf(os.Stderr, "zenthros-agent init: %v\n", err)
		}
		os.Exit(runInit(argv, env, secrets))
	}
	// The agent proper: what init took out of the environment.
	if err := applyHandoff(); err != nil {
		fmt.Fprintf(os.Stderr, "zenthros-agent: %v\n", err)
	}
	zapOpts := zap.Options{Development: false}
	maxLogBytes := envconf.Int("ZENTHROS_AGENT_MAX_LOG_BYTES", 0)
	flag.IntVar(&maxLogBytes, "max-log-bytes", maxLogBytes, "Per-request StreamLogs byte cap. 0 uses the 8 MiB default. When hit, the stream ends with a truncated signal; leftover stays on the pod (kubectl logs) or unit (journalctl).")
	maxLogBytesHourly := envconf.Int("ZENTHROS_AGENT_MAX_LOG_BYTES_HOURLY", 0)
	flag.IntVar(&maxLogBytesHourly, "max-log-bytes-hourly", maxLogBytesHourly, "Agent-wide StreamLogs hourly budget. 0 uses the 50 MiB default. Distinct from --max-log-bytes.")
	metricsPublic := envconf.Bool("ZENTHROS_AGENT_METRICS_PUBLIC", false)
	flag.BoolVar(&metricsPublic, "metrics-public", metricsPublic, "Bind metrics on all interfaces. Default rewrites :8080 to 127.0.0.1:8080.")
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	raw := zap.NewRaw(zap.UseFlagOptions(&zapOpts))
	ctrl.SetLogger(zapr.NewLogger(raw))
	logger := ctrl.Log.WithName("agent-main")
	agent.SetHostLog(ctrl.Log)
	// fatal is the agent stopping for a reason of its own: its last line, at
	// level fatal, then exit 1. A host that stops the container leaves no
	// such line (test/cluster hostFault tells the two apart by it).
	fatal := func(err error, msg string) { raw.Named("agent-main").Fatal(msg, uzap.Error(err)) }

	cfg := agent.Config{
		ControllerAddr:    os.Getenv("ZENTHROS_CONTROLLER_ADDR"),
		BackendID:         os.Getenv("ZENTHROS_BACKEND_ID"),
		BackendType:       os.Getenv("ZENTHROS_BACKEND_TYPE"),
		Ownership:         infrav1alpha1.BackendOwnership(envconf.Or("ZENTHROS_BACKEND_OWNERSHIP", string(infrav1alpha1.OwnershipExternal))),
		Version:           envconf.Or("ZENTHROS_AGENT_VERSION", buildinfo.Release),
		HealthAddr:        envconf.Or("ZENTHROS_AGENT_HEALTH_ADDR", ":8081"),
		MetricsAddr:       envconf.Or("ZENTHROS_AGENT_METRICS_ADDR", ":8080"),
		MetricsPublic:     metricsPublic,
		MaxJobs:           envconf.Int("ZENTHROS_AGENT_MAX_JOBS", defaultMaxJobs(envconf.Or("ZENTHROS_AGENT_MODE", agentModeVM))),
		MaxMessageBytes:   envconf.Int("ZENTHROS_MAX_MESSAGE_BYTES", 0),
		Network:           envNetwork(os.Getenv("ZENTHROS_AGENT_MODE")),
		MaxLogBytes:       maxLogBytes,
		MaxLogBytesHourly: maxLogBytesHourly,
		ReportListen:      envconf.Or("ZENTHROS_REPORT_LISTEN", defaultReportListen(os.Getenv("ZENTHROS_AGENT_MODE"))),
		ReportEndpoint:    envconf.Or("ZENTHROS_REPORT_ENDPOINT", defaultReportEndpoint(os.Getenv("ZENTHROS_AGENT_MODE"))),
		PreemptDir:        envconf.Or("ZENTHROS_PREEMPT_DIR", defaultPreemptDir(os.Getenv("ZENTHROS_AGENT_MODE"))),
		ReportSpoolPath:   envconf.Or("ZENTHROS_REPORT_SPOOL", defaultReportSpool(os.Getenv("ZENTHROS_AGENT_MODE"))),
		ServiceDir:        envconf.Or("ZENTHROS_SERVICE_DIR", defaultServiceDir(os.Getenv("ZENTHROS_AGENT_MODE"))),
		HeartbeatInterval: envconf.Duration("ZENTHROS_HEARTBEAT_INTERVAL", 0),
		JobGrace:          envconf.Duration(containerhosts.EnvJobGrace, 0),
		TLS: tlsconfig.ClientConfig{
			CertFile:           os.Getenv("ZENTHROS_TLS_CERT"),
			KeyFile:            os.Getenv("ZENTHROS_TLS_KEY"),
			CAFile:             os.Getenv("ZENTHROS_TLS_CA"),
			ServerName:         os.Getenv("ZENTHROS_TLS_SERVER_NAME"),
			InsecureSkipVerify: os.Getenv("ZENTHROS_TLS_INSECURE") == "true",
		},
	}
	// Only a machine the installation created has a power switch for the hub:
	// its cloud account may stop taking the provisioner's terminate.
	if envconf.Or("ZENTHROS_AGENT_MODE", agentModeVM) == agentModeVM &&
		infrav1alpha1.NormalizeOwnership(cfg.Ownership) == infrav1alpha1.OwnershipManaged {
		cfg.PowerOff = agent.SystemPowerOff(os.Getenv("ZENTHROS_USE_SUDO") == "true")
	}
	backendType, err := backendTypeFor(envconf.Or("ZENTHROS_AGENT_MODE", agentModeVM), cfg.BackendType)
	if err != nil {
		fatal(err, "invalid agent configuration")
	}
	cfg.BackendType = backendType
	if os.Getenv(agent.EnvLocal) != "" {
		// No hub: nothing to enrol with, to reach or to serve a port to.
		applyLocalDefaults(&cfg)
		if err := runAgent(cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
			fatal(err, "agent exited with error")
		}
		return
	}
	node, err := prepareToConnect(context.Background(), &cfg, upTailnet, logger)
	if err != nil {
		fatal(err, "the agent cannot reach its controller")
	}
	defer func() { _ = node.Close() }()
	if err := cfg.ValidateForRun(); err != nil {
		fatal(err, "invalid agent configuration")
	}
	if err := runAgent(cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
		if errors.Is(err, agent.ErrDetached) {
			logger.Info("this machine was detached from the hub; the agent stops for good", "reason", err.Error())
			os.Exit(agent.ExitDetached)
		}
		fatal(err, "agent exited with error")
	}
}

// backendTypeFor is the backend type the agent advertises: what its mode
// runs. ZENTHROS_BACKEND_TYPE may repeat it but not contradict it; an agent
// that ran RayJobs on Kubernetes while advertising "vm" would be scheduled as
// a machine.
func backendTypeFor(mode, declared string) (string, error) {
	var derived string
	switch mode {
	case agentModeVM, "":
		derived = backend.TypeVM
	case agentModeK8s:
		derived = backend.TypeK8s
	case agentModeContainer:
		derived = backend.TypeContainer
	default:
		return "", fmt.Errorf("unknown ZENTHROS_AGENT_MODE %q (want %q, %q or %q)", mode, agentModeVM, agentModeK8s, agentModeContainer)
	}
	if declared != "" && declared != derived {
		return "", fmt.Errorf("ZENTHROS_BACKEND_TYPE=%q contradicts ZENTHROS_AGENT_MODE=%q, which runs as backend type %q", declared, mode, derived)
	}
	return derived, nil
}

func runAgent(cfg agent.Config, logger logr.Logger) error {
	mode := envconf.Or("ZENTHROS_AGENT_MODE", agentModeVM)
	executor, bundle, err := newExecutor(mode, logger)
	if err != nil {
		return err
	}
	if vm, ok := executor.(agent.SystemdExecutor); ok {
		vm.ReportSocketDir = reportSocketDir(cfg.ReportListen)
		executor = vm
		sweepCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		if removed, err := vm.SweepJobNetworks(sweepCtx); err != nil {
			logger.Error(err, "sweep of job networks no unit runs failed")
		} else if len(removed) > 0 {
			logger.Info("removed job networks no unit runs", "namespaces", removed)
		}
		cancel()
	}
	cfg.AgeRecipients = bundle.Recipients()
	if k8s, ok := executor.(*k8sagent.Executor); ok && mode == agentModeK8s && cfg.TLS.CertFile != "" {
		// The certificate files are the agent's Secret, mounted read-only.
		cfg.SaveCertificate = k8sagent.CertificateSecret(k8s.Client(), envconf.Or("POD_NAMESPACE", "zenthros-system"),
			envconf.Or("ZENTHROS_TLS_SECRET", "zenthros-agent-tls"), filepath.Base(cfg.TLS.KeyFile), filepath.Base(cfg.TLS.CertFile))
	}
	if cfg.ReportSpoolPath != "" {
		key, err := loadReportKey(mode, executor)
		if err != nil {
			return err
		}
		cfg.ReportTokenKey = key
	}
	logger.Info("starting agent",
		"mode", mode,
		"controllerAddr", cfg.ControllerAddr,
		"backendID", cfg.BackendID,
		"backendType", cfg.BackendType,
		"ownership", infrav1alpha1.NormalizeOwnership(cfg.Ownership),
		"ageRecipients", len(cfg.AgeRecipients),
		"maxLogBytes", cfg.MaxLogBytes,
	)
	a := agent.NewWithExecutor(cfg, executor, logger)
	proc, inContainer := executor.(*procagent.Executor)
	if !inContainer {
		ctx := ctrl.SetupSignalHandler()
		if k8s, ok := executor.(*k8sagent.Executor); ok && os.Getenv("ZENTHROS_NODE_SUPPLY") == "true" {
			// Reconciles the Nodes Zenthros supplies this cluster with their
			// slots on the hub (docs/design/K8S_NODE_SUPPLY.md).
			conn, err := a.DialGateway()
			if err != nil {
				return fmt.Errorf("node supply: %w", err)
			}
			defer conn.Close()
			nodes := &k8sagent.NodeReconciler{Client: k8s.Client(), Hub: agentpb.NewNodeSupplyClient(conn),
				Logger: logger.WithName("node-supply")}
			if serverURL := os.Getenv("ZENTHROS_NODE_SUPPLY_SERVER_URL"); serverURL != "" {
				// New machines get a node credential issued here, never a
				// long-lived cluster token (docs/design/K8S_NODE_SUPPLY.md).
				if nodes.Join, err = k8s.JoinIssuer(serverURL); err != nil {
					return fmt.Errorf("node supply: %w", err)
				}
				// The hub creates machines only while the cluster can
				// take them as nodes: the agent checks and reports it.
				var clusterDNS []string
				for _, ip := range strings.Split(os.Getenv("ZENTHROS_NODE_SUPPLY_CLUSTER_DNS"), ",") {
					if ip = strings.TrimSpace(ip); ip != "" {
						clusterDNS = append(clusterDNS, ip)
					}
				}
				nodes.Cluster = k8s.ClusterChecker(clusterDNS, os.Getenv("ZENTHROS_NODE_SUPPLY_CLUSTER_DOMAIN"))
			}
			go nodes.Run(ctx, 5*time.Second)
			// Cluster Autoscaler's cloud provider, served in the cluster and
			// forwarded on this agent's connection (the chart's
			// clusterAutoscaler.enabled): CA never holds the agent's
			// certificate.
			if listen := os.Getenv("ZENTHROS_CA_PROXY_LISTEN"); listen != "" {
				dir := envconf.Or("ZENTHROS_CA_PROXY_TLS_DIR", "/etc/zenthros/ca-proxy")
				tlsCfg, err := k8sagent.CAProxyTLS(filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key"), filepath.Join(dir, "ca.crt"))
				if err != nil {
					return err
				}
				go func() {
					if err := k8sagent.ServeCAProxy(ctx, listen, tlsCfg, externalgrpcpb.NewCloudProviderClient(conn), logger.WithName("ca-proxy")); err != nil {
						logger.Error(err, "Cluster Autoscaler's cloud provider stopped")
					}
				}()
			}
		}
		if os.Getenv(agent.EnvLocal) != "" {
			return a.RunLocal(ctx, agent.LocalOptions{Socket: localSocket(), StateDir: localStateDir(mode), Shared: os.Getenv(agent.EnvLocalShared)})
		}
		return a.Run(ctx)
	}
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	signalled := containerSignalContext(sigs, a.Interrupt, func() bool { return proc.Running() == 0 },
		envconf.Duration(containerhosts.EnvPreemptGrace, defaultPreemptGrace), logger)
	ctx, stop := context.WithCancelCause(signalled)
	defer stop(nil)
	_, host := proc.Declare(ctx)
	prepared, err := prepareRuntime(ctx, os.Getenv(pyenv.EnvToolchains), runtimeSteps{
		prepare: preparePython(logger),
		checkGPU: gpuCheck(host.GPUCount > 0, infrav1alpha1.NormalizeOwnership(cfg.Ownership) == infrav1alpha1.OwnershipManaged,
			func(ctx context.Context) containerfacts.CUDA {
				return containerfacts.AskCUDA(ctx, nil, envconf.Or("ZENTHROS_PYTHON", "python3"))
			}, proc.WithoutGPU, logger),
		markPending: runtimegate.MarkPending,
		markReady:   runtimegate.MarkReady,
		markFailed:  markToolchainFailed,
	}, containerToolchain, logger)
	if err != nil {
		return err
	}
	if prepared != nil {
		go func() {
			afterPrepare(<-prepared, os.Getenv(agent.EnvLocal) != "", stop, logger)
		}()
	}
	if os.Getenv(agent.EnvLocal) != "" {
		err = a.RunLocal(ctx, agent.LocalOptions{
			Socket: localSocket(), StateDir: localStateDir(mode),
			IdleExit: envconf.Duration(envLocalIdleExit, defaultLocalIdleExit),
			Busy:     func() bool { return proc.Running() > 0 },
		})
	} else {
		err = a.Run(ctx)
	}
	// A container's jobs never outlive its agent.
	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	proc.Close(stopCtx)
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

// loadReportKey is the key report tokens are derived from: a file on a
// machine, a Secret in the agent's namespace on Kubernetes.
func loadReportKey(mode string, executor agent.Executor) ([]byte, error) {
	if k8s, ok := executor.(*k8sagent.Executor); ok && mode == agentModeK8s {
		return reportkey.LoadSecret(context.Background(), k8s.Client(), envconf.Or("POD_NAMESPACE", "zenthros-system"),
			envconf.Or("ZENTHROS_REPORT_KEY_SECRET", reportkey.DefaultSecretName))
	}
	if mode == agentModeContainer {
		return reportkey.LoadFile(envconf.Or("ZENTHROS_REPORT_KEY", containerPath("report.key")))
	}
	return reportkey.LoadFile(envconf.Or("ZENTHROS_REPORT_KEY", reportkey.DefaultPath))
}

func newExecutor(mode string, logger logr.Logger) (agent.Executor, agepayload.Bundle, error) {
	switch mode {
	case agentModeVM:
		bundle, err := agepayload.LoadDir(envconf.Or("ZENTHROS_AGE_IDENTITY_DIR", "/var/lib/zenthros/age"))
		if err != nil {
			return nil, agepayload.Bundle{}, err
		}
		scope := envconf.Or("ZENTHROS_SYSTEMD_SCOPE", systemd.ScopeSystem)
		useSudo := os.Getenv("ZENTHROS_USE_SUDO") == "true"
		manager := systemd.NewManager(systemd.Config{
			Scope:        scope,
			UseSudo:      useSudo,
			RuntimeImage: strings.TrimSpace(os.Getenv("ZENTHROS_RUNTIME_IMAGE")),
			HostRoots:    systemd.HostRoots(os.Getenv(systemd.HostRootsEnv)),
		}, systemd.LocalRunner{}, logger)
		images, err := newImageGC(manager, logger)
		if err != nil {
			return nil, agepayload.Bundle{}, err
		}
		executor := agent.SystemdExecutor{Manager: manager, Age: agepayload.Config{Bundle: bundle}, Registry: ctd.DefaultRegistryAuth(), Images: images}
		if self, err := os.Executable(); err == nil {
			executor.AgentBin = self
		}
		// The installer puts uv there before the runtime is ready, and
		// restarts the agent when it is.
		if _, err := os.Stat(agent.HostUV); err == nil {
			executor.UV = agent.HostUV
		}
		if scope == systemd.ScopeSystem {
			// Asked at each start: the installer restarts the agent once
			// containerd is there.
			probe, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			can, why := ctd.UserNamespaces(probe, manager.Runner(), manager.Config().SudoPrefix())
			cancel()
			executor.UserNS = can
			if can {
				logger.Info("container jobs run in a user namespace of their Workspace")
			} else {
				logger.Info("container jobs run without a user namespace", "why", why)
			}
		}
		if root := envconf.Or("ZENTHROS_CACHE_DIR", cache.DefaultRoot); root != "off" {
			maxBytes, _ := strconv.ParseInt(os.Getenv("ZENTHROS_CACHE_MAX_BYTES"), 10, 64)
			warm, err := agent.NewVMCache(manager, executor.Registry, root, maxBytes, images)
			if err != nil {
				// Jobs still run; Prewarms are refused with this error.
				logger.Error(err, "warm cache disabled")
			} else {
				executor.Cache = warm
				images.Forget = warm.ForgetImage
			}
		}
		// For the agent's life: a machine's images are swept while it runs.
		go images.Run(context.Background())
		return executor, bundle, nil
	case agentModeK8s:
		executor, err := newK8sExecutor(logger)
		if err != nil {
			return nil, agepayload.Bundle{}, err
		}
		namespace := envconf.Or("POD_NAMESPACE", "zenthros-system")
		executor.Namespace = namespace
		// The PSA level the agent enforces on the Workspace namespaces it
		// creates; the hub's controller takes the same setting.
		executor.PodSecurity = envconf.Or("ZENTHROS_WORKSPACE_POD_SECURITY", podsecurity.Default)
		// The chart names the role the agent binds in each Workspace
		// namespace so its jobs read their Secrets.
		if os.Getenv(agent.EnvLocal) != "" {
			uid, gid, err := k8sagent.ParseLocalUser(os.Getenv(agent.EnvLocalUser))
			if err != nil {
				return nil, agepayload.Bundle{}, err
			}
			executor.UseLocal(&k8sagent.LocalCluster{Folder: os.Getenv(agent.EnvLocalShared), UID: uid, GID: gid}, namespace)
		} else {
			role := os.Getenv("ZENTHROS_JOB_SECRETS_CLUSTER_ROLE")
			if role == "" {
				return nil, agepayload.Bundle{}, errors.New("ZENTHROS_JOB_SECRETS_CLUSTER_ROLE is not set: install the agent with its chart")
			}
			executor.SecretAccess = k8sagent.SecretAccess{
				ClusterRole:    role,
				ServiceAccount: envconf.Or("ZENTHROS_SERVICE_ACCOUNT", "zenthros-agent"),
				Namespace:      namespace,
			}
		}
		bundle, err := agepayload.LoadSecret(context.Background(), executor.Client(), namespace, envconf.Or("ZENTHROS_AGE_IDENTITY_SECRET", agepayload.DefaultSecretName))
		if err != nil {
			return nil, agepayload.Bundle{}, err
		}
		executor.Decryptor = bundle
		return executor, bundle, nil
	case agentModeContainer:
		bundle, err := agepayload.LoadDir(envconf.Or("ZENTHROS_AGE_IDENTITY_DIR", containerPath("age")))
		if err != nil {
			return nil, agepayload.Bundle{}, err
		}
		executor, err := newContainerExecutor(logger, containerToolchain)
		if err != nil {
			return nil, agepayload.Bundle{}, err
		}
		executor.SetAge(agepayload.Config{Bundle: bundle})
		return executor, bundle, nil
	default:
		return nil, agepayload.Bundle{}, fmt.Errorf("unknown ZENTHROS_AGENT_MODE %q (want %q, %q or %q)", mode, agentModeVM, agentModeK8s, agentModeContainer)
	}
}

// newImageGC is the machine's image sweeper: its policy from
// ZENTHROS_IMAGE_MAX_AGE and ZENTHROS_IMAGE_MAX_SPACE (a value it cannot read
// stops the agent, saying which), its ledger from the machine. A ledger it
// cannot read starts again, every image counted as used now: nothing is
// removed for an age it no longer knows.
func newImageGC(manager *systemd.Manager, logger logr.Logger) (*agent.ImageGC, error) {
	policy, err := imagegc.ParsePolicy(os.Getenv("ZENTHROS_IMAGE_MAX_AGE"), os.Getenv("ZENTHROS_IMAGE_MAX_SPACE"))
	if err != nil {
		return nil, err
	}
	ledger, err := imagegc.Load(agent.ImagesLedger)
	if err != nil {
		logger.Error(err, "the images' ledger cannot be read; starting a new one")
		ledger = imagegc.New(agent.ImagesLedger)
	}
	return agent.NewImageGC(manager.Runner(), manager.Config().SudoPrefix(), ledger, policy, nil, agent.RunningUnits(manager), logger.WithName("imagegc")), nil
}

func newK8sExecutor(logger logr.Logger) (*k8sagent.Executor, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add core scheme: %w", err)
	}
	if err := rayv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add ray scheme: %w", err)
	}
	if err := spark.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add SparkApplication scheme: %w", err)
	}
	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("resolve kubeconfig (in-cluster or KUBECONFIG): %w", err)
	}
	c, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	executor, err := k8sagent.New(c, restCfg, logger)
	if err != nil {
		return nil, err
	}
	// Status polls read the agent's own Jobs and pods from a watch, not the
	// apiserver. Without it they read the
	// apiserver, as before.
	namespace := ""
	if os.Getenv(agent.EnvLocal) != "" {
		// All it may watch on a person's cluster.
		namespace = os.Getenv("POD_NAMESPACE")
	}
	if statusCache, err := k8sagent.StartStatusCache(context.Background(), restCfg, scheme, namespace); err != nil {
		logger.Error(err, "status cache unavailable; status reads go to the apiserver")
	} else {
		executor.StatusCache = statusCache
	}
	return executor, nil
}

// defaultMaxJobs is 1 on a dedicated VM (one running job) and unlimited on a
// Kubernetes agent (ephemeral RayJobs create private clusters).
//
// A local agent has no count either: the container's own limits are its
// budget, and Kubernetes' scheduler shares a cluster.
func defaultMaxJobs(mode string) int {
	if mode == agentModeK8s || os.Getenv(agent.EnvLocal) != "" {
		return 0
	}
	return 1
}

// envNetwork reads ZENTHROS_NETWORK. Unset means tailscale, the mode every
// agent had before direct existed; a container has no tailnet device of its
// own, so unset is direct there.
func envNetwork(mode string) string {
	fallback := agent.NetworkTailscale
	if mode == agentModeContainer {
		fallback = agent.NetworkDirect
	}
	switch value := envconf.Or("ZENTHROS_NETWORK", fallback); value {
	case agent.NetworkTailscale, agent.NetworkDirect, agent.NetworkTailscaleUserspace:
		return value
	default:
		fmt.Fprintf(os.Stderr, "invalid ZENTHROS_NETWORK=%q: want %q, %q or %q\n", value,
			agent.NetworkTailscale, agent.NetworkDirect, agent.NetworkTailscaleUserspace)
		os.Exit(1)
		return ""
	}
}

func defaultReportListen(mode string) string {
	switch mode {
	case agentModeK8s:
		return ":9445"
	case agentModeContainer:
		return containerPath("run", "reports.sock")
	}
	return vmReportSocket
}

// vmReportSocket is in a directory of its own: a container job sees that
// directory read-only, and nothing else of the host's /var/run/zenthros.
const vmReportSocket = "/var/run/zenthros/agent/reports.sock"

// reportSocketDir is the directory a container job binds to reach the
// report socket: the socket's own, "" when the agent listens on TCP.
func reportSocketDir(listen string) string {
	if !strings.HasPrefix(listen, "/") {
		return ""
	}
	return filepath.Dir(listen)
}

func defaultReportEndpoint(mode string) string {
	switch mode {
	case agentModeK8s:
		return "http://zenthros-agent.zenthros-system.svc.cluster.local:9445"
	case agentModeContainer:
		return "unix://" + containerPath("run", "reports.sock")
	}
	return "unix://" + vmReportSocket
}

// defaultPreemptDir is empty on Kubernetes: a node's notice is the node's,
// and the agent does not act on it for the pods it runs.
func defaultPreemptDir(mode string) string {
	switch mode {
	case agentModeK8s:
		return ""
	case agentModeContainer:
		return containerPath("run", "preempt")
	}
	return agent.DefaultPreemptDir
}

// defaultServiceDir is empty on Kubernetes: the in-cluster agent runs no
// Service Job; a Manifest's endpoints are read from its object.
func defaultServiceDir(mode string) string {
	switch mode {
	case agentModeK8s:
		return ""
	case agentModeContainer:
		return containerPath("services")
	}
	return "/var/lib/zenthros/services"
}

func defaultReportSpool(mode string) string {
	if mode == agentModeContainer {
		return containerPath("reports.sqlite")
	}
	return "/var/lib/zenthros/reports.sqlite"
}
