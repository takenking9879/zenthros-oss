// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	agentpb "github.com/takenking9879/zenthros-oss/operator/internal/grpc/gen"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
	"github.com/takenking9879/zenthros-oss/operator/pkg/fault"
)

// The node log reader: zenthros-agent node-log-reader, the agent chart's
// DaemonSet. It serves the log files of the pods on its node to the
// cluster's agent alone (NodeLogs, mTLS with a CA of the chart's own: the
// logs and the request cross the pod network encrypted, and only the
// agent's certificate is served), and it is ready only once it can read
// them: a node where it cannot is read through the API.

const (
	// NodeLogReaderName is the name on the reader's certificate.
	NodeLogReaderName = "zenthros-node-log-reader"
	// NodeLogClientName is the name on the agent's: the one caller served.
	NodeLogClientName = "zenthros-agent"
	// NodeLogPort is the reader's NodeLogs port.
	NodeLogPort = 9447
	// maxNodeLogReads bounds the reads a reader serves at once: each holds
	// files and an inotify instance (the kernel's default is 128 a user).
	maxNodeLogReads = 64
)

// NodeLogServer serves NodeLogs from the pods' log files under Root.
type NodeLogServer struct {
	agentpb.UnimplementedNodeLogsServer
	Root  string
	reads chan struct{}
}

// NewNodeLogServer serves the log files under root.
func NewNodeLogServer(root string) *NodeLogServer {
	return &NodeLogServer{Root: root, reads: make(chan struct{}, maxNodeLogReads)}
}

// ReadPodLog reads the container req names, by its pod's UID.
func (s *NodeLogServer) ReadPodLog(req *agentpb.PodLogRequest, stream agentpb.NodeLogs_ReadPodLogServer) error {
	if err := callerIsTheAgent(stream.Context()); err != nil {
		return err
	}
	dir, err := PodLogDir(s.Root, req.GetNamespace(), req.GetPod(), req.GetUid(), req.GetContainer())
	if err != nil {
		return fault.GRPC(err)
	}
	select {
	case s.reads <- struct{}{}:
		defer func() { <-s.reads }()
	default:
		return status.Errorf(codes.ResourceExhausted, "the node's log reader serves %d reads at once", maxNodeLogReads)
	}
	chunks, err := ReadPodLog(stream.Context(), dir, backend.LogRequest{
		Follow:       req.GetFollow(),
		TailLines:    int(req.GetTailLines()),
		HistoryBytes: req.GetHistoryBytes(),
		After:        req.GetAfter(),
	})
	if err != nil {
		return fault.GRPC(err)
	}
	// The read is taken: the agent waits for this before it reads on.
	if err := stream.SendHeader(metadata.Pairs("zenthros-read", "open")); err != nil {
		return err
	}
	for chunk := range chunks {
		if err := stream.Send(backend.ProtoLogChunk(chunk)); err != nil {
			return err
		}
	}
	return nil
}

// callerIsTheAgent checks the caller's certificate, which TLS verified
// against the chart's CA, is the agent's.
func callerIsTheAgent(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if ok {
		if info, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(info.State.VerifiedChains) > 0 &&
			len(info.State.VerifiedChains[0]) > 0 && info.State.VerifiedChains[0][0].Subject.CommonName == NodeLogClientName {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "the node's log reader serves the cluster's agent only")
}

// NodeLogReaderConfig is how a node log reader runs.
type NodeLogReaderConfig struct {
	// Listen is NodeLogs' address; Health the probes'.
	Listen, Health string
	// Root is where the node's pod logs are mounted.
	Root string
	// Dir holds the reader's certificate of the chart's CA: server.crt,
	// server.key, ca.crt.
	Dir string
	// Self is the reader's own container: it is ready once it can read its
	// log.
	Namespace, Pod, UID, Container string
}

// RunNodeLogReader serves NodeLogs until ctx ends.
func RunNodeLogReader(ctx context.Context, cfg NodeLogReaderConfig, logger logr.Logger) error {
	cert, err := tls.LoadX509KeyPair(cfg.Dir+"/server.crt", cfg.Dir+"/server.key")
	if err != nil {
		return fmt.Errorf("node log reader: %w", err)
	}
	ca, err := os.ReadFile(cfg.Dir + "/ca.crt")
	if err != nil {
		return fmt.Errorf("node log reader: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return fmt.Errorf("node log reader: no certificate in %s/ca.crt", cfg.Dir)
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	})
	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("node log reader: %w", err)
	}
	server := grpc.NewServer(grpc.Creds(creds))
	agentpb.RegisterNodeLogsServer(server, NewNodeLogServer(cfg.Root))

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if err := canRead(cfg); err != nil {
			http.Error(w, err.Error(), fault.HTTPStatus(err))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	health := &http.Server{Addr: cfg.Health, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(err, "node log reader probes")
		}
	}()
	go func() {
		<-ctx.Done()
		// Followed reads run until their caller ends them: stopped, not waited for.
		server.Stop()
		_ = health.Close()
	}()
	logger.Info("node log reader serving", "listen", cfg.Listen, "root", cfg.Root)
	if err := server.Serve(lis); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

// canRead is whether the reader can read the node's pod logs, as its own
// shows: a node whose files it may not read is read through the API.
func canRead(cfg NodeLogReaderConfig) error {
	dir, err := PodLogDir(cfg.Root, cfg.Namespace, cfg.Pod, cfg.UID, cfg.Container)
	if err != nil {
		return err
	}
	files, err := listCRI(dir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no log file of its own in %s yet", dir)
	}
	_, closer, err := files[len(files)-1].open(dir)
	if err != nil {
		return err
	}
	return closer.Close()
}
