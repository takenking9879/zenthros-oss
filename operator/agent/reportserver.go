// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/metrics"
	"github.com/zenthros/operator/pkg/reportspool"
)

const (
	EnvReportEndpoint = jobspec.EnvReportEndpoint
	EnvJobID          = jobspec.EnvJobID
	EnvReportToken    = jobspec.EnvReportToken
)

type reportServer struct {
	spool      *reportspool.Spool
	logger     Logger
	notify     func()
	checkpoint func(context.Context, *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error)
	children   func(context.Context, *agentpb.ChildSpawnRequest) (*agentpb.ChildSpawnAck, error)
	httpSrv    *http.Server
}

func newReportServerWithCheckpoint(
	spool *reportspool.Spool,
	logger Logger,
	notify func(),
	checkpoint func(context.Context, *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error),
) *reportServer {
	mux := http.NewServeMux()
	server := &reportServer{
		spool: spool, logger: logger.WithName("report-ingest"), notify: notify, checkpoint: checkpoint,
	}
	// The mux answers any other method 405, with the Allow header.
	mux.HandleFunc("POST /v1/reports", server.handlePublish)
	mux.HandleFunc("POST /v1/checkpoints", server.handleCheckpoint)
	mux.HandleFunc("POST /v1/children", server.handleChildren)
	server.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return server
}

func (s *reportServer) serve(ctx context.Context, network, addr string) error {
	var listener net.Listener
	var err error
	if network == "unix" {
		if err := os.MkdirAll(filepath.Dir(addr), 0o755); err != nil {
			return err
		}
		_ = os.Remove(addr)
		listener, err = net.Listen("unix", addr)
		if err != nil {
			return err
		}
		_ = os.Chmod(addr, 0o666)
	} else {
		listener, err = net.Listen(network, addr)
		if err != nil {
			return err
		}
	}
	go func() {
		<-ctx.Done()
		_ = s.httpSrv.Close()
		_ = listener.Close()
	}()
	s.logger.Info("report ingest listening", "network", network, "addr", addr)
	err = s.httpSrv.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *reportServer) handlePublish(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(io.LimitReader(req.Body, int64(executionreport.MaxPayloadBytes)+64*1024))
	if err != nil {
		writeReportError(w, http.StatusBadRequest, executionreport.CodeInvalidJSON, "failed to read body")
		return
	}
	var snapshot executionreport.Snapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		writeReportError(w, http.StatusBadRequest, executionreport.CodeInvalidJSON, "body is not a valid ExecutionReport")
		return
	}
	if snapshot.APIVersion == "" {
		snapshot.APIVersion = executionreport.APIVersion
	}
	if snapshot.Kind == "" {
		snapshot.Kind = executionreport.Kind
	}
	if snapshot.ProducedAt.IsZero() {
		snapshot.ProducedAt = time.Now().UTC()
	}
	if !s.authorize(w, req, snapshot.JobID) {
		return
	}
	result, err := s.spool.Insert(req.Context(), snapshot)
	if err != nil {
		code := executionreport.CodeOf(err)
		metrics.RecordAgentSpoolIngest(spoolIngestResult(code))
		s.refreshSpoolMetrics(req.Context())
		writeReportError(w, ingestStatus(code), code, err.Error())
		return
	}
	if result.Duplicate {
		metrics.RecordAgentSpoolIngest(metrics.SpoolIngestDuplicate)
	} else {
		metrics.RecordAgentSpoolIngest(metrics.SpoolIngestAccepted)
	}
	s.refreshSpoolMetrics(req.Context())
	if s.notify != nil {
		s.notify()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(result.Receipt)
}

// authorize checks the job's capability token for jobID. A token that does
// not match is 401; a spool that could not be read is 503, which the SDK
// retries: a store outage is never the job's own bad token (05#2).
func (s *reportServer) authorize(w http.ResponseWriter, req *http.Request, jobID string) bool {
	token := bearerToken(req.Header.Get("Authorization"))
	if token == "" || jobID == "" {
		writeReportError(w, http.StatusUnauthorized, executionreport.CodeUnauthorized, "job capability token or job id is missing")
		return false
	}
	ok, err := s.spool.LookupToken(req.Context(), jobID, token)
	if err != nil {
		writeReportError(w, http.StatusServiceUnavailable, executionreport.CodeStoreUnavailable, "read the job's capability token: "+err.Error())
		return false
	}
	if !ok {
		writeReportError(w, http.StatusUnauthorized, executionreport.CodeUnauthorized, "job capability token is invalid")
		return false
	}
	return true
}

// ingestStatus is the HTTP status of a report the spool did not take.
func ingestStatus(code string) int {
	switch code {
	case executionreport.CodeSpoolFull:
		return http.StatusInsufficientStorage
	case executionreport.CodePayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	case executionreport.CodeStoreUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadRequest
	}
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if strings.HasPrefix(header, prefix) {
		return strings.TrimSpace(header[len(prefix):])
	}
	return strings.TrimSpace(header)
}

func writeReportError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error_code": code, "error": message})
}

func (s *reportServer) handleCheckpoint(w http.ResponseWriter, req *http.Request) {
	if s.checkpoint == nil {
		writeReportError(w, http.StatusServiceUnavailable, executionreport.CodeStoreUnavailable, "checkpoint forwarder is not configured")
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, 64*1024))
	if err != nil {
		writeReportError(w, http.StatusBadRequest, executionreport.CodeInvalidJSON, "failed to read body")
		return
	}
	var commit agentpb.CheckpointCommit
	if err := json.Unmarshal(body, &commit); err != nil {
		writeReportError(w, http.StatusBadRequest, executionreport.CodeInvalidJSON, "body is not a valid CheckpointCommit")
		return
	}
	if !s.authorize(w, req, commit.GetJobId()) {
		return
	}
	ack, err := s.checkpoint(req.Context(), &commit)
	if err != nil {
		writeReportError(w, http.StatusServiceUnavailable, executionreport.CodeStoreUnavailable, err.Error())
		return
	}
	if ack.GetErrorCode() == executionreport.CodeStoreUnavailable {
		// No decision: the commit may be kept. A 409 here would have the job
		// reuse the seq with other bytes, refused against the kept commit.
		writeReportError(w, http.StatusServiceUnavailable, executionreport.CodeStoreUnavailable, ack.GetError())
		return
	}
	if !ack.GetAccepted() {
		// The controller's code goes through as it is: the job tells a
		// frozen cursor from a lease being handed over by it.
		writeReportError(w, http.StatusConflict, ack.GetErrorCode(), ack.GetError())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"accepted": true, "phase": ack.GetPhase(), "seq": ack.GetSeq(),
	})
}

// childrenBody is POST /v1/children (docs/interfaces/job-protocol.md). The
// backend is never taken from the body: the gateway stamps the link's.
type childrenBody struct {
	JobID      string            `json:"job_id"`
	Generation int64             `json:"generation"`
	Group      string            `json:"group"`
	Count      int32             `json:"count"`
	Command    []string          `json:"command"`
	Same       bool              `json:"same"`
	Env        map[string]string `json:"env"`
	List       bool              `json:"list"`
}

// handleChildren forwards a job's spawn (or list) after checking its token.
// The controller decides; its refusal comes back in its words.
func (s *reportServer) handleChildren(w http.ResponseWriter, req *http.Request) {
	if s.children == nil {
		writeReportError(w, http.StatusServiceUnavailable, executionreport.CodeStoreUnavailable, "child spawn forwarder is not configured")
		return
	}
	dec := json.NewDecoder(io.LimitReader(req.Body, 64*1024))
	dec.DisallowUnknownFields()
	var body childrenBody
	if err := dec.Decode(&body); err != nil {
		writeReportError(w, http.StatusBadRequest, executionreport.CodeInvalidJSON, "body is not a valid spawn: "+err.Error())
		return
	}
	if !s.authorize(w, req, body.JobID) {
		return
	}
	ack, err := s.children(req.Context(), &agentpb.ChildSpawnRequest{
		JobId: body.JobID, Generation: body.Generation, Group: body.Group, Count: body.Count,
		Command: body.Command, Same: body.Same, Env: body.Env, List: body.List,
	})
	if err != nil {
		writeReportError(w, http.StatusServiceUnavailable, executionreport.CodeStoreUnavailable, err.Error())
		return
	}
	if !ack.GetAccepted() {
		status := http.StatusConflict
		if ack.GetErrorCode() == "STORE_UNAVAILABLE" || ack.GetErrorCode() == v1.SpawnLeasePending {
			status = http.StatusServiceUnavailable
		}
		writeReportError(w, status, ack.GetErrorCode(), ack.GetError())
		return
	}
	type child struct {
		Group   string `json:"group"`
		Index   int32  `json:"index"`
		Name    string `json:"name"`
		Phase   string `json:"phase"`
		Message string `json:"message"`
	}
	children := make([]child, 0, len(ack.GetChildren()))
	for _, c := range ack.GetChildren() {
		children = append(children, child{c.GetGroup(), c.GetIndex(), c.GetName(), c.GetPhase(), c.GetMessage()})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "children": children})
}

func (a *Agent) serveReports(ctx context.Context) {
	if a.spool == nil || a.cfg.ReportListen == "" {
		return
	}
	notify := func() {
		if a.forwarder != nil {
			a.forwarder.Wake()
		}
	}
	commitFn := a.commit
	if commitFn == nil && a.checkpoints != nil {
		commitFn = a.checkpoints.Commit
	}
	server := newReportServerWithCheckpoint(a.spool, a.logger, notify, commitFn)
	server.children = a.SpawnChildren
	network, addr := reportListen(a.cfg.ReportListen)
	go func() {
		if err := server.serve(ctx, network, addr); err != nil && ctx.Err() == nil {
			a.logger.Error(err, "report ingest server exited")
		}
	}()
}

func (s *reportServer) refreshSpoolMetrics(ctx context.Context) {
	if s.spool == nil {
		return
	}
	stats, err := s.spool.Stats(ctx)
	if err != nil {
		return
	}
	metrics.SetAgentSpoolPending(stats.Count, stats.Bytes, stats.OldestAge)
}

func spoolIngestResult(code string) string {
	switch code {
	case executionreport.CodeSpoolFull:
		return metrics.SpoolIngestFull
	case executionreport.CodePayloadTooLarge:
		return metrics.SpoolIngestPayloadTooLarge
	default:
		return metrics.SpoolIngestInvalid
	}
}

func reportListen(listen string) (string, string) {
	if strings.HasPrefix(listen, "/") || strings.HasSuffix(listen, ".sock") {
		return "unix", listen
	}
	return "tcp", listen
}
