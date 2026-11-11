// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/reportspool"
)

func TestReportServerLocalAckIsDurable(t *testing.T) {
	dir := t.TempDir()
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(dir, "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	token, err := spool.IssueToken(context.Background(), "job-1")
	if err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, "reports.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := newReportServer(spool, logr.Discard(), nil)
	go func() { _ = server.serve(ctx, "unix", sock) }()
	deadline := time.Now().Add(2 * time.Second)
	var conn net.Conn
	for time.Now().Before(deadline) {
		conn, err = net.Dial("unix", sock)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("ingest socket did not come up")
	}
	_ = conn.Close()

	body, _ := json.Marshal(executionreport.Snapshot{
		APIVersion: executionreport.APIVersion, Kind: executionreport.Kind,
		JobID: "job-1", ProducerID: "worker-0", ReportID: "rep-1",
		ProducedAt: time.Now().UTC(), Payload: json.RawMessage(`{"n":1}`),
	})
	req, err := http.NewRequest(http.MethodPost, "http://localhost/v1/reports", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", sock)
		},
	}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	unacked, err := spool.Unacked(context.Background())
	if err != nil || len(unacked) != 1 {
		t.Fatalf("unacked = %+v err=%v", unacked, err)
	}
}

func TestReportServerRejectsBadToken(t *testing.T) {
	dir := t.TempDir()
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(dir, "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	_, _ = spool.IssueToken(context.Background(), "job-1")
	server := newReportServer(spool, logr.Discard(), nil)
	body, _ := json.Marshal(map[string]any{
		"apiVersion": executionreport.APIVersion, "kind": executionreport.Kind,
		"job_id": "job-1", "producer_id": "p", "report_id": "r",
		"produced_at": time.Now().UTC(), "payload": map[string]int{"n": 1},
	})
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/reports", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer wrong")
	rec := &captureResponse{}
	server.handlePublish(rec, req)
	if rec.status != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.status)
	}
}

type captureResponse struct {
	status int
	hdr    http.Header
	buf    bytes.Buffer
}

func (c *captureResponse) Header() http.Header {
	if c.hdr == nil {
		c.hdr = http.Header{}
	}
	return c.hdr
}
func (c *captureResponse) Write(b []byte) (int, error) { return c.buf.Write(b) }
func (c *captureResponse) WriteHeader(status int)      { c.status = status }

// newReportServer is a report server without checkpoint commits.
func newReportServer(spool *reportspool.Spool, logger Logger, notify func()) *reportServer {
	return newReportServerWithCheckpoint(spool, logger, notify, nil)
}

// A spool that cannot be read says nothing about the job's token: the
// publish, the checkpoint and the spawn get 503, which the SDK retries, and
// never the 401 that ends a job's reporting (05#2).
func TestAnUnreadableSpoolIsNotABadToken(t *testing.T) {
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(t.TempDir(), "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	token, err := spool.IssueToken(context.Background(), "job-1")
	if err != nil {
		t.Fatal(err)
	}
	server := newReportServer(spool, logr.Discard(), nil)
	server.checkpoint = func(context.Context, *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error) {
		t.Fatal("a checkpoint went on without its token checked")
		return nil, nil
	}
	server.children = func(context.Context, *agentpb.ChildSpawnRequest) (*agentpb.ChildSpawnAck, error) {
		t.Fatal("a spawn went on without its token checked")
		return nil, nil
	}
	_ = spool.Close()
	report, _ := json.Marshal(executionreport.Snapshot{
		APIVersion: executionreport.APIVersion, Kind: executionreport.Kind,
		JobID: "job-1", ProducerID: "worker-0", ReportID: "rep-1",
		ProducedAt: time.Now().UTC(), Payload: json.RawMessage(`{"n":1}`),
	})
	for path, body := range map[string]string{
		"/v1/reports":     string(report),
		"/v1/checkpoints": `{"job_id":"job-1"}`,
		"/v1/children":    `{"job_id":"job-1","count":1}`,
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		server.httpSrv.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), executionreport.CodeStoreUnavailable) {
			t.Errorf("%s with an unreadable spool: %d %s, want 503 %s", path, rec.Code, rec.Body.String(), executionreport.CodeStoreUnavailable)
		}
	}
}

// Each endpoint takes only POST; the mux answers anything else 405 and
// names the method it allows.
func TestReportEndpointsAnswerOnlyPost(t *testing.T) {
	server := newReportServer(nil, logr.Discard(), nil)
	for _, path := range []string{"/v1/reports", "/v1/checkpoints", "/v1/children"} {
		rec := httptest.NewRecorder()
		server.httpSrv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
			t.Fatalf("GET %s: %d Allow=%q, want 405 Allow=POST", path, rec.Code, rec.Header().Get("Allow"))
		}
	}
}
