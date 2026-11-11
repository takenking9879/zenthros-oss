// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/jobcli"
	"github.com/zenthros/operator/pkg/reportspool"
)

// `zenthros-agent job spawn` against the agent's own socket: the token is
// checked before anything is forwarded, the forward carries what the job
// asked (and no backend: the gateway stamps the link's), and the
// controller's refusal comes back to the job word for word.
func TestChildrenEndpointForwardsAfterToken(t *testing.T) {
	dir, err := os.MkdirTemp("", "zjc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spool, err := reportspool.Open(ctx, reportspool.Config{Path: filepath.Join(dir, "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	token, err := spool.IssueToken(ctx, "job-1")
	if err != nil {
		t.Fatal(err)
	}
	var forwarded []*agentpb.ChildSpawnRequest
	var answer *agentpb.ChildSpawnAck
	server := newReportServer(spool, logr.Discard(), nil)
	server.children = func(_ context.Context, req *agentpb.ChildSpawnRequest) (*agentpb.ChildSpawnAck, error) {
		forwarded = append(forwarded, req)
		return answer, nil
	}
	sock := filepath.Join(dir, "r.sock")
	go func() { _ = server.serve(ctx, "unix", sock) }()
	for deadline := time.Now().Add(2 * time.Second); ; {
		if conn, err := net.Dial("unix", sock); err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket did not come up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	env := map[string]string{jobcli.EnvReportEndpoint: "unix://" + sock, jobcli.EnvReportToken: "nope", jobcli.EnvJobID: "job-1",
		jobcli.EnvStateDir: dir, "ZENTHROS_GENERATION": "4"}
	run := func(args ...string) (int, string) {
		var out, errOut bytes.Buffer
		c := &jobcli.CLI{Env: func(k string) string { return env[k] }, Stdout: &out, Stderr: &errOut, Attempts: 1}
		code := c.Run(ctx, args)
		return code, out.String() + errOut.String()
	}

	if code, out := run("spawn", "--name", "g", "--count", "2", "--same"); code != jobcli.ExitFailed ||
		strings.TrimSpace(out) != "UNAUTHORIZED: job capability token is invalid" || len(forwarded) != 0 {
		t.Fatalf("bad token: %d %q, forwarded %d", code, out, len(forwarded))
	}

	env[jobcli.EnvReportToken] = token
	answer = &agentpb.ChildSpawnAck{Accepted: true, Children: []*agentpb.ChildStatus{
		{Group: "g", Index: 0, Name: "train-g-0", Phase: "Pending"}, {Group: "g", Index: 1, Name: "train-g-1", Phase: "Pending"}}}
	code, out := run("spawn", "--name", "g", "--count", "2", "--env", "K=two words", "--cmd", "sh", "-c", "echo $TASK_INDEX")
	if code != 0 || out != "train-g-0 Pending\ntrain-g-1 Pending\n" {
		t.Fatalf("spawn: %d %q", code, out)
	}
	want := &agentpb.ChildSpawnRequest{JobId: "job-1", Generation: 4, Group: "g", Count: 2,
		Command: []string{"sh", "-c", "echo $TASK_INDEX"}, Env: map[string]string{"K": "two words"}}
	if len(forwarded) != 1 || !reflect.DeepEqual(forwarded[0].String(), want.String()) {
		t.Fatalf("forwarded %v, want %v", forwarded, want)
	}

	answer = &agentpb.ChildSpawnAck{ErrorCode: "SPAWN_QUOTA_EXCEEDED",
		Error: "300 children asked, 0 already spawned: a job may spawn at most 256 (controller setting ZENTHROS_SPAWN_MAX_CHILDREN_PER_JOB)"}
	code, out = run("spawn", "--name", "big", "--count", "300", "--same")
	if code != jobcli.ExitFailed || strings.TrimSpace(out) != "SPAWN_QUOTA_EXCEEDED: "+answer.Error {
		t.Fatalf("refusal: %d %q", code, out)
	}
	if forwarded[1].GetBackendId() != "" {
		t.Fatalf("the job named a backend: %q", forwarded[1].GetBackendId())
	}

	// The controller has not recorded the lease yet (the job spawns the
	// moment it starts): 503, which the CLI retries until it is accepted.
	answers := []*agentpb.ChildSpawnAck{
		{ErrorCode: "LEASE_PENDING", Error: "train has no lease recorded yet: its submission is still being written; ask again"},
		{Accepted: true, Children: []*agentpb.ChildStatus{{Group: "early", Index: 0, Name: "train-early-0", Phase: "Pending"}}},
	}
	server.children = func(_ context.Context, req *agentpb.ChildSpawnRequest) (*agentpb.ChildSpawnAck, error) {
		forwarded = append(forwarded, req)
		next := answers[0]
		answers = answers[1:]
		return next, nil
	}
	var early bytes.Buffer
	pending := &jobcli.CLI{Env: func(k string) string { return env[k] }, Stdout: &early, Stderr: &early, Attempts: 3, Backoff: time.Millisecond}
	if code := pending.Run(ctx, []string{"spawn", "--name", "early", "--count", "1", "--same"}); code != 0 ||
		early.String() != "train-early-0 Pending\n" || len(answers) != 0 {
		t.Fatalf("pending lease: %d %q, %d answers left", code, early.String(), len(answers))
	}
	forwarded = forwarded[:2]

	// A body with a field the protocol does not have is refused, not guessed.
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/children", strings.NewReader(`{"job_id":"job-1","backend_id":"m9"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	reply, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(reply), `unknown field \"backend_id\"`) || len(forwarded) != 2 {
		t.Fatalf("unknown field: %d %s", resp.StatusCode, reply)
	}
}
