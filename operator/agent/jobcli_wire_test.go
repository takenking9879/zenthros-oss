// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/jobcli"
	"github.com/zenthros/operator/pkg/reportspool"
)

// `zenthros-agent job ...` against the agent's own report socket: the real
// token check, the real spool and the real CheckpointCommit decode. A body
// the CLI writes that the server reads differently fails here, not on a VM.
func TestJobCLIAgainstRealReportServer(t *testing.T) {
	dir, err := os.MkdirTemp("", "zjw")
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
	var commits []*agentpb.CheckpointCommit
	frozenAfter := int64(2)
	commit := func(_ context.Context, c *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error) {
		commits = append(commits, c)
		if c.GetSeq() > frozenAfter {
			return &agentpb.CheckpointAck{Error: "cursor frozen: replacement submitted at 2026-01-02T03:04:05Z", ErrorCode: executionreport.CodeCheckpointFrozen}, nil
		}
		return &agentpb.CheckpointAck{JobId: c.GetJobId(), Phase: c.GetPhase(), Seq: c.GetSeq(), Accepted: true}, nil
	}
	sock := filepath.Join(dir, "r.sock")
	server := newReportServerWithCheckpoint(spool, logr.Discard(), nil, commit)
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
	root := filepath.Join(dir, "ckpt")
	env := map[string]string{
		jobcli.EnvReportEndpoint: "unix://" + sock, jobcli.EnvReportToken: token, jobcli.EnvJobID: "job-1",
		jobcli.EnvProducerID: "m1", jobcli.EnvStateDir: dir, "ZENTHROS_GENERATION": "4",
		"ZENTHROS_CHECKPOINT_PREFIX": "file://" + root,
	}
	run := func(args ...string) (int, string) {
		var out, errOut bytes.Buffer
		// A spool write slower than the CLI's 10s timeout (full suite on a busy
		// disk) is retried as on a VM; the spool dedupes the report.
		c := &jobcli.CLI{Env: func(k string) string { return env[k] }, Stdout: &out, Stderr: &errOut, Attempts: 3, Backoff: 10 * time.Millisecond}
		code := c.Run(ctx, args)
		return code, out.String() + errOut.String()
	}

	if code, out := run("report", "set", "epoch=1", `metrics={"loss":0.5}`, "--final"); code != 0 {
		t.Fatalf("report: %d %s", code, out)
	}
	rows, err := spool.Unacked(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("spool rows %+v %v", rows, err)
	}
	if got := string(rows[0].Payload); got != `{"epoch":1,"metrics":{"loss":0.5}}` || !rows[0].Final || rows[0].ProducerID != "m1" {
		t.Fatalf("stored %+v payload %s", rows[0], got)
	}

	file := filepath.Join(dir, "state.bin")
	os.WriteFile(file, []byte("step-1"), 0o644)
	for i := 0; i < 2; i++ {
		if code, out := run("checkpoint", "commit", "--phase", "train", "--file", file); code != 0 {
			t.Fatalf("commit %d: %d %s", i, code, out)
		}
	}
	if len(commits) != 2 || commits[1].GetGeneration() != 4 || commits[1].GetSeq() != 2 ||
		commits[1].GetUri() != "file://"+root+"/jobs/job-1/gen/4/train/2" || len(commits[1].GetSha256()) != 64 {
		t.Fatalf("commits %+v", commits)
	}
	// The third is refused: the controller's code and words, and exit 3.
	code, out := run("checkpoint", "commit", "--phase", "train", "--file", file, "--complete")
	if code != jobcli.ExitFrozen || !strings.Contains(out, "CHECKPOINT_FROZEN: cursor frozen: replacement submitted at 2026-01-02T03:04:05Z") {
		t.Fatalf("frozen: %d %q", code, out)
	}
	if !commits[2].GetComplete() {
		t.Fatal("--complete not sent")
	}

	// A wrong token is the server's refusal, verbatim.
	env[jobcli.EnvReportToken] = "nope"
	if code, out := run("report", "set", "a=1"); code != jobcli.ExitFailed || strings.TrimSpace(out) != "UNAUTHORIZED: job capability token is invalid" {
		t.Fatalf("bad token: %d %q", code, out)
	}
}
