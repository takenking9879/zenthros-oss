// SPDX-License-Identifier: Apache-2.0

package jobcli

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// `job spawn` sends the bytes zenthros.spawn() sends
// (sdk/python/tests/test_job_protocol.py checks the SDK against the same
// fixture): a shell job and a Python job fan out alike.
func TestAgentJobCLISpawnMatchesSDKWire(t *testing.T) {
	fx := loadFixture(t)
	a := newAgent(t, stubReply{http.StatusOK, string(fx.Spawn.Reply)})
	c, out, errOut := cli(t, fx.Env, a)
	if code := c.Run(context.Background(), append([]string{"spawn"}, fx.Spawn.Args...)); code != ExitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if code := c.Run(context.Background(), []string{"spawn", "--name", "again", "--count", "2", "--same"}); code != ExitOK {
		t.Fatalf("--same: exit %d: %s", code, errOut)
	}
	if code := c.Run(context.Background(), []string{"children", "--name", "shards"}); code != ExitOK {
		t.Fatalf("children: exit %d: %s", code, errOut)
	}
	reqs := a.got()
	if len(reqs) != 3 {
		t.Fatalf("requests = %d", len(reqs))
	}
	for i, want := range []map[string]any{fx.Spawn.Body, fx.Spawn.SameBody, fx.Spawn.ListBody} {
		if reqs[i].Path != fx.Spawn.Path || reqs[i].Auth != fx.Authorization {
			t.Fatalf("request %d: path %q auth %q", i, reqs[i].Path, reqs[i].Auth)
		}
		if got := decode(t, reqs[i].Body); !reflect.DeepEqual(got, want) {
			t.Fatalf("request %d body\n got %v\nwant %v", i, got, want)
		}
	}
	if !strings.Contains(out.String(), "train-shards-0 Succeeded\ntrain-shards-1 Failed\n") {
		t.Fatalf("stdout %q", out)
	}
}

// --wait returns once every child has ended, and fails with each failed
// child's words, whole.
func TestSpawnWaitFailsWithChildMessage(t *testing.T) {
	old := ChildPoll
	ChildPoll = time.Millisecond
	t.Cleanup(func() { ChildPoll = old })
	running := `{"accepted":true,"children":[{"group":"g","index":0,"name":"j-g-0","phase":"Running"},{"group":"g","index":1,"name":"j-g-1","phase":"Pending"}]}`
	ended := `{"accepted":true,"children":[{"group":"g","index":0,"name":"j-g-0","phase":"Succeeded"},` +
		`{"group":"g","index":1,"name":"j-g-1","phase":"Failed","message":"exit status 3: shard 1: bad row 17\nsecond line"}]}`
	a := newAgent(t, stubReply{http.StatusOK, running}, stubReply{http.StatusOK, running}, stubReply{http.StatusOK, ended})
	c, _, errOut := cli(t, map[string]string{EnvJobID: "uid", EnvReportToken: "t"}, a)
	code := c.Run(context.Background(), []string{"spawn", "--name", "g", "--count", "2", "--wait", "--cmd", "sh", "-c", "work"})
	if code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
	if want := "CHILD_FAILED: j-g-1 ended Failed: exit status 3: shard 1: bad row 17\nsecond line\n"; errOut.String() != want {
		t.Fatalf("stderr %q, want %q", errOut, want)
	}
	if n := len(a.got()); n != 3 {
		t.Fatalf("requests = %d: spawn, then polls until all ended", n)
	}

	a = newAgent(t, stubReply{http.StatusOK, strings.ReplaceAll(ended, `"Failed"`, `"Succeeded"`)})
	c, _, errOut = cli(t, map[string]string{EnvJobID: "uid", EnvReportToken: "t"}, a)
	if code := c.Run(context.Background(), []string{"children", "--wait"}); code != ExitOK {
		t.Fatalf("all succeeded: exit %d: %s", code, errOut)
	}
}

// A wait for children lasts hours, one call to the agent 45 s: an agent or
// controller away for longer than a call is waited out, not a failed parent
// whose children run on (R4 ronda 5, P2).
func TestWaitOutlastsAnAgentAwayLongerThanACall(t *testing.T) {
	old := ChildPoll
	ChildPoll = time.Millisecond
	t.Cleanup(func() { ChildPoll = old })
	away := stubReply{http.StatusServiceUnavailable, `{"error_code":"STORE_UNAVAILABLE","error":"hub down"}`}
	ended := `{"accepted":true,"children":[{"group":"g","index":0,"name":"j-g-0","phase":"Succeeded"}]}`
	a := newAgent(t, away, away, away, away, away, away, stubReply{http.StatusOK, ended})
	c, _, errOut := cli(t, map[string]string{EnvJobID: "uid", EnvReportToken: "t"}, a)
	if code := c.Run(context.Background(), []string{"children", "--wait"}); code != ExitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if n := len(a.got()); n != 7 {
		t.Fatalf("requests = %d: two calls the agent left unanswered, then the one that saw the children end", n)
	}
}

// A refused spawn is the controller's refusal, word for word, and a
// non-zero exit; nothing is retried.
func TestSpawnRefusalIsVerbatim(t *testing.T) {
	refusal := `{"error_code":"SPAWN_QUOTA_EXCEEDED","error":"300 children asked, 0 already spawned: a job may spawn at most 256 (controller setting ZENTHROS_SPAWN_MAX_CHILDREN_PER_JOB)"}`
	a := newAgent(t, stubReply{http.StatusConflict, refusal})
	c, _, errOut := cli(t, map[string]string{EnvJobID: "uid", EnvReportToken: "t"}, a)
	if code := c.Run(context.Background(), []string{"spawn", "--name", "g", "--count", "300", "--same"}); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
	want := "SPAWN_QUOTA_EXCEEDED: 300 children asked, 0 already spawned: a job may spawn at most 256 (controller setting ZENTHROS_SPAWN_MAX_CHILDREN_PER_JOB)\n"
	if errOut.String() != want || len(a.got()) != 1 {
		t.Fatalf("stderr %q (requests %d)", errOut, len(a.got()))
	}
}

func TestSpawnUsage(t *testing.T) {
	c, _, _ := cli(t, nil, nil)
	for _, args := range [][]string{
		{"spawn"},
		{"spawn", "--name", "g"},
		{"spawn", "--name", "g", "--count", "2"},
		{"spawn", "--name", "g", "--count", "2", "--same", "--cmd", "x"},
		{"spawn", "--name", "g", "--count", "2", "--cmd"},
		{"children", "extra"},
	} {
		if code := c.Run(context.Background(), args); code != ExitUsage {
			t.Fatalf("%v: exit %d", args, code)
		}
	}
}

// A spawn names the generation the job's checkpoints are written under:
// one number for both, 1 before the job's first lease. A spawn sent 0 where
// the checkpoints said 1.
func TestSpawnSendsTheGenerationOfItsCheckpoints(t *testing.T) {
	a := newAgent(t, stubReply{http.StatusOK, `{"accepted":true,"children":[]}`})
	c, _, errOut := cli(t, map[string]string{EnvJobID: "uid", EnvReportToken: "t"}, a)
	if code := c.Run(context.Background(), []string{"spawn", "--name", "g", "--count", "1", "--cmd", "true"}); code != ExitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if got := decode(t, a.got()[0].Body)["generation"]; got != float64(c.generation()) || c.generation() != 1 {
		t.Fatalf("spawn generation %v, checkpoints under %d", got, c.generation())
	}
}
