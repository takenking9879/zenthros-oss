// SPDX-License-Identifier: Apache-2.0

package jobcli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fixture struct {
	Env           map[string]string `json:"env"`
	Authorization string            `json:"authorization"`
	Retry         struct {
		WaitsSeconds    []float64 `json:"waits_seconds"`
		TotalSeconds    float64   `json:"total_seconds"`
		RetriedStatuses []int     `json:"retried_statuses"`
		FinalStatuses   []int     `json:"final_statuses"`
	} `json:"retry"`
	Checkpoint struct {
		Path  string         `json:"path"`
		Phase string         `json:"phase"`
		Blob  string         `json:"blob"`
		Body  map[string]any `json:"body"`
	} `json:"checkpoint"`
	Spawn struct {
		Path     string          `json:"path"`
		Args     []string        `json:"args"`
		Body     map[string]any  `json:"body"`
		SameBody map[string]any  `json:"same_body"`
		ListBody map[string]any  `json:"list_body"`
		Reply    json.RawMessage `json:"reply"`
	} `json:"spawn"`
	Report struct {
		Path       string         `json:"path"`
		ReportID   string         `json:"report_id"`
		ProducedAt string         `json:"produced_at"`
		Set        []string       `json:"set"`
		Final      bool           `json:"final"`
		Body       map[string]any `json:"body"`
	} `json:"report"`
}

func loadFixture(t *testing.T) fixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/wire.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx fixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	return fx
}

type request struct {
	Path, Auth string
	Body       []byte
}

// agentStub is the agent's report socket: it records every request and
// answers with the next reply (the last one repeats).
type agentStub struct {
	mu       sync.Mutex
	requests []request
	replies  []stubReply
	sock     string
}

type stubReply struct {
	status int
	body   string
}

// hangUp is a reply that closes the connection without an answer, as an
// agent restarting mid-request does.
const hangUp = -1

func newAgent(t *testing.T, replies ...stubReply) *agentStub {
	t.Helper()
	if len(replies) == 0 {
		replies = []stubReply{{http.StatusAccepted, `{"accepted":true}`}}
	}
	dir, err := os.MkdirTemp("", "zj")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	a := &agentStub{replies: replies, sock: filepath.Join(dir, "r.sock")}
	ln, err := net.Listen("unix", a.sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		a.mu.Lock()
		a.requests = append(a.requests, request{r.URL.Path, r.Header.Get("Authorization"), body})
		reply := a.replies[0]
		if len(a.replies) > 1 {
			a.replies = a.replies[1:]
		}
		a.mu.Unlock()
		if reply.status == hangUp {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		w.WriteHeader(reply.status)
		io.WriteString(w, reply.body)
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return a
}

func (a *agentStub) got() []request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]request(nil), a.requests...)
}

func cli(t *testing.T, env map[string]string, a *agentStub) (*CLI, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	full := map[string]string{EnvStateDir: t.TempDir()}
	for k, v := range env {
		full[k] = v
	}
	if a != nil {
		full[EnvReportEndpoint] = "unix://" + a.sock
	}
	var out, errOut bytes.Buffer
	return &CLI{
		Env:      func(k string) string { return full[k] },
		Stdout:   &out,
		Stderr:   &errOut,
		Attempts: 3,
		Backoff:  time.Millisecond,
	}, &out, &errOut
}

func decode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	return m
}

// The bytes a shell job sends are the bytes the Python SDK sends:
// sdk/python/tests/test_job_protocol.py checks the SDK against the same
// fixture. If either drifts, a job in one language breaks where the other
// works.
func TestAgentJobCLICheckpointMatchesSDKWire(t *testing.T) {
	fx := loadFixture(t)
	a := newAgent(t, stubReply{http.StatusAccepted, `{"accepted":true,"phase":"train","seq":2}`})
	c, out, errOut := cli(t, fx.Env, a)
	file := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(file, []byte(fx.Checkpoint.Blob), 0o644); err != nil {
		t.Fatal(err)
	}
	code := c.Run(context.Background(), []string{"checkpoint", "commit",
		"--phase", fx.Checkpoint.Phase, "--file", file, "--uri", fx.Checkpoint.Body["uri"].(string)})
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	reqs := a.got()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d", len(reqs))
	}
	if reqs[0].Path != fx.Checkpoint.Path || reqs[0].Auth != fx.Authorization {
		t.Fatalf("path %q auth %q", reqs[0].Path, reqs[0].Auth)
	}
	if got := decode(t, reqs[0].Body); !reflect.DeepEqual(got, fx.Checkpoint.Body) {
		t.Fatalf("commit body\n got %v\nwant %v", got, fx.Checkpoint.Body)
	}
	if !strings.Contains(out.String(), `"seq":2`) {
		t.Fatalf("stdout %q", out)
	}
	// The next seq is taken after the acked one, as the SDK's Context does.
	if code := c.Run(context.Background(), []string{"checkpoint", "next", "--phase", "train"}); code != ExitOK {
		t.Fatal(code)
	}
	if !strings.Contains(out.String(), `"seq":3,"uri":"file:///var/lib/zenthros/ckpt/jobs/0f5c-uid/gen/3/train/3"`) {
		t.Fatalf("next: %q", out)
	}
}

func TestAgentJobCLIReportMatchesSDKWire(t *testing.T) {
	fx := loadFixture(t)
	a := newAgent(t)
	c, _, errOut := cli(t, fx.Env, a)
	c.NewID = func() string { return fx.Report.ReportID }
	c.Now = func() time.Time { t, _ := time.Parse(time.RFC3339, fx.Report.ProducedAt); return t }
	args := append([]string{"report", "set"}, fx.Report.Set...)
	if fx.Report.Final {
		args = append(args, "--final")
	}
	if code := c.Run(context.Background(), args); code != ExitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	reqs := a.got()
	if len(reqs) != 1 || reqs[0].Path != fx.Report.Path || reqs[0].Auth != fx.Authorization {
		t.Fatalf("requests %+v", reqs)
	}
	if got := decode(t, reqs[0].Body); !reflect.DeepEqual(got, fx.Report.Body) {
		t.Fatalf("report body\n got %v\nwant %v", got, fx.Report.Body)
	}
}

// A report is a whole snapshot: a later set sends what the earlier ones
// set, the way the SDK's Report keeps its dictionary.
func TestReportSetSendsTheWholeSnapshot(t *testing.T) {
	a := newAgent(t)
	c, _, errOut := cli(t, map[string]string{EnvJobID: "j", EnvReportToken: "t"}, a)
	for _, args := range [][]string{
		{"report", "set", "epoch=1", "loss=0.9"},
		{"report", "set", "--artifact", "s3://b/model=model", "epoch=2"},
	} {
		if code := c.Run(context.Background(), args); code != ExitOK {
			t.Fatalf("%v: exit %d: %s", args, code, errOut)
		}
	}
	last := decode(t, a.got()[1].Body)
	if want := map[string]any{"epoch": 2.0, "loss": 0.9}; !reflect.DeepEqual(last["payload"], want) {
		t.Fatalf("payload %v", last["payload"])
	}
	if want := []any{map[string]any{"uri": "s3://b/model", "name": "model"}}; !reflect.DeepEqual(last["artifacts"], want) {
		t.Fatalf("artifacts %v", last["artifacts"])
	}
}

// The agent's refusal reaches the job word for word.
func TestRefusalIsVerbatim(t *testing.T) {
	text := `job capability token is invalid`
	a := newAgent(t, stubReply{http.StatusUnauthorized, `{"error_code":"UNAUTHORIZED","error":"` + text + `"}`})
	c, _, errOut := cli(t, map[string]string{EnvJobID: "j"}, a)
	if code := c.Run(context.Background(), []string{"report", "set", "a=1"}); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
	if got := strings.TrimSpace(errOut.String()); got != "UNAUTHORIZED: "+text {
		t.Fatalf("stderr %q", got)
	}
	if len(a.got()) != 1 {
		t.Fatalf("a 401 is not retried: %d requests", len(a.got()))
	}
}

// A replacement already took the cursor: the job is told to stop (exit 3),
// with the controller's words, and the seq is not remembered as committed.
// It goes by the code: a refusal that only reads like one is no freeze.
func TestFrozenCommitExitsThree(t *testing.T) {
	msg := `cursor frozen: replacement submitted at 2026-01-02T03:04:05Z`
	look := newAgent(t, stubReply{http.StatusConflict, `{"error_code":"REJECTED","error":"` + msg + `"}`})
	lc, _, _ := cli(t, map[string]string{EnvJobID: "j"}, look)
	lookFile := filepath.Join(t.TempDir(), "f")
	os.WriteFile(lookFile, []byte("x"), 0o644)
	if code := lc.Run(context.Background(), []string{"checkpoint", "commit", "--phase", "p", "--file", lookFile, "--uri", "s3://b/k"}); code == ExitFrozen {
		t.Fatal("a REJECTED refusal was read as a frozen cursor by its words")
	}
	a := newAgent(t, stubReply{http.StatusConflict, `{"error_code":"CHECKPOINT_FROZEN","error":"` + msg + `"}`})
	c, _, errOut := cli(t, map[string]string{EnvJobID: "j"}, a)
	file := filepath.Join(t.TempDir(), "f")
	os.WriteFile(file, []byte("x"), 0o644)
	code := c.Run(context.Background(), []string{"checkpoint", "commit", "--phase", "p", "--file", file, "--uri", "s3://b/k"})
	if code != ExitFrozen {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if !strings.Contains(errOut.String(), msg) {
		t.Fatalf("stderr %q", errOut)
	}
	st, _ := c.loadState()
	if st.Seqs["p"] != 0 {
		t.Fatalf("a refused seq was remembered: %v", st.Seqs)
	}
}

// A commit with no answer may have landed: a gateway that dies after the
// controller stored the commit takes the ack with it. Committing the next
// checkpoint under the same seq was refused for good ("conflicting commit at
// acked seq"), the phase's COMPLETE with it, and the job failed though it
// ran (scale lab, 2026-10-10). The next commit takes the seq after, and
// the bytes the acked cursor may point at stay as they were.
func TestCommitWithNoAnswerSpendsItsSeq(t *testing.T) {
	root := t.TempDir()
	down := stubReply{http.StatusServiceUnavailable, `{"error_code":"STORE_UNAVAILABLE","error":"checkpoint ack timed out"}`}
	a := newAgent(t, down, down, down, stubReply{http.StatusAccepted, `{"accepted":true}`})
	c, _, errOut := cli(t, map[string]string{
		EnvJobID: "uid", "ZENTHROS_CHECKPOINT_PREFIX": "file://" + root,
	}, a)
	commit := func(payload string) int {
		file := filepath.Join(t.TempDir(), "f")
		os.WriteFile(file, []byte(payload), 0o644)
		return c.Run(context.Background(), []string{"checkpoint", "commit", "--phase", "fit", "--file", file})
	}
	if code := commit("step 1"); code != ExitFailed {
		t.Fatalf("a commit with no answer exits %d: %s", code, errOut)
	}
	if code := commit("step 2"); code != ExitOK {
		t.Fatalf("the next commit exits %d: %s", code, errOut)
	}
	got := a.got()
	if len(got) != 4 {
		t.Fatalf("requests %d, want 3 attempts and the next commit", len(got))
	}
	for i, req := range got[:3] {
		if body := decode(t, req.Body); body["seq"] != float64(1) {
			t.Fatalf("attempt %d of the first commit: %v", i, body)
		}
	}
	base := "file://" + root + "/jobs/uid/gen/1/fit/"
	if body := decode(t, got[3].Body); body["seq"] != float64(2) || body["uri"] != base+"2" {
		t.Fatalf("the commit after one with no answer: %v, want seq 2 at %s2", body, base)
	}
	for seq, want := range map[string]string{"1": "step 1", "2": "step 2"} {
		raw, err := os.ReadFile(strings.TrimPrefix(base+seq, "file://"))
		if err != nil || string(raw) != want {
			t.Fatalf("bytes at seq %s: %q %v, want %q", seq, raw, err, want)
		}
	}
}

// 503 while the agent's forwarder is down is retried; the commit lands.
func TestTransientReplyIsRetried(t *testing.T) {
	a := newAgent(t,
		stubReply{http.StatusServiceUnavailable, `{"error_code":"STORE_UNAVAILABLE","error":"no link"}`},
		stubReply{http.StatusAccepted, `{"accepted":true}`})
	c, _, errOut := cli(t, map[string]string{EnvJobID: "j"}, a)
	if code := c.Run(context.Background(), []string{"report", "set", "a=1"}); code != ExitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if n := len(a.got()); n != 2 {
		t.Fatalf("requests %d", n)
	}
}

// Without --uri, the file lands at the URI the SDK would have written under
// a local prefix, and that is the URI committed.
func TestCommitCopiesToLocalPrefix(t *testing.T) {
	root := t.TempDir()
	a := newAgent(t)
	c, _, errOut := cli(t, map[string]string{
		EnvJobID: "uid", "ZENTHROS_GENERATION": "2", "ZENTHROS_CHECKPOINT_PREFIX": "file://" + root,
	}, a)
	file := filepath.Join(t.TempDir(), "f")
	os.WriteFile(file, []byte("weights"), 0o644)
	for i := 0; i < 2; i++ {
		if code := c.Run(context.Background(), []string{"checkpoint", "commit", "--phase", "fit", "--file", file}); code != ExitOK {
			t.Fatalf("exit %d: %s", code, errOut)
		}
	}
	for seq, req := range a.got() {
		body := decode(t, req.Body)
		want := "file://" + root + "/jobs/uid/gen/2/fit/" + []string{"1", "2"}[seq]
		if body["uri"] != want || body["seq"] != float64(seq+1) {
			t.Fatalf("commit %d: %v", seq, body)
		}
		raw, err := os.ReadFile(strings.TrimPrefix(want, "file://"))
		if err != nil || string(raw) != "weights" {
			t.Fatalf("copy at %s: %q %v", want, raw, err)
		}
	}
	// An object-store prefix is the job's own to write.
	c2, _, errOut2 := cli(t, map[string]string{EnvJobID: "uid", "ZENTHROS_CHECKPOINT_PREFIX": "s3://bucket/p"}, a)
	if code := c2.Run(context.Background(), []string{"checkpoint", "commit", "--phase", "fit", "--file", file}); code != ExitFailed {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(errOut2.String(), "pass --uri") {
		t.Fatalf("stderr %q", errOut2)
	}
}

func TestCursorReadsEnvLikeTheSDK(t *testing.T) {
	fx := loadFixture(t)
	c, out, _ := cli(t, fx.Env, nil)
	if code := c.Run(context.Background(), []string{"checkpoint", "cursor", "--phase", "train"}); code != ExitOK {
		t.Fatal(code)
	}
	if got := strings.TrimSpace(out.String()); got != `{"seq":1,"uri":"file:///var/lib/zenthros/ckpt/jobs/0f5c-uid/gen/2/train/1","complete":false}` {
		t.Fatalf("cursor %s", got)
	}
	if code := c.Run(context.Background(), []string{"checkpoint", "cursor", "--phase", "eval"}); code != ExitFailed {
		t.Fatalf("no cursor: exit %d", code)
	}
	// The single-phase env is the latest cursor when CURSORS lacks the phase.
	c2, out2, _ := cli(t, map[string]string{
		"ZENTHROS_CHECKPOINT_PHASE": "eval", "ZENTHROS_CHECKPOINT_URI": "s3://b/k", "ZENTHROS_CHECKPOINT_SEQ": "4",
		"ZENTHROS_CHECKPOINT_COMPLETE": "true",
	}, nil)
	c2.Run(context.Background(), []string{"checkpoint", "cursor", "--phase", "eval"})
	if got := strings.TrimSpace(out2.String()); got != `{"seq":4,"uri":"s3://b/k","complete":true}` {
		t.Fatalf("cursor %s", got)
	}
}

func TestPreemptedReadsTheNoticeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.json")
	c, out, _ := cli(t, map[string]string{EnvPreemptFile: path}, nil)
	if code := c.Run(context.Background(), []string{"preempted"}); code != ExitFailed {
		t.Fatalf("no notice: exit %d", code)
	}
	notice := `{"source":"aws","action":"terminate","deadline":"2026-01-02T03:06:05Z"}`
	os.WriteFile(path, []byte(notice), 0o644)
	if code := c.Run(context.Background(), []string{"preempted"}); code != ExitOK {
		t.Fatalf("notice: exit %d", code)
	}
	if strings.TrimSpace(out.String()) != notice {
		t.Fatalf("stdout %q", out)
	}
}

// `job task` prints the task's place (TASK_INDEX of TASK_COUNT) and its
// job; nothing a task could mistake for a rendezvous.
func TestTaskPrintsTaskEnv(t *testing.T) {
	c, out, _ := cli(t, map[string]string{"TASK_INDEX": "1", "TASK_COUNT": "2", "RANK": "1",
		"MASTER_ADDR": "10.0.0.4", "ZENTHROS_JOB_ID": "uid-1"}, nil)
	c.Run(context.Background(), []string{"task"})
	if out.String() != "TASK_INDEX=1\nTASK_COUNT=2\nZENTHROS_JOB_ID=uid-1\n" {
		t.Fatalf("got\n%s", out)
	}
}

func TestUsageExitsTwo(t *testing.T) {
	c, _, _ := cli(t, nil, nil)
	for _, args := range [][]string{nil, {"nope"}, {"report"}, {"report", "set"}, {"checkpoint", "commit", "--phase", "p"}} {
		if code := c.Run(context.Background(), args); code != ExitUsage {
			t.Fatalf("%v: exit %d", args, code)
		}
	}
}

// A job that cannot reach its agent (it is restarting, or the hub answers
// 502/503/504) insists for as long as the controller would wait for the
// machine itself: ten tries 2 s apart and five more 5.4 s apart, the last one
// 45 s after the first. The SDK waits the same
// (sdk/python/tests/test_agent.py reads the same table).
func TestTransientAnswerIsRetriedForAJobsGrace(t *testing.T) {
	fx := loadFixture(t)
	want := make([]time.Duration, len(fx.Retry.WaitsSeconds))
	var total time.Duration
	for i, seconds := range fx.Retry.WaitsSeconds {
		want[i] = time.Duration(seconds * float64(time.Second))
		total += want[i]
	}
	if total != time.Duration(fx.Retry.TotalSeconds*float64(time.Second)) || total != 45*time.Second {
		t.Fatalf("the table adds up to %s, want 45s", total)
	}

	a := newAgent(t, stubReply{http.StatusServiceUnavailable, `{"error_code":"STORE_UNAVAILABLE","error":"hub down"}`})
	var out, errOut bytes.Buffer
	var waited []time.Duration
	c := &CLI{
		Env:    func(k string) string { return map[string]string{EnvReportEndpoint: "unix://" + a.sock}[k] },
		Stdout: &out, Stderr: &errOut,
		sleep: func(_ context.Context, wait time.Duration) error {
			waited = append(waited, wait)
			return nil
		},
	}
	c.defaults()
	if _, err := c.post(context.Background(), "/v1/reports", []byte(`{}`)); err == nil {
		t.Fatal("an agent that never answered anything but 503 was reported as answering")
	}
	if got := len(a.got()); got != len(want)+1 {
		t.Fatalf("%d tries, want %d", got, len(want)+1)
	}
	if !reflect.DeepEqual(waited, want) {
		t.Fatalf("waited %v between tries, want %v", waited, want)
	}
}

// What is tried again is one table with the SDK (testdata/wire.json, which
// sdk/python/tests/test_agent.py reads too): a 507 SPOOL_FULL is the spool
// waiting on the controller, and a connection the agent closed with no
// answer is it restarting. Raised at once, a job's result digest was lost
// for good while the hub was down (R4 ronda 4, G1).
func TestWhatIsTriedAgainIsTheSDKsTable(t *testing.T) {
	fx := loadFixture(t)
	ok := stubReply{http.StatusAccepted, `{"accepted":true}`}
	answers := map[string]stubReply{"closed with no answer": {hangUp, ""}}
	for _, status := range fx.Retry.RetriedStatuses {
		answers[fmt.Sprintf("%d", status)] = stubReply{status, `{"error_code":"X","error":"not now"}`}
	}
	for name, first := range answers {
		a := newAgent(t, first, ok)
		c, _, errOut := cli(t, map[string]string{EnvJobID: "j"}, a)
		if code := c.Run(context.Background(), []string{"report", "set", "a=1"}); code != ExitOK || len(a.got()) != 2 {
			t.Errorf("%s: exit %d after %d tries, want it tried again: %s", name, code, len(a.got()), errOut)
		}
	}
	for _, status := range fx.Retry.FinalStatuses {
		a := newAgent(t, stubReply{status, `{"error_code":"X","error":"no"}`})
		c, _, _ := cli(t, map[string]string{EnvJobID: "j"}, a)
		if code := c.Run(context.Background(), []string{"report", "set", "a=1"}); code == ExitOK || len(a.got()) != 1 {
			t.Errorf("%d: exit %d after %d tries, want it raised at once", status, code, len(a.got()))
		}
	}
}
