// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/reportspool"
)

// localRecorder keeps what it is handed: a local Submit and a gateway's
// must hand the executor the same thing.
type localRecorder struct {
	mu     sync.Mutex
	specs  []jobspec.JobSpec
	envs   []map[string]string
	state  backend.JobState
	logs   string
	cancel []string
}

func (e *localRecorder) Submit(_ context.Context, spec jobspec.JobSpec, env map[string]string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.specs, e.envs = append(e.specs, spec), append(e.envs, env)
	return spec.ID, nil
}

func (e *localRecorder) Cancel(_ context.Context, handle string, _ owner.Ref) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancel = append(e.cancel, handle)
	return nil
}

func (e *localRecorder) cancelledCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.cancel)
}

func (e *localRecorder) Status(context.Context, string) (backend.JobState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state, nil
}

func (e *localRecorder) Logs(context.Context, string, bool, int) (<-chan backend.LogChunk, error) {
	out := make(chan backend.LogChunk, 1)
	out <- backend.LogChunk{Data: []byte(e.logs)}
	close(out)
	return out, nil
}

func checkpointCommitFor(jobID, phase string, seq int64, sha string) *agentpb.CheckpointCommit {
	return &agentpb.CheckpointCommit{JobId: jobID, Phase: phase, Seq: seq, Sha256: sha, Uri: "file:///x/" + phase}
}

func localJob(id string) *v1.ZenthrosJob {
	job := &v1.ZenthrosJob{}
	job.Name, job.UID = "hola", "00000000-0000-0000-0000-00000000000a"
	if id != "" {
		job.UID = ""
		job.Annotations = map[string]string{v1.AnnotationJobID: id}
	}
	job.Spec.Workload.Kind = v1.WorkloadKindJob
	job.Spec.Workload.Template.Command = []string{"true"}
	job.Spec.Workload.Template.Artifacts.Output = "file:///tmp/store/hola"
	return job
}

// startLocal runs a local agent over a socket; the returned function runs a
// verb with what the SDK would pass.
func startLocal(t *testing.T, executor Executor, backendType string) func(verb string, stdin any, args ...string) (string, string, int) {
	t.Helper()
	dir, err := os.MkdirTemp("", "zl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	cfg := Config{BackendID: LocalWorkspace, BackendType: backendType, ReportSpoolPath: filepath.Join(dir, "reports.sqlite"), Version: "test"}
	a := NewWithExecutor(cfg, executor, logr.Discard())
	socket := filepath.Join(dir, "local", "agent.sock")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.RunLocal(ctx, LocalOptions{Socket: socket, StateDir: filepath.Join(dir, "local")}) }()
	t.Cleanup(func() {
		cancel()
		<-done
		if a.spool != nil {
			_ = a.spool.Close()
		}
	})
	return func(verb string, stdin any, args ...string) (string, string, int) {
		var in bytes.Buffer
		if stdin != nil {
			if err := json.NewEncoder(&in).Encode(stdin); err != nil {
				t.Fatal(err)
			}
		}
		var out, errOut bytes.Buffer
		runCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		// The first command may come before the socket: the SDK waits with
		// info --wait.
		if verb != "info" {
			if code := RunLocalCLI(runCtx, []string{"info", "--wait", "5s"}, socket, &in, &bytes.Buffer{}, &bytes.Buffer{}); code != 0 {
				t.Fatalf("the agent never came up (%d)", code)
			}
		}
		code := RunLocalCLI(runCtx, append([]string{verb}, args...), socket, &in, &out, &errOut)
		return out.String(), errOut.String(), code
	}
}

// The same handlers: what a local submit hands the executor is what the
// gateway's Submit would (FromZenthrosJob, WithDataReferences, the agent's env).
func TestLocalSubmitGoesThroughTheSameHandler(t *testing.T) {
	executor := &localRecorder{}
	run := startLocal(t, executor, backend.TypeContainer)
	out, stderr, code := run("submit", localJob(""))
	if code != 0 {
		t.Fatalf("submit: %d %s", code, stderr)
	}
	var job LocalJob
	if err := json.Unmarshal([]byte(out), &job); err != nil {
		t.Fatal(err)
	}
	if len(job.Tasks) != 1 || job.Tasks[0].ID != "00000000-0000-0000-0000-00000000000a" {
		t.Fatalf("tasks = %+v", job.Tasks)
	}
	if len(executor.specs) != 1 {
		t.Fatalf("the executor got %d jobs", len(executor.specs))
	}
	spec, env := executor.specs[0], executor.envs[0]
	if spec.Runtime != v1.RuntimeProcess || spec.Workspace != LocalWorkspace {
		t.Fatalf("spec = %+v", spec)
	}
	if env[backend.DataOutputEnv] != "file:///tmp/store/hola" {
		t.Fatalf("no data reference: %v", env)
	}
	if env[jobspec.EnvReportToken] == "" || env[jobspec.EnvJobID] == "" {
		t.Fatalf("the agent's own keys are missing: %v", env)
	}
}

func TestLocalSubmitRefusesAFieldItDoesNotKnow(t *testing.T) {
	run := startLocal(t, &localRecorder{}, backend.TypeContainer)
	_, stderr, code := run("submit", map[string]any{"metadata": map[string]any{"uid": "x"}, "nonsense": true})
	if code == 0 || !strings.Contains(stderr, "nonsense") {
		t.Fatalf("code %d, stderr %q", code, stderr)
	}
}

// A map on a container is one process per rank, named and told their rank as
// the controller's tasks are.
func TestLocalMapSplitsIntoRanksOnAContainer(t *testing.T) {
	job := localJob("")
	job.Spec.Workload.Template.Distribution = &v1.Distribution{Count: 3, Mode: "Independent"}
	specs, err := LocalSpecs(job, backend.TypeContainer)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 3 {
		t.Fatalf("%d specs", len(specs))
	}
	for rank, spec := range specs {
		if spec.Distribution != nil || spec.Env[v1.EnvTaskCount] != "3" || spec.Env[v1.EnvTaskIndex] != []string{"0", "1", "2"}[rank] {
			t.Fatalf("rank %d: %+v", rank, spec)
		}
		if !strings.HasSuffix(spec.ID, []string{"-0", "-1", "-2"}[rank]) {
			t.Fatalf("rank %d id %s", rank, spec.ID)
		}
	}
	// A cluster runs a map natively: one Indexed Job.
	specs, err = LocalSpecs(job, backend.TypeK8s)
	if err != nil || len(specs) != 1 || specs[0].Distribution == nil {
		t.Fatalf("on a cluster: %+v, %v", specs, err)
	}
}

func TestLocalStatusAddsTasksUpAsTheController(t *testing.T) {
	failed := int32(3)
	executor := &localRecorder{state: backend.JobState{Phase: v1.PhaseFailed, Message: "boom", ExitCode: &failed,
		Reasons:   []v1.Reason{{Code: v1.ReasonProcessFailed, Detail: "boom"}},
		StartTime: &metav1.Time{Time: time.Unix(100, 0)}, CompletionTime: &metav1.Time{Time: time.Unix(200, 0)}}}
	run := startLocal(t, executor, backend.TypeContainer)
	one := LocalJob{ID: "j", Tasks: []LocalTask{{Rank: -1, ID: "j", Handle: "j"}}}
	out, stderr, code := run("status", one)
	if code != 0 {
		t.Fatalf("status: %s", stderr)
	}
	var status LocalStatus
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatal(err)
	}
	if status.Phase != v1.PhaseFailed || status.Reason != v1.ReasonProcessFailed || status.ExitCode == nil || *status.ExitCode != 3 {
		t.Fatalf("status = %+v", status)
	}
	if status.StartTime == nil || status.StartTime.Unix() != 100 || status.CompletionTime.Unix() != 200 {
		t.Fatalf("times = %+v", status)
	}
	many := LocalJob{ID: "j", Count: 2, Tasks: []LocalTask{{Rank: 0, ID: "j-0", Handle: "j-0"}, {Rank: 1, ID: "j-1", Handle: "j-1"}}}
	out, stderr, code = run("status", many)
	if code != 0 {
		t.Fatalf("status: %s", stderr)
	}
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatal(err)
	}
	if status.Phase != v1.PhaseFailed || !strings.Contains(status.Message, "member rank 0") {
		t.Fatalf("status = %+v", status)
	}
}

// The socket is its owner's alone, and no moment of it is another's.
func TestLocalSocketIsOnlyItsOwners(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "local", "agent.sock")
	listener, err := listenLocal(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode %o", info.Mode().Perm())
	}
	parent, err := os.Stat(filepath.Dir(socket))
	if err != nil {
		t.Fatal(err)
	}
	if parent.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode %o", parent.Mode().Perm())
	}
}

// A commit is decided by the controller's rule: only forward, never another
// file at an acked seq; the cursor survives the agent.
func TestLocalCheckpointsFollowTheCursorRule(t *testing.T) {
	progress := &localProgress{dir: t.TempDir()}
	ctx := context.Background()
	commit := func(seq int64, sha string) (bool, string) {
		ack, err := progress.Commit(ctx, checkpointCommitFor("00000000-0000-0000-0000-00000000000a", "train", seq, sha))
		if err != nil {
			t.Fatal(err)
		}
		return ack.GetAccepted(), ack.GetError()
	}
	if ok, why := commit(2, "aa"); !ok {
		t.Fatalf("refused: %s", why)
	}
	if ok, _ := commit(1, "aa"); ok {
		t.Fatal("a commit behind the cursor was taken")
	}
	if ok, _ := commit(2, "bb"); ok {
		t.Fatal("another file at an acked seq was taken")
	}
	if ok, _ := commit(2, "aa"); !ok {
		t.Fatal("the same commit again was refused")
	}
	reread := &localProgress{dir: progress.dir}
	cursor, err := reread.read("00000000-0000-0000-0000-00000000000a")
	if err != nil || cursor == nil || cursor.Seq != 2 {
		t.Fatalf("cursor %+v, %v", cursor, err)
	}
	if _, err := reread.read("../outside"); err == nil {
		t.Fatal("a job id out of the directory was read")
	}
}

func TestLocalReportsComeFromTheSpool(t *testing.T) {
	dir := t.TempDir()
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(dir, "r.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	got, err := spool.Reports(context.Background(), "nobody")
	if err != nil || len(got) != 0 {
		t.Fatalf("reports of nobody: %v %v", got, err)
	}
}

func TestLocalIdleExitWaitsForWork(t *testing.T) {
	var busy struct {
		sync.Mutex
		is bool
	}
	busy.is = true
	stopped := make(chan struct{})
	opts := LocalOptions{IdleExit: 40 * time.Millisecond, Busy: func() bool {
		busy.Lock()
		defer busy.Unlock()
		return busy.is
	}}
	idle := newLocalIdle(opts, func() { close(stopped) })
	idle.tick = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go idle.watch(ctx)
	select {
	case <-stopped:
		t.Fatal("stopped with a job running")
	case <-time.After(120 * time.Millisecond):
	}
	busy.Lock()
	busy.is = false
	busy.Unlock()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal(errors.New("did not stop once idle"))
	}
}

// A Service here has no controller to say it is ready, and the SDK waits for
// that condition: the agent states it from what the executor found.
func TestLocalStatusSaysAServiceIsReady(t *testing.T) {
	ready := localStatusOne(&agentpb.JobStatus{Phase: string(v1.PhaseRunning),
		Endpoints: []*agentpb.Endpoint{{Name: "http", Port: 8000, Url: "http://127.0.0.1:8000"}}})
	if len(ready.Conditions) != 1 || ready.Conditions[0].Type != v1.ConditionServiceReady || ready.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("conditions %+v, want ServiceReady True", ready.Conditions)
	}
	waiting := localStatusOne(&agentpb.JobStatus{Phase: string(v1.PhaseRunning), EndpointProblem: "the port does not answer yet"})
	if len(waiting.Conditions) != 1 || waiting.Conditions[0].Status != metav1.ConditionFalse || waiting.Conditions[0].Message != "the port does not answer yet" {
		t.Fatalf("conditions %+v, want ServiceReady False with why", waiting.Conditions)
	}
	if plain := localStatusOne(&agentpb.JobStatus{Phase: string(v1.PhaseRunning)}); len(plain.Conditions) != 0 {
		t.Fatalf("a job that serves nothing has conditions %+v", plain.Conditions)
	}
}

// A time limit is kept with nobody asking: the process that sent the job may
// be gone by then, and no controller watches it here.
func TestLocalJobIsStoppedAtItsTimeLimit(t *testing.T) {
	recorder := &localRecorder{}
	agent := startLocal(t, recorder, "container")
	job := localJob("")
	seconds := int64(1)
	job.Spec.ActiveDeadlineSeconds = &seconds
	if _, stderr, code := agent("submit", job); code != 0 {
		t.Fatalf("submit: %s", stderr)
	}
	deadline := time.Now().Add(10 * time.Second)
	for recorder.cancelledCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the job was not cancelled at its time limit")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// The SDK writes a mark in the folder it means to share and asks the agent
// what it reads there: the same words is the only proof the cluster has it.
func TestLocalInfoSaysTheMarkOfTheSharedFolder(t *testing.T) {
	shared := t.TempDir()
	if err := os.WriteFile(filepath.Join(shared, LocalSharedMark), []byte("abc123\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := readSharedMark(shared); got != "abc123" {
		t.Errorf("mark = %q, want abc123", got)
	}
	if got := readSharedMark(t.TempDir()); got != "" {
		t.Errorf("mark of an empty folder = %q, want none", got)
	}
	if got := readSharedMark(""); got != "" {
		t.Errorf("mark with no folder = %q, want none", got)
	}
}
