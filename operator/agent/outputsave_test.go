// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/takenking9879/zenthros-oss/operator/api/v1"
	"github.com/takenking9879/zenthros-oss/operator/internal/grpc/envelope"
	agentpb "github.com/takenking9879/zenthros-oss/operator/internal/grpc/gen"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
	"github.com/takenking9879/zenthros-oss/operator/pkg/fault"
	"github.com/takenking9879/zenthros-oss/operator/pkg/jobspec"
)

// outputsExecutor runs its jobs on the machine's disk, each under root, and
// keeps the env of the last Submit.
type outputsExecutor struct {
	*mockExecutor
	root string
	mu   sync.Mutex
	env  map[string]string
}

func (e *outputsExecutor) Submit(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (string, error) {
	e.mu.Lock()
	e.env = env
	e.mu.Unlock()
	return e.mockExecutor.Submit(ctx, spec, env)
}

func (e *outputsExecutor) OutputDir(spec jobspec.JobSpec) string {
	scoped, _ := jobspec.AttemptScoped(spec, nil)
	return filepath.Join(e.root, scoped.ID, "output")
}

// fakeFiles is a bucket that takes files once gate opens, or refuses them
// with err.
type fakeFiles struct {
	mu   sync.Mutex
	got  map[string]string
	gate chan struct{}
	err  error
	puts int
}

func (f *fakeFiles) Key(parts ...string) string { return "runs/r1/" + strings.Join(parts, "/") }

func (f *fakeFiles) PutFile(_ context.Context, key string, file *os.File, size int64) error {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.err != nil {
		return f.err
	}
	raw, err := io.ReadAll(io.NewSectionReader(file, 0, size))
	if err != nil {
		return err
	}
	if f.got == nil {
		f.got = map[string]string{}
	}
	f.got[key] = string(raw)
	return nil
}

func outputsAgent(t *testing.T, exec *outputsExecutor, state string, files *fakeFiles) *Agent {
	t.Helper()
	a := NewWithExecutor(Config{BackendID: "vm-a", BackendType: "vm", MaxJobs: 2, OutputStateDir: state}, exec, logr.Discard())
	a.outputs.open = func(context.Context, outputPlan) (fileStore, error) { return files, nil }
	return a
}

func submitWithOutput(t *testing.T, a *Agent, output string) string {
	t.Helper()
	spec := jobspec.JobSpec{ID: "job-1", Name: "j", Namespace: "tenant-a", Kind: v1.WorkloadKindJob, Entrypoint: "true",
		Env: map[string]string{backend.DataOutputEnv: output, backend.DataOutputURIEnv: output}}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatal(err)
	}
	resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r", Job: job})
	if resp.GetError() != "" {
		t.Fatal(resp.GetError())
	}
	return resp.GetHandleName()
}

func statusOf(a *Agent, handle string) *agentpb.JobStatus {
	return a.Status(context.Background(), &agentpb.StatusRequest{Id: "job-1", HandleName: handle}).GetStatus()
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("never: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A job on a machine writes its outputs to a folder of its own, whatever
// writes them (open, savefig, a shell's >), and its end is reported only
// once the folder is in its bucket: the next step reads them there. A
// restarted agent does not send them again (19).
func TestAJobsEndWaitsForItsOutputsInTheBucket(t *testing.T) {
	exec := &outputsExecutor{mockExecutor: newMockExecutor(), root: t.TempDir()}
	state := t.TempDir()
	files := &fakeFiles{gate: make(chan struct{})}
	a := outputsAgent(t, exec, state, files)
	handle := submitWithOutput(t, a, "s3://b/runs/r1")

	folder := filepath.Join(exec.root, "job-1", "output")
	if exec.env[backend.DataOutputEnv] != folder || exec.env[backend.DataOutputURIEnv] != "s3://b/runs/r1" {
		t.Fatalf("the job's env: %s=%q %s=%q", backend.DataOutputEnv, exec.env[backend.DataOutputEnv],
			backend.DataOutputURIEnv, exec.env[backend.DataOutputURIEnv])
	}
	if err := os.MkdirAll(filepath.Join(folder, "plots"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"sim.json": `{"ok":true}`, "plots/explore.png": "PNG"} {
		if err := os.WriteFile(filepath.Join(folder, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	exec.setPhase(handle, v1.PhaseSucceeded)
	if got := statusOf(a, handle); got.GetPhase() != string(v1.PhaseRunning) || !strings.Contains(got.GetMessage(), "saving its outputs to s3://b/runs/r1") {
		t.Fatalf("an ended job whose outputs are being saved: %v", got)
	}
	close(files.gate)
	eventually(t, "the end once the outputs are saved", func() bool { return statusOf(a, handle).GetPhase() == string(v1.PhaseSucceeded) })
	if got := statusOf(a, handle); len(got.GetReasons()) != 0 {
		t.Fatalf("reasons %v", got.GetReasons())
	}
	want := map[string]string{"runs/r1/sim.json": `{"ok":true}`, "runs/r1/plots/explore.png": "PNG"}
	if len(files.got) != len(want) || files.got["runs/r1/sim.json"] != want["runs/r1/sim.json"] || files.got["runs/r1/plots/explore.png"] != "PNG" {
		t.Fatalf("the bucket holds %v, want %v", files.got, want)
	}

	again := &fakeFiles{err: errors.New("sent twice")}
	restarted := outputsAgent(t, exec, state, again)
	if got := statusOf(restarted, handle); got.GetPhase() != string(v1.PhaseSucceeded) || again.puts != 0 {
		t.Fatalf("after a restart: %v, %d files sent again", got, again.puts)
	}
	// Its Cancel forgets the plan.
	if resp := restarted.Cancel(context.Background(), &agentpb.CancelRequest{Id: "job-1", HandleName: handle}); resp.GetError() != "" {
		t.Fatal(resp.GetError())
	}
	if _, err := os.Stat(restarted.outputs.path(handle)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the plan stayed after the Cancel: %v", err)
	}
}

// The outputs go where the job's own process writes its bucket, with what
// it writes with: the endpoint, region and keys its env gives as the AWS
// SDKs read them. The lab's demo gave AWS_ENDPOINT_URL_S3 and its keys as
// values; the agent sent them to Amazon S3 with the machine's identity (19).
func TestTheOutputsGoWhereTheJobsOwnProcessWrites(t *testing.T) {
	exec := &outputsExecutor{mockExecutor: newMockExecutor(), root: t.TempDir()}
	var opened outputPlan
	a := outputsAgent(t, exec, t.TempDir(), &fakeFiles{})
	a.outputs.open = func(_ context.Context, plan outputPlan) (fileStore, error) {
		opened = plan
		return &fakeFiles{}, nil
	}
	spec := jobspec.JobSpec{ID: "job-1", Name: "j", Namespace: "tenant-a", Kind: v1.WorkloadKindJob, Entrypoint: "true",
		Env: map[string]string{backend.DataOutputEnv: "s3://b/runs/r1", backend.DataOutputURIEnv: "s3://b/runs/r1",
			"AWS_ENDPOINT_URL_S3": "http://10.0.0.5:4566", "AWS_DEFAULT_REGION": "us-east-1",
			"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "secret"}}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatal(err)
	}
	resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r", Job: job})
	if resp.GetError() != "" {
		t.Fatal(resp.GetError())
	}
	folder := exec.env[backend.DataOutputEnv]
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "a.png"), []byte("PNG"), 0o644); err != nil {
		t.Fatal(err)
	}
	exec.setPhase(resp.GetHandleName(), v1.PhaseSucceeded)
	statusOf(a, resp.GetHandleName())
	eventually(t, "the end", func() bool { return statusOf(a, resp.GetHandleName()).GetPhase() == string(v1.PhaseSucceeded) })
	if opened.Endpoint != "http://10.0.0.5:4566" || opened.Region != "us-east-1" ||
		opened.Keys["AWS_ACCESS_KEY_ID"] != "test" || opened.Keys["AWS_SECRET_ACCESS_KEY"] != "secret" {
		t.Fatalf("the bucket opened at %q in %q with %d keys", opened.Endpoint, opened.Region, len(opened.Keys))
	}
}

// A bucket that refuses the outputs is the job's reason, never its failure:
// the job Succeeded, and the reason says which file and why (19).
func TestOutputsTheBucketRefusesAreTheJobsReason(t *testing.T) {
	exec := &outputsExecutor{mockExecutor: newMockExecutor(), root: t.TempDir()}
	files := &fakeFiles{err: fault.Wrap(fault.Unauthorized, "put", errors.New("AccessDenied"))}
	a := outputsAgent(t, exec, t.TempDir(), files)
	handle := submitWithOutput(t, a, "gs://b/runs/r1")
	folder := exec.env[backend.DataOutputEnv]
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "model.pt"), []byte("w"), 0o644); err != nil {
		t.Fatal(err)
	}
	exec.setPhase(handle, v1.PhaseSucceeded)
	statusOf(a, handle)
	eventually(t, "the end", func() bool { return statusOf(a, handle).GetPhase() == string(v1.PhaseSucceeded) })
	reasons := statusOf(a, handle).GetReasons()
	if len(reasons) != 1 || reasons[0].GetCode() != v1.ReasonOutputsNotSaved ||
		!strings.Contains(reasons[0].GetDetail(), "model.pt") || !strings.Contains(reasons[0].GetDetail(), "AccessDenied") {
		t.Fatalf("reasons %v", reasons)
	}
	if files.puts != 1 {
		t.Fatalf("a refusal was asked %d times", files.puts)
	}
}

// A job whose output is a folder already (the person's own computer) or
// names none keeps its env, and its end goes at once.
func TestAnOutputThatIsNoBucketIsLeftAsItIs(t *testing.T) {
	for _, output := range []string{"/home/me/.zenthros/local/out", ""} {
		exec := &outputsExecutor{mockExecutor: newMockExecutor(), root: t.TempDir()}
		a := outputsAgent(t, exec, t.TempDir(), &fakeFiles{err: errors.New("nothing is sent")})
		handle := submitWithOutput(t, a, output)
		if exec.env[backend.DataOutputEnv] != output {
			t.Fatalf("%q became %q", output, exec.env[backend.DataOutputEnv])
		}
		exec.setPhase(handle, v1.PhaseSucceeded)
		if got := statusOf(a, handle); got.GetPhase() != string(v1.PhaseSucceeded) {
			t.Fatalf("%q: %v", output, got)
		}
	}
}

// The agent reads the job's folder as root, and the job chose every name in
// it: a link it made (to a file or a folder elsewhere), a FIFO or a socket
// is left out and named, never followed or read; its files and folders go
// with their paths (19).
func TestTheOutputsWalkNeverFollowsWhatTheJobLinked(t *testing.T) {
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("root's"), 0o600); err != nil {
		t.Fatal(err)
	}
	folder := t.TempDir()
	for name, body := range map[string]string{"a.txt": "a", "sub/b.txt": "b", "sub/deep/c.txt": "c"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(folder, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(folder, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{"link": filepath.Join(outside, "secret"), "sub/dir": outside} {
		if err := os.Symlink(target, filepath.Join(folder, link)); err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.Mkfifo(filepath.Join(folder, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	left, err := walkOutputs(folder, func(rel string, file *os.File, size int64) error {
		raw, err := io.ReadAll(io.NewSectionReader(file, 0, size))
		got[rel] = string(raw)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got["a.txt"] != "a" || got["sub/b.txt"] != "b" || got["sub/deep/c.txt"] != "c" {
		t.Fatalf("sent %v", got)
	}
	slices.Sort(left)
	if !slices.Equal(left, []string{"link", "pipe", "sub/dir"}) {
		t.Fatalf("left out %v", left)
	}
	// No folder (a job that wrote nothing) is nothing to save.
	if left, err := walkOutputs(filepath.Join(folder, "none"), nil); err != nil || left != nil {
		t.Fatalf("no folder: %v, %v", left, err)
	}
}
