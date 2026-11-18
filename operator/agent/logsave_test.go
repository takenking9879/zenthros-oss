// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/klauspost/compress/zstd"

	"github.com/zenthros/operator/agent/logpersist"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/checkpoint"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/objectput"
	"github.com/zenthros/operator/pkg/owner"
)

// loggingExecutor runs one job whose log the test writes, read as a file is
// (a cursor after each chunk), Running until it ends. It resolves the log's
// credentials to fixed keys, and keeps the env the job was given.
type loggingExecutor struct {
	mu    sync.Mutex
	log   []byte
	ended bool
	env   map[string]string
}

func (e *loggingExecutor) Submit(_ context.Context, _ jobspec.JobSpec, env map[string]string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.env = env
	return "handle", nil
}

func (e *loggingExecutor) Cancel(context.Context, string, owner.Ref) error { return nil }

func (e *loggingExecutor) Status(context.Context, string) (backend.JobState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ended {
		return backend.JobState{Phase: v1.PhaseSucceeded}, nil
	}
	return backend.JobState{Phase: v1.PhaseRunning}, nil
}

func (e *loggingExecutor) Logs(ctx context.Context, _ string, req backend.LogRequest) (<-chan backend.LogChunk, error) {
	pos, _ := strconv.Atoi(req.After)
	out := make(chan backend.LogChunk)
	go func() {
		defer close(out)
		for {
			e.mu.Lock()
			data, ended := e.log, e.ended
			e.mu.Unlock()
			if len(data) > pos {
				select {
				case out <- backend.LogChunk{Data: append([]byte(nil), data[pos:]...), Cursor: strconv.Itoa(len(data))}:
				case <-ctx.Done():
					return
				}
				pos = len(data)
				continue
			}
			if !req.Follow || ended {
				return
			}
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (e *loggingExecutor) LogCredentials(_ context.Context, spec jobspec.JobSpec) (map[string]string, error) {
	out := map[string]string{}
	for _, ref := range spec.SecretSources {
		out[ref.Materialization.EnvName] = map[string]string{objectput.EnvAccessKeyID: "AKIDLOGS", objectput.EnvSecretAccessKey: "secret"}[ref.Materialization.EnvName]
	}
	return out, nil
}

func (e *loggingExecutor) write(b []byte) {
	e.mu.Lock()
	e.log = append(e.log, b...)
	e.mu.Unlock()
}

func (e *loggingExecutor) end() {
	e.mu.Lock()
	e.ended = true
	e.mu.Unlock()
}

// logBucket is an S3 server that keeps what it is sent, or refuses it.
type logBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	refuse  int
	auth    []string
}

func (b *logBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.auth = append(b.auth, r.Header.Get("Authorization"))
	if b.refuse != 0 {
		http.Error(w, "AccessDenied", b.refuse)
		return
	}
	if r.Method == http.MethodPut {
		b.objects[r.URL.Path] = body
	}
}

func (b *logBucket) object(path string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, ok := b.objects[path]
	return o, ok
}

// logJob is a job whose log goes to bucket, sent to a through the wire.
func submitLoggedJob(t *testing.T, a *Agent, endpoint string, ca []byte) {
	t.Helper()
	cred := func(name string) v1.SecretSourceRef {
		return v1.SecretSourceRef{Type: v1.SecretSourceTypeWorkspace, Name: strings.ToLower(strings.ReplaceAll(name, "_", "-")),
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: name}}
	}
	spec := jobspec.JobSpec{ID: "uid-logs", Name: "train", Namespace: "tenant-a", Kind: v1.WorkloadKindJob, Entrypoint: "python train.py",
		OwnerUID: "uid-9", Attempt: 1, Env: map[string]string{checkpoint.EnvGeneration: "4"},
		Logs: &v1.LogsSpec{Destination: "s3://b/runs", Endpoint: endpoint, CAData: string(ca),
			Credentials: []v1.SecretSourceRef{cred(objectput.EnvAccessKeyID), cred(objectput.EnvSecretAccessKey)}}}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatal(err)
	}
	if resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r", Job: job}); resp.GetError() != "" {
		t.Fatalf("Submit: %s", resp.GetError())
	}
}

func loggingAgent(t *testing.T, exec *loggingExecutor) (*Agent, *logBucket, string, []byte) {
	t.Helper()
	bucket := &logBucket{objects: map[string][]byte{}}
	server := httptest.NewTLSServer(bucket)
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	a := NewWithExecutor(Config{BackendID: "vm-1", BackendType: "vm", LogQueueDir: t.TempDir()}, exec, logr.Discard())
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		a.logs.Drain(ctx)
	})
	return a, bucket, server.URL, ca
}

// A job with logs has its whole log saved by its agent from its Submit, under
// <job>/<uid>/g<generation>-a<attempt>/<handle>/, with credentials the agent
// resolved for itself: the job's environment never holds them.
func TestAJobsLogIsSavedByItsAgentNotByTheJob(t *testing.T) {
	exec := &loggingExecutor{}
	a, bucket, endpoint, ca := loggingAgent(t, exec)
	submitLoggedJob(t, a, endpoint, ca)
	for name := range exec.env {
		if strings.HasPrefix(name, "AWS_") {
			t.Fatalf("the job's environment holds %s: the log's credentials are the agent's", name)
		}
	}
	text := strings.Repeat("epoch 1 loss=0.25\n", 5000)
	exec.write([]byte(text))
	exec.end()
	prefix := "/b/runs/train/uid-9/g4-a1/handle/"
	testutil.Eventually(t, "END.json in the bucket", func() bool {
		_, ok := bucket.object(prefix + "END.json")
		return ok
	})
	object, ok := bucket.object(prefix + logpersist.ObjectName(0))
	if !ok {
		t.Fatalf("no first object; the bucket holds %v", bucket.objects)
	}
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	if raw, err := dec.DecodeAll(object, nil); err != nil || string(raw) != text {
		t.Fatalf("the object unpacks to %d bytes (%v), want the log's %d", len(raw), err, len(text))
	}
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if !strings.Contains(bucket.auth[0], "Credential=AKIDLOGS/") {
		t.Fatalf("signed with %q, not the log's credentials", bucket.auth[0])
	}
}

// A bucket that refuses the log is the job's reason while it runs, LOGS_NOT_SAVED:
// pushed although the phase stays Running, and the job runs on.
func TestALogTheBucketRefusesIsAReasonOfTheRunningJob(t *testing.T) {
	exec := &loggingExecutor{}
	a, bucket, endpoint, ca := loggingAgent(t, exec)
	bucket.refuse = http.StatusForbidden
	a.logs.Hurry() // the first object sealed at once
	submitLoggedJob(t, a, endpoint, ca)
	before := statusKey(a.decorate(context.Background(), "handle", backend.JobState{Phase: v1.PhaseRunning}))
	exec.write([]byte("a line\n"))
	var status *agentpb.JobStatus
	testutil.Eventually(t, "LOGS_NOT_SAVED in the job's status", func() bool {
		status = a.Status(context.Background(), &agentpb.StatusRequest{Id: "uid-logs", HandleName: "handle"}).GetStatus()
		for _, r := range status.GetReasons() {
			if r.GetCode() == v1.ReasonLogsNotSaved {
				return true
			}
		}
		return false
	})
	if status.GetPhase() != string(v1.PhaseRunning) {
		t.Fatalf("phase %s: a log not saved never ends the job", status.GetPhase())
	}
	after := statusKey(a.decorate(context.Background(), "handle", backend.JobState{Phase: v1.PhaseRunning}))
	if before == after {
		t.Fatal("the reason does not change what the watcher compares: it would never be pushed")
	}
}
