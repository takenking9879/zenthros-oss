// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	"github.com/zenthros/operator/internal/grpc/errorclass"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/backend/k8serr"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/systemd"
)

// fakeRunner records commands for the systemd manager.
type fakeRunner struct {
	mu       sync.Mutex
	commands []string
	show     string
	// files answers `cat` of a path containing the key; unlisted reads get show.
	files map[string]string
	// fail, when set, is the error of a command (nil runs it as usual).
	fail func(cmd string) error
}

func (f *fakeRunner) Run(_ context.Context, cmd string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, cmd)
	if f.fail != nil {
		if err := f.fail(cmd); err != nil {
			return "", err
		}
	}
	if strings.Contains(cmd, "cat ") {
		for path, content := range f.files {
			if strings.Contains(cmd, path) {
				return content, nil
			}
		}
	}
	return f.show, nil
}

func testAgent(show string) (*Agent, *fakeRunner) {
	runner := &fakeRunner{show: show}
	manager := systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem}, runner, logr.Discard())
	return New(Config{BackendID: "vm-test", BackendType: "vm"}, manager, logr.Discard()), runner
}

func TestSubmitInstallsUnitAndReturnsHandle(t *testing.T) {
	a, runner := testAgent("")
	spec := jobspec.JobSpec{ID: "uid-1", Name: "hello", Namespace: "tenant-default", Kind: v1.WorkloadKindJob, Entrypoint: "echo hi"}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r1", Job: job})
	if resp.GetError() != "" {
		t.Fatalf("Submit error: %s", resp.GetError())
	}
	if resp.GetRequestId() != "r1" {
		t.Errorf("requestId = %q, want r1", resp.GetRequestId())
	}
	if resp.GetHandleName() != "zenthros-uid-1.service" {
		t.Errorf("handleName = %q", resp.GetHandleName())
	}
	if len(runner.commands) == 0 {
		t.Error("expected the manager to run systemctl commands")
	}
}

func TestSubmitRejectsUnsupportedKind(t *testing.T) {
	a, _ := testAgent("")
	spec := jobspec.JobSpec{ID: "uid-2", Kind: "SparkJob", Entrypoint: "echo hi"}
	job, _ := envelope.Encode(spec)

	resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r2", Job: job})
	if resp.GetError() == "" {
		t.Error("expected an error for unsupported kind")
	}
}

func TestSubmitStampsSpecMismatchClass(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, mismatchExecutor{}, logr.Discard())
	spec := jobspec.JobSpec{ID: "uid-mm", Name: "hello", Namespace: "tenant-default", Kind: v1.WorkloadKindJob, Entrypoint: "echo hi", Image: "ray:latest"}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r-mm", Job: job})
	if resp.GetError() == "" {
		t.Fatal("expected spec mismatch")
	}
	if resp.GetErrorClass() != agentpb.ErrorClass_SPEC_MISMATCH {
		t.Fatalf("error_class = %v, want SPEC_MISMATCH", resp.GetErrorClass())
	}
}

type mismatchExecutor struct{}

func (mismatchExecutor) Submit(context.Context, jobspec.JobSpec, map[string]string) (string, error) {
	return "", backend.NewSpecMismatchError("entrypoint", "echo hi", "echo old")
}
func (mismatchExecutor) Cancel(context.Context, string, owner.Ref) error { return nil }
func (mismatchExecutor) Status(context.Context, string) (backend.JobState, error) {
	return backend.JobState{}, nil
}
func (mismatchExecutor) Logs(context.Context, string, bool, int) (<-chan backend.LogChunk, error) {
	ch := make(chan backend.LogChunk)
	close(ch)
	return ch, nil
}

func TestCancelRemovesUnit(t *testing.T) {
	a, runner := testAgent("")
	resp := a.Cancel(context.Background(), &agentpb.CancelRequest{RequestId: "r3", Id: "uid-1", HandleName: "zenthros-uid-1.service"})
	if resp.GetError() != "" || !resp.GetCancelled() {
		t.Fatalf("Cancel = %+v", resp)
	}
	found := false
	for _, c := range runner.commands {
		if contains(c, "rm -f") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected an rm command; ran %v", runner.commands)
	}
}

// A Cancel by job ID, for an attempt whose Submit reply the controller never
// got, stops the unit that runs under that job; with no such job it runs
// nothing and reports the job cancelled.
func TestCancelByJobIDFindsTheHandle(t *testing.T) {
	a, runner := testAgent("")
	spec := jobspec.JobSpec{ID: "uid-9", Name: "lost-reply", Namespace: "tenant-default", Kind: v1.WorkloadKindJob, Entrypoint: "echo hi"}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatal(err)
	}
	if resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r1", Job: job}); resp.GetError() != "" {
		t.Fatalf("Submit: %s", resp.GetError())
	}

	runner.commands = nil
	resp := a.Cancel(context.Background(), &agentpb.CancelRequest{RequestId: "r2", Id: "uid-other"})
	if resp.GetError() != "" || !resp.GetCancelled() || len(runner.commands) != 0 {
		t.Fatalf("cancel of an unknown job ID = %+v, ran %v", resp, runner.commands)
	}
	resp = a.Cancel(context.Background(), &agentpb.CancelRequest{RequestId: "r3", Id: "uid-9"})
	if resp.GetError() != "" || !resp.GetCancelled() {
		t.Fatalf("Cancel by ID = %+v", resp)
	}
	stopped := false
	for _, c := range runner.commands {
		if contains(c, "zenthros-uid-9.service") {
			stopped = true
		}
	}
	if !stopped || a.tracker.Tracked("zenthros-uid-9.service") {
		t.Fatalf("the job's unit was not stopped (ran %v)", runner.commands)
	}
}

// Cancel uninstalls a unit before the status watcher forgets it, and systemd
// shows a missing unit as inactive/Result=success. A missing unit is an error,
// never a phase: found in the floci E2E, where a job that exited 7 was
// reported Succeeded.
func TestStatusOfMissingUnitIsAnError(t *testing.T) {
	a, _ := testAgent("LoadState=not-found\nActiveState=inactive\nResult=success\n")
	resp := a.Status(context.Background(), &agentpb.StatusRequest{RequestId: "r5", Id: "uid-1", HandleName: "zenthros-uid-1.service"})
	if resp.GetError() == "" {
		t.Fatalf("status of a missing unit = %q, want an error", resp.GetStatus().GetPhase())
	}
	// NOT_FOUND is what lets the controller resubmit after an agent restart.
	if resp.GetErrorClass() != agentpb.ErrorClass_NOT_FOUND {
		t.Fatalf("class = %s, want NOT_FOUND", resp.GetErrorClass())
	}
}

// statusRefusingExecutor answers Status and Cancel with a Kubernetes refusal.
type statusRefusingExecutor struct {
	mismatchExecutor
	err error
}

func (e statusRefusingExecutor) Status(context.Context, string) (backend.JobState, error) {
	return backend.JobState{}, e.err
}
func (e statusRefusingExecutor) Cancel(context.Context, string, owner.Ref) error { return e.err }

// What an executor says reaches the controller unchanged: the response text
// is the executor's error text and the class survives decoding, for Status and
// for Cancel.
func TestWireErrorPreservesMessage(t *testing.T) {
	apiErr := apierrors.NewForbidden(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, "hello",
		errors.New(`User "system:serviceaccount:zenthros-system:zenthros-agent" cannot get resource "rayjobs"`))
	execErr := k8serr.Classify(apiErr)
	a := NewWithExecutor(Config{BackendID: "k8s-test", BackendType: "kubernetes"}, statusRefusingExecutor{err: execErr}, logr.Discard())

	status := a.Status(context.Background(), &agentpb.StatusRequest{RequestId: "r1", Id: "uid-1", HandleName: "rayjob:hello"})
	cancel := a.Cancel(context.Background(), &agentpb.CancelRequest{RequestId: "r2", Id: "uid-1", HandleName: "rayjob:hello"})
	for name, got := range map[string]struct {
		text  string
		class agentpb.ErrorClass
	}{
		"status": {status.GetError(), status.GetErrorClass()},
		"cancel": {cancel.GetError(), cancel.GetErrorClass()},
	} {
		if got.text != apiErr.Error() {
			t.Fatalf("%s error = %q, want the API server's %q", name, got.text, apiErr.Error())
		}
		decoded := errorclass.Decode(got.class, got.text)
		if !errors.Is(decoded, backend.ErrForbidden) || decoded.Error() != apiErr.Error() {
			t.Fatalf("%s decoded = %v (forbidden=%v), want the same text classed forbidden", name, decoded, errors.Is(decoded, backend.ErrForbidden))
		}
	}
}

// A Status or Cancel error the executor did not classify stays unclassified
// on the wire. Classing it REJECTED made the controller hold a running job on
// "unit not loaded" instead of resubmitting it.
func TestStatusUnclassifiedErrorStaysUnclassified(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-a"}, statusRefusingExecutor{err: errors.New("unit zenthros-x.service not loaded")}, logr.Discard())
	status := a.Status(context.Background(), &agentpb.StatusRequest{RequestId: "r1", Id: "uid-1", HandleName: "zenthros-x.service"})
	cancel := a.Cancel(context.Background(), &agentpb.CancelRequest{RequestId: "r2", Id: "uid-1", HandleName: "zenthros-x.service"})
	for name, class := range map[string]agentpb.ErrorClass{"status": status.GetErrorClass(), "cancel": cancel.GetErrorClass()} {
		if class != agentpb.ErrorClass_UNSPECIFIED {
			t.Fatalf("%s class = %s, want UNSPECIFIED", name, class)
		}
		if backend.HasSentinel(errorclass.Decode(class, "unit zenthros-x.service not loaded")) {
			t.Fatalf("%s decodes with a sentinel; the controller would not resubmit", name)
		}
	}
}

// The bug the floci E2E found: a job that exited 7 was reported Succeeded,
// because systemd shows a finished unit like one that never ran. The unit's
// own record of how it ended decides.
func TestStatusReportsTheRecordedEnd(t *testing.T) {
	a, runner := testAgent("LoadState=loaded\nActiveState=inactive\nResult=success\n")
	unit := systemd.RenderUnit(jobspec.JobSpec{ID: "uid-1", Entrypoint: "exit 7"}, nil, systemd.Config{Scope: systemd.ScopeSystem}, "")
	runner.files = map[string]string{
		"/etc/systemd/system/zenthros-uid-1.service": unit,
		"/results/zenthros-uid-1.service":            "result=exit-code code=exited status=7",
	}
	resp := a.Status(context.Background(), &agentpb.StatusRequest{RequestId: "r6", Id: "uid-1", HandleName: "zenthros-uid-1.service"})
	if resp.GetError() != "" || resp.GetStatus().GetPhase() != string(v1.PhaseFailed) {
		t.Fatalf("status of a unit that recorded exit 7 = %q (error %q), want Failed", resp.GetStatus().GetPhase(), resp.GetError())
	}

	delete(runner.files, "/results/zenthros-uid-1.service")
	resp = a.Status(context.Background(), &agentpb.StatusRequest{RequestId: "r7", Id: "uid-1", HandleName: "zenthros-uid-1.service"})
	if resp.GetError() == "" {
		t.Fatalf("status of a unit that has not run = %q, want an error", resp.GetStatus().GetPhase())
	}
}

func TestStatusMapsSucceeded(t *testing.T) {
	a, runner := testAgent("ActiveState=inactive\nResult=success\nSubState=exited\n")
	// The unit's ExecStopPost recorded how it ended.
	runner.files = map[string]string{"/var/lib/zenthros/results/zenthros-uid-1.service": "result=success code=exited status=0"}
	resp := a.Status(context.Background(), &agentpb.StatusRequest{RequestId: "r4", Id: "uid-1", HandleName: "zenthros-uid-1.service"})
	if resp.GetError() != "" {
		t.Fatalf("Status error: %s", resp.GetError())
	}
	if resp.GetStatus().GetPhase() != string(v1.PhaseSucceeded) {
		t.Errorf("phase = %q, want Succeeded", resp.GetStatus().GetPhase())
	}
}

func TestToProtoStatusPreservesBackendRetryCount(t *testing.T) {
	status := toProtoStatus(backend.JobState{Phase: v1.PhaseRunning, Restarts: 4})
	if status.GetRetryCount() != 4 {
		t.Fatalf("retryCount = %d, want 4", status.GetRetryCount())
	}
}

func TestDispatchRoutesAndIgnoresEmpty(t *testing.T) {
	a, _ := testAgent("ActiveState=active\n")
	spec := jobspec.JobSpec{ID: "uid-5", Kind: v1.WorkloadKindJob, Entrypoint: "echo hi"}
	job, _ := envelope.Encode(spec)

	if got := a.Dispatch(context.Background(), &agentpb.RegisterResponse{Submit: &agentpb.SubmitRequest{RequestId: "s", Job: job}}); got.GetSubmitResponse() == nil {
		t.Error("Dispatch(Submit) did not return a submit response")
	}
	if got := a.Dispatch(context.Background(), &agentpb.RegisterResponse{}); got != nil {
		t.Error("Dispatch(empty) should return nil")
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// recordingExecutor keeps the spec the agent handed it.
type recordingExecutor struct {
	mismatchExecutor
	got *jobspec.JobSpec
}

func (r recordingExecutor) Submit(_ context.Context, spec jobspec.JobSpec, _ map[string]string) (string, error) {
	*r.got = spec
	return "handle", nil
}

// The executor sees what the controller computed, through the real wire codec:
// a spoke cluster honours Retain only if Lifespan survives the envelope.
func TestAgentSubmitSeesLifespanThroughEnvelope(t *testing.T) {
	var got jobspec.JobSpec
	a := NewWithExecutor(Config{BackendID: "k8s-test", BackendType: "k8s"}, recordingExecutor{got: &got}, logr.Discard())
	spec := jobspec.JobSpec{
		ID: "uid-ls", Name: "retain", Namespace: "tenant-a", Tenant: "a", Kind: v1.WorkloadKindJob,
		Entrypoint: "python x.py", Image: "ray:2", Lifespan: v1.LifespanRetain,
	}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r-ls", Job: job}); resp.GetError() != "" {
		t.Fatalf("Submit error: %s", resp.GetError())
	}
	if got.Lifespan != v1.LifespanRetain || got.Tenant != "a" {
		t.Fatalf("executor saw lifespan=%q tenant=%q, want Retain and a", got.Lifespan, got.Tenant)
	}
}

// runningUntilReleased reads a job as Running, and holds the read until the
// test lets it go.
type runningUntilReleased struct {
	mismatchExecutor
	read, release chan struct{}
}

func (e runningUntilReleased) Status(context.Context, string) (backend.JobState, error) {
	e.read <- struct{}{}
	<-e.release
	return backend.JobState{Phase: v1.PhaseRunning}, nil
}

// Status runs beside Cancel: a job read as Running just before a Cancel
// removed it was tracked again, counted in the heartbeat and polled for ever.
// A Status with no Cancel in between still
// re-tracks the job a restarted agent forgot.
func TestStatusDoesNotRetrackAJobACancelRemoved(t *testing.T) {
	exec := runningUntilReleased{read: make(chan struct{}), release: make(chan struct{})}
	a := NewWithExecutor(Config{BackendID: "vm-1", BackendType: "vm"}, exec, logr.Discard())
	a.tracker.Add("uid-1", "h-1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Status(context.Background(), &agentpb.StatusRequest{RequestId: "r1", Id: "uid-1", HandleName: "h-1"})
	}()
	<-exec.read
	if resp := a.Cancel(context.Background(), &agentpb.CancelRequest{RequestId: "r2", Id: "uid-1", HandleName: "h-1"}); !resp.GetCancelled() {
		t.Fatalf("Cancel = %+v", resp)
	}
	close(exec.release)
	<-done
	if a.tracker.Tracked("h-1") || a.tracker.Count() != 0 {
		t.Fatal("the cancelled job is tracked again")
	}
	go func() { <-exec.read }()
	a.Status(context.Background(), &agentpb.StatusRequest{RequestId: "r3", Id: "uid-2", HandleName: "h-2"})
	if !a.tracker.Tracked("h-2") {
		t.Fatal("a running job the agent forgot is not tracked again")
	}
}

// The other order: the Cancel had begun when Status read the count and ended
// before Status re-tracked. The count only moved when a Cancel began, so the
// job the Cancel removed was tracked again (05#4).
func TestStatusDoesNotRetrackAJobWhoseCancelEndedDuringIt(t *testing.T) {
	tracker := newJobTracker()
	tracker.Add("uid-1", "h-1")
	tracker.BeginCancel("h-1")
	cancels := tracker.Cancels() // Status reads, then asks the executor: Running
	tracker.Remove("h-1")
	tracker.EndCancel("h-1")
	if tracker.Retrack("uid-1", "h-1", cancels) {
		t.Fatal("a job whose Cancel ended during the Status is tracked again")
	}
}
