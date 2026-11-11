// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/reportspool"
)

// signalRecorder is an executor that, when asked to signal a job, records
// what that job's notice file said at that moment.
type signalRecorder struct {
	mismatchExecutor
	dir  string
	seen map[string]string
}

func (s signalRecorder) SignalPreempt(_ context.Context, handle string) (bool, error) {
	jobID := map[string]string{"unit-1": "uid-1", "unit-2": "uid-2"}[handle]
	raw, _ := os.ReadFile(preemptFile(s.dir, jobID))
	s.seen[handle] = string(raw)
	return true, nil
}

// Every job's notice file is written whole before any job is signalled: a
// process woken by its signal always finds its notice.
func TestNoticeWritesFileBeforeSignal(t *testing.T) {
	dir := t.TempDir()
	rec := signalRecorder{dir: dir, seen: map[string]string{}}
	a := NewWithExecutor(Config{BackendID: "vm-a", BackendType: "vm", PreemptDir: dir}, rec, logr.Discard())
	a.tracker.Add("uid-1", "unit-1")
	a.tracker.Add("uid-2", "unit-2")
	deadline := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	a.deliverNotice(context.Background(), preemptionNotice{Preempted: true, Source: noticeAWSIMDS, Action: "terminate", Deadline: deadline})

	for _, handle := range []string{"unit-1", "unit-2"} {
		var got noticeFile
		if err := json.Unmarshal([]byte(rec.seen[handle]), &got); err != nil {
			t.Fatalf("%s signalled before its notice was readable (%q): %v", handle, rec.seen[handle], err)
		}
		if got.Source != noticeAWSIMDS || got.Action != "terminate" || got.Deadline != "2026-09-25T12:00:00Z" {
			t.Fatalf("%s notice = %+v", handle, got)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Fatalf("notice dir holds %d entries, want the two notices and no temporary file", len(entries))
	}
}

// Every job on a machine is told where its notice will be, and the path is
// left out of the spec hash like the other per-run variables.
func TestSubmitEnvCarriesPreemptFile(t *testing.T) {
	a := NewWithExecutor(Config{BackendID: "vm-a", BackendType: "vm", PreemptDir: "/var/run/zenthros/preempt"}, mismatchExecutor{}, logr.Discard())
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(t.TempDir(), "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spool.Close() })
	a.spool = spool
	env := a.injectReportEnv(context.Background(), &jobspec.JobSpec{ID: "uid-1"}, nil)
	if env[jobspec.EnvPreemptFile] != "/var/run/zenthros/preempt/uid-1.json" {
		t.Fatalf("env = %v", env)
	}
	if !jobspec.ControllerDerivedEnv(jobspec.EnvPreemptFile) {
		t.Fatal("the notice path is in the spec hash")
	}
	k8s := NewWithExecutor(Config{BackendID: "k8s-a", BackendType: "k8s"}, mismatchExecutor{}, logr.Discard())
	k8s.spool = a.spool
	if _, ok := k8s.injectReportEnv(context.Background(), &jobspec.JobSpec{ID: "uid-1"}, nil)[jobspec.EnvPreemptFile]; ok {
		t.Fatal("a Kubernetes agent, which acts on no notice, promised a notice file")
	}
}
