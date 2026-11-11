// SPDX-License-Identifier: Apache-2.0

package checkpoint

import (
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/zenthros/operator/api/v1"
)

func TestObjectKeyIsGenerationScoped(t *testing.T) {
	key := ObjectKey("uid-1", 3, "train", 5)
	if key != "jobs/uid-1/gen/3/train/5" {
		t.Fatalf("ObjectKey = %q", key)
	}
	uri := URI("s3://bucket/prefix", "uid-1", 3, "train", 5)
	if uri != "s3://bucket/prefix/jobs/uid-1/gen/3/train/5" {
		t.Fatalf("URI = %q", uri)
	}
}

func TestApplyResumeEnvLoadsFrozenURINotALaterSeq(t *testing.T) {
	job := &v1.ZenthrosJob{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("uid-1")},
		Spec: v1.ZenthrosJobSpec{
			Workload: v1.Workload{Template: v1.WorkloadTemplate{
				Artifacts: v1.Artifacts{Output: "s3://bucket/out"},
			}},
		},
		Status: v1.ZenthrosJobStatus{
			Lease: &v1.JobLease{Generation: 2},
			Progress: &v1.JobProgress{
				URI:      "s3://bucket/out/jobs/uid-1/gen/1/train/2",
				Phase:    "train",
				Seq:      2,
				SHA256:   "abc",
				Complete: false,
			},
		},
	}
	env := map[string]string{}
	ApplyResumeEnv(env, job)
	if env[EnvURI] != "s3://bucket/out/jobs/uid-1/gen/1/train/2" {
		t.Fatalf("resume URI = %q, want frozen ack", env[EnvURI])
	}
	if strings.Contains(env[EnvURI], "/3") {
		t.Fatal("resume must not pick a later object-storage seq")
	}
	if env[EnvSeq] != "2" {
		t.Fatalf("seq = %q, want 2 (replacement starts at seq+1)", env[EnvSeq])
	}
	if env[EnvGeneration] != "2" {
		t.Fatalf("generation = %q, want replacement gen 2 (zombie writes stay in gen/1)", env[EnvGeneration])
	}
	if env[EnvComplete] != "false" {
		t.Fatalf("complete = %q", env[EnvComplete])
	}
}

func TestAllowsSucceededRequiresAckedComplete(t *testing.T) {
	if !AllowsSucceeded(&v1.ZenthrosJob{}) {
		t.Fatal("jobs that never checkpointed must still succeed")
	}
	job := &v1.ZenthrosJob{Status: v1.ZenthrosJobStatus{
		Progress: &v1.JobProgress{URI: "s3://b/x", Phase: "train", Seq: 1, Complete: false},
	}}
	if AllowsSucceeded(job) {
		t.Fatal("Succeeded without COMPLETE must be blocked")
	}
	job.Status.Progress.Complete = true
	job.Status.Progress.Phases = []v1.PhaseCursor{{Name: "train", URI: "s3://b/x", Seq: 1, Complete: true}}
	if !AllowsSucceeded(job) {
		t.Fatal("acked COMPLETE must allow Succeeded")
	}
}

// A resumed job gets every phase's acked cursor, so it skips each phase it
// completed, not only the latest, and resumes the one it had not finished.
func TestCursorsEnvCarriesAllCompletedPhases(t *testing.T) {
	job := &v1.ZenthrosJob{Status: v1.ZenthrosJobStatus{Progress: &v1.JobProgress{
		URI: "s3://b/eval/1", Phase: "eval", Seq: 1, SHA256: "e", Complete: false,
		Phases: []v1.PhaseCursor{
			{Name: "prep", URI: "s3://b/prep/2", Seq: 2, SHA256: "p", Complete: true},
			{Name: "train", URI: "s3://b/train/9", Seq: 9, SHA256: "t", Complete: true},
			{Name: "eval", URI: "s3://b/eval/1", Seq: 1, SHA256: "e"},
		},
	}}}
	env := map[string]string{}
	ApplyResumeEnv(env, job)
	var got map[string]cursorJSON
	if err := json.Unmarshal([]byte(env[EnvCursors]), &got); err != nil {
		t.Fatalf("cursors %q: %v", env[EnvCursors], err)
	}
	want := map[string]cursorJSON{
		"prep":  {Seq: 2, URI: "s3://b/prep/2", SHA256: "p", Complete: true},
		"train": {Seq: 9, URI: "s3://b/train/9", SHA256: "t", Complete: true},
		"eval":  {Seq: 1, URI: "s3://b/eval/1", SHA256: "e"},
	}
	if len(got) != len(want) {
		t.Fatalf("cursors = %+v", got)
	}
	for name, cursor := range want {
		if got[name] != cursor {
			t.Errorf("%s = %+v, want %+v", name, got[name], cursor)
		}
	}
	// The SDK reads exactly these keys.
	for _, key := range []string{`"seq"`, `"uri"`, `"sha256"`, `"complete"`} {
		if !strings.Contains(env[EnvCursors], key) {
			t.Errorf("cursors lack %s: %s", key, env[EnvCursors])
		}
	}
	if env[EnvPhase] != "eval" {
		t.Fatalf("latest cursor = %q", env[EnvPhase])
	}
	if AllowsSucceeded(job) {
		t.Fatal("eval never completed, yet Succeeded is allowed")
	}
	if got := IncompletePhases(job); len(got) != 1 || got[0] != "eval" {
		t.Fatalf("incomplete = %v", got)
	}
}

// An earlier phase left without COMPLETE blocks Succeeded even when the
// latest phase completed.
func TestAllowsSucceededNeedsEveryPhaseComplete(t *testing.T) {
	job := &v1.ZenthrosJob{Status: v1.ZenthrosJobStatus{Progress: &v1.JobProgress{
		URI: "s3://b/train/9", Phase: "train", Seq: 9, Complete: true,
		Phases: []v1.PhaseCursor{{Name: "prep", Seq: 1}, {Name: "train", Seq: 9, Complete: true}},
	}}}
	if AllowsSucceeded(job) {
		t.Fatal("prep never completed, yet Succeeded is allowed")
	}
}
