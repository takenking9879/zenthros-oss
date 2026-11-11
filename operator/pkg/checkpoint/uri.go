// SPDX-License-Identifier: Apache-2.0

// Package checkpoint builds generation-scoped object-storage URIs and the
// resume env a replacement worker loads. The controller-acked cursor is the
// source of truth; callers must not list the bucket.
package checkpoint

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
)

const (
	EnvGeneration = "ZENTHROS_GENERATION"
	EnvURI        = "ZENTHROS_CHECKPOINT_URI"
	EnvPhase      = "ZENTHROS_CHECKPOINT_PHASE"
	EnvSeq        = "ZENTHROS_CHECKPOINT_SEQ"
	EnvSHA256     = "ZENTHROS_CHECKPOINT_SHA256"
	EnvComplete   = "ZENTHROS_CHECKPOINT_COMPLETE"
	EnvPrefix     = "ZENTHROS_CHECKPOINT_PREFIX"
	// EnvCursors is every acked phase cursor as JSON:
	// {"<phase>": {"seq", "uri", "sha256", "complete"}}.
	EnvCursors = "ZENTHROS_CHECKPOINT_CURSORS"
)

// MaxPhases bounds status.progress.phases; the API caps it at the same size.
const MaxPhases = 64

// cursorJSON is one phase in EnvCursors, the shape the SDK reads.
type cursorJSON struct {
	Seq      int64  `json:"seq"`
	URI      string `json:"uri"`
	SHA256   string `json:"sha256,omitempty"`
	Complete bool   `json:"complete"`
}

// ObjectKey is the path under the customer prefix:
// jobs/<uid>/gen/<generation>/<phase>/<seq>.
func ObjectKey(jobUID string, generation int64, phase string, seq int64) string {
	if generation <= 0 {
		generation = 1
	}
	return fmt.Sprintf("jobs/%s/gen/%d/%s/%d", jobUID, generation, phase, seq)
}

// URI joins a customer prefix with ObjectKey. prefix may be s3://, file://, or a path.
func URI(prefix, jobUID string, generation int64, phase string, seq int64) string {
	return strings.TrimRight(prefix, "/") + "/" + ObjectKey(jobUID, generation, phase, seq)
}

// ApplyResumeEnv stamps generation and the frozen acked cursor onto env.
// It copies status.progress.URI as-is and never lists object storage.
func ApplyResumeEnv(env map[string]string, job *v1.ZenthrosJob) {
	if env == nil || job == nil {
		return
	}
	env[EnvGeneration] = strconv.FormatInt(job.LeaseGeneration(), 10)
	if prefix := job.Spec.Workload.Template.Artifacts.Output; prefix != "" {
		env[EnvPrefix] = prefix
	}
	progress := job.Status.Progress
	if progress == nil || progress.URI == "" {
		return
	}
	env[EnvURI] = progress.URI
	env[EnvPhase] = progress.Phase
	env[EnvSeq] = strconv.FormatInt(progress.Seq, 10)
	if progress.SHA256 != "" {
		env[EnvSHA256] = progress.SHA256
	}
	if progress.Complete {
		env[EnvComplete] = "true"
	} else {
		env[EnvComplete] = "false"
	}
	if cursors := Cursors(progress); cursors != "" {
		env[EnvCursors] = cursors
	}
}

// Cursors renders every acked phase as EnvCursors, so a resumed job skips
// each phase it completed and resumes each it did not, not only the latest.
// Empty when no phase was recorded.
func Cursors(progress *v1.JobProgress) string {
	if progress == nil || len(progress.Phases) == 0 {
		return ""
	}
	out := make(map[string]cursorJSON, len(progress.Phases))
	for _, p := range progress.Phases {
		out[p.Name] = cursorJSON{Seq: p.Seq, URI: p.URI, SHA256: p.SHA256, Complete: p.Complete}
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(raw)
}

// IncompletePhases lists the phases the job checkpointed without an acked
// COMPLETE, in the order they were first seen.
func IncompletePhases(job *v1.ZenthrosJob) []string {
	if job == nil || job.Status.Progress == nil {
		return nil
	}
	progress := job.Status.Progress
	var out []string
	seen := false
	for _, p := range progress.Phases {
		seen = seen || p.Name == progress.Phase
		if !p.Complete {
			out = append(out, p.Name)
		}
	}
	// A cursor written before phases[] existed is its own record.
	if !seen && progress.Phase != "" && !progress.Complete {
		out = append(out, progress.Phase)
	}
	return out
}

// AllowsSucceeded reports whether a Succeeded status may be accepted.
// Jobs that never checkpointed still succeed. Once a cursor exists, every
// phase the job checkpointed must have an acked COMPLETE.
func AllowsSucceeded(job *v1.ZenthrosJob) bool {
	if job == nil || job.Status.Progress == nil {
		return true
	}
	progress := job.Status.Progress
	if progress.Phase == "" && progress.URI == "" {
		return true
	}
	if !progress.Complete {
		return false
	}
	return len(IncompletePhases(job)) == 0
}
