// SPDX-License-Identifier: Apache-2.0

package checkpoint

import (
	"fmt"

	v1 "github.com/zenthros/operator/api/v1"
)

// Commit is what a job says it stored: the checkpoint it asks to be acked.
type Commit struct {
	URI, Phase, SHA256 string
	Seq                int64
	Complete           bool
}

// Valid is why a commit cannot be acked for lack of a field, or "".
func (c Commit) Valid() string {
	if c.URI == "" || c.Phase == "" || c.SHA256 == "" || c.Seq <= 0 {
		return "uri, phase, sha256, and seq are required"
	}
	return ""
}

// Advance is current with commit acked: the next cursor, whether it differs
// (a commit already on the cursor changes nothing), or why the commit is
// refused (behind the cursor, or conflicting with its seq). The one rule of
// the cursor, whoever keeps it: the hub's job, its row, or a local agent.
func Advance(current *v1.JobProgress, commit Commit) (next *v1.JobProgress, dirty bool, refused string) {
	if current != nil && current.Phase == commit.Phase {
		if commit.Seq < current.Seq {
			return nil, false, "seq behind acked cursor"
		}
		if commit.Seq == current.Seq {
			same := current.URI == commit.URI && current.SHA256 == commit.SHA256 &&
				current.Complete == commit.Complete
			if !same {
				return nil, false, "conflicting commit at acked seq"
			}
			return current, false, ""
		}
	}
	progress := &v1.JobProgress{
		URI:      commit.URI,
		Phase:    commit.Phase,
		Seq:      commit.Seq,
		SHA256:   commit.SHA256,
		Complete: commit.Complete,
	}
	if current != nil {
		progress.Phases = append([]v1.PhaseCursor(nil), current.Phases...)
	}
	phases, ok := recordPhase(progress.Phases, v1.PhaseCursor{
		Name: commit.Phase, URI: commit.URI, Seq: commit.Seq,
		SHA256: commit.SHA256, Complete: commit.Complete,
	})
	if !ok {
		return nil, false, fmt.Sprintf("too many checkpoint phases: a job records at most %d", MaxPhases)
	}
	progress.Phases = phases
	return progress, true, ""
}

// recordPhase sets a phase's cursor, keeping the order phases were first
// seen. A phase's own seq only moves forward: an older cursor never
// replaces a newer one.
func recordPhase(phases []v1.PhaseCursor, cursor v1.PhaseCursor) ([]v1.PhaseCursor, bool) {
	for i := range phases {
		if phases[i].Name == cursor.Name {
			if cursor.Seq >= phases[i].Seq {
				phases[i] = cursor
			}
			return phases, true
		}
	}
	if len(phases) >= MaxPhases {
		return phases, false
	}
	return append(phases, cursor), true
}
