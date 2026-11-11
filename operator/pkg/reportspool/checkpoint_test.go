// SPDX-License-Identifier: Apache-2.0

package reportspool

import (
	"context"
	"path/filepath"
	"testing"
)

// The spool holds a phase's newest commit, in the order the phases were
// held, never an older one over it, and a release forgets only the commit
// the controller decided.
func TestHeldCheckpointsKeepEachPhasesNewestInOrder(t *testing.T) {
	ctx := context.Background()
	spool, err := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	hold := func(job, phase string, seq int64) {
		t.Helper()
		if err := spool.HoldCheckpoint(ctx, HeldCheckpoint{JobID: job, Phase: phase, Seq: seq, Commit: []byte{byte(seq)}}); err != nil {
			t.Fatal(err)
		}
	}
	hold("job-1", "prep", 3)
	hold("job-1", "prep", 4) // replaces 3
	hold("job-1", "prep", 2) // older: dropped
	hold("job-1", "train", 1)
	hold("job-2", "train", 7)

	held, err := spool.HeldCheckpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []HeldCheckpoint{{"job-1", "prep", 4, []byte{4}}, {"job-1", "train", 1, []byte{1}}, {"job-2", "train", 7, []byte{7}}}
	if len(held) != len(want) {
		t.Fatalf("held = %+v, want %+v", held, want)
	}
	for i := range want {
		if held[i].JobID != want[i].JobID || held[i].Phase != want[i].Phase || held[i].Seq != want[i].Seq || held[i].Commit[0] != want[i].Commit[0] {
			t.Fatalf("held[%d] = %+v, want %+v", i, held[i], want[i])
		}
	}

	hold("job-1", "train", 2) // held while seq 1 was being decided
	if err := spool.ReleaseCheckpoint(ctx, "job-1", "train", 1); err != nil {
		t.Fatal(err)
	}
	if err := spool.ReleaseCheckpoint(ctx, "job-1", "prep", 4); err != nil {
		t.Fatal(err)
	}
	if holds, err := spool.HoldsCheckpoints(ctx, "job-1"); err != nil || !holds {
		t.Fatalf("job-1 holds = %v, %v; want its train seq 2 still held", holds, err)
	}
	if err := spool.ReleaseCheckpoint(ctx, "job-1", "train", 2); err != nil {
		t.Fatal(err)
	}
	if holds, err := spool.HoldsCheckpoints(ctx, "job-1"); err != nil || holds {
		t.Fatalf("job-1 holds = %v, %v after every release", holds, err)
	}
}
