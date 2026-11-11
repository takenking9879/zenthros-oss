// SPDX-License-Identifier: Apache-2.0

package executionreport

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func validSnapshot() Snapshot {
	return Snapshot{
		APIVersion: APIVersion,
		Kind:       Kind,
		JobID:      "uid-1",
		ProducerID: "worker-0",
		ReportID:   "rep-1",
		ProducedAt: time.Unix(1, 0).UTC(),
		Payload:    json.RawMessage(`{"accuracy":0.9}`),
	}
}

func TestValidateSnapshotAcceptsCompleteObject(t *testing.T) {
	if err := ValidateSnapshot(validSnapshot()); err != nil {
		t.Fatalf("ValidateSnapshot: %v", err)
	}
}

func TestValidateSnapshotTable(t *testing.T) {
	oversize := Snapshot(validSnapshot())
	oversize.Payload = json.RawMessage("{" + strings.Repeat(`"k":1,`, MaxPayloadBytes) + `"x":1}`)

	cases := []struct {
		name string
		mut  func(*Snapshot)
		code string
	}{
		{"wrong version", func(s *Snapshot) { s.APIVersion = "v0" }, CodeInvalidSnapshot},
		{"wrong kind", func(s *Snapshot) { s.Kind = "LogChunk" }, CodeInvalidSnapshot},
		{"missing job", func(s *Snapshot) { s.JobID = "" }, CodeInvalidSnapshot},
		{"missing producer", func(s *Snapshot) { s.ProducerID = "" }, CodeInvalidSnapshot},
		{"missing report id", func(s *Snapshot) { s.ReportID = "" }, CodeInvalidSnapshot},
		{"zero time", func(s *Snapshot) { s.ProducedAt = time.Time{} }, CodeInvalidSnapshot},
		{"array payload", func(s *Snapshot) { s.Payload = json.RawMessage(`[1]`) }, CodeInvalidJSON},
		{"empty payload", func(s *Snapshot) { s.Payload = nil }, CodeInvalidJSON},
		// The store's JSON cannot keep a NUL: the job hears it here (20#3).
		{"NUL in a value", func(s *Snapshot) { s.Payload = json.RawMessage(`{"log":"a\u0000b"}`) }, CodeInvalidJSON},
		{"NUL in a nested key", func(s *Snapshot) { s.Payload = json.RawMessage(`{"m":[{"k\u0000":1}]}`) }, CodeInvalidJSON},
		{"oversize payload", func(s *Snapshot) { *s = oversize }, CodePayloadTooLarge},
		{"file uri", func(s *Snapshot) {
			s.Artifacts = []ArtifactRef{{URI: "file:///tmp/out"}}
		}, CodeInvalidArtifactURI},
		{"postgres uri", func(s *Snapshot) {
			s.Artifacts = []ArtifactRef{{URI: "postgres://db/table"}}
		}, CodeInvalidArtifactURI},
		{"too many artifacts", func(s *Snapshot) {
			s.Artifacts = make([]ArtifactRef, MaxArtifactCount+1)
			for i := range s.Artifacts {
				s.Artifacts[i] = ArtifactRef{URI: "s3://bucket/obj"}
			}
		}, CodeInvalidArtifactURI},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := validSnapshot()
			tc.mut(&snapshot)
			err := ValidateSnapshot(snapshot)
			if err == nil {
				t.Fatal("expected error")
			}
			if CodeOf(err) != tc.code {
				t.Fatalf("code = %s, want %s (%v)", CodeOf(err), tc.code, err)
			}
		})
	}
}

func TestValidateSnapshotAllowsObjectStoreURIs(t *testing.T) {
	snapshot := validSnapshot()
	snapshot.Artifacts = []ArtifactRef{
		{URI: "s3://bucket/key", Name: "weights"},
		{URI: "gs://bucket/key"},
		{URI: "https://objects.example.com/a"},
	}
	if err := ValidateSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
}

func TestDecideReplay(t *testing.T) {
	incoming := validSnapshot()
	retry := AttemptFloor(1)
	cases := []struct {
		current  Snapshot
		reportID string
		seq      int64
		final    bool
		seen     bool
		want     string
	}{
		{Snapshot{}, "rep-1", 1, false, false, ReplayInsertCurrent},
		{Snapshot{ReportID: "rep-1", Sequence: 1}, "rep-1", 1, false, true, ReplayIdempotent},
		{Snapshot{ReportID: "rep-1", Sequence: 1}, "rep-2", 2, false, false, ReplayInsertCurrent},
		{Snapshot{ReportID: "rep-2", Sequence: 2}, "rep-1", 1, false, true, ReplayInsertHistory},
		// rep-1 sent again after its ack, renumbered above rep-2.
		{Snapshot{ReportID: "rep-2", Sequence: 2}, "rep-1", 3, false, true, ReplayInsertHistory},
		// A post the job had given up on, taken by the spool after its final
		// (2026-11-09): numbered after it, it is still older.
		{Snapshot{ReportID: "final", Sequence: 4, Final: true}, "stale", 5, false, false, ReplayInsertHistory},
		// The final arriving after that stale report takes current from it.
		{Snapshot{ReportID: "stale", Sequence: 5}, "final", 4, true, false, ReplayInsertCurrent},
		// A later final of the same attempt, and any report of the next one.
		{Snapshot{ReportID: "final", Sequence: 4, Final: true}, "final-2", 6, true, false, ReplayInsertCurrent},
		{Snapshot{ReportID: "final", Sequence: 4, Final: true}, "a1-1", retry + 1, false, false, ReplayInsertCurrent},
		{Snapshot{ReportID: "a1-1", Sequence: retry + 1}, "late-final", 4, true, false, ReplayInsertHistory},
	}
	for _, tc := range cases {
		incoming.ReportID, incoming.Sequence, incoming.Final = tc.reportID, tc.seq, tc.final
		got := DecideReplay(tc.current, incoming, tc.seen)
		if got != tc.want {
			t.Errorf("DecideReplay(%+v, %s/%d final=%v) = %s, want %s",
				tc.current, tc.reportID, tc.seq, tc.final, got, tc.want)
		}
	}
}

func TestIsRetryableACK(t *testing.T) {
	if IsRetryableACK(ControllerAck{Accepted: true}) {
		t.Fatal("accepted ACK is not retryable")
	}
	if !IsRetryableACK(ControllerAck{ErrorCode: CodeStoreUnavailable}) {
		t.Fatal("store unavailable must replay")
	}
	if IsRetryableACK(ControllerAck{ErrorCode: CodeInvalidSnapshot}) {
		t.Fatal("validation rejection must not replay")
	}
}

// A backslash and "u0000" as text is no NUL: it is kept.
func TestAnEscapedBackslashIsNoNUL(t *testing.T) {
	snapshot := validSnapshot()
	snapshot.Payload = json.RawMessage(`{"log":"\\u0000"}`)
	if err := ValidateSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
}

// CodeOf reads the code through any wrapping, and an error without one is
// STORE_UNAVAILABLE: retried, never read as the report's own fault (c4-C2).
func TestCodeOfSeesThroughWrapsAndRetriesTheUnknown(t *testing.T) {
	if got := CodeOf(fmt.Errorf("store: %w", NewError(CodeJobMismatch, "x"))); got != CodeJobMismatch {
		t.Fatalf("wrapped = %s, want %s", got, CodeJobMismatch)
	}
	if got := CodeOf(errors.New("connection reset")); got != CodeStoreUnavailable {
		t.Fatalf("unknown = %s, want %s", got, CodeStoreUnavailable)
	}
}
