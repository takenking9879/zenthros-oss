// SPDX-License-Identifier: Apache-2.0

package reportspool

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/executionreport"
)

func testSpool(t *testing.T) *Spool {
	t.Helper()
	dir := t.TempDir()
	spool, err := Open(context.Background(), Config{Path: filepath.Join(dir, "reports.sqlite"), MaxReports: 4, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.Close() })
	return spool
}

func snapshot(id string) executionreport.Snapshot {
	return executionreport.Snapshot{
		APIVersion: executionreport.APIVersion,
		Kind:       executionreport.Kind,
		JobID:      "job-1",
		ProducerID: "worker-0",
		ReportID:   id,
		ProducedAt: time.Unix(1, 0).UTC(),
		Payload:    json.RawMessage(`{"n":1}`),
	}
}

func TestSpoolInsertAssignsSequenceAndAcksLocally(t *testing.T) {
	spool := testSpool(t)
	first, err := spool.Insert(context.Background(), snapshot("rep-1"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Receipt.Sequence != 1 || first.Duplicate {
		t.Fatalf("first = %+v", first)
	}
	second, err := spool.Insert(context.Background(), snapshot("rep-2"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Receipt.Sequence != 2 {
		t.Fatalf("second sequence = %d", second.Receipt.Sequence)
	}
}

func TestSpoolDeduplicatesRetries(t *testing.T) {
	spool := testSpool(t)
	first, err := spool.Insert(context.Background(), snapshot("rep-1"))
	if err != nil {
		t.Fatal(err)
	}
	again, err := spool.Insert(context.Background(), snapshot("rep-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.Receipt.Sequence != first.Receipt.Sequence {
		t.Fatalf("again = %+v", again)
	}
}

func TestSpoolReplayAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reports.sqlite")
	ctx := context.Background()
	spool, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Insert(ctx, snapshot("rep-1")); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool, err = Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	unacked, err := spool.Unacked(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(unacked) != 1 || unacked[0].ReportID != "rep-1" || unacked[0].Sequence != 1 {
		t.Fatalf("unacked = %+v", unacked)
	}
}

func TestSpoolStatsCountsUnacked(t *testing.T) {
	spool := testSpool(t)
	ctx := context.Background()
	if _, err := spool.Insert(ctx, snapshot("rep-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Insert(ctx, snapshot("rep-2")); err != nil {
		t.Fatal(err)
	}
	stats, err := spool.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Count != 2 {
		t.Fatalf("count = %d, want 2", stats.Count)
	}
	if stats.Bytes <= 0 {
		t.Fatalf("bytes = %d, want > 0", stats.Bytes)
	}
	created, ok, err := spool.CreatedAt(ctx, snapshot("rep-1"))
	if err != nil || !ok || created.IsZero() {
		t.Fatalf("CreatedAt = %v ok=%v err=%v", created, ok, err)
	}
}

func TestSpoolAckDeletesRow(t *testing.T) {
	spool := testSpool(t)
	ctx := context.Background()
	inserted, err := spool.Insert(ctx, snapshot("rep-1"))
	if err != nil {
		t.Fatal(err)
	}
	snap := snapshot("rep-1")
	snap.Sequence = inserted.Receipt.Sequence
	if err := spool.Ack(ctx, snap, executionreport.ControllerAck{Accepted: true, ReportID: "rep-1"}); err != nil {
		t.Fatal(err)
	}
	unacked, err := spool.Unacked(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(unacked) != 0 {
		t.Fatalf("unacked after ack = %+v", unacked)
	}
}

func TestSpoolFullRejects(t *testing.T) {
	spool := testSpool(t)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if _, err := spool.Insert(ctx, snapshot("rep-"+string(rune('a'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	_, err := spool.Insert(ctx, snapshot("rep-z"))
	if executionreport.CodeOf(err) != executionreport.CodeSpoolFull {
		t.Fatalf("err = %v", err)
	}
}

func TestSpoolTokenRoundTrip(t *testing.T) {
	spool := testSpool(t)
	ctx := context.Background()
	token, err := spool.IssueToken(ctx, "job-1")
	if err != nil || token == "" {
		t.Fatalf("token = %q err=%v", token, err)
	}
	ok, err := spool.LookupToken(ctx, "job-1", token)
	if err != nil || !ok {
		t.Fatalf("lookup = %v %v", ok, err)
	}
	ok, err = spool.LookupToken(ctx, "job-1", "nope")
	if err != nil || ok {
		t.Fatal("wrong token must not match")
	}
	again, err := spool.IssueToken(ctx, "job-1")
	if err != nil || again != token {
		t.Fatalf("reissue = %q err=%v, want stable token %q", again, err, token)
	}
}

func TestSpoolWALFileCreated(t *testing.T) {
	spool := testSpool(t)
	if _, err := os.Stat(spool.Path()); err != nil {
		t.Fatal(err)
	}
}

// With a key, a job's token is the same from any spool: an agent whose
// spool was lost (an emptyDir on a restarted pod) accepts the reports of a
// job it gave a token before.
func TestHMACTokenStableAcrossSpoolLoss(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	ctx := context.Background()
	first, err := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "a.db"), TokenKey: key})
	if err != nil {
		t.Fatal(err)
	}
	token, err := first.IssueToken(ctx, "job-1")
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "b.db"), TokenKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := fresh.LookupToken(ctx, "job-1", token); err != nil || !ok {
		t.Fatalf("a fresh spool refused the job's token: %v %v", ok, err)
	}
	if again, _ := fresh.IssueToken(ctx, "job-1"); again != token {
		t.Fatal("a fresh spool issued a different token")
	}
	for _, bad := range []struct{ job, token string }{{"job-2", token}, {"job-1", ""}, {"job-1", token[:len(token)-1] + "0"}} {
		if ok, _ := fresh.LookupToken(ctx, bad.job, bad.token); ok && bad.token != token {
			t.Fatalf("accepted %+v", bad)
		}
	}
	if ok, _ := fresh.LookupToken(ctx, "job-2", token); ok {
		t.Fatal("job-1's token is accepted for job-2")
	}
	other, _ := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "c.db"), TokenKey: bytes.Repeat([]byte{8}, 32)})
	if ok, _ := other.LookupToken(ctx, "job-1", token); ok {
		t.Fatal("another agent's key accepted the token")
	}
}

// A cancelled job's reports not yet acked stay until the controller acks
// them: the controller cancels a finished job seconds after its end, while
// its result may still wait for a Hub that refused it. Its token goes: the
// job reports nothing more.
func TestEndedJobKeepsItsUnackedReports(t *testing.T) {
	spool := testSpool(t)
	ctx := context.Background()
	token, err := spool.IssueToken(ctx, "job-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Insert(ctx, snapshot("result")); err != nil {
		t.Fatal(err)
	}
	if err := spool.EndJob(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	if unacked, err := spool.Unacked(ctx); err != nil || len(unacked) != 1 || unacked[0].ReportID != "result" {
		t.Fatalf("unacked after the job ended = %+v %v, want the result", unacked, err)
	}
	if ok, err := spool.LookupToken(ctx, "job-1", token); err != nil || ok {
		t.Fatalf("token after the job ended: ok=%v err=%v", ok, err)
	}
}

// A job's next attempt numbers its reports above every report of the
// attempts before it, on any machine: its sequence starts at the attempt's
// floor. On another machine it started again at 1, below the lost
// attempt's, and the job's current report stayed the lost attempt's.
func TestAnAttemptNumbersItsReportsAboveTheAttemptsBefore(t *testing.T) {
	ctx := context.Background()
	spool, err := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	insert := func(id string) int64 {
		t.Helper()
		got, err := spool.Insert(ctx, executionreport.Snapshot{APIVersion: executionreport.APIVersion, Kind: executionreport.Kind,
			JobID: "uid-1", ProducerID: "p", ReportID: id, ProducedAt: time.Unix(1, 0), Payload: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		return got.Receipt.Sequence
	}
	if err := spool.BeginAttempt(ctx, "uid-1", 2); err != nil {
		t.Fatal(err)
	}
	first := insert("a2-1")
	if first != executionreport.AttemptFloor(2)+1 {
		t.Fatalf("attempt 2's first report = %d, want %d", first, executionreport.AttemptFloor(2)+1)
	}
	if next := insert("a2-2"); next != first+1 {
		t.Fatalf("next report = %d, want %d", next, first+1)
	}
}
