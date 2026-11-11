// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/reportspool"
)

// The cancel of a job's old attempt, while its next attempt runs on the
// same machine, does not end the job's reports: the next report goes on
// from the sequence the job reached. Ended there, the sequence started
// again at 1, below the old attempt's, and the job's current report stayed
// the old attempt's.
func TestCancelOfAnOldAttemptKeepsTheJobsReportSequence(t *testing.T) {
	ctx := context.Background()
	a, _ := testAgent("")
	spool, err := reportspool.Open(ctx, reportspool.Config{Path: filepath.Join(t.TempDir(), "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	a.spool = spool
	report := func(id string) int64 {
		t.Helper()
		got, err := spool.Insert(ctx, executionreport.Snapshot{APIVersion: executionreport.APIVersion, Kind: executionreport.Kind,
			JobID: "uid-1", ProducerID: "p", ReportID: id, ProducedAt: time.Unix(1, 0), Payload: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		return got.Receipt.Sequence
	}
	report("attempt-0")
	a.tracker.Add("uid-1", "zenthros-uid-1-a1.service") // the next attempt runs
	if resp := a.Cancel(ctx, &agentpb.CancelRequest{RequestId: "r", Id: "uid-1", HandleName: "zenthros-uid-1.service"}); !resp.GetCancelled() {
		t.Fatalf("Cancel = %+v", resp)
	}
	if seq := report("attempt-1"); seq != 2 {
		t.Fatalf("the next attempt's report got sequence %d, want 2", seq)
	}
}
