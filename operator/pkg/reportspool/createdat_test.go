// SPDX-License-Identifier: Apache-2.0

package reportspool

import (
	"context"
	"testing"
	"time"
)

// When a report entered the spool is kept to the millisecond: in seconds the
// delivery time (ingest to the hub's ack) read up to a second whatever it
// was, a uniform 0-1 s on every report, and E10's "ack" was that rounding.
func TestTheSpoolKeepsWhenAReportArrivedToTheMillisecond(t *testing.T) {
	spool := testSpool(t)
	ctx := context.Background()
	before := time.Now()
	if _, err := spool.Insert(ctx, snapshot("rep-ms")); err != nil {
		t.Fatal(err)
	}
	created, ok, err := spool.CreatedAt(ctx, snapshot("rep-ms"))
	if err != nil || !ok {
		t.Fatalf("CreatedAt: %v %v", ok, err)
	}
	if lag := created.Sub(before); lag < -time.Millisecond || lag > 200*time.Millisecond {
		t.Fatalf("created %v, inserted at %v: %v apart, want the same millisecond", created, before, lag)
	}
}
