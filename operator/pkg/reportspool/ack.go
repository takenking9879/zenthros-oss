// SPDX-License-Identifier: Apache-2.0

package reportspool

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zenthros/operator/pkg/executionreport"
)

// Key names one report.
type Key struct{ JobID, ProducerID, ReportID string }

// UnackedKeys lists the reports the controller has not acked, in the order
// they go, without their payloads: a forwarder reads the payload of one it
// sends, not of every one waiting for its ack (05#3).
func (s *Spool) UnackedKeys(ctx context.Context) ([]Key, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id, producer_id, report_id
		FROM reports
		WHERE controller_acked = 0
		ORDER BY sequence, created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		var key Key
		if err := rows.Scan(&key.JobID, &key.ProducerID, &key.ReportID); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}

// Load reads one unacked report; ok is false when it was acked or dropped
// since it was listed.
func (s *Spool) Load(ctx context.Context, key Key) (executionreport.Snapshot, bool, error) {
	out, err := s.unacked(ctx, ` AND job_id = ? AND producer_id = ? AND report_id = ?`, key.JobID, key.ProducerID, key.ReportID)
	if err != nil || len(out) == 0 {
		return executionreport.Snapshot{}, false, err
	}
	return out[0], true, nil
}

func (s *Spool) Unacked(ctx context.Context) ([]executionreport.Snapshot, error) {
	return s.unacked(ctx, "")
}

func (s *Spool) unacked(ctx context.Context, where string, args ...any) ([]executionreport.Snapshot, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id, producer_id, report_id, sequence, produced_at, payload, artifacts, api_version, is_final
		FROM reports
		WHERE controller_acked = 0`+where+`
		ORDER BY sequence, created_at`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []executionreport.Snapshot
	for rows.Next() {
		var snapshot executionreport.Snapshot
		var producedAt int64
		var artifacts string
		var payload []byte
		var final int
		if err := rows.Scan(
			&snapshot.JobID, &snapshot.ProducerID, &snapshot.ReportID, &snapshot.Sequence,
			&producedAt, &payload, &artifacts, &snapshot.APIVersion, &final,
		); err != nil {
			return nil, err
		}
		snapshot.Kind = executionreport.Kind
		snapshot.ProducedAt = time.Unix(producedAt, 0).UTC()
		snapshot.Payload = json.RawMessage(payload)
		snapshot.Final = final == 1
		if artifacts != "" {
			_ = json.Unmarshal([]byte(artifacts), &snapshot.Artifacts)
		}
		out = append(out, snapshot)
	}
	return out, rows.Err()
}

func (s *Spool) Ack(ctx context.Context, snapshot executionreport.Snapshot, ack executionreport.ControllerAck) error {
	if !ack.Accepted && executionreport.IsRetryableACK(ack) {
		return nil
	}
	// An acked report is done: it is deleted, not kept marked (the mark
	// and the delete were one transaction that left nothing marked).
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM reports
		WHERE job_id = ? AND producer_id = ? AND report_id = ?`,
		snapshot.JobID, snapshot.ProducerID, snapshot.ReportID,
	)
	return err
}

// Reports are the job's reports the spool holds, in order. Without a hub to
// take them they stay here: a local agent's way of giving them to the SDK.
func (s *Spool) Reports(ctx context.Context, jobID string) ([]executionreport.Snapshot, error) {
	return s.unacked(ctx, ` AND job_id = ?`, jobID)
}

// DropReports forgets the job's reports for good: its record is gone.
func (s *Spool) DropReports(ctx context.Context, jobID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM reports WHERE job_id = ?`, jobID)
	return err
}
