// SPDX-License-Identifier: Apache-2.0

package reportspool

import (
	"context"
	"database/sql"
	"errors"
)

// HeldCheckpoint is a job's checkpoint commit the controller has not
// decided, as the agent marshalled it.
type HeldCheckpoint struct {
	JobID  string
	Phase  string
	Seq    int64
	Commit []byte
}

// HoldCheckpoint keeps a commit until the controller decides it, in place of
// an older one of its job and phase: the controller takes a phase's newer
// seq without the ones before it, so only the newest has to go. An older
// commit than the one held is dropped.
func (s *Spool) HoldCheckpoint(ctx context.Context, held HeldCheckpoint) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE job_id = ? AND phase = ? AND seq <= ?`,
		held.JobID, held.Phase, held.Seq); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO checkpoints (job_id, phase, seq, commit_proto) VALUES (?, ?, ?, ?)`,
		held.JobID, held.Phase, held.Seq, held.Commit); err != nil {
		return err
	}
	return tx.Commit()
}

// HoldsCheckpoints reports whether the job has a commit held: its next one
// goes behind it.
func (s *Spool) HoldsCheckpoints(ctx context.Context, jobID string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM checkpoints WHERE job_id = ? LIMIT 1`, jobID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// HeldCheckpoints lists the held commits in the order they were held.
func (s *Spool) HeldCheckpoints(ctx context.Context) ([]HeldCheckpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT job_id, phase, seq, commit_proto FROM checkpoints ORDER BY rowid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HeldCheckpoint
	for rows.Next() {
		var held HeldCheckpoint
		if err := rows.Scan(&held.JobID, &held.Phase, &held.Seq, &held.Commit); err != nil {
			return nil, err
		}
		out = append(out, held)
	}
	return out, rows.Err()
}

// ReleaseCheckpoint forgets a commit the controller decided; a newer one of
// its phase held since stays.
func (s *Spool) ReleaseCheckpoint(ctx context.Context, jobID, phase string, seq int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM checkpoints WHERE job_id = ? AND phase = ? AND seq = ?`, jobID, phase, seq)
	return err
}
