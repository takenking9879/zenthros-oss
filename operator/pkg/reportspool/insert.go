// SPDX-License-Identifier: Apache-2.0

package reportspool

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/zenthros/operator/pkg/executionreport"
)

type InsertResult struct {
	Receipt   executionreport.LocalReceipt
	Duplicate bool
}

func (s *Spool) Insert(ctx context.Context, snapshot executionreport.Snapshot) (InsertResult, error) {
	if s == nil || s.db == nil {
		return InsertResult{}, errors.New("spool is not open")
	}
	if err := executionreport.ValidateSnapshot(snapshot); err != nil {
		return InsertResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return InsertResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	existing, found, err := loadReport(ctx, tx, snapshot.JobID, snapshot.ProducerID, snapshot.ReportID)
	if err != nil {
		return InsertResult{}, err
	}
	if found {
		if err := tx.Commit(); err != nil {
			return InsertResult{}, err
		}
		return InsertResult{Receipt: existing, Duplicate: true}, nil
	}
	if err := s.enforceBounds(ctx, tx, snapshot); err != nil {
		return InsertResult{}, err
	}
	sequence, err := nextSequence(ctx, tx, snapshot.JobID, snapshot.ProducerID)
	if err != nil {
		return InsertResult{}, err
	}
	now := time.Now().UTC()
	artifacts, _ := json.Marshal(snapshot.Artifacts)
	final := 0
	if snapshot.Final {
		final = 1
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO reports (
			job_id, producer_id, report_id, sequence, produced_at, payload, artifacts,
			api_version, is_final, controller_acked, created_at
		) VALUES (?,?,?,?,?,?,?,?,?,0,?)`,
		snapshot.JobID, snapshot.ProducerID, snapshot.ReportID, sequence, snapshot.ProducedAt.Unix(),
		[]byte(snapshot.Payload), string(artifacts), snapshot.APIVersion, final, now.UnixMilli(),
	)
	if err != nil {
		return InsertResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return InsertResult{}, err
	}
	return InsertResult{Receipt: executionreport.LocalReceipt{
		ReportID: snapshot.ReportID, ProducerID: snapshot.ProducerID, Sequence: sequence, LocalAckedAt: now,
	}}, nil
}

func (s *Spool) enforceBounds(ctx context.Context, tx *sql.Tx, snapshot executionreport.Snapshot) error {
	var count int
	var bytes int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)),0) FROM reports WHERE controller_acked = 0`).Scan(&count, &bytes); err != nil {
		return err
	}
	if count >= s.cfg.maxReports() || bytes+int64(len(snapshot.Payload)) > s.cfg.maxBytes() {
		return executionreport.NewError(executionreport.CodeSpoolFull, "local report spool is full")
	}
	return nil
}

// nextSequence is the producer's next sequence: one past its last, and
// above the floor of the job's attempt (BeginAttempt).
func nextSequence(ctx context.Context, tx *sql.Tx, jobID, producerID string) (int64, error) {
	var next int64
	err := tx.QueryRowContext(ctx, `SELECT next_sequence FROM sequences WHERE job_id = ? AND producer_id = ?`, jobID, producerID).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) {
		next = 1
	} else if err != nil {
		return 0, err
	}
	var attempt int32
	err = tx.QueryRowContext(ctx, `SELECT attempt FROM attempts WHERE job_id = ?`, jobID).Scan(&attempt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	next = max(next, executionreport.AttemptFloor(attempt)+1)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO sequences (job_id, producer_id, next_sequence) VALUES (?,?,?)
		ON CONFLICT (job_id, producer_id) DO UPDATE SET next_sequence = excluded.next_sequence`,
		jobID, producerID, next+1,
	); err != nil {
		return 0, err
	}
	return next, nil
}

func loadReport(ctx context.Context, tx *sql.Tx, jobID, producerID, reportID string) (executionreport.LocalReceipt, bool, error) {
	var receipt executionreport.LocalReceipt
	var created int64
	err := tx.QueryRowContext(ctx, `
		SELECT report_id, producer_id, sequence, created_at
		FROM reports WHERE job_id = ? AND producer_id = ? AND report_id = ?`,
		jobID, producerID, reportID,
	).Scan(&receipt.ReportID, &receipt.ProducerID, &receipt.Sequence, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return executionreport.LocalReceipt{}, false, nil
	}
	if err != nil {
		return executionreport.LocalReceipt{}, false, err
	}
	receipt.LocalAckedAt = spoolTime(created)
	return receipt, true, nil
}

// hmacToken is the job's token under the spool's key.
func (s *Spool) hmacToken(jobID string) string {
	mac := hmac.New(sha256.New, s.cfg.TokenKey)
	mac.Write([]byte(jobID))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Spool) IssueToken(ctx context.Context, jobID string) (string, error) {
	if len(s.cfg.TokenKey) > 0 {
		return s.hmacToken(jobID), nil
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	token := hex.EncodeToString(buf)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tokens (job_id, token, created_at) VALUES (?,?,?)
		ON CONFLICT (job_id) DO NOTHING`,
		jobID, token, time.Now().Unix(),
	)
	if err != nil {
		return "", err
	}
	var stored string
	err = s.db.QueryRowContext(ctx, `SELECT token FROM tokens WHERE job_id = ?`, jobID).Scan(&stored)
	return stored, err
}

func (s *Spool) LookupToken(ctx context.Context, jobID, token string) (bool, error) {
	if len(s.cfg.TokenKey) > 0 {
		return token != "" && hmac.Equal([]byte(token), []byte(s.hmacToken(jobID))), nil
	}
	var stored string
	err := s.db.QueryRowContext(ctx, `SELECT token FROM tokens WHERE job_id = ?`, jobID).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return stored == token && token != "", nil
}

// BeginAttempt records the attempt the job runs now on this machine: its
// reports are numbered above every earlier attempt's (AttemptFloor). An
// older attempt never lowers it.
func (s *Spool) BeginAttempt(ctx context.Context, jobID string, attempt int32) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO attempts (job_id, attempt) VALUES (?,?)
		ON CONFLICT (job_id) DO UPDATE SET attempt = max(attempt, excluded.attempt)`, jobID, attempt)
	return err
}

// EndJob forgets a job that reports nothing more: its token, sequences and
// attempt.
// Its reports not yet acked stay until the controller acks or refuses them
// (Ack): the controller cancels a finished job seconds after its end, and
// its result may still be waiting for a Hub that could not store it.
func (s *Spool) EndJob(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM sequences WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tokens WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM attempts WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	return tx.Commit()
}
