// SPDX-License-Identifier: Apache-2.0

// Package reportspool is the Agent-local durable ExecutionReport queue.
// One SQLite database in WAL mode holds all reports for the Agent, and the
// checkpoint commits the controller has not decided (checkpoint.go). Parquet
// is out of scope for delivery.
package reportspool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/zenthros/operator/pkg/executionreport"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS reports (
    job_id TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    report_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    produced_at INTEGER NOT NULL,
    payload BLOB NOT NULL,
    artifacts TEXT NOT NULL DEFAULT '[]',
    api_version TEXT NOT NULL,
    is_final INTEGER NOT NULL DEFAULT 0,
    controller_acked INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL, -- milliseconds since the epoch (spoolTime)
    PRIMARY KEY (job_id, producer_id, report_id)
);
CREATE INDEX IF NOT EXISTS reports_unacked_idx ON reports (controller_acked, sequence);
CREATE TABLE IF NOT EXISTS sequences (
    job_id TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    next_sequence INTEGER NOT NULL,
    PRIMARY KEY (job_id, producer_id)
);
CREATE TABLE IF NOT EXISTS attempts (
    job_id TEXT PRIMARY KEY,
    attempt INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints (
    job_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    seq INTEGER NOT NULL,
    commit_proto BLOB NOT NULL,
    PRIMARY KEY (job_id, phase)
);
CREATE TABLE IF NOT EXISTS tokens (
    job_id TEXT PRIMARY KEY,
    token TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
`

type Config struct {
	Path string
	// TokenKey, when set, makes a job's report token HMAC-SHA256(key, job
	// id): stable across agent restarts and spool loss. Without it tokens are
	// random and stored in the spool.
	TokenKey   []byte
	MaxReports int
	MaxBytes   int64
}

func (c Config) maxReports() int {
	if c.MaxReports > 0 {
		return c.MaxReports
	}
	return executionreport.DefaultMaxSpoolReports
}

func (c Config) maxBytes() int64 {
	if c.MaxBytes > 0 {
		return c.MaxBytes
	}
	return executionreport.DefaultMaxSpoolBytes
}

type Spool struct {
	db  *sql.DB
	cfg Config
}

func Open(ctx context.Context, cfg Config) (*Spool, error) {
	if cfg.Path == "" {
		return nil, errors.New("report spool path is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o700); err != nil {
		return nil, fmt.Errorf("create spool directory: %w", err)
	}
	db, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("open spool: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable spool WAL: %w", err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout=5000`); err != nil {
		db.Close()
		return nil, fmt.Errorf("set spool busy timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate spool: %w", err)
	}
	return &Spool{db: db, cfg: cfg}, nil
}

func (s *Spool) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Spool) Path() string {
	if s == nil {
		return ""
	}
	return s.cfg.Path
}

// PendingStats is the unacked backlog used by Agent spool gauges (A-19).
type PendingStats struct {
	Count     int
	Bytes     int64
	OldestAge time.Duration
}

// Stats returns unacked depth, payload bytes, and age of the oldest row.
func (s *Spool) Stats(ctx context.Context) (PendingStats, error) {
	if s == nil || s.db == nil {
		return PendingStats{}, errors.New("spool is not open")
	}
	var count int
	var bytes int64
	var oldest sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0), MIN(created_at)
		FROM reports WHERE controller_acked = 0`).Scan(&count, &bytes, &oldest)
	if err != nil {
		return PendingStats{}, err
	}
	stats := PendingStats{Count: count, Bytes: bytes}
	if oldest.Valid {
		stats.OldestAge = time.Since(spoolTime(oldest.Int64))
	}
	return stats, nil
}

// CreatedAt returns when a report was first ingested into the spool.
func (s *Spool) CreatedAt(ctx context.Context, snapshot executionreport.Snapshot) (time.Time, bool, error) {
	if s == nil || s.db == nil {
		return time.Time{}, false, errors.New("spool is not open")
	}
	var created int64
	err := s.db.QueryRowContext(ctx, `
		SELECT created_at FROM reports
		WHERE job_id = ? AND producer_id = ? AND report_id = ?`,
		snapshot.JobID, snapshot.ProducerID, snapshot.ReportID,
	).Scan(&created)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return spoolTime(created), true, nil
}

// spoolTime reads a report's created_at: milliseconds since the epoch.
// Seconds made the delivery time (ingest to the hub's ack) up to a second
// long whatever it was, a uniform 0-1 s on every report.
func spoolTime(value int64) time.Time {
	return time.UnixMilli(value).UTC()
}
