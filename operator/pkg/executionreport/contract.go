// SPDX-License-Identifier: Apache-2.0

// Package executionreport defines the versioned ExecutionReport primitive.
// Each report is a complete JSON snapshot for one producer. Deep merge, patch
// semantics, implicit accumulation, and telemetry use are out of scope.
package executionreport

import (
	"encoding/json"
	"time"
)

const (
	APIVersion = "zenthros.ai/v1"
	Kind       = "ExecutionReport"

	MaxPayloadBytes = 256 * 1024
	// MaxFinalReportsPerJob bounds what a job's machine can keep for
	// DefaultFinalRetention: about 256 MiB at the largest payload.
	MaxFinalReportsPerJob = 1024
	MaxArtifactCount      = 32
	MaxURILength          = 2048
	MaxProducerIDLength   = 128
	MaxReportIDLength     = 64
	MaxJobIDLength        = 253
	MaxNameLength         = 64

	DefaultMaxSpoolReports = 1024
	DefaultMaxSpoolBytes   = 64 * 1024 * 1024

	DefaultIntermediateRetention = 24 * time.Hour
	DefaultFinalRetention        = 30 * 24 * time.Hour
)

const (
	CodePayloadTooLarge    = "PAYLOAD_TOO_LARGE"
	CodeInvalidJSON        = "INVALID_JSON"
	CodeInvalidSnapshot    = "INVALID_SNAPSHOT"
	CodeInvalidArtifactURI = "INVALID_ARTIFACT_URI"
	CodeUnauthorized       = "UNAUTHORIZED"
	CodeSpoolFull          = "SPOOL_FULL"
	CodeRateLimited        = "RATE_LIMITED"
	CodeRejected           = "REJECTED"
	CodeJobMismatch        = "JOB_MISMATCH"
	CodeWorkspaceForbidden = "WORKSPACE_FORBIDDEN"
	CodeStoreUnavailable   = "STORE_UNAVAILABLE"
	CodeDuplicateReplay    = "DUPLICATE"
	CodeOutOfOrder         = "OUT_OF_ORDER"
	// A checkpoint refused for good: the replacement already took the
	// cursor, so no retry of the commit lands.
	CodeCheckpointFrozen = "CHECKPOINT_FROZEN"
	// A checkpoint refused while the job's lease is not live: a handover
	// may still land, and the commit may be sent again.
	CodeLeaseNotLive = "LEASE_NOT_LIVE"
)

// Snapshot is one complete ExecutionReport. Payload is the full producer
// dictionary at publish time; receivers never merge it with a prior snapshot.
type Snapshot struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	JobID      string          `json:"job_id"`
	ProducerID string          `json:"producer_id"`
	ReportID   string          `json:"report_id"`
	Sequence   int64           `json:"sequence,omitempty"`
	ProducedAt time.Time       `json:"produced_at"`
	Payload    json.RawMessage `json:"payload"`
	Artifacts  []ArtifactRef   `json:"artifacts,omitempty"`
	Final      bool            `json:"final,omitempty"`
}

// ArtifactRef is a data-plane URI. Zenthros stores the reference only.
type ArtifactRef struct {
	URI  string `json:"uri"`
	Name string `json:"name,omitempty"`
}

// LocalReceipt is the Agent ACK: the report is durable on the local SQLite
// spool, independent of Controller or PostgreSQL availability.
type LocalReceipt struct {
	ReportID     string    `json:"report_id"`
	ProducerID   string    `json:"producer_id"`
	Sequence     int64     `json:"sequence"`
	LocalAckedAt time.Time `json:"local_acked_at"`
}

// ControllerAck is returned after PostgreSQL commit. The Agent deletes the
// spool row only after this ACK. A rejected ACK is not retried with the same
// bytes unless the error is retryable (STORE_UNAVAILABLE).
type ControllerAck struct {
	ReportID   string `json:"report_id"`
	JobID      string `json:"job_id"`
	ProducerID string `json:"producer_id"`
	Sequence   int64  `json:"sequence"`
	Accepted   bool   `json:"accepted"`
	ErrorCode  string `json:"error_code,omitempty"`
	Error      string `json:"error,omitempty"`
}

// Record is a stored snapshot plus Controller-derived identity. Workspace and
// backend are never taken from the workload payload.
type Record struct {
	WorkspaceID string
	BackendID   string
	ReceivedAt  time.Time
	Snapshot    Snapshot
}

// StatusMetadata is the only report data written to ZenthrosJob.status.
type StatusMetadata struct {
	LastReportID string
	ProducerID   string
	Sequence     int64
	Timestamp    time.Time
}
