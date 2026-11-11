// SPDX-License-Identifier: Apache-2.0

package executionreport

// ReplayAction is how a store applies an incoming snapshot against what it
// already has for (workspace, job, producer).
const (
	ReplayInsertCurrent = "insert_current"
	ReplayInsertHistory = "insert_history"
	ReplayIdempotent    = "idempotent"
)

// DecideReplay implements duplicate and ordering rules against current, the
// producer's current report (ReportID "" for none):
//
//   - Same report_id is idempotent: ACK the original sequence, do not rewrite
//     current or append history.
//   - A new report_id that outranks current (Outranks) replaces current and
//     appends history.
//   - A new report_id that does not appends history only (out-of-order).
//     Current is unchanged.
//   - A report_id the store already holds (seen), sent again after its ack and
//     renumbered by the agent's spool, appends nothing new and never takes
//     current back: it is history.
//   - Sequence 0 is treated as "unassigned"; callers must assign before DecideReplay.
func DecideReplay(current, incoming Snapshot, seen bool) string {
	if incoming.ReportID == current.ReportID {
		return ReplayIdempotent
	}
	if seen {
		return ReplayInsertHistory
	}
	if current.ReportID == "" || Outranks(incoming, current) {
		return ReplayInsertCurrent
	}
	return ReplayInsertHistory
}

// Outranks is whether report a takes a producer's current report from b: the
// later attempt, then within one attempt a final over every report that is
// not, then the higher sequence. The agent numbers reports as its spool takes
// them, and a post the job had given up on was taken after its final (an
// agent frozen 96 s, 2026-11-09): numbered after the final, it is still the
// older report, and it hid the job's result. reportstore's putReport orders
// rows the same way.
func Outranks(a, b Snapshot) bool {
	if attemptA, attemptB := AttemptOf(a.Sequence), AttemptOf(b.Sequence); attemptA != attemptB {
		return attemptA > attemptB
	}
	if a.Final != b.Final {
		return a.Final
	}
	return a.Sequence > b.Sequence
}

// IsRetryableACK is true when the Agent should keep the spool row and replay
// after Controller or PostgreSQL recovery.
func IsRetryableACK(ack ControllerAck) bool {
	if ack.Accepted {
		return false
	}
	return ack.ErrorCode == CodeStoreUnavailable
}

// AttemptFloor is the sequence a job's attempt numbers its reports above:
// the attempt in the high 32 bits. Every report of an attempt sorts after
// every report of the attempts before it, whichever machine's spool
// numbered them; the first attempt (0) numbers from 1.
func AttemptFloor(attempt int32) int64 {
	return int64(attempt) << 32
}

// AttemptOf is the attempt a report's sequence was numbered in
// (AttemptFloor).
func AttemptOf(sequence int64) int32 {
	return int32(sequence >> 32)
}
