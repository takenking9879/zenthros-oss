// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/zenthros/operator/pkg/containerd"
)

// DefaultPreemptDir is where a VM agent writes interruption notices. It is
// under the report socket's directory, which a containerd job already sees,
// and bound read-only over it (pkg/containerd).
const DefaultPreemptDir = containerd.PreemptDir

// noticeFile is what a job reads at $ZENTHROS_PREEMPT_FILE.
type noticeFile struct {
	Source   string `json:"source"`
	Action   string `json:"action"`
	Deadline string `json:"deadline,omitempty"`
	NotedAt  string `json:"notedAt"`
}

func preemptFile(dir, jobID string) string {
	return filepath.Join(dir, jobID+".json")
}

// preemptSignaler sends a job the signal it asked for on a notice.
type preemptSignaler interface {
	SignalPreempt(ctx context.Context, handle string) (bool, error)
}

// deliverNotice tells every job on this machine that the machine is being
// taken back. Each job's file is written, whole, before any signal: a
// process woken by the signal always finds it. A file that cannot be
// written is logged and its job is still signalled. A job whose delivery
// failed (its signal, or its file when there is no signal) is told again on
// the next raise (05#7).
func (a *Agent) deliverNotice(ctx context.Context, notice preemptionNotice) {
	jobs := a.unnoticed(a.tracker.Snapshot())
	signaler, signals := a.executor.(preemptSignaler)
	if a.cfg.PreemptDir != "" {
		body := noticeFile{Source: notice.Source, Action: notice.Action, NotedAt: time.Now().UTC().Format(time.RFC3339)}
		if !notice.Deadline.IsZero() {
			body.Deadline = notice.Deadline.UTC().Format(time.RFC3339)
		}
		for _, job := range jobs {
			if err := writeNoticeFile(a.cfg.PreemptDir, job.jobID, body); err != nil {
				a.logger.Error(err, "failed to write the interruption notice", "job_id", job.jobID)
				if !signals {
					a.untold(job.jobID)
				}
			}
		}
	}
	if !signals {
		return
	}
	for _, job := range jobs {
		sent, err := signaler.SignalPreempt(ctx, job.handleName)
		if err != nil {
			a.logger.Error(err, "failed to signal the interruption notice", "job_id", job.jobID, "handle", job.handleName)
			a.untold(job.jobID)
		} else if sent {
			a.logger.Info("signalled the interruption notice", "job_id", job.jobID, "handle", job.handleName)
		}
	}
}

// untold forgets that jobID was told, for the next raise to try again.
func (a *Agent) untold(jobID string) {
	a.noticedMu.Lock()
	defer a.noticedMu.Unlock()
	delete(a.noticed, jobID)
}

// unnoticed keeps the jobs the notice was not delivered to yet, and records
// them as told: each control stream raises the notice again, and a second
// signal can kill the checkpoint the first one started.
func (a *Agent) unnoticed(jobs []activeJob) []activeJob {
	a.noticedMu.Lock()
	defer a.noticedMu.Unlock()
	if a.noticed == nil {
		a.noticed = map[string]bool{}
	}
	fresh := jobs[:0:0]
	for _, job := range jobs {
		if !a.noticed[job.jobID] {
			a.noticed[job.jobID] = true
			fresh = append(fresh, job)
		}
	}
	return fresh
}

// writeNoticeFile writes the notice beside its final name and renames it
// into place, so a reader never sees half of it.
func writeNoticeFile(dir, jobID string, body noticeFile) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".notice-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), preemptFile(dir, jobID)); err != nil {
		return fmt.Errorf("publish notice for %s: %w", jobID, err)
	}
	return nil
}
