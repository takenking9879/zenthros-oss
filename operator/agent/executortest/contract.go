// SPDX-License-Identifier: Apache-2.0

// Package executortest is what every agent executor promises, as one test
// each executor runs: systemd units on a VM, processes in a container,
// native resources in a cluster. A promise kept by two of them and not the
// third was a fix made in one place (R2).
package executortest

import (
	"context"
	"errors"
	"slices"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
)

// Executor is the part of agent.Executor the contract exercises (the agent
// package's own tests use this one, so it cannot import that one).
type Executor interface {
	Submit(ctx context.Context, spec jobspec.JobSpec, env map[string]string) (string, error)
	Cancel(ctx context.Context, handle string, job owner.Ref) error
	Status(ctx context.Context, handle string) (backend.JobState, error)
	EndedJobs(ctx context.Context, backendID string) ([]backend.InstalledJob, error)
}

// Harness gives the contract a fresh executor and a job it runs that does
// not end on its own, a way to end that job as if it had, and a handle of this
// executor's kind that names nothing.
type Harness struct {
	New     func(t *testing.T) Executor
	Spec    jobspec.JobSpec
	End     func(t *testing.T, e Executor, handle string)
	Missing string
}

// Contract runs every promise against h.
func Contract(t *testing.T, h Harness) {
	ctx := context.Background()
	job := owner.Ref{Kind: "ZenthrosJob", JobID: h.Spec.ID}
	// A job that ended keeps what it left, secrets included, until its Cancel:
	// a detached machine, which no Cancel reaches, asks for these and cancels
	// them itself. A running one is the tracker's, and one cancelled is gone.
	t.Run("a job that ended is listed until its Cancel", func(t *testing.T) {
		const self = "backend-self"
		e := h.New(t)
		spec := h.Spec
		spec.TargetBackend = self
		handle, err := e.Submit(ctx, spec, nil)
		if err != nil {
			t.Fatal(err)
		}
		listed := func() []backend.InstalledJob {
			t.Helper()
			jobs, err := e.EndedJobs(ctx, self)
			if err != nil {
				t.Fatal(err)
			}
			return jobs
		}
		if jobs := listed(); len(jobs) != 0 {
			t.Fatalf("a job that runs is listed as ended: %v", jobs)
		}
		h.End(t, e, handle)
		if jobs, want := listed(), []backend.InstalledJob{{JobID: spec.ID, Handle: handle}}; !slices.Equal(jobs, want) {
			t.Fatalf("ended = %v, want %v", jobs, want)
		}
		if err := e.Cancel(ctx, handle, job); err != nil {
			t.Fatal(err)
		}
		if jobs := listed(); len(jobs) != 0 {
			t.Fatalf("a job cancelled is still listed: %v", jobs)
		}
	})
	t.Run("a resubmit is the same job", func(t *testing.T) {
		e := h.New(t)
		first, err := e.Submit(ctx, h.Spec, nil)
		if err != nil {
			t.Fatal(err)
		}
		again, err := e.Submit(ctx, h.Spec, nil)
		if err != nil || again != first {
			t.Fatalf("resubmit = %q, %v; want %q", again, err, first)
		}
	})
	t.Run("a submitted job has not ended", func(t *testing.T) {
		e := h.New(t)
		handle, err := e.Submit(ctx, h.Spec, nil)
		if err != nil {
			t.Fatal(err)
		}
		state, err := e.Status(ctx, handle)
		if err != nil || state.Phase.Terminal() {
			t.Fatalf("Status of a job just submitted = %s, %v", state.Phase, err)
		}
	})
	t.Run("a cancel is idempotent", func(t *testing.T) {
		e := h.New(t)
		handle, err := e.Submit(ctx, h.Spec, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			if err := e.Cancel(ctx, handle, job); err != nil {
				t.Fatalf("cancel %d: %v", i+1, err)
			}
		}
		if err := e.Cancel(ctx, h.Missing, job); err != nil {
			t.Fatalf("a cancel of what is not there: %v", err)
		}
	})
	t.Run("a cancelled job does not run", func(t *testing.T) {
		e := h.New(t)
		handle, err := e.Submit(ctx, h.Spec, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Cancel(ctx, handle, job); err != nil {
			t.Fatal(err)
		}
		state, err := e.Status(ctx, handle)
		if err == nil && (state.Phase == v1.PhaseRunning || state.Phase == v1.PhasePending || state.Phase == v1.PhaseSubmitted) {
			t.Fatalf("Status after cancel = %s", state.Phase)
		}
	})
	t.Run("what names nothing is not found", func(t *testing.T) {
		_, err := h.New(t).Status(ctx, h.Missing)
		if !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("Status of a handle that names nothing = %v, want ErrNotFound", err)
		}
	})
}
