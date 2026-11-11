// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zenthros/operator/agent/awssecret"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/liveness"
	"github.com/zenthros/operator/pkg/systemd"
)

// The hub detached this machine (zenthros delete): the stream ends with the
// detached cause, or a redial is refused as revoked. The agent stops every
// job it runs, then ends for good. A job left running on a detached machine
// reported to no one (lab, 2026-10-18). Any other end is redialed.
func TestADetachedAgentStopsItsJobsAndEnds(t *testing.T) {
	for name, cause := range map[string]error{
		"its session closed": liveness.DetachedError("its access was removed"),
		"its redial refused": fmt.Errorf("recv register ack: %w", liveness.DetachedError(`backend "vm-test" is revoked`)),
	} {
		t.Run(name, func(t *testing.T) {
			a, runner := testAgent("")
			spec := jobspec.JobSpec{ID: "uid-7", Name: "nap", Namespace: "tenant-default", Kind: v1.WorkloadKindJob, Entrypoint: "sleep 600"}
			job, err := envelope.Encode(spec)
			if err != nil {
				t.Fatal(err)
			}
			if resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r1", Job: job}); resp.GetError() != "" {
				t.Fatalf("Submit: %s", resp.GetError())
			}
			runner.commands = nil

			end := a.detached(context.Background(), cause)
			if !errors.Is(end, ErrDetached) {
				t.Fatalf("Run goes on after %v (got %v), want ErrDetached", cause, end)
			}
			stopped := false
			for _, c := range runner.commands {
				if contains(c, "zenthros-uid-7.service") {
					stopped = true
				}
			}
			if !stopped || a.tracker.Tracked("zenthros-uid-7.service") {
				t.Fatalf("the detached machine's job kept running (ran %v)", runner.commands)
			}
		})
	}
	a, runner := testAgent("")
	for _, other := range []error{nil, status.Error(codes.Unavailable, "making room"), errors.New("connection reset"), status.Error(codes.PermissionDenied, liveness.Detached)} {
		if end := a.detached(context.Background(), other); end != nil {
			t.Fatalf("Run ends after %v: %v, want a redial", other, end)
		}
	}
	if len(runner.commands) != 0 {
		t.Fatalf("an ordinary end stopped jobs: %v", runner.commands)
	}
}

// A Cancel that fails is asked again: the agent exits only once its jobs are
// stopped. Exiting at the first failure left the job running with no agent
// to stop it (05#1).
func TestADetachedAgentAsksAgainUntilItsJobsStop(t *testing.T) {
	detachedRetry = time.Millisecond
	t.Cleanup(func() { detachedRetry = time.Second })
	a, runner := testAgent("")
	spec := jobspec.JobSpec{ID: "uid-8", Name: "nap", Namespace: "tenant-default", Kind: v1.WorkloadKindJob, Entrypoint: "sleep 600"}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatal(err)
	}
	if resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r1", Job: job}); resp.GetError() != "" {
		t.Fatalf("Submit: %s", resp.GetError())
	}
	refusals := 2
	runner.mu.Lock()
	runner.fail = func(cmd string) error {
		if contains(cmd, "rm -f") && contains(cmd, "zenthros-uid-8.service") && refusals > 0 {
			refusals--
			return errors.New("rm: cannot remove zenthros-uid-8.service: Read-only file system")
		}
		return nil
	}
	runner.mu.Unlock()

	if end := a.detached(context.Background(), liveness.DetachedError("its access was removed")); !errors.Is(end, ErrDetached) {
		t.Fatalf("got %v, want ErrDetached", end)
	}
	if refusals != 0 || a.tracker.Tracked("zenthros-uid-8.service") {
		t.Fatalf("the agent ended with its job still running (%d refusals left)", refusals)
	}
}

// No Cancel comes to a detached machine, and its agent stops for good: what a
// job that ended left there, its secrets above all, goes with the agent's
// access, or it stays for good. A job that ended is no longer tracked, so
// stopping what the agent tracks left it (lab, 2026-11-05: the finished Jobs of
// every attached cluster outlived their detach).
func TestADetachedMachineKeepsNothingOfAJobThatEnded(t *testing.T) {
	machine := &fakeSystemd{files: map[string]string{}, active: map[string]bool{}}
	root := t.TempDir()
	executor := SystemdExecutor{Manager: systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}, machine, logr.Discard()), Secrets: awssecret.Config{Root: root}}
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm"}, executor, logr.Discard())
	spec := vmTestSpec()
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatal(err)
	}
	resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r1", Job: job})
	if resp.GetError() != "" {
		t.Fatalf("Submit: %s", resp.GetError())
	}
	secrets := filepath.Join(root, spec.ID)
	if err := os.MkdirAll(secrets, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secrets, "token"), []byte("plaintext"), 0o600); err != nil {
		t.Fatal(err)
	}
	// It ended and its end was delivered: the agent no longer tracks it.
	machine.end(resp.GetHandleName())
	a.tracker.Remove(resp.GetHandleName())

	if end := a.detached(context.Background(), liveness.DetachedError("its access was removed")); !errors.Is(end, ErrDetached) {
		t.Fatalf("got %v, want ErrDetached", end)
	}
	if _, installed := machine.files["/etc/systemd/system/"+resp.GetHandleName()]; installed {
		t.Errorf("the unit of a job that ended stayed on the detached machine")
	}
	if _, err := os.Stat(secrets); !os.IsNotExist(err) {
		t.Errorf("the secrets of a job that ended stayed on the detached machine (stat: %v)", err)
	}
}
