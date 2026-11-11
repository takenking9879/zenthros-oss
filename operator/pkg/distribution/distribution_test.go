// SPDX-License-Identifier: Apache-2.0

package distribution

import (
	"strings"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
)

func independent(count int32, policy string) *v1.Distribution {
	return &v1.Distribution{Mode: v1.DistributionIndependent, Count: count, FailurePolicy: policy}
}

func running(rank int32) Member {
	return Member{Rank: rank, Present: true, Phase: v1.PhaseRunning}
}

func TestAggregateIndependentFailurePolicy(t *testing.T) {
	failed := Member{Rank: 1, Present: true, Phase: v1.PhaseFailed, Reason: v1.ReasonProcessFailed, Message: "shard 1: bad row 17"}
	ok := Member{Rank: 0, Present: true, Phase: v1.PhaseSucceeded}

	out := Aggregate(independent(3, ""), []Member{ok, failed, running(2)})
	if out.Phase != v1.PhaseFailed || !out.CancelRest || !strings.Contains(out.Message, "shard 1: bad row 17") {
		t.Fatalf("FailFast (default) must fail at the first failed task and cancel the rest: %+v", out)
	}
	out = Aggregate(independent(3, v1.FailurePolicyContinueOnFailure), []Member{ok, failed, running(2)})
	if out.Phase != v1.PhaseRunning || out.CancelRest {
		t.Fatalf("ContinueOnFailure keeps the others running: %+v", out)
	}
	done := Member{Rank: 2, Present: true, Phase: v1.PhaseSucceeded}
	out = Aggregate(independent(3, v1.FailurePolicyContinueOnFailure), []Member{ok, failed, done})
	if out.Phase != v1.PhaseFailed || !strings.Contains(out.Message, "shard 1: bad row 17") {
		t.Fatalf("ContinueOnFailure fails at the end with the task's words: %+v", out)
	}
	out = Aggregate(independent(2, ""), []Member{ok, {Rank: 1, Present: true, Phase: v1.PhaseSucceeded}})
	if out.Phase != v1.PhaseSucceeded {
		t.Fatalf("all tasks succeeded: %+v", out)
	}
	// A lost Independent task is its own job's business: it re-seats alone,
	// and the job is not failed or restarted for it.
	lost := Member{Rank: 1, Present: true, Phase: v1.PhasePending, Reason: v1.ReasonWorkerLost}
	out = Aggregate(independent(2, ""), []Member{running(0), lost})
	if out.Phase != v1.PhaseRunning || out.CancelRest {
		t.Fatalf("a re-seating task does not touch the others: %+v", out)
	}
}

func TestTaskIndexEnv(t *testing.T) {
	env := RankEnv(independent(6, ""), 5)
	if env["TASK_INDEX"] != "5" || env["TASK_COUNT"] != "6" {
		t.Fatalf("env %v", env)
	}
	if len(env) != 2 {
		t.Fatalf("a task sees TASK_INDEX and TASK_COUNT only: %v", env)
	}
	for _, k := range []string{"RANK", "WORLD_SIZE", "MASTER_PORT"} {
		if _, ok := env[k]; ok {
			t.Fatalf("Independent tasks do not rendezvous; %s set", k)
		}
	}
}

func TestMemberName(t *testing.T) {
	if MemberName("train", 3) != "train-3" {
		t.Fatal(MemberName("train", 3))
	}
}
