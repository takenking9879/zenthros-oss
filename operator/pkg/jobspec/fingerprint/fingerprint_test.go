// SPDX-License-Identifier: Apache-2.0

package fingerprint

import (
	"testing"

	"github.com/zenthros/operator/pkg/jobspec"
)

// An empty list is no list: a resubmit decoded with [] for an absent one
// adopts the running workload instead of failing SPEC_MISMATCH.
func TestEmptyListsAreNoneInTheFingerprint(t *testing.T) {
	spec := jobspec.JobSpec{ID: "job-1", Entrypoint: "python train.py"}
	empty := spec
	empty.Command, empty.Args = []string{}, []string{}
	if Of(spec, nil) != Of(empty, nil) {
		t.Fatal("empty command and args changed the fingerprint")
	}
	empty.Args = []string{"--epochs", "3"}
	if Of(spec, nil) == Of(empty, nil) {
		t.Fatal("args are part of the fingerprint")
	}
}

// What the agent and the control plane set per run is not the spec:
// moving the report socket changed ZENTHROS_REPORT_ENDPOINT under every
// running unit, and its resubmit failed SPEC_MISMATCH. The user's env is.
func TestDerivedEnvIsNotTheSpec(t *testing.T) {
	spec := jobspec.JobSpec{ID: "job-1", Entrypoint: "python train.py"}
	env := map[string]string{"A": "b", jobspec.EnvJobID: spec.ID, jobspec.EnvReportEndpoint: "unix:///var/run/zenthros/agent/reports.sock",
		jobspec.EnvAttempt: "0", jobspec.EnvReportToken: "t0"}
	moved := map[string]string{"A": "b", jobspec.EnvJobID: "other", jobspec.EnvReportEndpoint: "unix:///elsewhere/reports.sock",
		jobspec.EnvAttempt: "1", jobspec.EnvReportToken: "t1"}
	if Of(spec, env) != Of(spec, moved) {
		t.Fatal("a derived variable changed the fingerprint")
	}
	moved["A"] = "c"
	if Of(spec, env) == Of(spec, moved) {
		t.Fatal("the user's env is part of the fingerprint")
	}
}
