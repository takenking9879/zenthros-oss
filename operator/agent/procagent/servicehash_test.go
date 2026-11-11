// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

// hashSpecWithoutService is the fingerprint of serviceHashSpec() as agents
// before Services wrote it: a running job's resubmit after the upgrade is
// adopted only if this does not move.
const hashSpecWithoutService = "e4d8dda9f6fa930543ae5239c901f1c9889d234aec5bf54c767b857e33a983e5"

func serviceHashSpec() (jobspec.JobSpec, map[string]string) {
	spec := jobspec.JobSpec{ID: "job-1", Kind: v1.WorkloadKindJob, Command: []string{"python3", "-m", "http.server"}, Image: "python:3.13"}
	return spec, map[string]string{"MODEL": "qwen", jobspec.EnvJobID: "derived"}
}

func TestSpecHashOfAJobWithoutServiceIsTheOneBeforeServices(t *testing.T) {
	spec, env := serviceHashSpec()
	if got := specHash(spec, env); got != hashSpecWithoutService {
		t.Fatalf("specHash = %s, want %s: a job that is no Service must keep its fingerprint", got, hashSpecWithoutService)
	}
}

func TestSpecHashChangesWithService(t *testing.T) {
	spec, env := serviceHashSpec()
	plain := specHash(spec, env)
	spec.Service = &v1.ServiceSpec{Ports: []v1.ServicePort{{Name: "http", Port: 8000}}}
	if specHash(spec, env) == plain {
		t.Fatal("a Service's ports must be part of its fingerprint")
	}
}
