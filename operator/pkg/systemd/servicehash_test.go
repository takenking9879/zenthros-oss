// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

// hashSpecWithoutService is the fingerprint of testSpec() with its env: the
// one fingerprint both host executors write (pkg/jobspec/fingerprint). A unit
// an agent installed is adopted after its upgrade only if this does not move.
const hashSpecWithoutService = "49fa6b998d4f4daf10580ee0cf95bb6d8b8b1d806329197fe553fd42ec4ac506"

func serviceHashSpec() (jobspec.JobSpec, map[string]string) {
	return testSpec(), map[string]string{"MODEL": "qwen", jobspec.EnvJobID: "derived"}
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
	spec.Service = &v1.ServiceSpec{Ports: []v1.ServicePort{{Name: "http", Port: 8000, Protocol: v1.ProtocolHTTP}}}
	withPort := specHash(spec, env)
	if withPort == plain {
		t.Fatal("a Service's ports must be part of its fingerprint")
	}
	spec.Service.Ports[0].Port = 8001
	if specHash(spec, env) == withPort {
		t.Fatal("another port is another spec")
	}
}
