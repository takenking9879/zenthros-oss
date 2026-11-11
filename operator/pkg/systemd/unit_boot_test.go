// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"strings"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

func TestJobUnitIsNotEnabledAcrossReboot(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	unit := RenderUnit(jobspec.JobSpec{
		ID:         "uid-oneshot",
		Name:       "hello",
		Kind:       v1.WorkloadKindRayJob,
		Entrypoint: "echo hi",
	}, nil, cfg, "")
	if strings.Contains(unit, "WantedBy=multi-user.target") {
		t.Fatal("job unit is WantedBy multi-user.target; a reboot reruns a finished job the agent does not adopt")
	}
}
