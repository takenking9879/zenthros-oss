// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"errors"
	"strings"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

// A name or directory that could end its unit line must never reach the
// unit file: it would let a job spec add its own systemd directives.
func TestValidateUnitInputsRejectsLineBreakout(t *testing.T) {
	cases := map[string]struct {
		spec jobspec.JobSpec
		env  map[string]string
	}{
		"env name with quote and newline": {env: map[string]string{"A=1\"\nExecStartPre=/bin/sh -c id\n#": "x"}},
		"env name with space":             {env: map[string]string{"A B": "x"}},
		"env name starting with digit":    {env: map[string]string{"1A": "x"}},
		"workingDir with newline":         {spec: jobspec.JobSpec{WorkingDir: "/srv\nExecStartPre=/bin/id"}},
		"relative workingDir":             {spec: jobspec.JobSpec{WorkingDir: "srv/app"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidateUnitInputs(tc.spec, tc.env); !errors.Is(err, backend.ErrInvalid) {
				t.Fatalf("error = %v, want ErrInvalid", err)
			}
		})
	}
	ok := jobspec.JobSpec{ID: "uid-1", Workspace: "team-a", WorkingDir: "/srv/my app/%x"}
	if err := ValidateUnitInputs(ok, map[string]string{"PYTHONPATH": "/a:/b", "_X1": "multi\nline value"}); err != nil {
		t.Fatalf("valid inputs refused: %v", err)
	}
	// Values stay free-form: they are escaped, not validated.
	unit := RenderUnit(ok, map[string]string{"V": "a\"\nExecStartPre=/bin/id"}, Config{}, "")
	for _, line := range strings.Split(unit, "\n") {
		if strings.HasPrefix(line, "ExecStartPre=") {
			t.Fatalf("value broke out of its Environment line:\n%s", unit)
		}
	}
}

// The unit's WorkingDirectory falls back to the source's hostPath: a newline
// there wrote directives of its own as surely as one in workingDir (19#7).
func TestAHostPathTheUnitRunsInIsValidatedToo(t *testing.T) {
	spec := jobspec.JobSpec{ID: "uid-1", Source: v1.Source{HostPath: "/srv\nExecStartPre=/bin/sh -c id"}}
	if err := ValidateUnitInputs(spec, nil); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("ValidateUnitInputs = %v, want ErrInvalid", err)
	}
}
