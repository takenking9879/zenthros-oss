// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

// envNamePattern is a POSIX environment name. The unit writes names raw
// inside Environment="NAME=value"; a quote or a newline in a name would end
// that line and let the rest become directives of its own (ExecStartPre=...).
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// jobIDPattern: a job's ID names its unit and its directories: a UID, or an
// attempt's <uid>-a<n>. Never a path of its own (.., /).
var jobIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// workspacePattern: a Workspace's name is a DNS label, the API server's
// rule for it. It names the job's directories, its user and the paths its
// unit may write (ReadWritePaths): a quote in it would open others.
var workspacePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// ValidateUnitInputs refuses what renderUnit cannot write safely: a job ID or
// a Workspace that is not one plain name, an env name
// that is not a plain identifier, and a directory the unit runs in
// (effectiveWorkDir: the workingDir, else the source's hostPath) that is not
// an absolute path on one line (19#7). The CRD rejects both already; the
// agent still holds its own line, as the unit is a trust boundary: a spec
// that reaches it without the CRD (the Pipe, a forged request) cannot write
// directives of its own.
func ValidateUnitInputs(spec jobspec.JobSpec, env map[string]string) error {
	if !jobIDPattern.MatchString(spec.ID) {
		return fmt.Errorf("%w: job id %q is not a name a unit and its directories can take", backend.ErrInvalid, spec.ID)
	}
	if spec.Workspace != "" && !workspacePattern.MatchString(spec.Workspace) {
		return fmt.Errorf("%w: Workspace %q is not a DNS label", backend.ErrInvalid, spec.Workspace)
	}
	for name := range env {
		if !envNamePattern.MatchString(name) {
			return fmt.Errorf("%w: env name %q is not a valid environment variable name", backend.ErrInvalid, name)
		}
	}
	for field, dir := range map[string]string{"workingDir": spec.WorkingDir, "source.hostPath": spec.Source.HostPath} {
		if dir == "" {
			continue
		}
		if err := validateUnitPath(field, dir); err != nil {
			return err
		}
	}
	return nil
}

func validateUnitPath(field, value string) error {
	if strings.ContainsFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return fmt.Errorf("%w: %s contains a control character", backend.ErrInvalid, field)
	}
	if !path.IsAbs(value) {
		return fmt.Errorf("%w: %s %q must be an absolute path", backend.ErrInvalid, field, value)
	}
	return nil
}
