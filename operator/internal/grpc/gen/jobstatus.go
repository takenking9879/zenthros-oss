// SPDX-License-Identifier: Apache-2.0

package agentpb

// ExitCodeIfAny is what the job's process returned, nil when it returned nothing
// or the status is absent. GetExitCode cannot tell none from 0, and an agent may
// send a message with no status: reading the field directly panics on it.
func (s *JobStatus) ExitCodeIfAny() *int32 {
	if s == nil {
		return nil
	}
	return s.ExitCode
}
