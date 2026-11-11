// SPDX-License-Identifier: Apache-2.0

package backend

import (
	v1 "github.com/zenthros/operator/api/v1"
)

// Data-reference env names. Adapters translate a job's durable object-storage
// references (template.artifacts) into these workload env vars so producers and
// consumers agree on the same reference without any platform-specific API. Only
// the reference strings travel through the control plane; raw data and workload
// metrics never cross the controller.
const (
	DataInputEnv  = "ZENTHROS_DATA_INPUT"
	DataOutputEnv = "ZENTHROS_DATA_OUTPUT"
)

// DataReferenceEnv translates a job's artifact URIs into durable data-reference
// env vars. Returns nil when the job declares no references.
func DataReferenceEnv(artifacts v1.Artifacts) map[string]string {
	env := map[string]string{}
	if artifacts.Input != "" {
		env[DataInputEnv] = artifacts.Input
	}
	if artifacts.Output != "" {
		env[DataOutputEnv] = artifacts.Output
	}
	if len(env) == 0 {
		return nil
	}
	return env
}

// WithDataReferences returns env with the durable data-reference vars for
// artifacts layered on top. The artifact-derived references win over explicit
// env of the same name so the staged-pipeline contract stays deterministic.
// Returns env unchanged when the job declares no references.
func WithDataReferences(env map[string]string, artifacts v1.Artifacts) map[string]string {
	refs := DataReferenceEnv(artifacts)
	if len(refs) == 0 {
		return env
	}
	merged := make(map[string]string, len(refs)+len(env))
	for name, value := range env {
		merged[name] = value
	}
	for name, value := range refs {
		merged[name] = value
	}
	return merged
}
