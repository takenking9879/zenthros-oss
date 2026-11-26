// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"strings"

	v1 "github.com/takenking9879/zenthros-oss/operator/api/v1"
)

// Data-reference env names. Adapters translate a job's durable object-storage
// references (template.artifacts) into these workload env vars so producers and
// consumers agree on the same reference without any platform-specific API. Only
// the reference strings travel through the control plane; raw data and workload
// metrics never cross the controller.
//
// DataOutputEnv is where the job writes its outputs. The control plane sets
// it to the output URI, as DataOutputURIEnv; a machine's agent makes it a
// folder of the job's (LocalOutput) and saves the folder to the URI before it
// reports the job's end, so open(), savefig() and a shell's > write there as
// anywhere (19). A pod's stays the URI: its agent shares no disk with it.
const (
	DataInputEnv     = "ZENTHROS_DATA_INPUT"
	DataOutputEnv    = "ZENTHROS_DATA_OUTPUT"
	DataOutputURIEnv = "ZENTHROS_DATA_OUTPUT_URI"
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
		env[DataOutputURIEnv] = artifacts.Output
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

// LocalOutput is env with DataOutputEnv the job's folder when it names a
// bucket (s3:// or gs://), and the URI it named: what the agent saves the
// folder to. A folder already (a job on the person's own computer) or none
// leaves env as it is, and the URI empty.
func LocalOutput(env map[string]string, folder string) (map[string]string, string) {
	uri := env[DataOutputEnv]
	if !strings.HasPrefix(uri, "s3://") && !strings.HasPrefix(uri, "gs://") {
		return env, ""
	}
	out := make(map[string]string, len(env)+1)
	for name, value := range env {
		out[name] = value
	}
	out[DataOutputEnv] = folder
	out[DataOutputURIEnv] = uri
	return out, uri
}
