// SPDX-License-Identifier: Apache-2.0

// Package envelope serializes the backend-neutral job spec carried inside the gRPC
// control channel. It implements the versioned envelope {apiVersion, kind, spec}
// from architecture §7: the channel stays stable while the spec schema evolves.
package envelope

import (
	"encoding/json"
	"fmt"
	"strings"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/secretsource"
)

// apiVersion of the job spec schema carried in the envelope. The agent and the
// control plane are released together; a field one side does not know is a
// loud decode error, never a silently dropped setting.
const apiVersion = "zenthros.ai/v1"

// Encode packs a JobSpec into a versioned envelope. Identity fields (id, name,
// namespace, kind) live on the envelope; every other JobSpec field travels in
// spec_json exactly as the controller computed it. SecretSources remain
// references; secret values are never accepted by this wire contract.
func Encode(spec jobspec.JobSpec) (*agentpb.JobEnvelope, error) {
	if err := jobspec.ValidateWorkload(spec); err != nil {
		return nil, fmt.Errorf("encode job spec: %w", err)
	}
	if err := secretsource.Validate(spec.SecretSources); err != nil {
		return nil, fmt.Errorf("encode job spec: invalid secret source: %w", err)
	}
	raw, err := marshalSpec(spec)
	if err != nil {
		return nil, fmt.Errorf("encode job spec: %w", err)
	}
	return &agentpb.JobEnvelope{
		Id:         spec.ID,
		Name:       spec.Name,
		Namespace:  spec.Namespace,
		Kind:       spec.Kind,
		ApiVersion: apiVersion,
		SpecJson:   raw,
	}, nil
}

// Decode reconstructs a JobSpec from an envelope, restoring the identity fields
// from the envelope itself. A field this build does not know is rejected with
// the decoder's own message: an agent older than its control plane must fail
// the job loudly rather than run it without a setting it was asked to honour.
func Decode(env *agentpb.JobEnvelope) (jobspec.JobSpec, error) {
	if env == nil {
		return jobspec.JobSpec{}, fmt.Errorf("decode: nil envelope")
	}
	if !supportedAPIVersion(env.ApiVersion) {
		return jobspec.JobSpec{}, fmt.Errorf("decode: unsupported apiVersion %q (supported major: zenthros.ai/v1)", env.ApiVersion)
	}
	spec, err := unmarshalSpec(env.SpecJson)
	if err != nil {
		return jobspec.JobSpec{}, fmt.Errorf("decode job spec (apiVersion=%s): %w", env.ApiVersion, err)
	}
	if err := secretsource.Validate(spec.SecretSources); err != nil {
		return jobspec.JobSpec{}, fmt.Errorf("decode job spec: invalid secret source: %w", err)
	}
	spec.ID, spec.Name, spec.Namespace, spec.Kind = env.Id, env.Name, env.Namespace, env.Kind
	if err := jobspec.ValidateWorkload(spec); err != nil {
		return jobspec.JobSpec{}, fmt.Errorf("decode job spec: %w", err)
	}
	return spec, nil
}

func marshalSpec(spec jobspec.JobSpec) (string, error) {
	raw, err := json.Marshal(spec)
	return string(raw), err
}

func unmarshalSpec(raw string) (jobspec.JobSpec, error) {
	var spec jobspec.JobSpec
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&spec)
	return spec, err
}

func supportedAPIVersion(version string) bool {
	parts := strings.Split(version, "/")
	return len(parts) == 2 && parts[0] == "zenthros.ai" && parts[1] == "v1"
}
