// SPDX-License-Identifier: Apache-2.0

// Package fingerprint is the one fingerprint of what runs on a host, shared
// by the systemd and the process executors: a resubmit whose fingerprint is
// the recorded one adopts the running workload, and any other is a spec
// mismatch.
package fingerprint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"sort"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/secretsource"
)

// Of fingerprints the fields that define what runs and the env, without
// the variables the control plane or the agent derive per run (the report
// token, the attempt, the report endpoint): a resubmit after a checkpoint
// ack or a new lease is the same workload.
func Of(spec jobspec.JobSpec, env map[string]string) string {
	h := sha256.New()
	fields := struct {
		Entrypoint    string
		Command, Args []string
		WorkingDir    string
		Image         string
		Source        v1.Source
		Secrets       []string
		PreemptSignal string
	}{spec.Entrypoint, orNil(spec.Command), orNil(spec.Args), spec.WorkingDir, spec.Image, spec.Source, nil, spec.PreemptSignal}
	for _, ref := range spec.SecretSources {
		fields.Secrets = append(fields.Secrets, secretsource.Fingerprint(ref))
	}
	raw, _ := json.Marshal(fields)
	h.Write(raw)
	// Only a Service has it, so every other job keeps its fingerprint.
	if spec.Service != nil {
		io.WriteString(h, "\nservice="+jobspec.ServiceFingerprint(spec.Service))
	}
	names := make([]string, 0, len(env))
	for name := range env {
		if !jobspec.ControllerDerivedEnv(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		io.WriteString(h, "\nenv="+name+"="+env[name])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// orNil is an empty list as none: a resubmit decoded with [] for an absent
// list is the same spec.
func orNil(list []string) []string {
	if len(list) == 0 {
		return nil
	}
	return list
}
