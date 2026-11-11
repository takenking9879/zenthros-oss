// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/zenthros/operator/agent/agepayload"
	"github.com/zenthros/operator/agent/awssecret"
	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

type countingSecrets struct {
	kind  string
	calls int
}

func (c *countingSecrets) Type() string { return c.kind }
func (c *countingSecrets) Bindings(_ context.Context, spec jobspec.JobSpec) ([]secretfile.Binding, error) {
	c.calls++
	var out []secretfile.Binding
	for _, ref := range spec.SecretSources {
		if ref.Type == c.kind {
			out = append(out, secretfile.Binding{Ref: ref, Value: "v"})
		}
	}
	return out, nil
}

// A secret type no backend here resolves refuses the job before any backend
// reads a secret; known types resolve through their own backend.
func TestResolveSecretsRefusesUnknownTypeFirst(t *testing.T) {
	aws := &countingSecrets{kind: v1.SecretSourceTypeAWSSecretsManager}
	spec := jobspec.JobSpec{SecretSources: []v1.SecretSourceRef{
		{Type: v1.SecretSourceTypeAWSSecretsManager},
		{Type: v1.SecretSourceTypeKubernetes},
	}}
	_, err := resolveSecrets(context.Background(), spec, []secretBackend{aws})
	var secretErr *backend.SecretSourceError
	if !errors.As(err, &secretErr) || secretErr.Code != v1.ReasonUnsupportedSecretSource {
		t.Fatalf("err = %v, want %s", err, v1.ReasonUnsupportedSecretSource)
	}
	if aws.calls != 0 {
		t.Fatal("a backend resolved secrets for a job that is refused")
	}

	spec.SecretSources = spec.SecretSources[:1]
	bindings, err := resolveSecrets(context.Background(), spec, []secretBackend{aws})
	if err != nil || len(bindings) != 1 {
		t.Fatalf("bindings = %v err = %v", bindings, err)
	}
}

func TestMachineSecretBackendTypes(t *testing.T) {
	var types []string
	for _, b := range machineSecretBackends(awssecret.Config{}, agepayload.Config{}) {
		types = append(types, b.Type())
	}
	if len(types) != 2 || types[0] != v1.SecretSourceTypeAWSSecretsManager || types[1] != v1.SecretSourceTypeEncryptedPayload {
		t.Fatalf("machine secret types = %v", types)
	}
}

// A file under the default root is a default one: the job is told where
// its secrets dir is ($ZENTHROS_SECRETS_DIR), as for one with no path.
func TestAFileUnderTheDefaultRootNamesTheSecretsDir(t *testing.T) {
	file := func(path string) jobspec.JobSpec {
		return jobspec.JobSpec{SecretSources: []v1.SecretSourceRef{{Type: v1.SecretSourceTypeEncryptedPayload,
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationFile, Path: path}}}}
	}
	for path, want := range map[string]bool{"": true, "/var/run/zenthros/secrets/workspace/hf": true, "/run/secrets/x": false} {
		if got := hasDefaultFileSecret(file(path)); got != want {
			t.Errorf("hasDefaultFileSecret(%q) = %v, want %v", path, got, want)
		}
	}
}
