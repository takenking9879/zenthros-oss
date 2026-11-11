// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"

	"github.com/zenthros/operator/agent/agepayload"
	"github.com/zenthros/operator/agent/awssecret"
	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/secretsource"
)

// secretBackend resolves the secretSources of one type on the machine. A
// provider's secret store is one more backend (Secrets Manager today; Key
// Vault or Secret Manager next), not a change to materialization.
type secretBackend interface {
	Type() string
	Bindings(ctx context.Context, spec jobspec.JobSpec) ([]secretfile.Binding, error)
}

type awsSecretsBackend struct{ cfg awssecret.Config }

func (awsSecretsBackend) Type() string { return v1.SecretSourceTypeAWSSecretsManager }
func (b awsSecretsBackend) Bindings(ctx context.Context, spec jobspec.JobSpec) ([]secretfile.Binding, error) {
	return awssecret.Bindings(ctx, spec, b.cfg)
}

type encryptedPayloadBackend struct{ cfg agepayload.Config }

func (encryptedPayloadBackend) Type() string { return v1.SecretSourceTypeEncryptedPayload }
func (b encryptedPayloadBackend) Bindings(_ context.Context, spec jobspec.JobSpec) ([]secretfile.Binding, error) {
	return agepayload.Bindings(spec, b.cfg)
}

// machineSecretBackends are the secret types a machine resolves itself.
func machineSecretBackends(awsCfg awssecret.Config, ageCfg agepayload.Config) []secretBackend {
	return []secretBackend{awsSecretsBackend{awsCfg}, encryptedPayloadBackend{ageCfg}}
}

// resolveSecrets refuses a job naming a secret type no backend here
// resolves, before resolving any: running it without the secret would fail
// later and less clearly.
func resolveSecrets(ctx context.Context, spec jobspec.JobSpec, backends []secretBackend) ([]secretfile.Binding, error) {
	known := map[string]bool{}
	for _, b := range backends {
		known[b.Type()] = true
	}
	for _, ref := range spec.SecretSources {
		if !known[ref.Type] {
			return nil, backend.NewSecretSourceError(v1.ReasonUnsupportedSecretSource,
				fmt.Sprintf("secret source type %q is not resolved on this machine", ref.Type))
		}
	}
	var bindings []secretfile.Binding
	for _, b := range backends {
		resolved, err := b.Bindings(ctx, spec)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, resolved...)
	}
	return bindings, nil
}

func applyDestinationSecrets(ctx context.Context, spec jobspec.JobSpec, root string, awsCfg awssecret.Config, ageCfg agepayload.Config) (secretfile.Result, error) {
	bindings, err := resolveSecrets(ctx, spec, machineSecretBackends(awsCfg, ageCfg))
	if err != nil {
		return secretfile.Result{}, err
	}
	return secretfile.Apply(root, spec.ID, bindings)
}

// hasDefaultFileSecret: the job has a file secret without an explicit path,
// which it finds under $ZENTHROS_SECRETS_DIR.
func hasDefaultFileSecret(spec jobspec.JobSpec) bool {
	for _, ref := range spec.SecretSources {
		if ref.Materialization.Mode == v1.SecretMaterializationFile && secretsource.AtDefaultPlace(ref) {
			return true
		}
	}
	return false
}

func cleanupDestinationSecrets(root, jobID string) error {
	return secretfile.CleanupRoot(root, jobID)
}
