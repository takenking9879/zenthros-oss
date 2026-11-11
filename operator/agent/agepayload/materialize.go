// SPDX-License-Identifier: Apache-2.0

package agepayload

import (
	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

// Config is destination-only decryption state used by the VM executor.
type Config struct {
	Root   string
	Bundle Bundle
}

// Bindings decrypts encrypted-payload references into destination-local values.
func Bindings(spec jobspec.JobSpec, cfg Config) ([]secretfile.Binding, error) {
	var bindings []secretfile.Binding
	for _, ref := range spec.SecretSources {
		if ref.Type != v1.SecretSourceTypeEncryptedPayload {
			continue
		}
		if ref.EncryptedPayload == nil {
			return nil, backend.NewSecretSourceError(v1.ReasonInvalidSecretSource, "encryptedPayload is required")
		}
		plaintext, err := cfg.Bundle.Decrypt(ref.EncryptedPayload.Ciphertext)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, secretfile.Binding{Ref: ref, Value: string(plaintext)})
	}
	return bindings, nil
}
