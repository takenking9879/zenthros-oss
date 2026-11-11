// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"fmt"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/agepayload"
)

func validateEncryptedRef(ref v1.SecretSourceRef) error {
	if ref.Namespace != "" || ref.Name != "" || ref.Key != "" {
		return fmt.Errorf("Kubernetes coordinates are not valid for encrypted-payload")
	}
	if ref.AWSSecretsManager != nil {
		return fmt.Errorf("awsSecretsManager is only valid for aws-secrets-manager")
	}
	if ref.EncryptedPayload == nil {
		return fmt.Errorf("encryptedPayload is required")
	}
	payload := *ref.EncryptedPayload
	if strings.TrimSpace(payload.Ciphertext) == "" {
		return fmt.Errorf("ciphertext is required")
	}
	if len(payload.Ciphertext) > agepayload.MaxCiphertextBytes {
		return fmt.Errorf("ciphertext exceeds %d bytes", agepayload.MaxCiphertextBytes)
	}
	if !agepayload.LooksLikeCiphertext(payload.Ciphertext) {
		return fmt.Errorf("ciphertext is not ASCII-armored age ciphertext")
	}
	if _, err := agepayload.ParseRecipients(payload.Recipients); err != nil {
		return err
	}
	if payload.KeyID != "" && len(payload.KeyID) > 32 {
		return fmt.Errorf("keyId is invalid")
	}
	if payload.WorkspaceSecret != "" && !v1.WorkspaceSecretName(payload.WorkspaceSecret) {
		return fmt.Errorf("workspaceSecret %q is not a Workspace secret name", payload.WorkspaceSecret)
	}
	if ref.Materialization.Mode == v1.SecretMaterializationEnv && ref.Materialization.EnvName == "" {
		return fmt.Errorf("envName is required for encrypted-payload env materialization")
	}
	return nil
}

// RecipientsOverlap reports whether ciphertext recipients include a destination key.
func RecipientsOverlap(ciphertextRecipients, advertised []string) bool {
	if len(ciphertextRecipients) == 0 || len(advertised) == 0 {
		return false
	}
	have := make(map[string]struct{}, len(advertised))
	for _, recipient := range advertised {
		have[recipient] = struct{}{}
	}
	for _, recipient := range ciphertextRecipients {
		if _, ok := have[recipient]; ok {
			return true
		}
	}
	return false
}
