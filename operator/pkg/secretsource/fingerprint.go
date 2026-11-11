// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
)

// Fingerprint is a stable identity of a reference for spec hashing. It never
// includes a secret value.
func Fingerprint(ref v1.SecretSourceRef) string {
	switch ref.Type {
	case v1.SecretSourceTypeAWSSecretsManager:
		awsRef := v1.AWSSecretsManagerRef{}
		if ref.AWSSecretsManager != nil {
			awsRef = *ref.AWSSecretsManager
		}
		return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s",
			ref.Type, awsRef.SecretID, awsRef.Region, awsRef.VersionID, awsRef.VersionStage, awsRef.JSONKey,
			ref.Materialization.Mode, ref.Materialization.EnvName, ref.Materialization.Path)
	case v1.SecretSourceTypeEncryptedPayload:
		payload := v1.EncryptedPayloadRef{}
		if ref.EncryptedPayload != nil {
			payload = *ref.EncryptedPayload
		}
		sealed := ciphertextFingerprint(payload.Ciphertext)
		if payload.WorkspaceSecret != "" {
			// Sealed by the controller at each send, with fresh randomness:
			// the same secret on a resend is the same spec.
			sealed = "workspace:" + payload.WorkspaceSecret
		}
		return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
			ref.Type, strings.Join(payload.Recipients, ","), payload.KeyID, sealed,
			ref.Materialization.Mode, ref.Materialization.EnvName, ref.Materialization.Path)
	default:
		return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
			ref.Type, ref.Namespace, ref.Name, ref.Key,
			ref.Materialization.Mode, ref.Materialization.EnvName, ref.Materialization.Path)
	}
}

func ciphertextFingerprint(ciphertext string) string {
	sum := sha256.Sum256([]byte(ciphertext))
	return hex.EncodeToString(sum[:8])
}
