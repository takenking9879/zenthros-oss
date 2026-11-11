// SPDX-License-Identifier: Apache-2.0

// Package secretsource validates and renders destination-side secret references.
// It handles reference metadata only and never accepts or returns secret values.
package secretsource

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
)

// DefaultFileRoot is where a file secret without an explicit path is seen:
// <DefaultFileRoot>/<secret-name>/<key>.
const DefaultFileRoot = "/var/run/zenthros/secrets"

var (
	envNamePattern    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	namespacePattern  = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	secretNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)
	secretKeyPattern  = regexp.MustCompile(`^[-._a-zA-Z0-9]+$`)
)

// Validate checks cross-field rules that must also hold for decoded envelopes.
func Validate(refs []v1.SecretSourceRef) error {
	envTargets := make(map[string]struct{})
	fileTargets := make(map[string]struct{})
	for index, ref := range refs {
		if err := validateRef(ref); err != nil {
			return fmt.Errorf("secretSources[%d]: %w", index, err)
		}
		switch ref.Materialization.Mode {
		case v1.SecretMaterializationEnv:
			target := EnvName(ref)
			if _, exists := envTargets[target]; exists {
				return fmt.Errorf("secretSources[%d]: duplicate env target %q", index, target)
			}
			envTargets[target] = struct{}{}
		case v1.SecretMaterializationFile:
			target := FilePath(ref)
			if _, exists := fileTargets[target]; exists {
				return fmt.Errorf("secretSources[%d]: duplicate file target %q", index, target)
			}
			fileTargets[target] = struct{}{}
		}
	}
	return nil
}

func validateRef(ref v1.SecretSourceRef) error {
	switch ref.Type {
	case v1.SecretSourceTypeKubernetes:
		if err := validateKubernetesRef(ref); err != nil {
			return err
		}
	case v1.SecretSourceTypeAWSSecretsManager:
		if err := validateAWSRef(ref); err != nil {
			return err
		}
	case v1.SecretSourceTypeEncryptedPayload:
		if err := validateEncryptedRef(ref); err != nil {
			return err
		}
	case v1.SecretSourceTypeWorkspace:
		if ref.Namespace != "" || ref.Key != "" || ref.AWSSecretsManager != nil || ref.EncryptedPayload != nil {
			return fmt.Errorf("workspace-secret takes only a name")
		}
		if !v1.WorkspaceSecretName(ref.Name) {
			return fmt.Errorf("Workspace secret name %q: lowercase letters, digits and '-', at most 63", ref.Name)
		}
	default:
		return fmt.Errorf("unsupported type %q", ref.Type)
	}
	return validateMaterialization(ref)
}

func validateKubernetesRef(ref v1.SecretSourceRef) error {
	if ref.AWSSecretsManager != nil {
		return fmt.Errorf("awsSecretsManager is only valid for aws-secrets-manager")
	}
	if ref.EncryptedPayload != nil {
		return fmt.Errorf("encryptedPayload is only valid for encrypted-payload")
	}
	if ref.Namespace == "" || ref.Name == "" || ref.Key == "" {
		return fmt.Errorf("namespace, name, and key are required")
	}
	if len(ref.Namespace) > 63 || !namespacePattern.MatchString(ref.Namespace) {
		return fmt.Errorf("namespace reference is invalid")
	}
	if len(ref.Name) > 253 || !secretNamePattern.MatchString(ref.Name) {
		return fmt.Errorf("Secret name reference is invalid")
	}
	if len(ref.Key) > 253 || !secretKeyPattern.MatchString(ref.Key) || ref.Key == "." || ref.Key == ".." {
		return fmt.Errorf("Secret key reference is invalid")
	}
	return nil
}

func validateMaterialization(ref v1.SecretSourceRef) error {
	switch ref.Materialization.Mode {
	case v1.SecretMaterializationEnv:
		if ref.Materialization.Path != "" {
			return fmt.Errorf("path is only valid for file materialization")
		}
		if !envNamePattern.MatchString(EnvName(ref)) {
			return fmt.Errorf("env target %q is invalid", EnvName(ref))
		}
	case v1.SecretMaterializationFile:
		if ref.Materialization.EnvName != "" {
			return fmt.Errorf("envName is only valid for env materialization")
		}
		target := FilePath(ref)
		if !path.IsAbs(target) || path.Clean(target) != target || target == "/" || strings.ContainsAny(target, "\n\r") {
			return fmt.Errorf("file target %q must be a clean absolute path", target)
		}
	default:
		return fmt.Errorf("materialization mode must be env or file")
	}
	return nil
}

// EnvName returns the explicit target or a deterministic name derived from the key.
func EnvName(ref v1.SecretSourceRef) string {
	if ref.Materialization.EnvName != "" {
		return ref.Materialization.EnvName
	}
	return normalizeEnvName(envSource(ref))
}

func envSource(ref v1.SecretSourceRef) string {
	if ref.Type == v1.SecretSourceTypeAWSSecretsManager && ref.AWSSecretsManager != nil {
		if ref.AWSSecretsManager.JSONKey != "" {
			return ref.AWSSecretsManager.JSONKey
		}
		return awsSecretName(ref.AWSSecretsManager.SecretID)
	}
	if ref.Type == v1.SecretSourceTypeEncryptedPayload && ref.EncryptedPayload != nil {
		if ref.EncryptedPayload.KeyID != "" {
			return ref.EncryptedPayload.KeyID
		}
		return "payload"
	}
	if ref.Type == v1.SecretSourceTypeWorkspace {
		return ref.Name
	}
	return ref.Key
}

func normalizeEnvName(source string) string {
	replacer := strings.NewReplacer("-", "_", ".", "_", "/", "_")
	name := strings.ToUpper(replacer.Replace(source))
	if name == "" {
		return ""
	}
	if name[0] >= '0' && name[0] <= '9' {
		return "_" + name
	}
	return name
}

// FilePath returns the explicit target or the stable default mount path.
func FilePath(ref v1.SecretSourceRef) string {
	if ref.Materialization.Path != "" {
		return ref.Materialization.Path
	}
	return path.Join(DefaultFileRoot, fileDirectory(ref), fileName(ref))
}

// AtDefaultPlace says whether a file secret lands where a job finds it
// without asking: no path, or one under DefaultFileRoot. Whatever runs the
// job, it reads it as $ZENTHROS_SECRETS_DIR/<the rest>; any other path is
// the job's own (a mount, or a link under the job's directories).
func AtDefaultPlace(ref v1.SecretSourceRef) bool {
	p := ref.Materialization.Path
	if p == "" {
		return true
	}
	clean := path.Clean(p)
	return clean != DefaultFileRoot && strings.HasPrefix(clean, DefaultFileRoot+"/")
}

func fileDirectory(ref v1.SecretSourceRef) string {
	if ref.Type == v1.SecretSourceTypeAWSSecretsManager && ref.AWSSecretsManager != nil {
		return sanitizePathSegment(awsSecretName(ref.AWSSecretsManager.SecretID))
	}
	if ref.Type == v1.SecretSourceTypeEncryptedPayload {
		if ref.EncryptedPayload != nil && ref.EncryptedPayload.KeyID != "" {
			return sanitizePathSegment(ref.EncryptedPayload.KeyID)
		}
		return "encrypted"
	}
	if ref.Type == v1.SecretSourceTypeWorkspace {
		return "workspace"
	}
	return ref.Name
}

func fileName(ref v1.SecretSourceRef) string {
	if ref.Type == v1.SecretSourceTypeAWSSecretsManager && ref.AWSSecretsManager != nil {
		if ref.AWSSecretsManager.JSONKey != "" {
			return ref.AWSSecretsManager.JSONKey
		}
		return "value"
	}
	if ref.Type == v1.SecretSourceTypeEncryptedPayload {
		return "value"
	}
	if ref.Type == v1.SecretSourceTypeWorkspace {
		return ref.Name
	}
	return ref.Key
}

func sanitizePathSegment(value string) string {
	replacer := strings.NewReplacer(":", "-", "/", "-", " ", "-")
	segment := replacer.Replace(value)
	if segment == "" {
		return "secret"
	}
	return segment
}
