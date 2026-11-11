// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"fmt"
	"regexp"
	"strings"

	v1 "github.com/zenthros/operator/api/v1"
)

var (
	awsRegionPattern     = regexp.MustCompile(`^[a-z]{2}-[a-z0-9-]+-\d+$`)
	awsSecretNamePattern = regexp.MustCompile(`^[A-Za-z0-9/_+=.@-]+$`)
	awsSecretARNPattern  = regexp.MustCompile(`^arn:aws:secretsmanager:[a-z0-9-]+:\d{12}:secret:.+`)
	awsVersionPattern    = regexp.MustCompile(`^[A-Za-z0-9-]+$`)
	awsStagePattern      = regexp.MustCompile(`^[A-Za-z0-9/_+=.@-]+$`)
)

func validateAWSRef(ref v1.SecretSourceRef) error {
	if ref.Namespace != "" || ref.Name != "" || ref.Key != "" {
		return fmt.Errorf("Kubernetes coordinates are not valid for aws-secrets-manager")
	}
	if ref.EncryptedPayload != nil {
		return fmt.Errorf("encryptedPayload is only valid for encrypted-payload")
	}
	if ref.AWSSecretsManager == nil {
		return fmt.Errorf("awsSecretsManager is required")
	}
	awsRef := *ref.AWSSecretsManager
	if awsRef.SecretID == "" || awsRef.Region == "" {
		return fmt.Errorf("secretId and region are required")
	}
	if len(awsRef.SecretID) > 512 || !validAWSSecretID(awsRef.SecretID) {
		return fmt.Errorf("secretId reference is invalid")
	}
	if len(awsRef.Region) > 32 || !awsRegionPattern.MatchString(awsRef.Region) {
		return fmt.Errorf("region %q is invalid", awsRef.Region)
	}
	if awsRef.VersionID != "" && awsRef.VersionStage != "" {
		return fmt.Errorf("versionId and versionStage are mutually exclusive")
	}
	if awsRef.VersionID != "" && !awsVersionPattern.MatchString(awsRef.VersionID) {
		return fmt.Errorf("versionId reference is invalid")
	}
	if awsRef.VersionStage != "" && !awsStagePattern.MatchString(awsRef.VersionStage) {
		return fmt.Errorf("versionStage reference is invalid")
	}
	if awsRef.JSONKey != "" && (len(awsRef.JSONKey) > 253 || !secretKeyPattern.MatchString(awsRef.JSONKey) || awsRef.JSONKey == "." || awsRef.JSONKey == "..") {
		return fmt.Errorf("jsonKey reference is invalid")
	}
	return nil
}

func validAWSSecretID(secretID string) bool {
	if strings.HasPrefix(secretID, "arn:") {
		return awsSecretARNPattern.MatchString(secretID)
	}
	return awsSecretNamePattern.MatchString(secretID)
}

func awsSecretName(secretID string) string {
	if !strings.HasPrefix(secretID, "arn:") {
		return secretID
	}
	const marker = ":secret:"
	index := strings.LastIndex(secretID, marker)
	if index < 0 {
		return secretID
	}
	return secretID[index+len(marker):]
}
