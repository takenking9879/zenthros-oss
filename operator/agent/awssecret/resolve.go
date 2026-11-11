// SPDX-License-Identifier: Apache-2.0

package awssecret

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/fault"
)

const (
	maxResolveAttempts = 3
	retryBackoff       = 200 * time.Millisecond
)

// GetRequest is the destination-side lookup. It carries reference metadata only.
type GetRequest struct {
	SecretID     string
	Region       string
	VersionID    string
	VersionStage string
}

// Getter fetches a secret string. Implementations must not log the value.
type Getter interface {
	Get(ctx context.Context, req GetRequest) (string, error)
}

// Factory builds a region-scoped Getter using the worker credential chain.
type Factory func(ctx context.Context, region string) (Getter, error)

type resolveConfig struct {
	factory Factory
	sleep   func(time.Duration)
}

func resolveSecret(ctx context.Context, cfg resolveConfig, ref v1.SecretSourceRef) (string, error) {
	if ref.AWSSecretsManager == nil {
		return "", backend.NewSecretSourceError(v1.ReasonInvalidSecretSource, "awsSecretsManager is required")
	}
	awsRef := *ref.AWSSecretsManager
	getter, err := cfg.factory(ctx, awsRef.Region)
	if err != nil {
		return "", redactedResolveError(err)
	}
	req := GetRequest{
		SecretID:     awsRef.SecretID,
		Region:       awsRef.Region,
		VersionID:    awsRef.VersionID,
		VersionStage: awsRef.VersionStage,
	}
	raw, err := getWithRetry(ctx, cfg, getter, req)
	if err != nil {
		return "", redactedResolveError(err)
	}
	if awsRef.JSONKey == "" {
		return raw, nil
	}
	value, err := selectJSONKey(raw, awsRef.JSONKey)
	if err != nil {
		return "", err
	}
	return value, nil
}

func getWithRetry(ctx context.Context, cfg resolveConfig, getter Getter, req GetRequest) (string, error) {
	sleep := cfg.sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	var last error
	for attempt := 1; attempt <= maxResolveAttempts; attempt++ {
		value, err := getter.Get(ctx, req)
		if err == nil {
			return value, nil
		}
		last = err
		if !isRetryableAWSError(err) || attempt == maxResolveAttempts {
			return "", err
		}
		sleep(retryBackoff * time.Duration(attempt))
	}
	return "", last
}

func selectJSONKey(raw, key string) (string, error) {
	decoded := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return "", backend.NewSecretSourceError(v1.ReasonSecretKeyNotFound, "referenced AWS secret is not JSON")
	}
	rawValue, ok := decoded[key]
	if !ok {
		return "", backend.NewSecretSourceError(v1.ReasonSecretKeyNotFound, "referenced JSON key was not found in the AWS secret")
	}
	var asString string
	if err := json.Unmarshal(rawValue, &asString); err == nil {
		return asString, nil
	}
	return string(rawValue), nil
}

// isRetryableAWSError reports an AWS answer worth asking again: one whose
// HTTP status says so (fault.TransientHTTP: 408, 429, 5xx), or a throttling code,
// which AWS's JSON protocols send with a 400.
func isRetryableAWSError(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	var answer *smithyhttp.ResponseError
	if errors.As(err, &answer) && fault.TransientHTTP(answer.HTTPStatusCode()) {
		return true
	}
	switch apiErr.ErrorCode() {
	case "ThrottlingException", "RequestLimitExceeded", "InternalServiceError", "InternalFailure", "ServiceUnavailable", "RequestTimeout":
		return true
	default:
		return false
	}
}

func redactedResolveError(err error) error {
	if err == nil {
		return nil
	}
	var secretErr *backend.SecretSourceError
	if errors.As(err, &secretErr) {
		return secretErr
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDeniedException", "UnrecognizedClientException", "InvalidClientTokenId", "AuthFailure", "UnauthorizedOperation":
			return backend.NewSecretSourceError(v1.ReasonSecretAccessDenied, "worker identity is not authorized to read the referenced AWS secret")
		case "ResourceNotFoundException", "ResourceNotFound":
			return backend.NewSecretSourceError(v1.ReasonSecretNotFound, "referenced AWS secret or version was not found")
		case "InvalidParameterException", "InvalidRequestException", "ValidationException":
			return backend.NewSecretSourceError(v1.ReasonInvalidSecretSource, "AWS secret reference is invalid")
		}
	}
	if noInstanceProfile(err) {
		return backend.NewSecretSourceError(v1.ReasonUnsupportedSecretSource, "aws-secrets-manager references require a Managed VM worker identity")
	}
	if errors.As(err, &apiErr) && !isRetryableAWSError(err) {
		return backend.NewSecretSourceError(v1.ReasonSecretResolutionFailed, "destination agent could not read the referenced AWS secret")
	}
	// Throttled past the retries, failing, or not reached: nothing is known
	// of the secret, and the job retries within its submit budget (c4-A7).
	// AWS's message stays out, as in every refusal above.
	return backend.Classified(backend.ErrDestinationUnavailable, errors.New("AWS Secrets Manager did not answer the agent"))
}
