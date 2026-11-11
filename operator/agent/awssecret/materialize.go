// SPDX-License-Identifier: Apache-2.0

package awssecret

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

const (
	defaultStage  = "AWSCURRENT"
	secretFileDir = "files"
)

// Result is destination-local materialization. Values never leave this host.
type Result = secretfile.Result

// Config controls agent-side resolution. Tests inject a fake Getter and temp root.
type Config struct {
	// Root is where the executor writes a job's secret files.
	Root     string
	Factory  Factory
	Identity IdentityProbe
	Sleep    func(time.Duration)
}

type awsGetter struct {
	client *secretsmanager.Client
}

func (g awsGetter) Get(ctx context.Context, req GetRequest) (string, error) {
	input := &secretsmanager.GetSecretValueInput{SecretId: aws.String(req.SecretID)}
	if req.VersionID != "" {
		input.VersionId = aws.String(req.VersionID)
	} else if req.VersionStage != "" {
		input.VersionStage = aws.String(req.VersionStage)
	} else {
		input.VersionStage = aws.String(defaultStage)
	}
	out, err := g.client.GetSecretValue(ctx, input)
	if err != nil {
		return "", err
	}
	if out.SecretString != nil {
		return *out.SecretString, nil
	}
	if len(out.SecretBinary) > 0 {
		return string(out.SecretBinary), nil
	}
	return "", fmt.Errorf("empty secret payload")
}

// DefaultFactory uses the worker default credential chain. It never accepts
// static provisioner credentials.
func DefaultFactory(ctx context.Context, region string) (Getter, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return awsGetter{client: secretsmanager.NewFromConfig(cfg)}, nil
}

// Bindings resolves AWS secret references into destination-local values.
func Bindings(ctx context.Context, spec jobspec.JobSpec, cfg Config) ([]secretfile.Binding, error) {
	if !hasAWSSecret(spec) {
		return nil, nil
	}
	if err := requireWorkerIdentity(ctx, cfg.Identity); err != nil {
		return nil, err
	}
	factory := cfg.Factory
	if factory == nil {
		factory = DefaultFactory
	}
	typed := resolveConfig{factory: factory, sleep: cfg.Sleep}
	var bindings []secretfile.Binding
	for _, ref := range spec.SecretSources {
		if ref.Type != v1.SecretSourceTypeAWSSecretsManager {
			continue
		}
		value, err := resolveSecret(ctx, typed, ref)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, secretfile.Binding{Ref: ref, Value: value})
	}
	return bindings, nil
}

func hasAWSSecret(spec jobspec.JobSpec) bool {
	for _, ref := range spec.SecretSources {
		if ref.Type == v1.SecretSourceTypeAWSSecretsManager {
			return true
		}
	}
	return false
}
