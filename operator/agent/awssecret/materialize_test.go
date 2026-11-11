// SPDX-License-Identifier: Apache-2.0

package awssecret

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/zenthros/operator/agent/secretfile"
	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/secretsource"
)

const plaintext = "s3cret-must-never-leak"

type fakeGetter struct {
	region  string
	values  map[string]string
	err     error
	calls   int
	failFor int
}

func (f *fakeGetter) Get(_ context.Context, req GetRequest) (string, error) {
	f.calls++
	if f.region != "" && req.Region != f.region {
		return "", &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: plaintext}
	}
	if f.failFor > 0 && f.calls <= f.failFor {
		if f.err != nil {
			return "", f.err
		}
		return "", &smithy.GenericAPIError{Code: "ThrottlingException", Message: plaintext}
	}
	if f.err != nil && f.failFor == 0 {
		return "", f.err
	}
	value, ok := f.values[req.SecretID]
	if !ok {
		return "", &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: plaintext}
	}
	if req.VersionID != "" && req.VersionID != "current" {
		return "", &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: plaintext}
	}
	return value, nil
}

func awsSpec(mode, path string) jobspec.JobSpec {
	ref := v1.SecretSourceRef{
		Type: v1.SecretSourceTypeAWSSecretsManager,
		AWSSecretsManager: &v1.AWSSecretsManagerRef{
			SecretID: "workload/token", Region: "us-east-2", JSONKey: "token",
		},
		Materialization: v1.SecretMaterialization{Mode: mode, Path: path, EnvName: "T14_SECRET"},
	}
	if mode == v1.SecretMaterializationFile {
		ref.Materialization.EnvName = ""
	}
	return jobspec.JobSpec{
		ID: "job-uid", Name: "job", Namespace: "tenant-default",
		Kind: v1.WorkloadKindRayJob, Entrypoint: "python main.py", Image: "ray:latest",
		SecretSources: []v1.SecretSourceRef{ref},
	}
}

func presentIdentity(context.Context) (bool, error) { return true, nil }

func missingIdentity(context.Context) (bool, error) { return false, nil }

func TestMaterializeEnvAndFile(t *testing.T) {
	root := t.TempDir()
	payload, _ := json.Marshal(map[string]string{"token": plaintext})
	factory := func(_ context.Context, region string) (Getter, error) {
		return &fakeGetter{region: "us-east-2", values: map[string]string{"workload/token": string(payload)}}, nil
	}

	envSpec := awsSpec(v1.SecretMaterializationEnv, "")
	envResult, err := materialize(context.Background(), envSpec, root, Config{Factory: factory, Identity: presentIdentity})
	if err != nil {
		t.Fatalf("env Materialize: %v", err)
	}
	if envResult.WrapperPath == "" {
		t.Fatal("env materialization must write a wrapper")
	}
	raw, err := os.ReadFile(filepath.Join(root, envSpec.ID, secretFileDir, "T14_SECRET"))
	if err != nil {
		t.Fatalf("read env file: %v", err)
	}
	if string(raw) != plaintext {
		t.Fatalf("env file = %q", raw)
	}
	info, err := os.Stat(filepath.Join(root, envSpec.ID, secretFileDir, "T14_SECRET"))
	if err != nil || info.Mode().Perm() != secretfile.FileMode {
		t.Fatalf("env file mode = %v", info)
	}

	filePath := filepath.Join(root, "run", "secrets", "t14")
	fileSpec := awsSpec(v1.SecretMaterializationFile, filePath)
	fileResult, err := materialize(context.Background(), fileSpec, root, Config{Factory: factory, Identity: presentIdentity})
	if err != nil {
		t.Fatalf("file Materialize: %v", err)
	}
	// The value stays in the job's root; the runtime puts it at filePath.
	if len(fileResult.Mounts) != 1 || fileResult.Mounts[0].Target != filePath {
		t.Fatalf("mounts = %+v", fileResult.Mounts)
	}
	stored := fileResult.Mounts[0].Source
	info, err = os.Stat(stored)
	if err != nil || info.Mode().Perm() != secretfile.FileMode {
		t.Fatalf("file mode = %v", info)
	}

	if err := secretfile.CleanupRoot(root, envSpec.ID); err != nil {
		t.Fatalf("CleanupRoot env: %v", err)
	}
	if err := secretfile.CleanupRoot(root, fileSpec.ID); err != nil {
		t.Fatalf("CleanupRoot file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, envSpec.ID)); !os.IsNotExist(err) {
		t.Fatal("env secret dir remained after cleanup")
	}
	if _, err := os.Stat(stored); !os.IsNotExist(err) {
		t.Fatal("materialized file remained after cleanup")
	}
}

func TestMaterializeFailuresAreRedacted(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name     string
		err      error
		identity IdentityProbe
		mutate   func(*jobspec.JobSpec)
		code     string
	}{
		{name: "iam denial", err: &smithy.GenericAPIError{Code: "AccessDeniedException", Message: plaintext}, code: v1.ReasonSecretAccessDenied},
		{name: "missing secret", err: &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: plaintext}, code: v1.ReasonSecretNotFound},
		{name: "missing version", err: &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: plaintext}, mutate: func(spec *jobspec.JobSpec) {
			spec.SecretSources[0].AWSSecretsManager.VersionID = "missing"
		}, code: v1.ReasonSecretNotFound},
		{name: "region mismatch", err: &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: plaintext}, mutate: func(spec *jobspec.JobSpec) {
			spec.SecretSources[0].AWSSecretsManager.Region = "us-west-2"
		}, code: v1.ReasonSecretNotFound},
		{name: "missing json key", mutate: func(spec *jobspec.JobSpec) {
			spec.SecretSources[0].AWSSecretsManager.JSONKey = "absent"
		}, code: v1.ReasonSecretKeyNotFound},
		{name: "no instance profile", identity: missingIdentity, code: v1.ReasonUnsupportedSecretSource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := awsSpec(v1.SecretMaterializationEnv, "")
			if test.mutate != nil {
				test.mutate(&spec)
			}
			identity := presentIdentity
			if test.identity != nil {
				identity = test.identity
			}
			factory := func(_ context.Context, region string) (Getter, error) {
				values := map[string]string{"workload/token": `{"token":"` + plaintext + `"}`}
				return &fakeGetter{region: "us-east-2", values: values, err: test.err}, nil
			}
			_, err := materialize(context.Background(), spec, root, Config{Factory: factory, Identity: identity})
			code, ok := backend.SecretSourceReason(err)
			if !ok || code != test.code {
				t.Fatalf("error = %v, want %s", err, test.code)
			}
			if strings.Contains(err.Error(), plaintext) {
				t.Fatal("error leaked plaintext")
			}
		})
	}
}

func TestResolveRetriesThrottling(t *testing.T) {
	slept := 0
	factory := func(context.Context, string) (Getter, error) {
		return &fakeGetter{
			region:  "us-east-2",
			values:  map[string]string{"workload/token": `{"token":"` + plaintext + `"}`},
			failFor: 2,
		}, nil
	}
	_, err := materialize(context.Background(), awsSpec(v1.SecretMaterializationEnv, ""), t.TempDir(), Config{
		Factory: factory, Identity: presentIdentity,
		Sleep: func(time.Duration) { slept++ },
	})
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if slept != 2 {
		t.Fatalf("retries slept %d times, want 2", slept)
	}
}

func TestRotationFetchesCurrentValue(t *testing.T) {
	root := t.TempDir()
	current := plaintext
	factory := func(context.Context, string) (Getter, error) {
		return getterFunc(func(_ context.Context, req GetRequest) (string, error) {
			return `{"token":"` + current + `"}`, nil
		}), nil
	}
	spec := awsSpec(v1.SecretMaterializationEnv, "")
	cfg := Config{Factory: factory, Identity: presentIdentity}
	if _, err := materialize(context.Background(), spec, root, cfg); err != nil {
		t.Fatalf("first Materialize: %v", err)
	}
	current = "rotated-value-not-cached"
	spec.ID = "job-uid-2"
	if _, err := materialize(context.Background(), spec, root, cfg); err != nil {
		t.Fatalf("second Materialize: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, spec.ID, secretFileDir, secretsource.EnvName(spec.SecretSources[0])))
	if err != nil {
		t.Fatalf("read rotated file: %v", err)
	}
	if string(raw) != "rotated-value-not-cached" {
		t.Fatalf("rotated value = %q", raw)
	}
}

type getterFunc func(context.Context, GetRequest) (string, error)

func (f getterFunc) Get(ctx context.Context, req GetRequest) (string, error) {
	return f(ctx, req)
}

func TestRedactedResolveErrorNeverIncludesAWSMessage(t *testing.T) {
	err := redactedResolveError(&smithy.GenericAPIError{Code: "AccessDeniedException", Message: plaintext})
	if err == nil || strings.Contains(err.Error(), plaintext) {
		t.Fatalf("redacted error = %v", err)
	}
	if !errors.Is(err, backend.ErrSecretSource) {
		t.Fatalf("unwrap = %v", err)
	}
}

// The metadata service failing on an AWS machine is not a machine without an
// identity: the job retries instead of failing UNSUPPORTED_SECRET_SOURCE
// (c4-A7). No metadata service at all still fails it.
func TestAMetadataOutageIsNotAMissingIdentity(t *testing.T) {
	outage := func(context.Context) (bool, error) { return false, statusError{503} }
	err := requireWorkerIdentity(context.Background(), outage)
	if _, refused := backend.SecretSourceReason(err); refused || backend.Classify(err) != backend.ClassTransient {
		t.Fatalf("metadata 503: %v, want transient", err)
	}
	offAWS := func(context.Context) (bool, error) {
		return false, errors.New("dial tcp 169.254.169.254:80: connect: no route to host")
	}
	if code, ok := backend.SecretSourceReason(requireWorkerIdentity(context.Background(), offAWS)); !ok || code != v1.ReasonUnsupportedSecretSource {
		t.Fatalf("no metadata service: %s %v, want %s", code, ok, v1.ReasonUnsupportedSecretSource)
	}
}

type statusError struct{ code int }

func (e statusError) Error() string       { return fmt.Sprintf("metadata: HTTP %d", e.code) }
func (e statusError) HTTPStatusCode() int { return e.code }

// materialize is the agent's path (secretmaterialize.go): resolve, then
// write under root.
func materialize(ctx context.Context, spec jobspec.JobSpec, root string, cfg Config) (secretfile.Result, error) {
	bindings, err := Bindings(ctx, spec, cfg)
	if err != nil {
		return secretfile.Result{}, err
	}
	return secretfile.Apply(root, spec.ID, bindings)
}

// Secrets Manager throttling past the local retries, failing, or a network
// that did not reach it says nothing of the secret: the job retries within
// its submit budget, and the AWS message stays out of the error.
func TestAnAWSOutageIsRetriedNotRefused(t *testing.T) {
	for name, fail := range map[string]error{
		"throttled":   &smithy.GenericAPIError{Code: "ThrottlingException", Message: plaintext},
		"unavailable": &smithy.GenericAPIError{Code: "ServiceUnavailable", Message: plaintext},
		"network":     errors.New("dial tcp: lookup secretsmanager.us-east-2.amazonaws.com: i/o timeout " + plaintext),
		// A 5xx whose code no table lists: the status says what it is.
		"5xx of a code nobody listed": &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusBadGateway}},
			Err:      &smithy.GenericAPIError{Code: "KMSInternalException", Message: plaintext},
		},
	} {
		factory := func(context.Context, string) (Getter, error) {
			return &fakeGetter{region: "us-east-2", err: fail}, nil
		}
		_, err := materialize(context.Background(), awsSpec(v1.SecretMaterializationEnv, ""), t.TempDir(),
			Config{Factory: factory, Identity: presentIdentity, Sleep: func(time.Duration) {}})
		if _, refused := backend.SecretSourceReason(err); err == nil || refused || backend.Classify(err) != backend.ClassTransient {
			t.Errorf("%s: %v (class %v), want transient and no refusal", name, err, backend.Classify(err))
		}
		if err != nil && strings.Contains(err.Error(), plaintext) {
			t.Errorf("%s: error leaked the AWS message: %v", name, err)
		}
	}
}
