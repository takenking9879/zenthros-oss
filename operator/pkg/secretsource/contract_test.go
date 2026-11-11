// SPDX-License-Identifier: Apache-2.0

package secretsource

import (
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		refs    []v1.SecretSourceRef
		wantErr bool
	}{
		{name: "empty"},
		{name: "env", refs: []v1.SecretSourceRef{testRef(v1.SecretMaterializationEnv)}},
		{name: "file", refs: []v1.SecretSourceRef{testRef(v1.SecretMaterializationFile)}},
		{name: "unsupported type", refs: []v1.SecretSourceRef{{Type: "inline"}}, wantErr: true},
		{name: "aws env", refs: []v1.SecretSourceRef{testAWSRef(v1.SecretMaterializationEnv)}},
		{name: "aws missing region", refs: []v1.SecretSourceRef{{
			Type:              v1.SecretSourceTypeAWSSecretsManager,
			AWSSecretsManager: &v1.AWSSecretsManagerRef{SecretID: "workload/token"},
			Materialization:   v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv},
		}}, wantErr: true},
		{name: "aws k8s coordinates mixed", refs: []v1.SecretSourceRef{{
			Type: v1.SecretSourceTypeAWSSecretsManager, Namespace: "tenant-default", Name: "token", Key: "value",
			AWSSecretsManager: &v1.AWSSecretsManagerRef{SecretID: "workload/token", Region: "us-east-2"},
			Materialization:   v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv},
		}}, wantErr: true},
		{name: "aws version conflict", refs: []v1.SecretSourceRef{{
			Type: v1.SecretSourceTypeAWSSecretsManager,
			AWSSecretsManager: &v1.AWSSecretsManagerRef{
				SecretID: "workload/token", Region: "us-east-2", VersionID: "v1", VersionStage: "AWSCURRENT",
			},
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv},
		}}, wantErr: true},
		{name: "missing reference", refs: []v1.SecretSourceRef{{Type: v1.SecretSourceTypeKubernetes}}, wantErr: true},
		{name: "wrong target field", refs: []v1.SecretSourceRef{{
			Type: v1.SecretSourceTypeKubernetes, Namespace: "tenant-default", Name: "token", Key: "value",
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, Path: "/secret"},
		}}, wantErr: true},
		{name: "duplicate env", refs: []v1.SecretSourceRef{
			testRef(v1.SecretMaterializationEnv), testRef(v1.SecretMaterializationEnv),
		}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Validate(test.refs)
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestDefaultTargets(t *testing.T) {
	ref := testRef(v1.SecretMaterializationEnv)
	ref.Key = "api-token.value"
	if got := EnvName(ref); got != "API_TOKEN_VALUE" {
		t.Errorf("EnvName() = %q", got)
	}
	ref.Materialization.Mode = v1.SecretMaterializationFile
	if got := FilePath(ref); got != "/var/run/zenthros/secrets/token/api-token.value" {
		t.Errorf("FilePath() = %q", got)
	}
	awsRef := testAWSRef(v1.SecretMaterializationEnv)
	if got := EnvName(awsRef); got != "TOKEN" {
		t.Errorf("EnvName(aws jsonKey) = %q", got)
	}
	awsRef.AWSSecretsManager.JSONKey = ""
	awsRef.Materialization.Mode = v1.SecretMaterializationFile
	if got := FilePath(awsRef); got != "/var/run/zenthros/secrets/workload-token/value" {
		t.Errorf("FilePath(aws) = %q", got)
	}
}

func testRef(mode string) v1.SecretSourceRef {
	return v1.SecretSourceRef{
		Type:            v1.SecretSourceTypeKubernetes,
		Namespace:       "tenant-default",
		Name:            "token",
		Key:             "value",
		Materialization: v1.SecretMaterialization{Mode: mode},
	}
}

func testAWSRef(mode string) v1.SecretSourceRef {
	return v1.SecretSourceRef{
		Type: v1.SecretSourceTypeAWSSecretsManager,
		AWSSecretsManager: &v1.AWSSecretsManagerRef{
			SecretID: "workload/token", Region: "us-east-2", JSONKey: "token",
		},
		Materialization: v1.SecretMaterialization{Mode: mode},
	}
}

// A file secret under DefaultFileRoot is where a job finds it without
// asking, whatever runtime runs it ($ZENTHROS_SECRETS_DIR/<the rest>): a
// Workspace secret is resolved to such a path, and a machine with no mount
// namespace must not take it for a path of the job's own (found on a
// container here: the agent refused /var/run/zenthros/secrets/... as
// outside the job's directory).
func TestAPathUnderTheDefaultRootIsTheDefaultPlace(t *testing.T) {
	for path, want := range map[string]bool{
		"":                                  true,
		DefaultFileRoot + "/workspace/hf":   true,
		DefaultFileRoot + "/a/../b":         true,
		DefaultFileRoot:                     false,
		DefaultFileRoot + "-other/x":        false,
		DefaultFileRoot + "/../../../etc/x": false,
		"/run/secrets/x":                    false,
		"relative/x":                        false,
	} {
		ref := v1.SecretSourceRef{Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationFile, Path: path}}
		if got := AtDefaultPlace(ref); got != want {
			t.Errorf("AtDefaultPlace(%q) = %v, want %v", path, got, want)
		}
	}
}
