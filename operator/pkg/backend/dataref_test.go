// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"reflect"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
)

func TestDataReferenceEnv(t *testing.T) {
	tests := []struct {
		name      string
		artifacts v1.Artifacts
		want      map[string]string
	}{
		{name: "no references", artifacts: v1.Artifacts{}, want: nil},
		{
			name:      "input only",
			artifacts: v1.Artifacts{Input: "s3a://warehouse/features"},
			want:      map[string]string{DataInputEnv: "s3a://warehouse/features"},
		},
		{
			name:      "output only",
			artifacts: v1.Artifacts{Output: "s3a://warehouse/run-1"},
			want:      map[string]string{DataOutputEnv: "s3a://warehouse/run-1"},
		},
		{
			name:      "input and output",
			artifacts: v1.Artifacts{Input: "s3a://in", Output: "s3a://out"},
			want:      map[string]string{DataInputEnv: "s3a://in", DataOutputEnv: "s3a://out"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DataReferenceEnv(test.artifacts); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("DataReferenceEnv = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestWithDataReferences(t *testing.T) {
	artifacts := v1.Artifacts{Input: "s3a://warehouse/features"}

	merged := WithDataReferences(map[string]string{"MODEL": "xgboost"}, artifacts)
	if merged[DataInputEnv] != artifacts.Input || merged["MODEL"] != "xgboost" {
		t.Fatalf("merged = %#v", merged)
	}

	// Artifact-derived references win so the staged-pipeline contract stays deterministic.
	overridden := WithDataReferences(map[string]string{DataInputEnv: "user-value"}, artifacts)
	if overridden[DataInputEnv] != artifacts.Input {
		t.Fatalf("overridden = %#v, want artifact reference to win", overridden)
	}

	if got := WithDataReferences(nil, v1.Artifacts{}); got != nil {
		t.Fatalf("expected nil env without references, got %#v", got)
	}
}
