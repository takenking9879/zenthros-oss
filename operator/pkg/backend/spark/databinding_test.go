// SPDX-License-Identifier: Apache-2.0

package spark

import (
	"errors"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

func TestValidateDataBinding(t *testing.T) {
	tests := []struct {
		output  string
		wantErr bool
	}{
		{output: "", wantErr: false},
		{output: "s3a://warehouse/run-1", wantErr: false},
		{output: "s3://warehouse/run-1", wantErr: false},
		{output: "gs://warehouse/run-1", wantErr: false},
		{output: "/data/run-1", wantErr: true},
		{output: "hdfs://nn/run-1", wantErr: true},
		{output: "s3a://", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.output, func(t *testing.T) {
			spec := testSpec()
			spec.Artifacts = v1.Artifacts{Output: test.output}
			err := ValidateDataBinding(spec)
			if test.wantErr {
				if !errors.Is(err, backend.ErrAdmissionRejected) {
					t.Fatalf("expected admission rejection, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
