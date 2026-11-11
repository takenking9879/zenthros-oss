// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

func TestPrepareSourceDoesNotSwallowS3(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	dir, err := PrepareSource(context.Background(), &fakeRunner{}, jobspec.JobSpec{
		ID:     "s3-job",
		Kind:   v1.WorkloadKindRayJob,
		Source: v1.Source{S3: "s3://bucket/code.tgz"},
	}, cfg)
	if err == nil && dir == "" {
		t.Fatal("PrepareSource ignored Source.S3 (returned empty with no error)")
	}
}
