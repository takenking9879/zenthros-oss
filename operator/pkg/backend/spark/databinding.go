// SPDX-License-Identifier: Apache-2.0

package spark

import (
	"fmt"
	"strings"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

// outputSchemes are the object stores a durable Spark output may name. A
// local or cluster path is gone when the driver pod is, so downstream stages
// could never read it.
var outputSchemes = []string{"s3://", "s3a://", "gs://"}

// ValidateDataBinding checks a Spark job's durable output before submission:
// when the job declares artifacts.output it must be an object-store URI. The
// credentials to write there come from the workload identity or secretSources,
// not from this reference. The gate runs once, before any resource is created.
func ValidateDataBinding(spec jobspec.JobSpec) error {
	output := spec.Artifacts.Output
	if output == "" {
		return nil
	}
	for _, scheme := range outputSchemes {
		if strings.HasPrefix(output, scheme) && len(output) > len(scheme) {
			return nil
		}
	}
	return fmt.Errorf(
		"%w: durable Spark output %q must be an object-store URI (%s)",
		backend.ErrAdmissionRejected, output, strings.Join(outputSchemes, ", "),
	)
}
