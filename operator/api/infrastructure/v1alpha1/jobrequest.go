// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// JobRequestID is the RequestID of the machine a job asks for itself when
// its pool has no Ready slot for it: one per job, generation of its lease
// and where it asks (lifecycle.MachineRequestID: the offer it was decided
// on, a fleet job's place in its walk; "" for neither). The Controller creates the
// request under it; the Scheduler knows by it that a job it placed already
// has its machine on the way. A replan to another offer asks for another
// machine: named by the job and its generation only, the machine of the
// offer the job left was asked for again on every pass, and let go again.
func JobRequestID(jobUID string, leaseGeneration int64, where string) string {
	if where == "" {
		return fmt.Sprintf("wl-%s-g%d", jobUID, leaseGeneration)
	}
	sum := sha256.Sum256([]byte(where))
	return fmt.Sprintf("wl-%s-g%d-%s", jobUID, leaseGeneration, hex.EncodeToString(sum[:4]))
}
