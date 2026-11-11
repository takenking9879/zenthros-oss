// SPDX-License-Identifier: Apache-2.0

package v1

// Annotations the controller keeps on a job about its workload (the
// zenthros.ai-zenthrosjob-controller-fields policy keeps them its own).
const (
	// AnnotationWorkloadCleaned records that the controller cancelled the
	// backend workload of an ended job.
	AnnotationWorkloadCleaned = "zenthros.ai/gc-workload-cleaned"
	// AnnotationOrphanReaped marks a job whose backend is gone for good:
	// there is no workload left to cancel.
	AnnotationOrphanReaped = "zenthros.ai/gc-orphan-reaped"
)

// CleanupPending reports whether job reads Cancelled while its workload may
// still run: the phase is written before the backend is told, and a backend
// that cannot be reached keeps it so (the Cleanup condition says why). zctl
// and the SDK show it, so "Cancelled" is not taken for "stopped".
func CleanupPending(job *ZenthrosJob) bool {
	if job.Status.Phase != PhaseCancelled {
		return false
	}
	_, cleaned := job.Annotations[AnnotationWorkloadCleaned]
	_, reaped := job.Annotations[AnnotationOrphanReaped]
	return !cleaned && !reaped
}
