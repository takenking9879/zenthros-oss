// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"regexp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AnnotationJobID is the job's identity on backends: the ID agents key their
// units, logs, reports and checkpoints by. The controller stamps it with the
// UID when it first admits the job. A restore re-creates the object under a
// new UID but keeps this annotation, so the agents adopt the work that is
// still running instead of running it again.
const AnnotationJobID = "zenthros.ai/job-id"

// FinalizerBackendCleanup guards a job's deletion until its backend work is
// cancelled. The controller adds it in its first write of the job, with the
// job ID and the job's place in the queue (TakenIn).
const FinalizerBackendCleanup = "zenthros.ai/backend-cleanup"

// jobIDPattern is the UID form. Only that form is honoured: the ID ends up in
// unit names and paths on the machine.
var jobIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ValidJobID reports whether id has the UID form.
func ValidJobID(id string) bool { return jobIDPattern.MatchString(id) }

// JobID is the stamped ID, or the UID for a job not yet stamped (created
// before stamping, or not yet admitted).
func (j *ZenthrosJob) JobID() string { return JobIDOf(j) }

// JobIDOf is JobID of a job read as its metadata alone.
func JobIDOf(job metav1.Object) string {
	if id := job.GetAnnotations()[AnnotationJobID]; ValidJobID(id) {
		return id
	}
	return string(job.GetUID())
}

// AnnotationSecretsOnUntrustedHost, "true" on a ZenthrosJob, sends its
// secretSources to a backend whose host a third party runs (Vast.ai). The
// job's owner accepts that the host's owner can read them.
const AnnotationSecretsOnUntrustedHost = "zenthros.ai/secrets-on-untrusted-host"

// AnnotationSpawnOnUntrustedHost says the job accepts that, on a host a third
// party runs (Vast.ai), its children are what that host says: the owner can
// act as the job, and a spawn runs a command on the job's other machines.
const AnnotationSpawnOnUntrustedHost = "zenthros.ai/spawn-on-untrusted-host"

// AnnotationResultCount, on a zenthros.run job, is how many results it
// writes (1, or a map's item count). A job that depends on it starts only
// once that many result digests are in its status, so the machine can check
// what it reads.
const AnnotationResultCount = "zenthros.ai/result-count"
