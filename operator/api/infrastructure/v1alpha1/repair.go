// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// Repair annotations are the durable operator intent the Provisioner consumes.
// zctl writes them; the Provisioner applies them from CR state, never from
// in-memory callbacks. Instance IDs stay in annotations and Events, never in
// metric labels.
const (
	AnnotationRepairAction  = "infrastructure.zenthros.io/repair-action"
	AnnotationRepairActor   = "infrastructure.zenthros.io/repair-actor"
	AnnotationRepairPool    = "infrastructure.zenthros.io/repair-pool"
	AnnotationCorrelationID = "infrastructure.zenthros.io/correlation-id"

	RepairRetry     = "retry"
	RepairAdopt     = "adopt"
	RepairTerminate = "terminate"
	RepairRelease   = "release"

	ActorZctl        = "zctl"
	ActorProvisioner = "provisioner"
)

// OrphanKind classifies why a provider resource or CapacityRequest was
// quarantined. Values are stable for conditions, Events, and inventory.
const (
	OrphanUntrackedInstance      = "UntrackedInstance"
	OrphanMissingResource        = "MissingResource"
	OrphanDuplicateInstance      = "DuplicateInstance"
	OrphanBackendWithoutResource = "BackendWithoutResource"
	OrphanEnrollmentTimeout      = "EnrollmentTimeout"
	OrphanForeignInstallation    = "ForeignInstallation"
	OrphanExternalInfrastructure = "ExternalInfrastructure"
)

// OrphanObservationPhase is the durable repair state of one finding.
const (
	OrphanObserving   = "Observing"
	OrphanQuarantined = "Quarantined"
	OrphanAdopted     = "Adopted"
	OrphanTerminated  = "Terminated"
	OrphanReleased    = "Released"
	OrphanIgnored     = "Ignored"
	// OrphanResolved is a finding no longer observed; it is collected after a TTL.
	OrphanResolved = "Resolved"
)
