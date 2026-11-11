// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import "fmt"

// BackendOwnership identifies who owns the infrastructure behind a backend.
// An agent that advertises none (a manual join) is External.
// +kubebuilder:validation:Enum=External;Managed
type BackendOwnership string

const (
	OwnershipExternal BackendOwnership = "External"
	OwnershipManaged  BackendOwnership = "Managed"
)

// InfrastructureOperation is an operation that can change infrastructure.
type InfrastructureOperation string

const (
	OperationDrain     InfrastructureOperation = "Drain"
	OperationScale     InfrastructureOperation = "Scale"
	OperationTerminate InfrastructureOperation = "Terminate"
	OperationAdopt     InfrastructureOperation = "Adopt"
)

// NormalizeOwnership is External for every backend that does not advertise
// Managed: a manually registered one advertises nothing.
func NormalizeOwnership(ownership BackendOwnership) BackendOwnership {
	if ownership == OwnershipManaged {
		return OwnershipManaged
	}
	return OwnershipExternal
}

// AuthorizeInfrastructureOperation enforces the ownership safety boundary.
// Draining is scheduling state and is safe for either ownership class. Scaling,
// terminating, and adopting infrastructure require explicit Managed ownership.
func AuthorizeInfrastructureOperation(ownership BackendOwnership, operation InfrastructureOperation) error {
	ownership = NormalizeOwnership(ownership)
	switch operation {
	case OperationDrain:
		return nil
	case OperationScale, OperationTerminate, OperationAdopt:
		if ownership == OwnershipManaged {
			return nil
		}
		return fmt.Errorf("%s operation is forbidden for %s backend", operation, ownership)
	default:
		return fmt.Errorf("unknown infrastructure operation %q", operation)
	}
}
