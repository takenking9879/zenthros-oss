// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import "testing"

func TestAuthorizeInfrastructureOperation(t *testing.T) {
	tests := []struct {
		name      string
		ownership BackendOwnership
		operation InfrastructureOperation
		wantErr   bool
	}{
		{name: "manual registration defaults external", operation: OperationScale, wantErr: true},
		{name: "external drain", ownership: OwnershipExternal, operation: OperationDrain},
		{name: "external terminate", ownership: OwnershipExternal, operation: OperationTerminate, wantErr: true},
		{name: "external adopt", ownership: OwnershipExternal, operation: OperationAdopt, wantErr: true},
		{name: "managed scale", ownership: OwnershipManaged, operation: OperationScale},
		{name: "managed terminate", ownership: OwnershipManaged, operation: OperationTerminate},
		{name: "managed adopt", ownership: OwnershipManaged, operation: OperationAdopt},
		{name: "unknown operation", ownership: OwnershipManaged, operation: "Replace", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AuthorizeInfrastructureOperation(tt.ownership, tt.operation)
			if (err != nil) != tt.wantErr {
				t.Fatalf("AuthorizeInfrastructureOperation() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
