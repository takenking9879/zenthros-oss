// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const QuotaLedgerName = "cluster"

// QuotaClaim is one durable reservation. ClaimID is the CapacityRequest
// requestID so a restart retries the same row instead of double-spending.
type QuotaClaim struct {
	// +kubebuilder:validation:MinLength=1
	ClaimID string `json:"claimID"`

	RequestNamespace string `json:"requestNamespace,omitempty"`
	RequestName      string `json:"requestName,omitempty"`
	Pool             string `json:"pool,omitempty"`
	Namespace        string `json:"namespace,omitempty"`
	WorkloadKind     string `json:"workloadKind,omitempty"`

	// Phase is requested, provisioning, ready, reserved, or allocated.
	Phase string `json:"phase,omitempty"`

	Instances int32 `json:"instances,omitempty"`
	GPUs      int32 `json:"gpus,omitempty"`
	VCPUs     int32 `json:"vcpus,omitempty"`

	HourlyCostCents   int64 `json:"hourlyCostCents,omitempty"`
	PriceKnown        bool  `json:"priceKnown,omitempty"`
	PriceStale        bool  `json:"priceStale,omitempty"`
	AccruedDailyCents int64 `json:"accruedDailyCents,omitempty"`

	ReservedAt    *metav1.Time `json:"reservedAt,omitempty"`
	LastAccruedAt *metav1.Time `json:"lastAccruedAt,omitempty"`
}

// QuotaLedgerSpec is empty: the ledger is a singleton status object.
type QuotaLedgerSpec struct{}

// QuotaLedgerStatus is the serialized accounting table. Updates use
// resourceVersion so concurrent Reserve calls cannot both spend the last unit.
type QuotaLedgerStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// DailyWindowStart is the UTC midnight that opened the current daily bucket.
	DailyWindowStart *metav1.Time `json:"dailyWindowStart,omitempty"`

	// DailyAccruedReleasedCents keeps spend from released claims inside the
	// current window so termination cannot reset the daily budget.
	DailyAccruedReleasedCents int64 `json:"dailyAccruedReleasedCents,omitempty"`

	// +listType=map
	// +listMapKey=claimID
	Claims []QuotaClaim `json:"claims,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=qled
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// QuotaLedger is the cluster-wide reservation table. QuotaPolicy objects are
// configuration; this object is the only CAS surface for spending quota.
type QuotaLedger struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QuotaLedgerSpec   `json:"spec,omitempty"`
	Status QuotaLedgerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type QuotaLedgerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QuotaLedger `json:"items"`
}

func init() {
	SchemeBuilder.Register(&QuotaLedger{}, &QuotaLedgerList{})
}
