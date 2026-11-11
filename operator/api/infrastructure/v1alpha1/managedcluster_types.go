// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	ManagedClusterFinalizer = "managedcluster.infrastructure.zenthros.io/unbind"
	LabelManagedClusterName = "infrastructure.zenthros.io/managedcluster"
	LabelManagedClusterUID  = "infrastructure.zenthros.io/managedcluster-uid"
	BlueprintAWSEKSAimlV1   = "aws-eks-aiml/v1"
)

// ManagedClusterPhase is the durable enrollment lifecycle of one managed
// Kubernetes cluster. Terraform owns create/destroy; this object never
// creates EKS or worker nodes.
// +kubebuilder:validation:Enum=Pending;AwaitingEnrollment;Ready;Terminating;Failed
type ManagedClusterPhase string

const (
	ManagedClusterPending            ManagedClusterPhase = "Pending"
	ManagedClusterAwaitingEnrollment ManagedClusterPhase = "AwaitingEnrollment"
	ManagedClusterReady              ManagedClusterPhase = "Ready"
	ManagedClusterTerminating        ManagedClusterPhase = "Terminating"
	ManagedClusterFailed             ManagedClusterPhase = "Failed"
)

// ManagedClusterSpec is immutable cluster identity used to bind enrollment.
// +kubebuilder:validation:XValidation:rule="self.backendID == oldSelf.backendID",message="backendID is immutable"
// +kubebuilder:validation:XValidation:rule="self.clusterName == oldSelf.clusterName",message="clusterName is immutable"
// +kubebuilder:validation:XValidation:rule="self.region == oldSelf.region",message="region is immutable"
// +kubebuilder:validation:XValidation:rule="self.accountID == oldSelf.accountID",message="accountID is immutable"
// +kubebuilder:validation:XValidation:rule="self.enrollmentRoleARN == oldSelf.enrollmentRoleARN",message="enrollmentRoleARN is immutable"
// +kubebuilder:validation:XValidation:rule="self.blueprint == oldSelf.blueprint",message="blueprint is immutable"
// +kubebuilder:validation:XValidation:rule="!has(self.providerRef.account) || size(self.providerRef.account) == 0",message="providerRef.account is for VMPools: a ManagedCluster names its own account in accountID"
type ManagedClusterSpec struct {
	// BackendID is the registered Kubernetes backend identifier.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	BackendID string `json:"backendID"`

	// ClusterName is the EKS cluster name. Combined with account and region
	// it determines the expected cluster ARN.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=100
	ClusterName string `json:"clusterName"`

	// Region is the AWS region of the cluster.
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`

	// AccountID is the AWS account that must own the enrollment role.
	// +kubebuilder:validation:Pattern=`^[0-9]{12}$`
	AccountID string `json:"accountID"`

	// EnrollmentRoleARN is the IAM role assumed by the in-cluster enrollment Job.
	// +kubebuilder:validation:MinLength=20
	EnrollmentRoleARN string `json:"enrollmentRoleARN"`

	// Blueprint is the pinned Terraform blueprint identity.
	// +kubebuilder:validation:Enum=aws-eks-aiml/v1
	Blueprint string `json:"blueprint"`

	// KubernetesVersion is the pinned EKS version this object expects.
	// +kubebuilder:validation:MinLength=1
	KubernetesVersion string `json:"kubernetesVersion"`

	ProviderRef ProviderReference `json:"providerRef"`

	// Ownership is always Managed for this resource class.
	// +kubebuilder:validation:Enum=Managed
	// +kubebuilder:default=Managed
	Ownership BackendOwnership `json:"ownership,omitempty"`

	Access AccessPolicy `json:"access"`
}

// ManagedClusterStatus is enrollment progress. Provider resource identity
// lives here so a restart cannot mint a second cluster.
type ManagedClusterStatus struct {
	// +kubebuilder:validation:Enum=Pending;AwaitingEnrollment;Ready;Terminating;Failed
	Phase ManagedClusterPhase `json:"phase,omitempty"`

	ProviderResourceID string           `json:"providerResourceID,omitempty"`
	BackendID          string           `json:"backendID,omitempty"`
	Address            string           `json:"address,omitempty"`
	Ownership          BackendOwnership `json:"ownership,omitempty"`

	ObservedIdentity *ObservedMachineIdentity `json:"observedIdentity,omitempty"`

	EnrollmentAttempts int32 `json:"enrollmentAttempts,omitempty"`
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	EnrollmentAt       *metav1.Time       `json:"enrollmentAt,omitempty"`
	ReadyAt            *metav1.Time       `json:"readyAt,omitempty"`
	LastTransitionTime *metav1.Time       `json:"lastTransitionTime,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// ClusterARN is the canonical EKS ARN this object will bind after enrollment.
func (s ManagedClusterSpec) ClusterARN() string {
	return "arn:aws:eks:" + s.Region + ":" + s.AccountID + ":cluster/" + s.ClusterName
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=mcl
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.spec.backendID`
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ManagedCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ManagedClusterSpec   `json:"spec,omitempty"`
	Status ManagedClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ManagedClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ManagedCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ManagedCluster{}, &ManagedClusterList{})
}
