// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// CloudAccountReady is True once every check of the account passed: the
	// provisioner assumed its credential, the cloud says it is the declared
	// account, and each permission it needs was tried.
	CloudAccountReady = "Ready"

	CloudCheckPassed  = "Passed"
	CloudCheckFailed  = "Failed"
	CloudCheckUnknown = "Unknown"

	// VMPoolCreateRehearsed is, on a pool of a cloud account, whether the
	// account's cloud would create one of its machines: the create rehearsed
	// with the pool's own network, image and instance profile, creating
	// nothing. False carries the cloud's answer.
	VMPoolCreateRehearsed = "CreateRehearsed"

	// AnnotationCloudAccountCheck asks for the account's checks to run again
	// now: any new value does (zenadm cloud-account check writes the time).
	AnnotationCloudAccountCheck = "infrastructure.zenthros.io/check-requested"

	// CloudAccountFinalizer holds a deleted account while a pool or a
	// capacity request still names it: the account's credential is how the
	// provisioner terminates their machines and sweeps what they left.
	CloudAccountFinalizer = "cloudaccount.infrastructure.zenthros.io/in-use"
)

// CloudRoleCredential is an identity of the account's own that the
// provisioner takes on, with nothing of the account stored in Zenthros: an
// IAM role it assumes on AWS.
type CloudRoleCredential struct {
	// AccountID is the cloud account the role is in. A credential that turns
	// out to act in another account is refused.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	AccountID string `json:"accountID"`

	// ARN names the role: arn:aws:iam::<accountID>:role/<name> on AWS.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	ARN string `json:"arn"`
}

// CloudSecretReference names a Secret of the installation's namespace that
// holds an API key, for a cloud with no roles to assume.
type CloudSecretReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Key is the Secret's key holding the credential.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Key string `json:"key"`
}

// CloudCredential is how the provisioner acts in the account: exactly one of
// its forms.
// +kubebuilder:validation:XValidation:rule="(has(self.role) ? 1 : 0) + (has(self.secretRef) ? 1 : 0) == 1",message="a credential is exactly one of role or secretRef"
type CloudCredential struct {
	// +optional
	Role *CloudRoleCredential `json:"role,omitempty"`

	// +optional
	SecretRef *CloudSecretReference `json:"secretRef,omitempty"`
}

// CloudAccountSpec registers one account of a cloud provider for pools to
// create their machines in.
// +kubebuilder:validation:XValidation:rule="self.provider == oldSelf.provider",message="provider is immutable: an account belongs to one cloud; register another"
// +kubebuilder:validation:XValidation:rule="has(self.access.workspaces) && size(self.access.workspaces) > 0",message="access.workspaces names the Workspaces whose pools may use the account: at least one"
type CloudAccountSpec struct {
	// Provider is the registry name of the provider the account is of ("aws").
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Provider string `json:"provider"`

	Credential CloudCredential `json:"credential"`

	// Access names the Workspaces whose pools may use the account. A pool
	// uses it only when every Workspace the pool admits is named here.
	Access AccessPolicy `json:"access"`
}

// CloudAccountCheck is one thing the provisioner tried in the account.
type CloudAccountCheck struct {
	// Name is what was tried, in the provider's words ("AssumeRole").
	Name string `json:"name"`

	// Status is Passed, Failed, or Unknown when the cloud's answer does not
	// say whether the permission is there.
	// +kubebuilder:validation:Enum=Passed;Failed;Unknown
	Status string `json:"status"`

	// Message is the cloud's own answer when the check did not pass.
	// +optional
	Message string `json:"message,omitempty"`
}

// CloudAccountStatus is what the provisioner found.
type CloudAccountStatus struct {
	// ExternalID is the secret Zenthros made for this account. The account's
	// owner puts it in the role's trust policy, so that nobody else can
	// register that role here and act through it.
	// +optional
	ExternalID string `json:"externalID,omitempty"`

	// VerifiedAccountID is the account the cloud says the credential acts
	// in, once it matched the declared one. Enrolling machines are compared
	// with it.
	// +optional
	VerifiedAccountID string `json:"verifiedAccountID,omitempty"`

	// Checks are the last run's results, one per permission.
	// +optional
	// +listType=map
	// +listMapKey=name
	Checks []CloudAccountCheck `json:"checks,omitempty"`

	// LastCheckedAt is when Checks were last run.
	// +optional
	LastCheckedAt *metav1.Time `json:"lastCheckedAt,omitempty"`

	// ObservedCheckRequest is the check request (AnnotationCloudAccountCheck)
	// the last run answered.
	// +optional
	ObservedCheckRequest string `json:"observedCheckRequest,omitempty"`

	// ObservedGeneration is the spec generation Checks were run for.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CloudAccount is an account of a cloud provider, other than the
// installation's own, that pools may create their machines in.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=cacct
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.provider`
// +kubebuilder:printcolumn:name="Account",type=string,JSONPath=`.status.verifiedAccountID`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type CloudAccount struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CloudAccountSpec   `json:"spec,omitempty"`
	Status CloudAccountStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type CloudAccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CloudAccount `json:"items"`
}

// IsReady reports whether the account's checks passed for its current spec.
func (a *CloudAccount) IsReady() bool {
	for _, condition := range a.Status.Conditions {
		if condition.Type == CloudAccountReady {
			return condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == a.Generation
		}
	}
	return false
}

// AllowsPool reports whether every Workspace the pool admits may use the
// account, and names the first that may not.
func (a *CloudAccount) AllowsPool(pool *VMPool) error {
	if len(pool.Spec.Access.Workspaces) == 0 {
		return fmt.Errorf("pool %s admits no Workspace, and cloud account %s is for named Workspaces only", pool.Name, a.Name)
	}
	for _, workspace := range pool.Spec.Access.Workspaces {
		if !a.Spec.Access.Allows(workspace) {
			return fmt.Errorf("cloud account %s does not admit Workspace %s, which pool %s does", a.Name, workspace, pool.Name)
		}
	}
	return nil
}

func init() {
	SchemeBuilder.Register(&CloudAccount{}, &CloudAccountList{})
}
