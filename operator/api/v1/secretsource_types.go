// SPDX-License-Identifier: Apache-2.0

package v1

import "regexp"

const (
	SecretSourceTypeKubernetes        = "k8s-secret"
	SecretSourceTypeAWSSecretsManager = "aws-secrets-manager"
	SecretSourceTypeEncryptedPayload  = "encrypted-payload"
	// SecretSourceTypeWorkspace is a secret the Workspace keeps (zenthros-api
	// writes it: PUT /secrets/{name}). The controller seals it to the age key
	// of the machine the job goes to, as it sends the job, so it reaches a
	// machine made after the job was written (a template's, one that slept).
	SecretSourceTypeWorkspace = "workspace-secret"

	// A Workspace secret is the Secret WorkspaceSecretObject(namespace, name)
	// in the secrets namespace (DefaultWorkspaceSecretsNamespace), its value
	// under WorkspaceSecretKey, labelled LabelWorkspaceSecret and
	// LabelWorkspaceNamespace: only such Secrets are ever read for a job. They
	// are kept apart from the Workspaces' own namespaces, so zenthros-api
	// holds Secrets there and nowhere else: bound in each Workspace's
	// namespace, it could read every Secret a Workspace keeps for itself
	// (pull secrets, k8s-secret sources).
	DefaultWorkspaceSecretsNamespace = "zenthros-workspace-secrets"
	WorkspaceSecretKey               = "value"
	LabelWorkspaceSecret             = "zenthros.ai/workspace-secret"
	LabelWorkspaceNamespace          = "zenthros.ai/workspace-namespace"
	// maxWorkspaceSecretName keeps the name the Secret used to have in the
	// Workspace's namespace (zenthros-secret-<name>, a DNS label).
	maxWorkspaceSecretName = 47

	SecretMaterializationEnv  = "env"
	SecretMaterializationFile = "file"
)

var workspaceSecretPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// WorkspaceSecretName says whether name can name a Workspace secret: a DNS
// label of at most 47 characters.
func WorkspaceSecretName(name string) bool {
	return len(name) <= maxWorkspaceSecretName && workspaceSecretPattern.MatchString(name)
}

// WorkspaceSecretObject is the name of the Secret that keeps the secret name
// of the Workspace whose namespace is namespace. A namespace is a DNS label
// and holds no dot, so two Workspaces never share one.
func WorkspaceSecretObject(namespace, name string) string {
	return namespace + "." + name
}

// SecretSourceRef identifies secret material that is resolved only in the
// destination environment. It never contains a secret value.
// +kubebuilder:validation:XValidation:rule="self.type == 'k8s-secret' || self.type == 'aws-secrets-manager' || self.type == 'encrypted-payload' || self.type == 'workspace-secret'",message="unsupported secret source type"
// +kubebuilder:validation:XValidation:rule="self.type != 'workspace-secret' || (has(self.name) && !has(self.namespace) && !has(self.key) && !has(self.awsSecretsManager) && !has(self.encryptedPayload))",message="workspace-secret requires name and forbids other source coordinates"
// +kubebuilder:validation:XValidation:rule="self.type != 'k8s-secret' || (has(self.namespace) && has(self.name) && has(self.key) && !has(self.awsSecretsManager) && !has(self.encryptedPayload))",message="k8s-secret requires namespace, name, and key"
// +kubebuilder:validation:XValidation:rule="self.type != 'aws-secrets-manager' || (has(self.awsSecretsManager) && !has(self.namespace) && !has(self.name) && !has(self.key) && !has(self.encryptedPayload))",message="aws-secrets-manager requires awsSecretsManager and forbids Kubernetes coordinates"
// +kubebuilder:validation:XValidation:rule="self.type != 'encrypted-payload' || (has(self.encryptedPayload) && !has(self.namespace) && !has(self.name) && !has(self.key) && !has(self.awsSecretsManager))",message="encrypted-payload requires encryptedPayload and forbids other source coordinates"
// +kubebuilder:validation:XValidation:rule="self.type != 'encrypted-payload' || self.materialization.mode != 'env' || has(self.materialization.envName)",message="encrypted-payload env materialization requires envName"
// +kubebuilder:validation:XValidation:rule="self.type != 'k8s-secret' || (self.key != '.' && self.key != '..')",message="key cannot be . or .."
// +kubebuilder:validation:XValidation:rule="self.materialization.mode != 'env' || !has(self.materialization.path)",message="path is only valid for file materialization"
// +kubebuilder:validation:XValidation:rule="self.materialization.mode != 'file' || !has(self.materialization.envName)",message="envName is only valid for env materialization"
type SecretSourceRef struct {
	// Type selects the destination-side resolver.
	// +kubebuilder:validation:Enum=k8s-secret;aws-secrets-manager;encrypted-payload;workspace-secret
	Type string `json:"type"`

	// Namespace is the destination Kubernetes namespace containing the Secret.
	// It must equal the namespace selected for the workload.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace,omitempty"`

	// Name is the destination Kubernetes Secret name; for workspace-secret,
	// the Workspace secret's name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Name string `json:"name,omitempty"`

	// Key selects one entry in the destination Kubernetes Secret.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	Key string `json:"key,omitempty"`

	// AWSSecretsManager identifies an AWS Secrets Manager secret resolved by the
	// destination worker identity. Required when type is aws-secrets-manager.
	AWSSecretsManager *AWSSecretsManagerRef `json:"awsSecretsManager,omitempty"`

	// EncryptedPayload is age ciphertext sealed to destination recipients.
	// Required when type is encrypted-payload. It never contains plaintext.
	EncryptedPayload *EncryptedPayloadRef `json:"encryptedPayload,omitempty"`

	// Materialization controls how the destination workload receives the value.
	Materialization SecretMaterialization `json:"materialization"`
}

// AWSSecretsManagerRef is a destination-side pointer at Secrets Manager. The
// controller transports these fields unchanged and never calls GetSecretValue.
// +kubebuilder:validation:XValidation:rule="!has(self.versionId) || !has(self.versionStage)",message="versionId and versionStage are mutually exclusive"
type AWSSecretsManagerRef struct {
	// SecretID is a secret name or full ARN.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^(arn:aws:secretsmanager:[a-z0-9-]+:\d{12}:secret:.+|[A-Za-z0-9/_+=.@-]+)$`
	SecretID string `json:"secretId"`

	// Region is the AWS region that hosts the secret. The destination agent
	// uses this region and does not inherit controller or provisioner region.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[a-z]{2}-[a-z0-9-]+-\d+$`
	Region string `json:"region"`

	// VersionID pins a specific secret version. Mutually exclusive with VersionStage.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9-]+$`
	VersionID string `json:"versionId,omitempty"`

	// VersionStage selects a staging label such as AWSCURRENT. When omitted
	// together with VersionID, the agent reads AWSCURRENT at resolve time.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9/_+=.@-]+$`
	VersionStage string `json:"versionStage,omitempty"`

	// JSONKey extracts one field from a JSON secret string. When omitted the
	// agent materializes the raw secret string.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	JSONKey string `json:"jsonKey,omitempty"`
}

// EncryptedPayloadRef is destination-sealed age ciphertext. The controller
// transports these fields unchanged and never decrypts.
// +kubebuilder:validation:XValidation:rule="!has(self.workspaceSecret)",message="workspaceSecret is set by the controller when it seals a Workspace secret: name it with type workspace-secret"
type EncryptedPayloadRef struct {
	// Ciphertext is ASCII-armored age ciphertext.
	// +kubebuilder:validation:MinLength=64
	// +kubebuilder:validation:MaxLength=65536
	Ciphertext string `json:"ciphertext"`

	// Recipients are the age1 X25519 public keys this ciphertext was sealed to.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	Recipients []string `json:"recipients"`

	// KeyID is an optional short fingerprint of the primary recipient.
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9]+$`
	KeyID string `json:"keyId,omitempty"`

	// WorkspaceSecret is the Workspace secret the controller sealed this
	// from, never set in a ZenthrosJob. age seals with fresh randomness, so
	// a resent job is known by this name rather than by its ciphertext.
	WorkspaceSecret string `json:"workspaceSecret,omitempty"`
}

// SecretMaterialization describes a destination-side env or file binding.
type SecretMaterialization struct {
	// Mode selects an environment variable or a read-only 0600 file.
	// +kubebuilder:validation:Enum=env;file
	Mode string `json:"mode"`

	// EnvName overrides the environment variable name. When omitted, the Secret
	// key (or AWS JSON key / secret name) is normalized to upper snake case.
	// Required for encrypted-payload env materialization.
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_]*$`
	EnvName string `json:"envName,omitempty"`

	// Path overrides the absolute file path. When omitted, the value is mounted
	// below /var/run/zenthros/secrets/<secret-name>/<key>.
	// +kubebuilder:validation:Pattern=`^/[^\n\r]*$`
	Path string `json:"path,omitempty"`
}
