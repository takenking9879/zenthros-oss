// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Prewarm target and item phases.
const (
	PrewarmPending = "Pending"
	PrewarmRunning = "Running"
	PrewarmReady   = "Ready"
	PrewarmFailed  = "Failed"
)

// PrewarmTargets selects the backends to warm. Every list adds backends; a
// Prewarm with no target warms nothing.
type PrewarmTargets struct {
	// Backends are backend ids.
	// +kubebuilder:validation:MaxItems=256
	// +optional
	Backends []string `json:"backends,omitempty"`
	// BackendSelector matches BackendAccess labels.
	// +optional
	BackendSelector *metav1.LabelSelector `json:"backendSelector,omitempty"`
	// Pools warms every machine of these VMPools.
	// +kubebuilder:validation:MaxItems=64
	// +optional
	Pools []string `json:"pools,omitempty"`
	// Fleets warms every machine of every member of these VMFleets.
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Fleets []string `json:"fleets,omitempty"`
	// All warms every enrolled backend.
	// +optional
	All bool `json:"all,omitempty"`
}

// PrewarmArtifact is a file jobs find at $ZENTHROS_CACHE_DIR/<path>. It is
// stored once per host by its digest; a zstd file (.zst) is verified as
// published and stored decompressed.
type PrewarmArtifact struct {
	// URI is an http(s) URL; use a presigned URL for object storage. Its
	// query is never repeated in status.
	// +kubebuilder:validation:Pattern=`^https?://`
	// +kubebuilder:validation:MaxLength=4096
	URI string `json:"uri"`
	// SHA256 is the digest of the bytes at URI.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	SHA256 string `json:"sha256"`
	// Path is the name jobs use, relative to the cache.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$`
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:XValidation:rule="!self.contains('..')",message="path cannot contain .."
	Path string `json:"path"`
}

// PrewarmToolchain is a host toolchain installed by the agent's installer
// module (VM backends only).
type PrewarmToolchain struct {
	// +kubebuilder:validation:Enum=python;node;go;java
	Name string `json:"name"`
	// +kubebuilder:validation:MaxLength=32
	// +optional
	Version string `json:"version,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=128
	// +optional
	Packages []string `json:"packages,omitempty"`
}

// PrewarmItem is one thing to warm: exactly one of image, artifact and
// toolchain.
// +kubebuilder:validation:XValidation:rule="(has(self.image) ? 1 : 0) + (has(self.artifact) ? 1 : 0) + (has(self.toolchain) ? 1 : 0) == 1",message="set exactly one of image, artifact and toolchain"
type PrewarmItem struct {
	// Image is an OCI image reference, best pinned by digest.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	Artifact *PrewarmArtifact `json:"artifact,omitempty"`
	// +optional
	Toolchain *PrewarmToolchain `json:"toolchain,omitempty"`
}

// Ref names the item in status: an artifact by the path jobs read it at,
// which is unique in a Prewarm; two paths may share one digest.
func (i PrewarmItem) Ref() string {
	switch {
	case i.Image != "":
		return "image:" + i.Image
	case i.Artifact != nil:
		return "artifact:" + i.Artifact.Path
	case i.Toolchain != nil:
		if i.Toolchain.Version == "" {
			return "toolchain:" + i.Toolchain.Name
		}
		return "toolchain:" + i.Toolchain.Name + "@" + i.Toolchain.Version
	}
	return ""
}

// PrewarmKubernetes is how a Kubernetes backend warms.
type PrewarmKubernetes struct {
	// NodeSelector limits the image pre-pull DaemonSet to these nodes.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// PVCName is the claim, in the agent's namespace, artifacts are written
	// to. Required for artifacts on a Kubernetes backend.
	// +optional
	PVCName string `json:"pvcName,omitempty"`
	// Image is a static busybox (the -musl variant): the pre-pull pods copy
	// its binary into every image to keep it running, and artifact Jobs use
	// its wget and sha256sum. The default busybox variant is dynamically
	// linked and cannot run inside another image.
	// +kubebuilder:default="busybox:1.36.1-musl"
	// +optional
	Image string `json:"image,omitempty"`
}

// PrewarmSpec is what to put on which machines before jobs need it.
type PrewarmSpec struct {
	Targets PrewarmTargets `json:"targets"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:XValidation:rule="self.all(x, !has(x.artifact) || self.exists_one(y, has(y.artifact) && y.artifact.path == x.artifact.path))",message="each artifact needs its own path"
	Items []PrewarmItem `json:"items"`
	// OnNewMembers warms each new machine of the targeted pools and fleets
	// before its slot is Ready: a Ready slot is a warm slot. A machine whose
	// warming failed is Ready anyway, with a Prewarmed=False condition.
	// +optional
	OnNewMembers bool `json:"onNewMembers,omitempty"`
	// Concurrency is how many items a machine downloads at once.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=16
	// +kubebuilder:default=4
	// +optional
	Concurrency int32 `json:"concurrency,omitempty"`
	// +optional
	Kubernetes *PrewarmKubernetes `json:"kubernetes,omitempty"`
}

// PrewarmItemStatus is one item on one backend. Message is the
// destination's own text on failure.
type PrewarmItemStatus struct {
	Ref   string `json:"ref"`
	Phase string `json:"phase"`
	// +optional
	Bytes int64 `json:"bytes,omitempty"`
	// +optional
	DurationMillis int64 `json:"durationMillis,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// PrewarmTargetStatus is one backend's progress.
type PrewarmTargetStatus struct {
	BackendID string `json:"backendID"`
	// Phase is Pending, Running, Ready or Failed (any item failed).
	Phase string `json:"phase"`
	// +optional
	Items []PrewarmItemStatus `json:"items,omitempty"`
	// Message is why the backend could not be asked (unreachable, refused).
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	UpdatedAt *metav1.Time `json:"updatedAt,omitempty"`
}

// PrewarmStatus is observed state.
type PrewarmStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +listType=map
	// +listMapKey=backendID
	// +optional
	Targets []PrewarmTargetStatus `json:"targets,omitempty"`
	// Ready and Total count targets.
	Ready int32 `json:"ready"`
	Total int32 `json:"total"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Target is backendID's status, or nil.
func (s *PrewarmStatus) Target(backendID string) *PrewarmTargetStatus {
	for i := range s.Targets {
		if s.Targets[i].BackendID == backendID {
			return &s.Targets[i]
		}
	}
	return nil
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=pw
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Targets",type=integer,JSONPath=`.status.total`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Prewarm puts images and artifacts on machines before jobs need them.
type Prewarm struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PrewarmSpec   `json:"spec,omitempty"`
	Status PrewarmStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PrewarmList contains a list of Prewarm.
type PrewarmList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Prewarm `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Prewarm{}, &PrewarmList{})
}
