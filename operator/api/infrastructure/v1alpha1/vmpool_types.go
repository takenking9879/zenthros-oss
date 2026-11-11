// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	VMPoolFinalizer      = "vmpool.infrastructure.zenthros.io/capacity-cleanup"
	CapacityManagementNS = "zenthros-system"
	LabelVMPoolName      = "infrastructure.zenthros.io/vmpool"
	LabelVMPoolUID       = "infrastructure.zenthros.io/vmpool-uid"
	LabelCapacitySlot    = "infrastructure.zenthros.io/slot"

	// AnnotationIdleAfterBoot marks a pool that keeps one machine up only
	// until it is Ready: then the provisioner sets minReady to 0 and drops
	// the mark, and the pool's idleTTL ends the machine once it has no job.
	// zenthros-api writes it on a machine made for one call, which must be
	// up before its job exists and must not outlive a caller that is gone.
	AnnotationIdleAfterBoot = "infrastructure.zenthros.io/idle-after-boot"
)

// MachineKind is what a pool's provider hands out.
// +kubebuilder:validation:Enum=VM;Container
type MachineKind string

const (
	// MachineKindVM is a whole machine: the installer runs as root, the
	// agent runs under systemd (backend type vm).
	MachineKindVM MachineKind = "VM"
	// MachineKindContainer is one unprivileged container whose entrypoint is
	// the agent (backend type container, docs/design/container-agent.md).
	// Nothing is installed: the image is the runtime, and only Python is
	// completed, with uv.
	MachineKindContainer MachineKind = "Container"
)

// PurchasingMode selects how provider capacity may be purchased.
// +kubebuilder:validation:Enum=OnDemand;Spot
type PurchasingMode string

const (
	PurchasingModeOnDemand PurchasingMode = "OnDemand"
	PurchasingModeSpot     PurchasingMode = "Spot"
)

// ProviderReference selects a configured provider capability by name.
type ProviderReference struct {
	// Name is the stable provider identifier, for example "fake" or "aws".
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Account names the CloudAccount the pool's machines are created in.
	// Empty is the installation's own account. The network, the instance
	// profile and the image the pool names are that account's.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Account string `json:"account,omitempty"`
}

// PlacementReference constrains provider placement without making a live
// provider query part of reconciliation.
type PlacementReference struct {
	// Region is the provider region identifier.
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`

	// Zone optionally pins one availability zone within Region.
	Zone string `json:"zone,omitempty"`
}

// NetworkReference identifies pre-existing network configuration.
type NetworkReference struct {
	// Name is the stable network configuration identifier. A VM pool must
	// set it (VMPoolSpec validation).
	Name string `json:"name"`

	// Zones are the networks of further availability zones, for a provider
	// whose network lives in one zone (an AWS subnet). Name is then the
	// network of placementRef.zone, and the pool places machines only in
	// that zone and these: a create that finds no capacity in one tries the
	// next, and a Spot node can move to a zone at less risk
	// (interruption.capacityRebalance).
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=zone
	Zones []ZonalNetwork `json:"zones,omitempty"`
}

// ZonalNetwork is the network of one availability zone.
type ZonalNetwork struct {
	// +kubebuilder:validation:MinLength=1
	Zone string `json:"zone"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// ZonalNetworks reports whether the pool names a network per zone.
func (p *VMPool) ZonalNetworks() bool {
	return p != nil && len(p.Spec.NetworkRef.Zones) > 0
}

// Zones are the zones a pool with zonal networks places machines in:
// placementRef.zone first, then networkRef.zones. Nil without zonal networks.
func (p *VMPool) Zones() []string {
	if !p.ZonalNetworks() {
		return nil
	}
	zones := []string{p.Spec.PlacementRef.Zone}
	for _, z := range p.Spec.NetworkRef.Zones {
		zones = append(zones, z.Zone)
	}
	return zones
}

// NetworkFor is the network a machine placed in zone uses: that zone's entry
// of networkRef.zones, or networkRef.name.
func (p *VMPool) NetworkFor(zone string) string {
	for _, z := range p.Spec.NetworkRef.Zones {
		if zone != "" && z.Zone == zone {
			return z.Name
		}
	}
	return p.Spec.NetworkRef.Name
}

// IdentitySpec contains references only; it never carries credentials.
type IdentitySpec struct {
	// InstanceProfile is the provider-native workload identity reference.
	InstanceProfile string `json:"instanceProfile,omitempty"`

	// ServiceAccountName is the Kubernetes service account used by managed
	// cluster capacity when applicable.
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

// InstancePolicy bounds which machine images and instance shapes a pool may use.
type InstancePolicy struct {
	// Image is a provider image reference (for example an AMI id or fake image).
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// AllowedTypes is the bounded set of provider instance types the pool may use.
	// +kubebuilder:validation:MinItems=1
	AllowedTypes []string `json:"allowedTypes"`

	// DiskGB is the machine's disk in GB, for providers whose default is too
	// small for an image and its data (a model's weights). Empty keeps the
	// provider's default. Only new machines take a change. A provider that
	// cannot size the disk refuses the pool.
	// +optional
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=2000
	DiskGB int32 `json:"diskGB,omitempty"`

	// MinCUDA is the least CUDA version the host's driver must support
	// (e.g. "12.9"), for an image built against a newer CUDA. A provider
	// that cannot filter hosts by it refuses the pool.
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]+\.[0-9]+$`
	MinCUDA string `json:"minCUDA,omitempty"`

	// SecurityGroupIDs are the security groups the machines launch in, in
	// place of the network's default one (AWS). Zenthros neither creates
	// nor edits them: they must allow the agent's egress, and the ingress
	// of any exposedPorts.
	// +optional
	// +kubebuilder:validation:MaxItems=5
	SecurityGroupIDs []string `json:"securityGroupIDs,omitempty"`
}

// ExposedPort is a port every machine of a pool publishes, so a Service on
// it can be reached. 8080 and 8081 are the Zenthros agent's own (metrics and
// health) on every machine: a Service there fails to bind and restarts until
// its retries run out (measured on EC2, 2026-10-04).
// +kubebuilder:validation:XValidation:rule="self.port != 8080 && self.port != 8081",message="8080 and 8081 are the Zenthros agent's metrics and health ports on the machine; a Service cannot listen there"
type ExposedPort struct {
	// Port is the port number.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`

	// Protocol is HTTP (the default) or TCP. A provider that publishes only
	// one of them refuses the other.
	// +optional
	// +kubebuilder:default=HTTP
	// +kubebuilder:validation:Enum=HTTP;TCP
	Protocol string `json:"protocol,omitempty"`
}

// ScalingPolicy controls the pool's ready-capacity envelope.
// +kubebuilder:validation:XValidation:rule="!has(self.minReady) || self.minReady <= self.maxReady",message="minReady must be less than or equal to maxReady"
// +kubebuilder:validation:XValidation:rule="!has(self.replicas) || self.replicas <= self.maxReady",message="replicas must be less than or equal to maxReady"
type ScalingPolicy struct {
	// Replicas is a size an external driver asks for (the scale subresource:
	// kubectl scale, or the cluster autoscaler of a joined pool). Unset, the
	// pool sizes itself from minReady and its jobs. Set, the pool keeps
	// replicas machines, never fewer than its reserved ones, within
	// minReady and maxReady.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// MinReady is the minimum ready, unallocated capacity to retain.
	// +kubebuilder:validation:Minimum=0
	MinReady int32 `json:"minReady,omitempty"`

	// MaxReady is the maximum ready capacity retained by this pool.
	// +kubebuilder:validation:Minimum=1
	MaxReady int32 `json:"maxReady"`

	// IdleTTL is how long unallocated capacity may remain idle before scale-down.
	// +kubebuilder:default="10m"
	IdleTTL *metav1.Duration `json:"idleTTL,omitempty"`

	// IdleGrace is how long a Ready slot with no job may stay silent before
	// it is treated as lost. Empty uses the cluster default (30s). Distinct
	// from IdleTTL (healthy excess scale-down) and from jobGrace (running jobs).
	// +optional
	IdleGrace *metav1.Duration `json:"idleGrace,omitempty"`

	// MaxLifetime is how long one machine of this pool may live, counted from
	// its CapacityRequest's creation, job or no job, so a machine somebody
	// forgot does not bill for ever. Past it the machine ends and the pool
	// keeps none up (minReady 0) until a job asks for one again. A job on it
	// is told as by a cloud's notice, and ends or moves with
	// MACHINE_LIFETIME. Empty is no limit.
	// +optional
	MaxLifetime *metav1.Duration `json:"maxLifetime,omitempty"`
}

// VMPoolSpec declares a bounded source of VM capacity.
// +kubebuilder:validation:XValidation:rule="self.ownership == oldSelf.ownership",message="ownership is immutable"
// +kubebuilder:validation:XValidation:rule="self.machineKind == oldSelf.machineKind",message="machineKind is immutable"
// +kubebuilder:validation:XValidation:rule="self.providerRef.name == oldSelf.providerRef.name",message="providerRef is immutable: a pool's machines belong to its provider; create another pool"
// +kubebuilder:validation:XValidation:rule="(has(self.providerRef.account) && size(self.providerRef.account) > 0 ? [self.providerRef.account] : []) == (has(oldSelf.providerRef.account) && size(oldSelf.providerRef.account) > 0 ? [oldSelf.providerRef.account] : [])",message="providerRef.account is immutable: a pool's machines are in its cloud account; create another pool"
// +kubebuilder:validation:XValidation:rule="(has(self.exposedPorts) ? self.exposedPorts : []) == (has(oldSelf.exposedPorts) ? oldSelf.exposedPorts : [])",message="exposedPorts is immutable: a machine gets its ports when it is created; create another pool"
// +kubebuilder:validation:XValidation:rule="self.machineKind == 'Container' || (has(self.networkRef) && size(self.networkRef.name) > 0)",message="a VM pool needs networkRef.name"
// +kubebuilder:validation:XValidation:rule="self.machineKind != 'Container' || !has(self.runtime) || !has(self.runtime.packages) || size(self.runtime.packages) == 0",message="a Container pool cannot install system packages: they need root and a package manager; put them in the image"
// +kubebuilder:validation:XValidation:rule="self.machineKind != 'Container' || !has(self.runtime) || ((!has(self.runtime.container) || (!has(self.runtime.container.engine) && !has(self.runtime.container.gpu))) && !has(self.runtime.defaultJobRuntime))",message="a Container pool runs its jobs as processes in its image: runtime.container and runtime.defaultJobRuntime do not apply"
// +kubebuilder:validation:XValidation:rule="self.machineKind != 'Container' || !has(self.runtime) || !has(self.runtime.toolchains) || self.runtime.toolchains.all(t, t.name == 'python')",message="a Container pool completes only the image's python; other toolchains belong in the image"
// +kubebuilder:validation:XValidation:rule="(has(self.join) ? [self.join] : []) == (has(oldSelf.join) ? [oldSelf.join] : [])",message="join is immutable: a machine joins its cluster when it is created; create another pool"
// +kubebuilder:validation:XValidation:rule="!has(self.join) || (self.machineKind != 'Container' && self.ownership != 'External' && size(self.instancePolicy.allowedTypes) == 1)",message="a joined pool is a Managed VM pool with exactly one allowed type: a node group is homogeneous"
// +kubebuilder:validation:XValidation:rule="!has(self.join) || has(self.scaling.replicas)",message="a joined pool declares scaling.replicas (0 is fine): its cluster's autoscaler sizes it through /scale, and /scale needs the field"
// +kubebuilder:validation:XValidation:rule="!has(self.networkRef) || !has(self.networkRef.zones) || size(self.networkRef.zones) == 0 || (has(self.placementRef.zone) && size(self.placementRef.zone) > 0 && self.networkRef.zones.all(z, z.zone != self.placementRef.zone))",message="networkRef.zones adds zones to placementRef.zone, whose network is networkRef.name: set placementRef.zone and do not list it again"
// +kubebuilder:validation:XValidation:rule="!has(self.interruption) || !has(self.interruption.capacityRebalance) || !self.interruption.capacityRebalance || (has(self.join) && self.purchasingMode == 'Spot')",message="interruption.capacityRebalance replaces the nodes of a joined Spot pool; set it on a joined pool with purchasingMode Spot"
type VMPoolSpec struct {
	// MachineKind is what the provider hands out: a VM (the default) or an
	// unprivileged container whose entrypoint is the agent. A Container
	// pool's instancePolicy.image is the container image every job runs in.
	// +kubebuilder:default=VM
	// +optional
	MachineKind MachineKind `json:"machineKind,omitempty"`

	// Ownership declares whether Zenthros manages this pool's infrastructure.
	// Managed pools may be provisioned, scaled, and terminated by the platform;
	// External pools describe capacity owned elsewhere and can never be used by
	// poolRef workloads or scaled by Zenthros.
	// +kubebuilder:validation:Enum=External;Managed
	// +kubebuilder:default=Managed
	Ownership BackendOwnership `json:"ownership,omitempty"`

	// Access is the Workspace allowlist for poolRef selection. Visibility and
	// infrastructure ownership are intentionally independent.
	Access AccessPolicy `json:"access,omitempty"`

	ProviderRef  ProviderReference  `json:"providerRef"`
	PlacementRef PlacementReference `json:"placementRef"`
	// NetworkRef is required for VM pools; a container has no network of
	// its own to choose.
	// +optional
	NetworkRef NetworkReference `json:"networkRef,omitempty"`
	Identity   IdentitySpec     `json:"identity,omitempty"`

	InstancePolicy InstancePolicy `json:"instancePolicy"`

	// ExposedPorts are the ports every machine of the pool publishes when
	// the provider creates it, so a Service (workload.template.service) can
	// be reached on them. A machine exists before its job, so they belong to
	// the pool; they cannot change: another set of ports is another pool.
	// +optional
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=port
	ExposedPorts []ExposedPort `json:"exposedPorts,omitempty"`

	// PurchasingMode is explicit so later Spot support cannot silently change
	// the cost and interruption behavior of an existing pool.
	// +kubebuilder:default=OnDemand
	PurchasingMode PurchasingMode `json:"purchasingMode,omitempty"`

	Scaling ScalingPolicy `json:"scaling"`

	// Runtime is what the installer prepares on every machine of the pool,
	// beyond the agent itself. It is language neutral: a container engine
	// runs jobs in OCI images of any language; host toolchains are optional.
	// +optional
	Runtime RuntimeSpec `json:"runtime,omitempty"`

	// Interruption is what the pool does for a job whose machine receives
	// an interruption notice (a Spot reclaim): where its replacement is
	// prepared while the machine drains.
	// +optional
	Interruption *InterruptionPolicy `json:"interruption,omitempty"`

	// Join makes the pool's machines nodes of a Kubernetes cluster
	// (docs/design/K8S_NODE_SUPPLY.md). A joined pool serves no job directly;
	// its size is scaling.replicas, which the cluster's autoscaler sets.
	// +optional
	Join *JoinSpec `json:"join,omitempty"`
}

// StandbyPolicy is where a job's replacement is prepared while its
// interrupted machine drains (docs/checkpoints.md).
type StandbyPolicy string

const (
	// StandbyIdleFirst claims a Ready idle slot of the pool at the notice;
	// without one, it creates a machine timed to be Ready when the drain
	// window ends. The pool refills the idle slot it gave.
	StandbyIdleFirst StandbyPolicy = "IdleFirst"
	// StandbyNewMachine always creates the machine, timed the same way, and
	// leaves idle slots to new jobs.
	StandbyNewMachine StandbyPolicy = "NewMachine"
	// StandbyNone prepares nothing: the replacement is seated when the drain
	// window ends.
	StandbyNone StandbyPolicy = "None"
)

// InterruptionPolicy is a pool's answer to an interruption notice.
type InterruptionPolicy struct {
	// Standby is where the replacement is prepared. IdleFirst by default.
	// +kubebuilder:validation:Enum=IdleFirst;NewMachine;None
	// +optional
	Standby StandbyPolicy `json:"standby,omitempty"`

	// CapacityRebalance replaces a joined Spot pool's node when its provider
	// says the machine is at elevated risk of interruption (an EC2 rebalance
	// recommendation; other providers send none). The replacement is created
	// first, only in a zone whose availability is the same or better, and
	// the node drains once the replacement is up (docs/interruptions.md,
	// "Rebalance recommendations"). Off by default: a recommendation is not
	// always followed by an interruption, and acting on it restarts pods.
	// +optional
	CapacityRebalance bool `json:"capacityRebalance,omitempty"`
}

// CapacityRebalance reports whether the pool acts on rebalance recommendations.
func (p *VMPool) CapacityRebalance() bool {
	return p != nil && p.Spec.Interruption != nil && p.Spec.Interruption.CapacityRebalance
}

// Standby is the pool's standby policy, IdleFirst when unset.
func (p *VMPool) Standby() StandbyPolicy {
	if p == nil || p.Spec.Interruption == nil || p.Spec.Interruption.Standby == "" {
		return StandbyIdleFirst
	}
	return p.Spec.Interruption.Standby
}

// MaxProvisioningSamples is how many recent provisioning times a pool keeps.
const MaxProvisioningSamples = 20

// ProvisioningSample is how long one of the pool's machines took from its
// request to Ready.
type ProvisioningSample struct {
	// Request is the CapacityRequest measured, so a machine counts once.
	Request string `json:"request"`
	// Seconds is requestedAt to readyAt.
	Seconds int32 `json:"seconds"`
}

// RuntimeSpec is a pool's machine runtime. Empty installs the Zenthros base
// only, and jobs run as systemd units on the host.
// +kubebuilder:validation:XValidation:rule="!has(self.defaultJobRuntime) || self.defaultJobRuntime != 'containerd' || (has(self.container) && has(self.container.engine) && self.container.engine == 'containerd')",message="defaultJobRuntime containerd needs container.engine containerd"
type RuntimeSpec struct {
	// +optional
	Container ContainerRuntime `json:"container,omitempty"`
	// DefaultJobRuntime is what a job that names no compute.runtime runs
	// under on these machines.
	// +kubebuilder:validation:Enum=systemd;containerd
	// +optional
	DefaultJobRuntime string `json:"defaultJobRuntime,omitempty"`
	// Toolchains are host toolchains installer modules add (python, node,
	// go, java). A name without a module fails the machine's install with
	// the installer's message.
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=name
	// +optional
	Toolchains []Toolchain `json:"toolchains,omitempty"`
	// Packages are system packages, installed with the distribution's
	// package manager (apt or dnf) under the names it knows.
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z0-9][A-Za-z0-9.+_:=~-]*$`
	// +optional
	Packages []string `json:"packages,omitempty"`
}

// ContainerRuntime is the machine's container engine.
type ContainerRuntime struct {
	// Engine installs containerd and runc. none runs jobs on the host only.
	// +kubebuilder:validation:Enum=containerd;none
	// +optional
	Engine string `json:"engine,omitempty"`
	// GPU installs the NVIDIA driver and, with containerd, its container
	// toolkit: nvidia always, auto when the machine has an NVIDIA device.
	// +kubebuilder:validation:Enum=auto;nvidia;none
	// +optional
	GPU string `json:"gpu,omitempty"`
}

// Toolchain is one host toolchain.
type Toolchain struct {
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9-]*$`
	// +kubebuilder:validation:MaxLength=32
	Name string `json:"name"`
	// Version is a prefix of the pinned version: "24" takes the locked 24.x.
	// Empty takes the locked version.
	// +kubebuilder:validation:Pattern=`^[0-9A-Za-z.+_-]*$`
	// +kubebuilder:validation:MaxLength=32
	// +optional
	Version string `json:"version,omitempty"`
	// Packages are the toolchain's own packages (uv pip, npm -g, go install).
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:Pattern=`^[^\s'"]+$`
	// +optional
	Packages []string `json:"packages,omitempty"`
}

// VMPoolStatus is an observed summary. CapacityRequest objects remain the source
// of truth for individual machine attempts.
type VMPoolStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	Desired            int32 `json:"desired,omitempty"`
	Provisioning       int32 `json:"provisioning,omitempty"`
	Booting            int32 `json:"booting,omitempty"`
	AwaitingEnrollment int32 `json:"awaitingEnrollment,omitempty"`
	Ready              int32 `json:"ready,omitempty"`
	Reserved           int32 `json:"reserved,omitempty"`
	Allocated          int32 `json:"allocated,omitempty"`
	Cleaning           int32 `json:"cleaning,omitempty"`
	Draining           int32 `json:"draining,omitempty"`
	Unhealthy          int32 `json:"unhealthy,omitempty"`
	Terminating        int32 `json:"terminating,omitempty"`
	Failed             int32 `json:"failed,omitempty"`
	InFlight           int32 `json:"inFlight,omitempty"`
	// Replicas is how many slots occupy the pool (the scale subresource's
	// current size).
	Replicas int32 `json:"replicas,omitempty"`
	// Selector selects the pool's CapacityRequests (the scale subresource's
	// label selector).
	Selector string `json:"selector,omitempty"`
	// ProvisioningTimes are the pool's most recent request-to-Ready times,
	// oldest first. A standby replacement is timed from them.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=20
	ProvisioningTimes []ProvisioningSample `json:"provisioningTimes,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.scaling.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:resource:scope=Cluster,shortName=vmp
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.providerRef.name`
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.spec.placementRef.region`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.purchasingMode`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VMPool declares managed VM capacity policy. It does not itself call a provider.
// The name global is the installation-wide suspension's (N9).
// +kubebuilder:validation:XValidation:rule="self.metadata.name != 'global'",message="global is reserved: it names the suspension of every pool"
type VMPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VMPoolSpec   `json:"spec,omitempty"`
	Status VMPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type VMPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VMPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VMPool{}, &VMPoolList{})
}

// LabelUID is the pool's LabelVMPoolUID value on its slots: its UID, or its
// name for a pool built in memory that the API server never stored.
func (p *VMPool) LabelUID() string {
	if p.UID != "" {
		return string(p.UID)
	}
	return p.Name
}
