// SPDX-License-Identifier: Apache-2.0

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// TopologyKey names a placement domain. Every seat has one per key:
//
//	machine   the backend (one VM, one cluster)
//	zone      the failure domain (AWS availability zone, GCP zone)
//	region    the provider region
//	provider  the cloud provider, or the operator's name for a site
//
// Managed machines take zone, region and provider from their CapacityRequest;
// machines brought by their owner from BackendAccess.spec.compute (zone,
// region) and its topology.zenthros.ai/provider label, which the operator
// sets, and otherwise from what the agent observed (instance metadata).
// +kubebuilder:validation:Enum=machine;zone;region;provider
type TopologyKey string

const (
	TopologyMachine  TopologyKey = "machine"
	TopologyZone     TopologyKey = "zone"
	TopologyRegion   TopologyKey = "region"
	TopologyProvider TopologyKey = "provider"
)

// LabelTopologyProvider on a BackendAccess names the provider of a machine
// its owner brought.
const LabelTopologyProvider = "topology.zenthros.ai/provider"

// PlacementPolicy constrains where a job's seats go. Every rule is hard: a
// job no seat satisfies stays Pending with PLACEMENT_UNSATISFIED and the
// rule that failed. Among seats that satisfy it, the usual order applies.
// On Kubernetes backends the rules become the pod's topologySpreadConstraints
// and pod (anti-)affinity; a Manifest object carries its own.
type PlacementPolicy struct {
	// Pack puts every member of the job in one domain (one zone, one
	// machine). It concerns the members of a distributed job.
	// +optional
	Pack *PackRule `json:"pack,omitempty"`

	// Spread keeps the jobs JobSelector matches (this one included) balanced
	// across domains: after this job is seated, no domain has more than
	// maxSkew jobs over the least loaded one.
	// +optional
	// +kubebuilder:validation:MaxItems=4
	Spread []SpreadRule `json:"spread,omitempty"`

	// SeparateMachines puts each member of a distributed job on its own
	// machine. Default true for distributed jobs.
	// +optional
	SeparateMachines *bool `json:"separateMachines,omitempty"`

	// Affinity seats the job in the same domain as another job's seat (data
	// locality, same machine as a producer).
	// +optional
	// +kubebuilder:validation:MaxItems=8
	Affinity []JobAffinityTerm `json:"affinity,omitempty"`

	// AntiAffinity keeps the job out of every domain a matching job is in.
	// +optional
	// +kubebuilder:validation:MaxItems=8
	AntiAffinity []JobAffinityTerm `json:"antiAffinity,omitempty"`

	// Regions allows only seats in these regions.
	// +optional
	// +kubebuilder:validation:MaxItems=16
	Regions []string `json:"regions,omitempty"`
}

// PackRule keeps every member in one domain of TopologyKey.
type PackRule struct {
	TopologyKey TopologyKey `json:"topologyKey"`
}

// SpreadRule balances matching jobs across domains of TopologyKey.
type SpreadRule struct {
	TopologyKey TopologyKey `json:"topologyKey"`

	// MaxSkew is the most a domain may have over the least loaded one.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	MaxSkew int32 `json:"maxSkew,omitempty"`

	// MinDomains counts domains that have no matching job and no candidate
	// seat as empty until there are this many: with minDomains 2 and one
	// zone to offer, a second job does not go to the same zone.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinDomains int32 `json:"minDomains,omitempty"`

	// JobSelector picks the jobs, in this job's namespace, that are counted.
	// Empty counts the jobs with all of this job's labels.
	// +optional
	JobSelector *metav1.LabelSelector `json:"jobSelector,omitempty"`
}

// JobAffinityTerm names other jobs, in this job's namespace, by name or by
// labels.
// +kubebuilder:validation:XValidation:rule="(has(self.jobName) && size(self.jobName) > 0) != has(self.jobSelector)",message="set exactly one of jobName and jobSelector"
type JobAffinityTerm struct {
	// +optional
	JobName string `json:"jobName,omitempty"`
	// +optional
	JobSelector *metav1.LabelSelector `json:"jobSelector,omitempty"`
	TopologyKey TopologyKey           `json:"topologyKey"`
}
