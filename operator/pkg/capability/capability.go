// SPDX-License-Identifier: Apache-2.0

// Package capability is what a backend says it can run. Agents declare it,
// the registry and the durable directory carry it, and admission and the
// Scheduler read it. Nothing outside an agent's executor decides which kinds a
// backend runs.
package capability

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
)

// Lifecycle is how a workload ends.
const (
	// RunToCompletion workloads finish on their own: Succeeded or Failed.
	RunToCompletion = "RunToCompletion"
	// Service workloads run until cancelled or failed; they never succeed.
	Service = "Service"
)

// Runtimes an executor can run a workload under.
const (
	RuntimeSystemd    = "systemd"
	RuntimeContainerd = "containerd"
	// RuntimeKubernetes is the in-cluster agent: the cluster runs the pods.
	RuntimeKubernetes = "kubernetes"
	// RuntimeProcess is the container agent: a child process in the
	// container's own image.
	RuntimeProcess = "process"
)

// Workload is one kind a backend runs, and how.
type Workload struct {
	Kind           string   `json:"kind"`
	Lifecycle      string   `json:"lifecycle,omitempty"`
	Runtimes       []string `json:"runtimes,omitempty"`
	DefaultRuntime string   `json:"defaultRuntime,omitempty"`
	// Endpoints: the executor runs this kind as a Service and publishes
	// its endpoints. A Job asks for it with template.service, a Manifest
	// with endpointFrom.
	Endpoints bool `json:"endpoints,omitempty"`
}

// Set is every kind a backend runs. A nil Set means the backend did not say,
// which is not the same as "runs nothing".
type Set []Workload

// Find returns the declaration for kind.
func (s Set) Find(kind string) (Workload, bool) {
	for _, w := range s {
		if w.Kind == kind {
			return w, true
		}
	}
	return Workload{}, false
}

// Kinds lists the declared kinds, sorted.
func (s Set) Kinds() []string {
	kinds := make([]string, 0, len(s))
	for _, w := range s {
		kinds = append(kinds, w.Kind)
	}
	sort.Strings(kinds)
	return kinds
}

// String is the declared kinds for messages: "Job, Manifest".
func (s Set) String() string { return strings.Join(s.Kinds(), ", ") }

// Normalize sorts kinds and runtimes so equal sets compare and hash equal.
func (s Set) Normalize() Set {
	if s == nil {
		return nil
	}
	out := make(Set, len(s))
	for i, w := range s {
		w.Runtimes = slices.Clone(w.Runtimes)
		sort.Strings(w.Runtimes)
		out[i] = w
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

// Node is one Kubernetes node: its allocatable CPU (thousandths), memory and
// GPUs, and what is left of them after the requests of its pods that have
// not ended.
type Node struct {
	Name            string `json:"name"`
	MilliCPU        int64  `json:"milliCPU,omitempty"`
	MemoryBytes     int64  `json:"memoryBytes,omitempty"`
	GPUCount        int32  `json:"gpuCount,omitempty"`
	FreeMilliCPU    int64  `json:"freeMilliCPU,omitempty"`
	FreeMemoryBytes int64  `json:"freeMemoryBytes,omitempty"`
}

// Host is what the agent observed about the machine or cluster it runs on.
type Host struct {
	Arch        string `json:"arch,omitempty"`
	OSID        string `json:"osID,omitempty"`
	OSVersion   string `json:"osVersion,omitempty"`
	CPU         int32  `json:"cpu,omitempty"`
	MemoryBytes int64  `json:"memoryBytes,omitempty"`
	// MilliCPU is CPU exactly, in thousandths, where the agent knows it: a
	// container's quota of 1.5 CPUs is CPU 2 and MilliCPU 1500. 0: CPU.
	MilliCPU int32 `json:"milliCPU,omitempty"`
	// Nodes are a Kubernetes cluster's nodes a job can land on (Ready, not
	// cordoned, no NoSchedule or NoExecute taint): what each has and what
	// its pods have not asked for yet. A job runs on one node, never across
	// them. Empty on machines.
	Nodes                  []Node `json:"nodes,omitempty"`
	GPUVendor              string `json:"gpuVendor,omitempty"`
	GPUModel               string `json:"gpuModel,omitempty"`
	GPUCount               int32  `json:"gpuCount,omitempty"`
	NvidiaContainerRuntime bool   `json:"nvidiaContainerRuntime,omitempty"`
	// PeerAddr is the host's address on its private network.
	PeerAddr string `json:"peerAddr,omitempty"`
	// NetworkID names the private network PeerAddr belongs to (a VPC id or
	// a subnet); hosts with the same id reach each other directly.
	NetworkID string `json:"networkID,omitempty"`
	// Zone, Region and Provider are where the host is, as it observed
	// (instance metadata) or was told (ZENTHROS_TOPOLOGY_*).
	Zone     string `json:"zone,omitempty"`
	Region   string `json:"region,omitempty"`
	Provider string `json:"provider,omitempty"`
	// CachedImages and CachedArtifacts are the host's warm cache, most
	// recently used first: image references and artifact sha256 digests.
	// The scheduler prefers a host where a job's image is warm.
	CachedImages    []string `json:"cachedImages,omitempty"`
	CachedArtifacts []string `json:"cachedArtifacts,omitempty"`
	// ContainerImage is the image a container agent runs in. Its jobs run in
	// this image and no other. Empty on machines and clusters.
	ContainerImage string `json:"containerImage,omitempty"`
	// JobGraceSeconds is the silence window the host's link needs while it
	// runs a job, as its provider set it (ZENTHROS_JOB_GRACE). The control
	// plane only lengthens its default with it (liveness.ResolveJobGraceOn).
	JobGraceSeconds int32 `json:"jobGraceSeconds,omitempty"`
	// SecretStores are the secretSources types the host resolves
	// (SecretStoresVM and the others). Nil when the host has not reported
	// yet: judged by its class alone (secretsource.Negotiate).
	SecretStores []string `json:"secretStores,omitempty"`
}

// JobGrace is JobGraceSeconds as a duration; 0 when the host declared none.
func (h Host) JobGrace() time.Duration {
	return time.Duration(h.JobGraceSeconds) * time.Second
}

// IsZero reports whether nothing was observed about the host.
func (h Host) IsZero() bool {
	return reflect.ValueOf(h).IsZero()
}

// Warm reports whether image is in the host's cache, however either side
// spells it (busybox:1.36 is docker.io/library/busybox:1.36).
func (h Host) Warm(image string) bool {
	image = NormalizeImage(image)
	if image == "" {
		return false
	}
	for _, cached := range h.CachedImages {
		if NormalizeImage(cached) == image {
			return true
		}
	}
	return false
}

// Resolves reports whether the host resolves secretSources of the type. A
// host that declared no stores is not judged here.
func (h Host) Resolves(storeType string) bool {
	return h.SecretStores == nil || slices.Contains(h.SecretStores, storeType)
}

// RunsImage reports whether a job naming image can run on this host. Only a
// container is bound to an image: it runs its own and no other, however
// either side spells it. A job that names no image runs in it. A job pinned
// by digest to the repository the container runs by tag runs in it too: a
// later attempt carries the digest its first attempt resolved on a VM (F11),
// and a container cannot pull another image to check it. A container that
// knows its own digest takes only that one, with or without the tag.
func (h Host) RunsImage(image string) bool {
	if h.ContainerImage == "" || strings.TrimSpace(image) == "" {
		return true
	}
	job, host := NormalizeImage(image), NormalizeImage(h.ContainerImage)
	if job == host {
		return true
	}
	jobRepo, jobDigest := splitDigest(job)
	hostRepo, hostDigest := splitDigest(host)
	return jobDigest != "" && (hostDigest == "" || hostDigest == jobDigest) && repository(jobRepo) == repository(hostRepo)
}

// splitDigest splits "repo@sha256:..." into the reference before the digest
// and the digest.
func splitDigest(ref string) (string, string) {
	if at := strings.LastIndexByte(ref, '@'); at != -1 {
		return ref[:at], ref[at+1:]
	}
	return ref, ""
}

// repository drops the tag of a reference without a digest. A colon before
// the last slash is a registry's port.
func repository(ref string) string {
	if colon := strings.LastIndexByte(ref, ':'); colon > strings.LastIndexByte(ref, '/') {
		return ref[:colon]
	}
	return ref
}

// NormalizeImage expands a Docker-style short name to the full reference a
// registry client (ctr images pull) accepts. It does not pin a tag or digest.
func NormalizeImage(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if strings.Contains(ref, "://") {
		return ref
	}
	slash := strings.IndexByte(ref, '/')
	if slash == -1 {
		return "docker.io/library/" + ref
	}
	host := ref[:slash]
	if strings.Contains(host, ".") || strings.Contains(host, ":") || host == "localhost" {
		return ref
	}
	return "docker.io/" + ref
}

// Hash identifies a declaration. Agents resend their capabilities on a
// heartbeat only when it changes.
func Hash(set Set, host Host) string {
	raw, _ := json.Marshal(struct {
		Set  Set  `json:"set"`
		Host Host `json:"host"`
	}{set.Normalize(), host})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}
