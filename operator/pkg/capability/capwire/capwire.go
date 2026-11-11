// SPDX-License-Identifier: Apache-2.0

// Package capwire converts capability declarations to and from the agent wire.
package capwire

import (
	"slices"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/capability"
)

// ToProto is the wire form of a Set.
func ToProto(set capability.Set) []*agentpb.WorkloadCapability {
	if set == nil {
		return nil
	}
	out := make([]*agentpb.WorkloadCapability, 0, len(set))
	for _, w := range set {
		out = append(out, &agentpb.WorkloadCapability{
			Kind: w.Kind, Lifecycle: w.Lifecycle, Runtimes: slices.Clone(w.Runtimes), DefaultRuntime: w.DefaultRuntime,
			Endpoints: w.Endpoints,
		})
	}
	return out
}

// FromProto reads a Set off the wire. An empty list is nil: the agent did
// not declare.
func FromProto(in []*agentpb.WorkloadCapability) capability.Set {
	if len(in) == 0 {
		return nil
	}
	out := make(capability.Set, 0, len(in))
	for _, w := range in {
		out = append(out, capability.Workload{
			Kind: w.GetKind(), Lifecycle: w.GetLifecycle(), Runtimes: slices.Clone(w.GetRuntimes()), DefaultRuntime: w.GetDefaultRuntime(),
			Endpoints: w.GetEndpoints(),
		})
	}
	return out.Normalize()
}

// HostToProto is the wire form of Host.
func HostToProto(h capability.Host) *agentpb.HostFacts {
	return &agentpb.HostFacts{
		Arch: h.Arch, OsId: h.OSID, OsVersion: h.OSVersion, Cpu: h.CPU, MemoryBytes: h.MemoryBytes,
		GpuVendor: h.GPUVendor, GpuModel: h.GPUModel, GpuCount: h.GPUCount,
		NvidiaContainerRuntime: h.NvidiaContainerRuntime, PeerAddr: h.PeerAddr, NetworkId: h.NetworkID,
		Zone: h.Zone, Region: h.Region, Provider: h.Provider,
		CachedImages: h.CachedImages, CachedArtifacts: h.CachedArtifacts,
		ContainerImage: h.ContainerImage, JobGraceSeconds: h.JobGraceSeconds,
		SecretStores: h.SecretStores, MilliCpu: h.MilliCPU, Nodes: nodesToProto(h.Nodes),
	}
}

func nodesToProto(nodes []capability.Node) []*agentpb.NodeFacts {
	var out []*agentpb.NodeFacts
	for _, n := range nodes {
		out = append(out, &agentpb.NodeFacts{Name: n.Name, MilliCpu: n.MilliCPU, MemoryBytes: n.MemoryBytes, GpuCount: n.GPUCount,
			FreeMilliCpu: n.FreeMilliCPU, FreeMemoryBytes: n.FreeMemoryBytes})
	}
	return out
}

func nodesFromProto(nodes []*agentpb.NodeFacts) []capability.Node {
	var out []capability.Node
	for _, n := range nodes {
		out = append(out, capability.Node{Name: n.GetName(), MilliCPU: n.GetMilliCpu(), MemoryBytes: n.GetMemoryBytes(),
			GPUCount: n.GetGpuCount(), FreeMilliCPU: n.GetFreeMilliCpu(), FreeMemoryBytes: n.GetFreeMemoryBytes()})
	}
	return out
}

// HostFromProto reads Host off the wire.
func HostFromProto(h *agentpb.HostFacts) capability.Host {
	if h == nil {
		return capability.Host{}
	}
	return capability.Host{
		Arch: h.GetArch(), OSID: h.GetOsId(), OSVersion: h.GetOsVersion(), CPU: h.GetCpu(), MemoryBytes: h.GetMemoryBytes(),
		GPUVendor: h.GetGpuVendor(), GPUModel: h.GetGpuModel(), GPUCount: h.GetGpuCount(),
		NvidiaContainerRuntime: h.GetNvidiaContainerRuntime(), PeerAddr: h.GetPeerAddr(), NetworkID: h.GetNetworkId(),
		Zone: h.GetZone(), Region: h.GetRegion(), Provider: h.GetProvider(),
		CachedImages: h.GetCachedImages(), CachedArtifacts: h.GetCachedArtifacts(),
		ContainerImage: h.GetContainerImage(), JobGraceSeconds: h.GetJobGraceSeconds(),
		SecretStores: h.GetSecretStores(), MilliCPU: h.GetMilliCpu(), Nodes: nodesFromProto(h.GetNodes()),
	}
}
