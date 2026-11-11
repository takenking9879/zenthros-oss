// SPDX-License-Identifier: Apache-2.0

// Package traits is what the providers declare about themselves as data, for
// the core and the agent to read without importing a provider: which secret
// stores a provider's machines resolve, which provider a cluster blueprint
// builds in, and whose machines a third party runs. A leaf with no cloud SDK, since the agent reads it
// (capability). Each provider name here is pinned to that provider's own
// ProviderName by a test in its package (traits_test.go).
package traits

import (
	"slices"

	v1 "github.com/zenthros/operator/api/v1"
)

// aws, gcp and vastai are the ProviderName of their packages.
const (
	aws    = "aws"
	gcp    = "gcp"
	vastai = "vastai"
)

// machineStores are the secret stores each provider runs: its machines read
// them with their own identity, so only that provider's VMs resolve them.
// Secret Manager or Key Vault would be one more entry.
var machineStores = map[string][]string{
	aws: {v1.SecretSourceTypeAWSSecretsManager},
}

// SecretStores is what the VMs of provider resolve with their own identity,
// besides sealed payloads; none for a provider that runs no store.
func SecretStores(provider string) []string {
	return slices.Clone(machineStores[provider])
}

// clusterProviders is the provider whose account each cluster blueprint
// builds its cluster in. A blueprint is named by its ID as text, as the agent
// links no blueprint: aws's traits_test pins it to eksblueprint.BlueprintID.
var clusterProviders = map[string]string{
	"aws-eks-aiml/v1": aws,
}

// ClusterProvider is the provider of the clusters blueprint builds.
func ClusterProvider(blueprint string) string {
	return clusterProviders[blueprint]
}

// untrustedHosts are the providers whose machines a third party runs: a
// Vast.ai host is someone's own computer, rented through a marketplace, and
// its owner sees the container's environment, its disk and every job's
// secrets, code and data. RunPod's pools use its Secure Cloud, in
// datacenters it vets, and are not counted here.
var untrustedHosts = map[string]bool{
	vastai: true,
}

// imageRegistries are the registries each provider's VMs log in to with their
// own identity (containerd.DefaultRegistryAuth), as a host or its ending.
var imageRegistries = map[string][]string{
	aws: {".amazonaws.com", ".amazonaws.com.cn"},
	gcp: {"-docker.pkg.dev", "gcr.io"},
}

// ImageRegistries is what the VMs of provider pull from with no credentials of
// the Workspace; none for a provider without a registry.
func ImageRegistries(provider string) []string {
	return imageRegistries[provider]
}

// UntrustedHost reports whether a third party runs provider's machines.
func UntrustedHost(provider string) bool {
	return untrustedHosts[provider]
}

// EnvEndpointTemplate tells a machine's agent where a port the provider
// published is reached: a URL with {port}, {name}, {scheme}, {env:VAR}
// (the agent's environment; no $, which a provider's launcher may expand),
// {publicIPv4} and {peerAddr}, and an optional
// ";scope=Public|Private|Cluster". The agent fills it in once the port
// accepts connections (agent/endpoints.go).
const EnvEndpointTemplate = "ZENTHROS_ENDPOINT_TEMPLATE"
