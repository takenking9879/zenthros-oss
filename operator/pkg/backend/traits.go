// SPDX-License-Identifier: Apache-2.0

package backend

// Traits are what a backend type implies for the control plane. Code that
// needs one of these asks for the trait, never for the type: a new type
// states each trait on purpose instead of inheriting a neighbour's
// behaviour by a type comparison. What an agent observed (a container's
// image, its runtimes) is a declaration, not a trait.
type Traits struct {
	// Cluster: the backend is a Kubernetes cluster. It takes a distributed
	// job whole (one Indexed Job), and placement judges it only on
	// cluster-level rules.
	Cluster bool
	// ExclusiveByDefault: while its agent has not said maxJobs, the backend
	// is taken to run one job at a time (the safe side).
	ExclusiveByDefault bool
	// ManagedNeedsPoolLease: a Managed backend of this type is reached only
	// through a pool reservation lease, never targeted directly.
	ManagedNeedsPoolLease bool
}

// TraitsOf returns the traits of backendType. A type that is not known gets
// the cautious answer: a Managed one is not targeted directly.
func TraitsOf(backendType string) Traits {
	switch backendType {
	case TypeK8s:
		return Traits{Cluster: true}
	case TypeVM:
		return Traits{ExclusiveByDefault: true, ManagedNeedsPoolLease: true}
	case TypeContainer:
		// A container has no runtime to choose: it runs its own image.
		return Traits{ExclusiveByDefault: true, ManagedNeedsPoolLease: true}
	default:
		return Traits{ManagedNeedsPoolLease: true}
	}
}
