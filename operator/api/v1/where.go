// SPDX-License-Identifier: Apache-2.0

package v1

import "slices"

// Where is everything a job's spec says about where it may sit: the
// constraints the Scheduler decides within. Decide filters its candidates
// with it before it scores any, and the score only breaks ties inside Any
// and for a job that names nothing.
type Where struct {
	// Backends are the backends the job names, in its order: the first
	// with room for it takes it; with none, the first usable one, where
	// it queues.
	Backends []string
	// Any adds every enrolled backend after the named ones.
	Any bool
	// Pool is the VMPool the job names, Fleet its VMFleet.
	Pool, Fleet string
	// Acquire is the steps of a first seat, Replace of a seat after a lost
	// worker.
	Acquire, Replace []SeatStep
	// Placement is spec.placement.
	Placement *PlacementPolicy
}

// WhereOf is spec's Where.
func WhereOf(spec ZenthrosJobSpec) Where {
	return Where{
		Backends:  spec.Target.NamedBackends(),
		Any:       slices.Contains(spec.Target.Backends, BackendFallbackAny),
		Pool:      spec.PoolRef,
		Fleet:     spec.FleetRef,
		Acquire:   spec.ResolvedAcquire(),
		Replace:   spec.ResolvedReplace(),
		Placement: spec.Placement,
	}
}

// Automatic reports a Where that names nothing: the Scheduler chooses
// among every candidate the job's Workspace may use.
func (w Where) Automatic() bool {
	return len(w.Backends) == 0 && !w.Any && w.Pool == "" && w.Fleet == ""
}

// Steps are the steps of the job's next seat: Replace after a lost worker
// (fenced), Acquire otherwise.
func (w Where) Steps(fenced bool) []SeatStep {
	if fenced {
		return w.Replace
	}
	return w.Acquire
}
