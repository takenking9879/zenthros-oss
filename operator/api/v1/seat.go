// SPDX-License-Identifier: Apache-2.0

package v1

import "fmt"

// SeatWhereMode is how a job names where it may sit.
type SeatWhereMode string

const (
	SeatWherePool      SeatWhereMode = "pool"
	SeatWhereBackends  SeatWhereMode = "backends"
	SeatWhereBoth      SeatWhereMode = "both"
	SeatWhereScheduler SeatWhereMode = "scheduler"
)

// SeatWhere classifies poolRef vs target.backends.
func SeatWhere(spec ZenthrosJobSpec) SeatWhereMode {
	hasPool := spec.NamesPool()
	hasBackends := spec.Target.HasBackends()
	switch {
	case hasPool && hasBackends:
		return SeatWhereBoth
	case hasPool:
		return SeatWherePool
	case hasBackends:
		return SeatWhereBackends
	default:
		return SeatWhereScheduler
	}
}

// DefaultSeatSteps is the omitted-list default for a where mode.
func DefaultSeatSteps(mode SeatWhereMode) []SeatStep {
	switch mode {
	case SeatWhereBackends:
		return []SeatStep{SeatStepBackends}
	case SeatWhereBoth:
		return []SeatStep{SeatStepBackends, SeatStepReuse, SeatStepCreate}
	default:
		return []SeatStep{SeatStepReuse, SeatStepCreate}
	}
}

// ResolvedAcquire is written acquire, or the mode default. Empty written lists
// are invalid at admission; this helper treats them as omitted.
func (s ZenthrosJobSpec) ResolvedAcquire() []SeatStep {
	if s.Capacity != nil && len(s.Capacity.Acquire) > 0 {
		return s.Capacity.Acquire
	}
	return DefaultSeatSteps(SeatWhere(s))
}

// ResolvedReplace is written replace, or the mode default.
func (s ZenthrosJobSpec) ResolvedReplace() []SeatStep {
	if s.Resilience != nil && len(s.Resilience.Replace) > 0 {
		return s.Resilience.Replace
	}
	return DefaultSeatSteps(SeatWhere(s))
}

// LeaseFenced reports a job whose worker was lost: the lease generation
// survives but no backend holds it, so the next seat walk is replace.
func (j *ZenthrosJob) LeaseFenced() bool {
	return j != nil && j.Status.Lease != nil && j.Status.Lease.Generation > 0 && j.Status.Lease.BackendID == ""
}

// ReplacingWorker reports a job whose next seat replaces a worker it lost:
// fenced, with a loss counted. A first seat fenced by its machine finding
// no capacity (a give-back NoCapacity) is still its first.
func (j *ZenthrosJob) ReplacingWorker() bool {
	return j.LeaseFenced() && j.Status.WorkerLostRetries > 0
}

// ValidSeatStep reports whether s is one of the three literals.
func ValidSeatStep(s SeatStep) bool {
	return s == SeatStepBackends || s == SeatStepReuse || s == SeatStepCreate
}

func legalSeatListForWhere(steps []SeatStep, mode SeatWhereMode) bool {
	if len(steps) == 0 || len(steps) > 3 {
		return false
	}
	seen := map[SeatStep]bool{}
	hasBackends, hasPoolStep := false, false
	for _, step := range steps {
		if !ValidSeatStep(step) || seen[step] {
			return false
		}
		seen[step] = true
		if step == SeatStepBackends {
			hasBackends = true
		}
		if step == SeatStepReuse || step == SeatStepCreate {
			hasPoolStep = true
		}
	}
	switch mode {
	case SeatWherePool, SeatWhereScheduler:
		return !hasBackends && hasPoolStep
	case SeatWhereBackends:
		return hasBackends && !hasPoolStep
	case SeatWhereBoth:
		return true
	default:
		return false
	}
}

func backendsListValid(backends []string) error {
	if len(backends) == 0 {
		return fmt.Errorf("spec.target.backends must have 1–16 items when set")
	}
	if len(backends) > 16 {
		return fmt.Errorf("spec.target.backends has %d items, max 16", len(backends))
	}
	seen := map[string]bool{}
	anyCount := 0
	for i, name := range backends {
		if name == "" {
			return fmt.Errorf("spec.target.backends[%d] is empty", i)
		}
		if len(name) > 253 {
			return fmt.Errorf("spec.target.backends[%d] exceeds 253 characters", i)
		}
		if seen[name] {
			return fmt.Errorf("spec.target.backends must be unique")
		}
		seen[name] = true
		if name == BackendFallbackAny {
			anyCount++
			if i != len(backends)-1 {
				return fmt.Errorf("any may appear only last in spec.target.backends")
			}
		}
	}
	if anyCount > 1 {
		return fmt.Errorf("any may appear only last in spec.target.backends")
	}
	return nil
}

func validateSeatList(field string, steps []SeatStep, written bool, spec ZenthrosJobSpec) error {
	if !written {
		return nil
	}
	if len(steps) == 0 {
		return fmt.Errorf("%s must be omitted rather than empty", field)
	}
	if len(steps) > 3 {
		return fmt.Errorf("%s has %d items, max 3", field, len(steps))
	}
	seen := map[SeatStep]bool{}
	for _, step := range steps {
		if !ValidSeatStep(step) {
			return fmt.Errorf("%s contains invalid step %q", field, step)
		}
		if seen[step] {
			return fmt.Errorf("%s must not contain duplicates", field)
		}
		seen[step] = true
	}
	mode := SeatWhere(spec)
	if !legalSeatListForWhere(steps, mode) {
		return fmt.Errorf("%s %v is not valid in %s mode", field, steps, mode)
	}
	return nil
}

// ValidateSeatPolicy mirrors CRD CEL for unit tests: 15 lists, 24 (lista, dónde)
// cells, and the closed invalid set. Empty written lists are invalid.
func ValidateSeatPolicy(spec ZenthrosJobSpec) error {
	if spec.Target.Backends != nil {
		if err := backendsListValid(spec.Target.Backends); err != nil {
			return err
		}
	}
	if spec.Capacity != nil && spec.Capacity.Acquire != nil {
		if err := validateSeatList("spec.capacity.acquire", spec.Capacity.Acquire, true, spec); err != nil {
			return err
		}
	}
	if spec.Resilience != nil && spec.Resilience.Replace != nil {
		if err := validateSeatList("spec.resilience.replace", spec.Resilience.Replace, true, spec); err != nil {
			return err
		}
	}
	return nil
}
