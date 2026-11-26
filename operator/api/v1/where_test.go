// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"slices"
	"testing"
)

func TestWhereOf(t *testing.T) {
	named := ZenthrosJobSpec{Target: Target{Backends: []string{"vm-b", "vm-a", BackendFallbackAny}}}
	w := WhereOf(named)
	if !slices.Equal(w.Backends, []string{"vm-b", "vm-a"}) || !w.Any || w.Automatic() {
		t.Fatalf("WhereOf(named) = %+v, want vm-b, vm-a in order and any", w)
	}
	if !slices.Equal(w.Acquire, []SeatStep{SeatStepBackends}) {
		t.Fatalf("named backends acquire %v, want Backends", w.Acquire)
	}

	both := ZenthrosJobSpec{PoolRef: "pool-a", Target: Target{Backends: []string{"vm-a"}},
		Resilience: &ResilienceSpec{Replace: []SeatStep{SeatStepCreate}}}
	w = WhereOf(both)
	if w.Pool != "pool-a" || !slices.Equal(w.Acquire, []SeatStep{SeatStepBackends, SeatStepReuse, SeatStepCreate}) ||
		!slices.Equal(w.Steps(true), []SeatStep{SeatStepCreate}) || !slices.Equal(w.Steps(false), w.Acquire) {
		t.Fatalf("WhereOf(both) = %+v", w)
	}

	if auto := WhereOf(ZenthrosJobSpec{}); !auto.Automatic() || !slices.Equal(auto.Acquire, []SeatStep{SeatStepReuse, SeatStepCreate}) {
		t.Fatalf("WhereOf of a job that names nothing = %+v", auto)
	}
	if fleet := WhereOf(ZenthrosJobSpec{FleetRef: "f"}); fleet.Fleet != "f" || fleet.Automatic() {
		t.Fatalf("WhereOf(fleet) = %+v", fleet)
	}
}
