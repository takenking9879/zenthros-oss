// SPDX-License-Identifier: Apache-2.0

package v1

import "testing"

func TestSeatWhereAndDefaults(t *testing.T) {
	tests := []struct {
		name string
		spec ZenthrosJobSpec
		mode SeatWhereMode
		want []SeatStep
	}{
		{name: "pool", spec: ZenthrosJobSpec{PoolRef: "gpu"}, mode: SeatWherePool, want: []SeatStep{SeatStepReuse, SeatStepCreate}},
		{name: "backends", spec: ZenthrosJobSpec{Target: Target{Backends: []string{"vm-a"}}}, mode: SeatWhereBackends, want: []SeatStep{SeatStepBackends}},
		{name: "both", spec: ZenthrosJobSpec{PoolRef: "gpu", Target: Target{Backends: []string{"vm-a"}}}, mode: SeatWhereBoth, want: []SeatStep{SeatStepBackends, SeatStepReuse, SeatStepCreate}},
		{name: "scheduler", spec: ZenthrosJobSpec{}, mode: SeatWhereScheduler, want: []SeatStep{SeatStepReuse, SeatStepCreate}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SeatWhere(test.spec); got != test.mode {
				t.Fatalf("SeatWhere = %q, want %q", got, test.mode)
			}
			got := test.spec.ResolvedAcquire()
			if !seatStepsEqual(got, test.want) {
				t.Fatalf("ResolvedAcquire = %v, want %v", got, test.want)
			}
			if !seatStepsEqual(test.spec.ResolvedReplace(), test.want) {
				t.Fatalf("ResolvedReplace = %v, want %v", test.spec.ResolvedReplace(), test.want)
			}
		})
	}
}

func TestResolvedListsPreferWritten(t *testing.T) {
	spec := ZenthrosJobSpec{
		PoolRef:  "gpu",
		Capacity: &CapacitySpec{Acquire: []SeatStep{SeatStepCreate}},
		Resilience: &ResilienceSpec{
			OnWorkerLost: OnWorkerLostReschedule,
			Replace:      []SeatStep{SeatStepReuse},
		},
	}
	if !seatStepsEqual(spec.ResolvedAcquire(), []SeatStep{SeatStepCreate}) {
		t.Fatalf("acquire = %v, want [Create]", spec.ResolvedAcquire())
	}
	if !seatStepsEqual(spec.ResolvedReplace(), []SeatStep{SeatStepReuse}) {
		t.Fatalf("replace = %v, want [Reuse]", spec.ResolvedReplace())
	}
}

var allFifteen = [][]SeatStep{
	{SeatStepBackends},
	{SeatStepReuse},
	{SeatStepCreate},
	{SeatStepBackends, SeatStepReuse},
	{SeatStepBackends, SeatStepCreate},
	{SeatStepReuse, SeatStepCreate},
	{SeatStepReuse, SeatStepBackends},
	{SeatStepCreate, SeatStepReuse},
	{SeatStepCreate, SeatStepBackends},
	{SeatStepBackends, SeatStepReuse, SeatStepCreate},
	{SeatStepBackends, SeatStepCreate, SeatStepReuse},
	{SeatStepReuse, SeatStepBackends, SeatStepCreate},
	{SeatStepReuse, SeatStepCreate, SeatStepBackends},
	{SeatStepCreate, SeatStepBackends, SeatStepReuse},
	{SeatStepCreate, SeatStepReuse, SeatStepBackends},
}

func TestFifteenListsAdmittedInBoth(t *testing.T) {
	if len(allFifteen) != 15 {
		t.Fatalf("allFifteen = %d, want 15", len(allFifteen))
	}
	for _, list := range allFifteen {
		spec := ZenthrosJobSpec{
			PoolRef:  "gpu",
			Target:   Target{Backends: []string{"vm-a"}},
			Capacity: &CapacitySpec{Acquire: list},
		}
		if err := ValidateSeatPolicy(spec); err != nil {
			t.Fatalf("both acquire %v: %v", list, err)
		}
		spec.Capacity = nil
		spec.Resilience = &ResilienceSpec{Replace: list}
		if err := ValidateSeatPolicy(spec); err != nil {
			t.Fatalf("both replace %v: %v", list, err)
		}
	}
}

func TestPlanSection3WhereCells(t *testing.T) {
	type cell struct {
		list                            []SeatStep
		pool, backends, both, scheduler bool
	}
	cells := []cell{
		{list: []SeatStep{SeatStepBackends}, backends: true, both: true},
		{list: []SeatStep{SeatStepReuse}, pool: true, both: true, scheduler: true},
		{list: []SeatStep{SeatStepCreate}, pool: true, both: true, scheduler: true},
		{list: []SeatStep{SeatStepReuse, SeatStepCreate}, pool: true, both: true, scheduler: true},
		{list: []SeatStep{SeatStepCreate, SeatStepReuse}, pool: true, both: true, scheduler: true},
		{list: []SeatStep{SeatStepBackends, SeatStepReuse}, both: true},
		{list: []SeatStep{SeatStepBackends, SeatStepCreate}, both: true},
		{list: []SeatStep{SeatStepReuse, SeatStepBackends}, both: true},
		{list: []SeatStep{SeatStepCreate, SeatStepBackends}, both: true},
		{list: []SeatStep{SeatStepBackends, SeatStepReuse, SeatStepCreate}, both: true},
		{list: []SeatStep{SeatStepBackends, SeatStepCreate, SeatStepReuse}, both: true},
		{list: []SeatStep{SeatStepReuse, SeatStepBackends, SeatStepCreate}, both: true},
		{list: []SeatStep{SeatStepReuse, SeatStepCreate, SeatStepBackends}, both: true},
		{list: []SeatStep{SeatStepCreate, SeatStepBackends, SeatStepReuse}, both: true},
		{list: []SeatStep{SeatStepCreate, SeatStepReuse, SeatStepBackends}, both: true},
	}
	modes := []struct {
		name SeatWhereMode
		spec ZenthrosJobSpec
		ok   func(cell) bool
	}{
		{name: SeatWherePool, spec: ZenthrosJobSpec{PoolRef: "gpu"}, ok: func(c cell) bool { return c.pool }},
		{name: SeatWhereBackends, spec: ZenthrosJobSpec{Target: Target{Backends: []string{"vm-a"}}}, ok: func(c cell) bool { return c.backends }},
		{name: SeatWhereBoth, spec: ZenthrosJobSpec{PoolRef: "gpu", Target: Target{Backends: []string{"vm-a"}}}, ok: func(c cell) bool { return c.both }},
		{name: SeatWhereScheduler, spec: ZenthrosJobSpec{}, ok: func(c cell) bool { return c.scheduler }},
	}
	legal := 0
	for _, c := range cells {
		for _, mode := range modes {
			spec := mode.spec
			spec.Capacity = &CapacitySpec{Acquire: append([]SeatStep(nil), c.list...)}
			err := ValidateSeatPolicy(spec)
			want := mode.ok(c)
			if want {
				legal++
				if err != nil {
					t.Fatalf("%s %v should be legal: %v", mode.name, c.list, err)
				}
				continue
			}
			if err == nil {
				t.Fatalf("%s %v should be rejected", mode.name, c.list)
			}
		}
	}
	if legal != 24 {
		t.Fatalf("legal (lista, dónde) cells = %d, want 24", legal)
	}
}

func TestPlanSection6Invalids(t *testing.T) {
	tests := []struct {
		name string
		spec ZenthrosJobSpec
	}{
		{name: "empty acquire", spec: ZenthrosJobSpec{PoolRef: "gpu", Capacity: &CapacitySpec{Acquire: []SeatStep{}}}},
		{name: "empty replace", spec: ZenthrosJobSpec{PoolRef: "gpu", Resilience: &ResilienceSpec{Replace: []SeatStep{}}}},
		{name: "duplicate create", spec: ZenthrosJobSpec{PoolRef: "gpu", Capacity: &CapacitySpec{Acquire: []SeatStep{SeatStepCreate, SeatStepCreate}}}},
		{name: "four items", spec: ZenthrosJobSpec{PoolRef: "gpu", Target: Target{Backends: []string{"vm-a"}}, Capacity: &CapacitySpec{Acquire: []SeatStep{SeatStepBackends, SeatStepReuse, SeatStepCreate, SeatStepBackends}}}},
		{name: "unknown step", spec: ZenthrosJobSpec{PoolRef: "gpu", Capacity: &CapacitySpec{Acquire: []SeatStep{"ReuseOrCreate"}}}},
		{name: "prefer-idle", spec: ZenthrosJobSpec{PoolRef: "gpu", Capacity: &CapacitySpec{Acquire: []SeatStep{"prefer-idle"}}}},
		{name: "backends without list", spec: ZenthrosJobSpec{PoolRef: "gpu", Capacity: &CapacitySpec{Acquire: []SeatStep{SeatStepBackends}}}},
		{name: "reuse without pool in backends-only", spec: ZenthrosJobSpec{Target: Target{Backends: []string{"vm-a"}}, Capacity: &CapacitySpec{Acquire: []SeatStep{SeatStepReuse}}}},
		{name: "create without pool in backends-only", spec: ZenthrosJobSpec{Target: Target{Backends: []string{"vm-a"}}, Capacity: &CapacitySpec{Acquire: []SeatStep{SeatStepCreate}}}},
		{name: "any not last", spec: ZenthrosJobSpec{Target: Target{Backends: []string{BackendFallbackAny, "vm-a"}}}},
		{name: "two any", spec: ZenthrosJobSpec{Target: Target{Backends: []string{"vm-a", BackendFallbackAny, BackendFallbackAny}}}},
		{name: "empty backend item", spec: ZenthrosJobSpec{Target: Target{Backends: []string{""}}}},
		{name: "duplicate backends", spec: ZenthrosJobSpec{Target: Target{Backends: []string{"vm-a", "vm-a"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateSeatPolicy(test.spec); err == nil {
				t.Fatal("expected reject")
			}
		})
	}
}

func TestFailReplaceIsAdmitted(t *testing.T) {
	spec := ZenthrosJobSpec{
		PoolRef:    "gpu",
		Capacity:   &CapacitySpec{Acquire: []SeatStep{SeatStepCreate}},
		Resilience: &ResilienceSpec{OnWorkerLost: OnWorkerLostFail, Replace: []SeatStep{SeatStepCreate}},
	}
	if err := ValidateSeatPolicy(spec); err != nil {
		t.Fatalf("Fail + replace Create must be admitted: %v", err)
	}
}

func TestAnyAloneIsLegal(t *testing.T) {
	spec := ZenthrosJobSpec{Target: Target{Backends: []string{BackendFallbackAny}}}
	if err := ValidateSeatPolicy(spec); err != nil {
		t.Fatalf("[any] must be legal: %v", err)
	}
}

func seatStepsEqual(got, want []SeatStep) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
