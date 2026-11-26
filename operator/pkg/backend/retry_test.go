// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"testing"
	"time"
)

func TestRetryPolicyDelayGrowsExponentially(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute}
	wants := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	for attempt, want := range wants {
		if got := p.Delay(attempt); got != want {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestRetryPolicyDelayCapsAtMax(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: 5 * time.Second}
	if got := p.Delay(10); got != 5*time.Second {
		t.Errorf("Delay(10) = %v, want cap 5s", got)
	}
}

func TestRetryPolicyZeroBaseDisables(t *testing.T) {
	if got := (RetryPolicy{}).Delay(3); got != 0 {
		t.Errorf("Delay with zero base = %v, want 0", got)
	}
}

func TestDefaultRetryPolicyBackoff(t *testing.T) {
	p := DefaultRetryPolicy()
	if got := p.Delay(0); got != 5*time.Second {
		t.Errorf("Delay(0) = %v, want 5s", got)
	}
	if got := p.Delay(1); got != 10*time.Second {
		t.Errorf("Delay(1) = %v, want 10s", got)
	}
	if got := p.Delay(100); got != 5*time.Minute {
		t.Errorf("Delay(100) = %v, want 5m cap", got)
	}
}

func TestBusyRetryPolicyBackoff(t *testing.T) {
	p := BusyRetryPolicy()
	if got := p.Delay(0); got != 250*time.Millisecond {
		t.Errorf("Delay(0) = %v, want 250ms", got)
	}
	if got := p.Delay(1); got != 500*time.Millisecond {
		t.Errorf("Delay(1) = %v, want 500ms", got)
	}
	if got := p.Delay(100); got != 5*time.Second {
		t.Errorf("Delay(100) = %v, want 5s cap", got)
	}
}
