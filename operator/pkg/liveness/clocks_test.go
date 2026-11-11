// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/zenthros/operator/pkg/infrastructure/provider/traits"
)

func TestJobAndIdleGraceAreDistinct(t *testing.T) {
	if DefaultJobGrace == DefaultIdleGrace {
		t.Fatal("jobGrace and idleGrace must be different constants")
	}
	if DefaultHeartbeatInterval != 15*time.Second {
		t.Fatalf("heartbeatInterval = %s, want 15s", DefaultHeartbeatInterval)
	}
	if DefaultJobGrace != 45*time.Second {
		t.Fatalf("jobGrace = %s, want 45s", DefaultJobGrace)
	}
	if DefaultIdleGrace != 30*time.Second {
		t.Fatalf("idleGrace = %s, want 30s", DefaultIdleGrace)
	}
}

func TestMissCountIsCeilGraceOverInterval(t *testing.T) {
	if got := MissCount(DefaultJobGrace, DefaultHeartbeatInterval); got != 3 {
		t.Fatalf("job misses = %d, want 3", got)
	}
	if got := MissCount(DefaultIdleGrace, DefaultHeartbeatInterval); got != 2 {
		t.Fatalf("idle misses = %d, want 2", got)
	}
	if got := MissCount(50*time.Second, 15*time.Second); got != 4 {
		t.Fatalf("ceil(50s/15s) = %d, want 4", got)
	}
	if got := MissCount(0, DefaultHeartbeatInterval); got != 0 {
		t.Fatalf("zero grace misses = %d, want 0", got)
	}
}

func TestResolveJobGraceOverrideAndFloor(t *testing.T) {
	short := metav1.Duration{Duration: 50 * time.Millisecond}
	if got := ResolveJobGrace(&short, DefaultJobGrace, 0); got != 50*time.Millisecond {
		t.Fatalf("short override with no interval = %s, want 50ms (tests; no rebuild)", got)
	}
	tooSmall := metav1.Duration{Duration: time.Second}
	if got := ResolveJobGrace(&tooSmall, DefaultJobGrace, DefaultHeartbeatInterval); got != 30*time.Second {
		t.Fatalf("override below 2×interval = %s, want 30s", got)
	}
	ok := metav1.Duration{Duration: 60 * time.Second}
	if got := ResolveJobGrace(&ok, DefaultJobGrace, DefaultHeartbeatInterval); got != 60*time.Second {
		t.Fatalf("valid override = %s, want 60s", got)
	}
	if got := ResolveJobGrace(nil, 80*time.Millisecond, DefaultHeartbeatInterval); got != 80*time.Millisecond {
		t.Fatalf("cluster default short = %s, want 80ms", got)
	}
}

func TestResolveIdleGracePoolOverride(t *testing.T) {
	if got := ResolveIdleGrace(nil, 0); got != DefaultIdleGrace {
		t.Fatalf("empty = %s, want default idle", got)
	}
	pool := metav1.Duration{Duration: 5 * time.Second}
	if got := ResolveIdleGrace(&pool, DefaultIdleGrace); got != 5*time.Second {
		t.Fatalf("pool override = %s, want 5s", got)
	}
	if got := ResolveIdleGrace(nil, 12*time.Second); got != 12*time.Second {
		t.Fatalf("cluster idle = %s, want 12s", got)
	}
}

// The drain window is the provider's deadline, capped per provider, and
// never in the past.
func TestDrainWindowCappedByProviderDeadline(t *testing.T) {
	now := time.Unix(1000, 0)
	cases := []struct {
		name   string
		notice Notice
		want   time.Duration
	}{
		{"aws two-minute warning is capped at 110s", Notice{Source: traits.NoticeAWSIMDS, Deadline: now.Add(2 * time.Minute)}, 110 * time.Second},
		{"aws deadline sooner than the cap wins", Notice{Source: traits.NoticeAWSIMDS, Deadline: now.Add(40 * time.Second)}, 40 * time.Second},
		{"aws without a time uses the cap", Notice{Source: traits.NoticeAWSIMDS}, 110 * time.Second},
		{"gcp Spot's two-minute notice is capped at 110s", Notice{Source: traits.NoticeGCPMetadata}, 110 * time.Second},
		{"unknown source uses the default", Notice{Source: "other"}, 25 * time.Second},
		{"a deadline already past ends the window now", Notice{Source: traits.NoticeAWSIMDS, Deadline: now.Add(-time.Second)}, 0},
	}
	for _, c := range cases {
		if got := DrainUntil(c.notice, now).Sub(now); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

// A backend's link can ask for more patience than the cluster's default (a
// RunPod pod on a userspace tailnet stalled for 45s, the whole default, and
// its finished task ran again elsewhere). It only ever lengthens the
// default, is capped, and an explicit job override still wins.
func TestResolveJobGraceOnABackend(t *testing.T) {
	const interval = 15 * time.Second
	for _, c := range []struct {
		name     string
		override *metav1.Duration
		backend  time.Duration
		want     time.Duration
	}{
		{"none declared", nil, 0, DefaultJobGrace},
		{"five heartbeats", nil, 75 * time.Second, 75 * time.Second},
		{"shorter than the default", nil, 10 * time.Second, DefaultJobGrace},
		{"capped", nil, time.Hour, MaxBackendJobGraceBeats * interval},
		{"job override wins", &metav1.Duration{Duration: 40 * time.Second}, 75 * time.Second, 40 * time.Second},
	} {
		if got := ResolveJobGraceOn(c.override, c.backend, DefaultJobGrace, interval); got != c.want {
			t.Errorf("%s: grace %v, want %v", c.name, got, c.want)
		}
	}
	if got := ResolveJobGraceOn(nil, time.Hour, 0, 0); got != MaxBackendJobGraceBeats*DefaultHeartbeatInterval {
		t.Errorf("unset clocks: cap %v, want %d default heartbeats", got, MaxBackendJobGraceBeats)
	}
}
