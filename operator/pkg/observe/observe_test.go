// SPDX-License-Identifier: Apache-2.0

package observe_test

import (
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/observe"
)

var now = time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)

func is(id string) func(string) bool { return func(s string) bool { return s == id } }

func TestPresenceNeedsACompleteViewToSayAbsent(t *testing.T) {
	partial := observe.View[string]{Items: []string{"a"}, Complete: false, AsOf: now}
	full := observe.View[string]{Items: []string{"a"}, Complete: true, AsOf: now}
	for _, tc := range []struct {
		view observe.View[string]
		id   string
		want observe.Presence
	}{
		{partial, "a", observe.Connected},
		{partial, "b", observe.Unknown},
		{full, "a", observe.Connected},
		{full, "b", observe.Absent},
	} {
		if got := observe.PresenceOf(tc.view, is(tc.id)); got != tc.want {
			t.Errorf("complete=%v id=%s: %s, want %s", tc.view.Complete, tc.id, got, tc.want)
		}
	}
	var zero observe.Presence
	if zero != observe.Unknown {
		t.Fatal("the zero Presence must be Unknown")
	}
}
