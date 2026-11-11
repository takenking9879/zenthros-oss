// SPDX-License-Identifier: Apache-2.0

// Package observe says how much an observation covers. A list built from
// replicas that did not all answer, or a NotFound from a cache, used to be
// read as the whole truth: a machine missing from a partial list was retired,
// and a job whose agent did not answer was failed.
//
// The rule: a positive fact (present, connected) may come from a partial
// view. A negative one that leads to destroying something or failing a job
// needs Absent from a complete View (PresenceOf), or a live read.
package observe

import (
	"errors"
	"time"
)

// View is a list and how much of the truth it covers.
type View[T any] struct {
	Items []T
	// Complete: every source that could hold an item answered.
	Complete bool
	// AsOf is when the observation was made.
	AsOf time.Time
}

// Find returns the first item has accepts.
func (v View[T]) Find(has func(T) bool) (T, bool) {
	for _, item := range v.Items {
		if has(item) {
			return item, true
		}
	}
	var zero T
	return zero, false
}

// Carried is what is known after reading seen over last, by key: seen as it
// is when the read was complete; otherwise seen with every item of last it
// does not hold, since an item behind a source that did not answer may
// still be there. The one rule for every view kept across reads.
func Carried[K comparable, V any](last, seen map[K]V, complete bool) map[K]V {
	if complete {
		if seen == nil {
			return map[K]V{}
		}
		return seen
	}
	out := make(map[K]V, len(last)+len(seen))
	for key, item := range last {
		out[key] = item
	}
	for key, item := range seen {
		out[key] = item
	}
	return out
}

// Presence is whether something is there, as far as an observation can say.
type Presence uint8

const (
	// Unknown: the observation cannot say (a source did not answer). The
	// zero value, so a Presence nobody set never reads as Absent.
	Unknown Presence = iota
	// Connected: it is there.
	Connected
	// Absent: it is not there, and the observation that says so was
	// complete.
	Absent
)

func (p Presence) String() string {
	switch p {
	case Connected:
		return "connected"
	case Absent:
		return "absent"
	default:
		return "unknown"
	}
}

// PresenceOf is what v says about the item has accepts: Connected if it is
// in the view, Absent only if the view is complete, Unknown otherwise.
func PresenceOf[T any](v View[T], has func(T) bool) Presence {
	if _, ok := v.Find(has); ok {
		return Connected
	}
	if v.Complete {
		return Absent
	}
	return Unknown
}

// ErrUnknown marks an answer nobody could give: a source did not answer
// about something it may hold. It is never a refusal: what waits on it is
// asked again, and nothing is failed or destroyed on it.
var ErrUnknown = errors.New("nobody could tell: a source did not answer")
