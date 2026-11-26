// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"time"

	"google.golang.org/grpc/codes"
)

// GatewayFull is the text of a Gateway's refusal when it holds its maximum
// of agent sessions (agentreg.ErrFull).
const GatewayFull = "gateway holds its maximum of agent sessions"

// FullError is the status that ends the stream of a machine a full Gateway
// refused: ResourceExhausted, with the GATEWAY_FULL reason.
func FullError(message string) error {
	return refusal(codes.ResourceExhausted, message, reasonGatewayFull)
}

// RefusedFull reports whether err is a Gateway's refusal for lack of room,
// by its reason. The agent retries soon then: its next dial may reach a
// replica with room.
func RefusedFull(err error) bool {
	return refusedFor(err, reasonGatewayFull)
}

// DefaultRefusedPatience is how long refusals alone keep a machine alive
// for the controller, from the last time it was heard through a session
// (Tracker.Refused). It covers a Gateway replica coming back with room, many
// times over; past it a job nobody can reach is judged like any silent one.
const DefaultRefusedPatience = 5 * time.Minute

// GatewayList is one Gateway replica's answer to the controller's sweep.
type GatewayList struct {
	// Held are the machines it holds a session of, with each one's last
	// heartbeat.
	Held map[string]time.Time
	// RefusedFull are the machines it refused a session for lack of room in
	// the last minutes, with each one's last refusal.
	RefusedFull map[string]time.Time
}
