// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Agent→Controller message types for zenthros_controller_agent_messages_received_total.
// Enrollment is a separate RPC and is never a type here (A-20).
const (
	MsgExecutionReport = "execution_report"
	MsgStatusUpdate    = "status_update"
	MsgHeartbeat       = "heartbeat"
	MsgCommandResponse = "command_response"
	MsgCheckpoint      = "checkpoint"
)

// AgentMessageTypes is the closed label set for the channel counter.
var AgentMessageTypes = []string{
	MsgExecutionReport,
	MsgStatusUpdate,
	MsgHeartbeat,
	MsgCommandResponse,
	MsgCheckpoint,
}

var agentMessagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "zenthros_controller_agent_messages_received_total",
	Help: "Agent control-stream messages classified as received, before the handler. backend_id is the Agent, not a job.",
}, []string{"type", "backend_id"})

// agentCertExpiry is when the certificate each connected agent presented
// expires. Agents renew with 10 of 30 days left; one that stays under that
// line is not renewing (revoked, or unable to write its key).
var agentCertExpiry = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "zenthros_agent_certificate_expiry_timestamp_seconds",
	Help: "NotAfter of the client certificate each connected agent presented, as a Unix timestamp.",
}, []string{"backend"})

// SetAgentCertExpiry records the certificate an agent registered with.
func SetAgentCertExpiry(backendID string, notAfter time.Time) {
	if backendID == "" || notAfter.IsZero() {
		return
	}
	agentCertExpiry.WithLabelValues(backendID).Set(float64(notAfter.Unix()))
}

func channelCollectors() []prometheus.Collector {
	return []prometheus.Collector{agentMessagesReceived, agentCertExpiry}
}

// KnownAgentMessageType reports whether typeLabel is in the approved A-20 set.
func KnownAgentMessageType(typeLabel string) bool {
	for _, known := range AgentMessageTypes {
		if typeLabel == known {
			return true
		}
	}
	return false
}

// RecordAgentMessageReceived increments the channel counter when a stream
// message has been classified, before the handler runs. Unknown types are
// ignored so cardinality stays linear in Agents × approved types.
func RecordAgentMessageReceived(typeLabel, backendID string) {
	if backendID == "" || !KnownAgentMessageType(typeLabel) {
		return
	}
	agentMessagesReceived.WithLabelValues(typeLabel, backendID).Inc()
}

// ForgetBackend drops every per-backend counter series for backendID. Managed
// VMs are ephemeral and each has a new ID, so without this the per-backend
// series grow for as long as the process lives. Call it when the backend is
// gone for good (terminated) or its stream ended with no replacement; a
// counter that later reappears starts at zero, which rate() reads as a reset.
func ForgetBackend(backendID string) {
	if backendID == "" {
		return
	}
	agentMessagesReceived.DeletePartialMatch(prometheus.Labels{"backend_id": backendID})
	submissionsTotal.DeletePartialMatch(prometheus.Labels{"backend": backendID})
	agentCertExpiry.DeleteLabelValues(backendID)
}
