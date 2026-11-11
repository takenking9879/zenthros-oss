// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAgentMessageTypeLabelSet(t *testing.T) {
	want := map[string]bool{
		MsgExecutionReport: true,
		MsgStatusUpdate:    true,
		MsgHeartbeat:       true,
		MsgCommandResponse: true,
		MsgCheckpoint:      true,
	}
	if len(AgentMessageTypes) != len(want) {
		t.Fatalf("AgentMessageTypes = %v, want 5 approved types", AgentMessageTypes)
	}
	for _, label := range AgentMessageTypes {
		if !want[label] {
			t.Errorf("unexpected type %q", label)
		}
	}
	if KnownAgentMessageType("enrollment") {
		t.Fatal("enrollment must not be a channel type")
	}
}

func TestRecordAgentMessageReceivedIgnoresUnknownAndEmpty(t *testing.T) {
	before := testutil.CollectAndCount(agentMessagesReceived)
	RecordAgentMessageReceived("enrollment", "vm-01")
	RecordAgentMessageReceived(MsgHeartbeat, "")
	RecordAgentMessageReceived("unknown", "vm-01")
	if got := testutil.CollectAndCount(agentMessagesReceived); got != before {
		t.Fatalf("unknown/empty labels changed series count %d -> %d", before, got)
	}

	RecordAgentMessageReceived(MsgHeartbeat, "vm-channel")
	RecordAgentMessageReceived(MsgStatusUpdate, "vm-channel")
	RecordAgentMessageReceived(MsgExecutionReport, "vm-channel")
	RecordAgentMessageReceived(MsgCommandResponse, "vm-channel")
	RecordAgentMessageReceived(MsgCheckpoint, "vm-channel")
	for _, typ := range AgentMessageTypes {
		if got := testutil.ToFloat64(agentMessagesReceived.WithLabelValues(typ, "vm-channel")); got != 1 {
			t.Errorf("%s count = %v, want 1", typ, got)
		}
	}
}

// Ephemeral backends must not leave their series behind forever.
func TestForgetBackendDropsItsSeries(t *testing.T) {
	RecordAgentMessageReceived(MsgHeartbeat, "vm-gone")
	RecordAgentMessageReceived(MsgHeartbeat, "vm-kept")
	RecordSubmission("vm-gone", SubmitAccepted)
	SetAgentCertExpiry("vm-gone", time.Now().Add(time.Hour))
	SetAgentCertExpiry("vm-kept", time.Now().Add(time.Hour))
	certs := testutil.CollectAndCount(agentCertExpiry)
	messages := testutil.CollectAndCount(agentMessagesReceived)
	submissions := testutil.CollectAndCount(submissionsTotal)

	ForgetBackend("vm-gone")

	if n := testutil.CollectAndCount(agentMessagesReceived); n != messages-1 {
		t.Fatalf("agent message series = %d, want %d", n, messages-1)
	}
	if n := testutil.CollectAndCount(submissionsTotal); n != submissions-1 {
		t.Fatalf("submission series = %d, want %d", n, submissions-1)
	}
	if n := testutil.CollectAndCount(agentCertExpiry); n != certs-1 {
		t.Fatalf("certificate expiry series = %d, want %d", n, certs-1)
	}
	if got := testutil.ToFloat64(agentMessagesReceived.WithLabelValues(MsgHeartbeat, "vm-kept")); got < 1 {
		t.Fatalf("vm-kept counter = %v", got)
	}
}
