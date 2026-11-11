// SPDX-License-Identifier: Apache-2.0

package executionreport

import (
	"encoding/json"
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

func SnapshotFromProto(msg *agentpb.ExecutionReport) Snapshot {
	if msg == nil {
		return Snapshot{}
	}
	snapshot := Snapshot{
		APIVersion: msg.GetApiVersion(),
		Kind:       Kind,
		JobID:      msg.GetJobId(),
		ProducerID: msg.GetProducerId(),
		ReportID:   msg.GetReportId(),
		Sequence:   msg.GetSequence(),
		Payload:    json.RawMessage(msg.GetPayloadJson()),
		Final:      msg.GetFinal(),
	}
	if snapshot.APIVersion == "" {
		snapshot.APIVersion = APIVersion
	}
	if msg.GetProducedAtUnix() != 0 {
		snapshot.ProducedAt = time.Unix(msg.GetProducedAtUnix(), 0).UTC()
	}
	for _, artifact := range msg.GetArtifacts() {
		snapshot.Artifacts = append(snapshot.Artifacts, ArtifactRef{URI: artifact.GetUri(), Name: artifact.GetName()})
	}
	return snapshot
}

func SnapshotToProto(snapshot Snapshot) *agentpb.ExecutionReport {
	msg := &agentpb.ExecutionReport{
		ApiVersion:     snapshot.APIVersion,
		JobId:          snapshot.JobID,
		ProducerId:     snapshot.ProducerID,
		ReportId:       snapshot.ReportID,
		Sequence:       snapshot.Sequence,
		ProducedAtUnix: snapshot.ProducedAt.Unix(),
		PayloadJson:    []byte(snapshot.Payload),
		Final:          snapshot.Final,
	}
	for _, artifact := range snapshot.Artifacts {
		msg.Artifacts = append(msg.Artifacts, &agentpb.ArtifactRef{Uri: artifact.URI, Name: artifact.Name})
	}
	return msg
}

func AckFromProto(msg *agentpb.ExecutionReportAck) ControllerAck {
	if msg == nil {
		return ControllerAck{}
	}
	return ControllerAck{
		ReportID:   msg.GetReportId(),
		JobID:      msg.GetJobId(),
		ProducerID: msg.GetProducerId(),
		Sequence:   msg.GetSequence(),
		Accepted:   msg.GetAccepted(),
		ErrorCode:  msg.GetErrorCode(),
		Error:      msg.GetError(),
	}
}

func AckToProto(ack ControllerAck) *agentpb.ExecutionReportAck {
	return &agentpb.ExecutionReportAck{
		ReportId:   ack.ReportID,
		JobId:      ack.JobID,
		ProducerId: ack.ProducerID,
		Sequence:   ack.Sequence,
		Accepted:   ack.Accepted,
		ErrorCode:  ack.ErrorCode,
		Error:      ack.Error,
	}
}

// ReportView is a current report as zctl, the console and zenthros-api
// print it: the payload decoded, times in Unix seconds, artifacts as
// {uri, name}. The SDK reads it (parse_report_body; the API's
// testdata/wire/routes.json).
type ReportView struct {
	WorkspaceID   string         `json:"workspace_id"`
	JobID         string         `json:"job_id"`
	ProducerID    string         `json:"producer_id"`
	ReportID      string         `json:"report_id"`
	Sequence      int64          `json:"sequence"`
	ProducedAt    int64          `json:"produced_at"`
	ReceivedAt    int64          `json:"received_at"`
	Final         bool           `json:"final"`
	ResolvedImage string         `json:"resolved_image,omitempty"`
	Artifacts     []ArtifactView `json:"artifacts,omitempty"`
	// Payload is the report's JSON decoded, or its text when it is not JSON.
	Payload any `json:"payload"`
}

// ArtifactView is one artifact a report names.
type ArtifactView struct {
	URI  string `json:"uri"`
	Name string `json:"name"`
}

// View is resp as a ReportView.
func View(resp *agentpb.GetCurrentReportResponse) ReportView {
	out := ReportView{
		WorkspaceID:   resp.GetWorkspaceId(),
		JobID:         resp.GetJobId(),
		ProducerID:    resp.GetProducerId(),
		ReportID:      resp.GetReportId(),
		Sequence:      resp.GetSequence(),
		ProducedAt:    resp.GetProducedAtUnix(),
		ReceivedAt:    resp.GetReceivedAtUnix(),
		Final:         resp.GetFinal(),
		ResolvedImage: resp.GetResolvedImage(),
	}
	for _, artifact := range resp.GetArtifacts() {
		out.Artifacts = append(out.Artifacts, ArtifactView{URI: artifact.GetUri(), Name: artifact.GetName()})
	}
	if err := json.Unmarshal(resp.GetPayloadJson(), &out.Payload); err != nil {
		out.Payload = string(resp.GetPayloadJson())
	}
	return out
}
