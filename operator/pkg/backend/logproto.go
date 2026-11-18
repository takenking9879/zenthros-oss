// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// ProtoLogChunk is chunk as the agent's protocol carries it.
func ProtoLogChunk(chunk LogChunk) *agentpb.LogChunk {
	out := &agentpb.LogChunk{
		Stream:        chunk.Stream,
		Data:          chunk.Data,
		TimestampUnix: chunk.Timestamp.Unix(),
		Cursor:        chunk.Cursor,
		Gap:           chunk.Gap,
		Omitted:       chunk.Omitted,
	}
	if chunk.GapReason == GapBudget {
		out.GapReason = agentpb.GapReason_GAP_BUDGET
	}
	return out
}

// LogChunkOf is a chunk the agent's protocol carried.
func LogChunkOf(chunk *agentpb.LogChunk) LogChunk {
	out := LogChunk{
		Stream:    chunk.GetStream(),
		Data:      chunk.GetData(),
		Timestamp: time.Unix(chunk.GetTimestampUnix(), 0),
		Cursor:    chunk.GetCursor(),
		Gap:       chunk.GetGap(),
		GapReason: GapFreed,
		Omitted:   chunk.GetOmitted(),
	}
	if chunk.GetGapReason() == agentpb.GapReason_GAP_BUDGET {
		out.GapReason = GapBudget
	}
	return out
}
