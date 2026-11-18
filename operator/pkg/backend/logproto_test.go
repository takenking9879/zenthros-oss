// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"reflect"
	"testing"
	"time"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// A chunk crosses the agent's protocol whole: the agent sends it, a node's
// log reader sends it to the agent.
func TestALogChunkCrossesTheProtocolWhole(t *testing.T) {
	for _, chunk := range []LogChunk{
		{Stream: "stdout", Data: []byte("hello\n"), Timestamp: time.Unix(1000, 0), Cursor: "6", Gap: 3, GapReason: GapBudget, Omitted: 9},
		{Stream: "stdout", Data: []byte("x\n"), Timestamp: time.Unix(1, 0), Gap: Uncounted, GapReason: GapFreed, Omitted: Uncounted},
	} {
		proto := ProtoLogChunk(chunk)
		if chunk.GapReason == GapBudget && proto.GetGapReason() != agentpb.GapReason_GAP_BUDGET {
			t.Fatalf("proto %v", proto)
		}
		if back := LogChunkOf(proto); !reflect.DeepEqual(back, chunk) {
			t.Fatalf("%+v came back %+v", chunk, back)
		}
	}
}
