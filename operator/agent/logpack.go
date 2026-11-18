// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"sync"

	"github.com/klauspost/compress/zstd"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// Each chunk a read sends leaves packed: its bytes in one zstd frame of its
// own, level 1 (benchmarks/loglab RESULTS.md, V1: 0.19x the bytes for +0.8
// mCPU at 145 KB/s; one zstd stream per reader cost +1.5 MiB a reader). A
// frame of its own lets the Gateway forward each chunk as it came and a
// reader unpack each alone, from any chunk on. The history leaves in the
// reads of the job's file (128 KiB, agent/logfile), the live log in the
// batches of the window (1 s or 16 KiB, logbatch.go). A chunk that packs no
// smaller leaves as it is: no chunk grows past the Gateway's size.

// logEncoder packs every read's chunks: one encoder, made on the first
// chunk sent. A chunk of 128 KiB packs in ~0.3 ms, so the reads of a
// machine (16 at most) wait little for it, and an idle agent holds none.
var logEncoder = sync.OnceValue(func() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithLowerEncoderMem(true))
	if err != nil {
		panic(err)
	}
	return enc
})

// packLog packs chunk's bytes, if that makes them smaller.
func packLog(chunk *agentpb.LogChunk) {
	if len(chunk.Data) == 0 || chunk.Encoding != agentpb.LogEncoding_LOG_RAW {
		return
	}
	frame := logEncoder().EncodeAll(chunk.Data, make([]byte, 0, len(chunk.Data)/2))
	if len(frame) >= len(chunk.Data) {
		return
	}
	chunk.RawLen = int64(len(chunk.Data))
	chunk.Data = frame
	chunk.Encoding = agentpb.LogEncoding_LOG_ZSTD
}
