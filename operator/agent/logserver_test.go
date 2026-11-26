// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "github.com/takenking9879/zenthros-oss/operator/internal/grpc/gen"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
)

type mockLogStream struct {
	grpc.ServerStream
	chunks  []*agentpb.LogChunk
	sendErr error
	ctx     context.Context
}

func (m *mockLogStream) Send(chunk *agentpb.LogChunk) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.chunks = append(m.chunks, chunk)
	return nil
}

func (m *mockLogStream) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

// testLogServer is a LogServer as an agent makes it.
func testLogServer(exec Executor) *LogServer {
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	server.readers = newLogReaders()
	return server
}

func TestLogServerStreamLogsEmptyHandle(t *testing.T) {
	server := testLogServer(newMockExecutor())
	if err := server.streamLogs(&agentpb.StreamLogsRequest{BackendId: "vm-test", JobId: "job-1"}, &mockLogStream{}); err == nil {
		t.Fatal("expected error for empty handle_name")
	}
}

type scriptedLogExecutor struct {
	mockExecutor
	chunks [][]byte
}

func (e *scriptedLogExecutor) Logs(ctx context.Context, _ string, _ backend.LogRequest) (<-chan backend.LogChunk, error) {
	ch := make(chan backend.LogChunk)
	go func() {
		defer close(ch)
		for _, data := range e.chunks {
			select {
			case ch <- backend.LogChunk{Stream: "stdout", Data: data, Timestamp: time.Unix(1, 0)}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func streamReq() *agentpb.StreamLogsRequest {
	return &agentpb.StreamLogsRequest{
		BackendId:  "vm-test",
		JobId:      "job-1",
		HandleName: "handle-1",
	}
}

// summaryReq asks for the Ray/train/tune summary (zctl logs --summary).
func summaryReq() *agentpb.StreamLogsRequest {
	req := streamReq()
	req.Summarize = true
	return req
}

func collectedData(stream *mockLogStream) string {
	var b strings.Builder
	for _, chunk := range stream.chunks {
		b.Write(chunk.GetData())
	}
	return b.String()
}

// A read had a cap of its own: a followed log stopped at 8 MiB, and the
// reader that came back saw it again from the start (P9). Only the
// machine's hourly budget bounds it now (channelLogSender).
func TestAReadHasNoCapOfItsOwn(t *testing.T) {
	block := bytes.Repeat([]byte("x"), 128<<10)
	var chunks [][]byte
	for i := 0; i < 100; i++ { // 12.5 MiB
		chunks = append(chunks, block)
	}
	server := testLogServer(&scriptedLogExecutor{chunks: chunks})
	stream := &mockLogStream{}
	if err := server.streamLogs(streamReq(), stream); err != nil {
		t.Fatal(err)
	}
	if got := len(collectedData(stream)); got != 100*len(block) {
		t.Fatalf("sent %d bytes of %d", got, 100*len(block))
	}
}

// A machine serves a bounded number of reads, and one job a share of them:
// each holds a file and an inotify instance.
func TestLogReadsAreCapped(t *testing.T) {
	readers := newLogReaders()
	var releases []func()
	for i := 0; i < maxLogReadersPerJob; i++ {
		release, err := readers.take("job-a")
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if _, err := readers.take("job-a"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("one read past the job's share: %v", err)
	}
	for i := maxLogReadersPerJob; i < maxLogReaders; i++ {
		release, err := readers.take("job-" + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if _, err := readers.take("job-z"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("one read past the machine's: %v", err)
	}
	releases[0]()
	if _, err := readers.take("job-a"); err != nil {
		t.Fatalf("a read after one ended: %v", err)
	}
}

// The summary drops the middle of a progress series and keeps the rest.
func TestStreamLogsSummaryCollapsesProgress(t *testing.T) {
	var chunks [][]byte
	chunks = append(chunks, []byte("[zenthros-resources] host CPU=2 RAM=1.00 GiB torch.cuda=False gpu=none\n"))
	chunks = append(chunks, []byte("[RAY CLUSTER RESOURCES]\n"))
	chunks = append(chunks, []byte("[synth] Starting training pipeline\n"))
	for i := 0; i < 20; i++ {
		chunks = append(chunks, []byte("[synth-train] Epoch "+strconv.Itoa(i)+" loss=1.39\n"))
	}
	chunks = append(chunks, []byte("[synth] Training job failed: boom\n"))
	server := testLogServer(&scriptedLogExecutor{chunks: chunks})
	stream := &mockLogStream{}
	if err := server.streamLogs(summaryReq(), stream); err != nil {
		t.Fatalf("StreamLogs() error = %v", err)
	}
	got := collectedData(stream)
	for _, want := range []string{"[zenthros-resources]", "[RAY CLUSTER RESOURCES]", "Epoch 0", "Training job failed"} {
		if !strings.Contains(got, want) {
			t.Errorf("the summary lost %q: %q", want, got)
		}
	}
	if strings.Contains(got, "Epoch 10") {
		t.Errorf("the summary kept the middle epochs: %q", got)
	}
}

func TestStreamLogsSecondRequestNewFilterWindow(t *testing.T) {
	chunks := [][]byte{
		[]byte("[synth-train] Epoch 0 loss=1.0\n"),
		[]byte("[synth-train] Epoch 1 loss=0.9\n"),
		[]byte("[synth-train] Epoch 2 loss=0.8\n"),
	}
	server := testLogServer(&scriptedLogExecutor{chunks: chunks})

	first := &mockLogStream{}
	if err := server.streamLogs(summaryReq(), first); err != nil {
		t.Fatalf("first StreamLogs() error = %v", err)
	}
	second := &mockLogStream{}
	if err := server.streamLogs(summaryReq(), second); err != nil {
		t.Fatalf("second StreamLogs() error = %v", err)
	}
	if collectedData(first) != collectedData(second) {
		t.Fatalf("second request is a new filter window\n first=%q\nsecond=%q", collectedData(first), collectedData(second))
	}
	if strings.Contains(collectedData(first), "Epoch 1") {
		t.Errorf("each window must collapse middle ticks, got %q", collectedData(first))
	}
	if !strings.Contains(collectedData(first), "Epoch 0") || !strings.Contains(collectedData(first), "Epoch 2") {
		t.Errorf("each window must keep first+last, got %q", collectedData(first))
	}
}

// Without --summary every line arrives as the job wrote it: the summary drops
// lines by Ray/train/tune patterns, and applied to every job it hid lines of
// jobs that are neither.
func TestStreamLogsSendsEveryLineUnlessSummaryIsAsked(t *testing.T) {
	chunks := [][]byte{
		[]byte("[synth-train] Epoch 0 loss=1.0\n"),
		[]byte("[synth-train] Epoch 1 loss=0.9\n"),
		[]byte("[synth-train] Epoch 2 loss=0.8\n"),
	}
	server := testLogServer(&scriptedLogExecutor{chunks: chunks})
	stream := &mockLogStream{}
	if err := server.streamLogs(streamReq(), stream); err != nil {
		t.Fatal(err)
	}
	if got, want := collectedData(stream), string(bytes.Join(chunks, nil)); got != want {
		t.Fatalf("raw logs = %q, want every line %q", got, want)
	}
}
