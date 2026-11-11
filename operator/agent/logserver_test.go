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

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
)

type logChunk struct {
	stream    string
	data      string
	truncated bool
}

type mockLogStream struct {
	grpc.ServerStream
	chunks   []logChunk
	sendErr  error
	recvErrs []error
	recvIdx  int
	ctx      context.Context
}

func (m *mockLogStream) Send(chunk *agentpb.LogChunk) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.chunks = append(m.chunks, logChunk{
		stream:    chunk.GetStream(),
		data:      string(chunk.GetData()),
		truncated: chunk.GetTruncated(),
	})
	return nil
}

func (m *mockLogStream) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func payloadData(stream *mockLogStream) string {
	var b strings.Builder
	for _, chunk := range stream.chunks {
		if chunk.truncated {
			continue
		}
		b.WriteString(chunk.data)
	}
	return b.String()
}

func TestLogServerStreamLogs(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	server := NewLogServer(exec, tracker, logr.Discard())

	stream := &mockLogStream{}
	req := &agentpb.StreamLogsRequest{
		BackendId:  "vm-test",
		JobId:      "job-1",
		HandleName: "handle-1",
		Follow:     false,
		TailLines:  10,
	}

	err := server.streamLogs(req, stream)
	if err != nil {
		t.Fatalf("StreamLogs() error = %v", err)
	}
}

func TestLogServerStreamLogsEmptyHandle(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	server := NewLogServer(exec, tracker, logr.Discard())

	stream := &mockLogStream{}
	req := &agentpb.StreamLogsRequest{
		BackendId: "vm-test",
		JobId:     "job-1",
	}

	err := server.streamLogs(req, stream)
	if err == nil {
		t.Fatal("expected error for empty handle_name")
	}
}

func TestToProtoLogChunk(t *testing.T) {
	chunk := backend.LogChunk{
		Stream:    "stdout",
		Data:      []byte("hello\n"),
		Timestamp: time.Unix(1000, 0),
	}
	proto := toProtoLogChunk(chunk)
	if proto.GetStream() != "stdout" {
		t.Errorf("stream = %q, want stdout", proto.GetStream())
	}
	if string(proto.GetData()) != "hello\n" {
		t.Errorf("data = %q, want hello\\n", proto.GetData())
	}
	if proto.GetTimestampUnix() != 1000 {
		t.Errorf("timestamp = %d, want 1000", proto.GetTimestampUnix())
	}
	if proto.GetTruncated() {
		t.Error("payload chunks must not set truncated")
	}
}

type scriptedLogExecutor struct {
	mockExecutor
	chunks [][]byte
}

func (e *scriptedLogExecutor) Logs(ctx context.Context, _ string, _ bool, _ int) (<-chan backend.LogChunk, error) {
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
		b.WriteString(chunk.data)
	}
	return b.String()
}

func hasTruncated(stream *mockLogStream) bool {
	for _, chunk := range stream.chunks {
		if chunk.truncated {
			return true
		}
	}
	return false
}

func TestStreamLogsUnderCapCompletes(t *testing.T) {
	exec := &scriptedLogExecutor{chunks: [][]byte{[]byte("hello\n"), []byte("world\n")}}
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	server.maxLogBytes = 64
	server.leftoverHint = leftoverHintFor("vm")

	stream := &mockLogStream{}
	if err := server.streamLogs(streamReq(), stream); err != nil {
		t.Fatalf("StreamLogs() error = %v", err)
	}
	if got := collectedData(stream); got != "hello\nworld\n" {
		t.Errorf("data = %q, want hello\\nworld\\n", got)
	}
	if hasTruncated(stream) {
		t.Error("under-cap stream must not send a truncated signal")
	}
}

func TestStreamLogsOverCapStopsAndSignals(t *testing.T) {
	exec := &scriptedLogExecutor{chunks: [][]byte{
		[]byte("AAAA"),
		[]byte("BBBB"),
		[]byte("CCCC"),
		[]byte("LEFTOVER"),
	}}
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	server.maxLogBytes = 6
	server.leftoverHint = leftoverHintFor("k8s")

	stream := &mockLogStream{}
	if err := server.streamLogs(streamReq(), stream); err != nil {
		t.Fatalf("StreamLogs() error = %v", err)
	}

	payload := ""
	var signal logChunk
	for _, chunk := range stream.chunks {
		if chunk.truncated {
			signal = chunk
			continue
		}
		payload += chunk.data
	}
	if payload != "AAAABB" {
		t.Errorf("payload = %q, want AAAABB (cap 6)", payload)
	}
	if !signal.truncated {
		t.Fatal("over-cap stream must include a truncated signal")
	}
	if !strings.Contains(signal.data, "truncated") {
		t.Errorf("truncated signal %q must name truncated", signal.data)
	}
	if !strings.Contains(signal.data, "kubectl logs") {
		t.Errorf("truncated signal %q must name kubectl logs", signal.data)
	}
	if strings.Contains(payload, "LEFTOVER") || strings.Contains(payload, "CCCC") {
		t.Errorf("payload %q must stop at the cap", payload)
	}
}

func TestStreamLogsHourlyBudgetDistinctFromRequestCap(t *testing.T) {
	exec := &scriptedLogExecutor{chunks: [][]byte{[]byte("0123456789")}}
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	server.maxLogBytes = 8 << 20
	server.hourly = newHourlyBudget(6)
	server.leftoverHint = leftoverHintFor("vm")

	first := &mockLogStream{}
	if err := server.streamLogs(streamReq(), first); err != nil {
		t.Fatalf("first StreamLogs() error = %v", err)
	}
	if payloadData(first) != "012345" {
		t.Fatalf("first hourly payload = %q, want 012345", payloadData(first))
	}
	if !hasTruncated(first) {
		t.Fatal("hourly cap must send a truncated signal")
	}
	got := ""
	for _, chunk := range first.chunks {
		if chunk.truncated {
			got = chunk.data
		}
	}
	if !strings.Contains(got, "hourly") {
		t.Errorf("hourly truncated signal %q must name hourly", got)
	}

	second := &mockLogStream{}
	if err := server.streamLogs(streamReq(), second); err != nil {
		t.Fatalf("second StreamLogs() error = %v", err)
	}
	if payloadData(second) != "" {
		t.Fatalf("exhausted hourly budget still sent %q", payloadData(second))
	}
	if !hasTruncated(second) {
		t.Fatal("exhausted hourly budget must truncate the next request")
	}
}

func TestStreamLogsHourlyWindowReset(t *testing.T) {
	now := time.Unix(1000, 0)
	exec := &scriptedLogExecutor{chunks: [][]byte{[]byte("AAAA")}}
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	server.maxLogBytes = 8 << 20
	server.hourly = newHourlyBudget(4)
	server.hourly.now = func() time.Time { return now }

	first := &mockLogStream{}
	if err := server.streamLogs(streamReq(), first); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour + time.Second)
	second := &mockLogStream{}
	if err := server.streamLogs(streamReq(), second); err != nil {
		t.Fatal(err)
	}
	if payloadData(second) != "AAAA" {
		t.Fatalf("after hourly window data = %q, want AAAA", payloadData(second))
	}
	if hasTruncated(second) {
		t.Fatal("new hourly window must not truncate a 4-byte request")
	}
}

func TestStreamLogsPerRequestCapUnchangedByHourly(t *testing.T) {
	exec := &scriptedLogExecutor{chunks: [][]byte{[]byte("0123456789")}}
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	server.maxLogBytes = 4
	server.hourly = newHourlyBudget(50 << 20)
	stream := &mockLogStream{}
	if err := server.streamLogs(streamReq(), stream); err != nil {
		t.Fatal(err)
	}
	if payloadData(stream) != "0123" {
		t.Fatalf("per-request cap data = %q, want 0123", payloadData(stream))
	}
	if !hasTruncated(stream) {
		t.Fatal("per-request cap must still truncate")
	}
}

func TestStreamLogsSecondRequestIsNewBudget(t *testing.T) {
	exec := &scriptedLogExecutor{chunks: [][]byte{[]byte("0123456789")}}
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	server.maxLogBytes = 4
	server.leftoverHint = leftoverHintFor("vm")

	first := &mockLogStream{}
	if err := server.streamLogs(streamReq(), first); err != nil {
		t.Fatalf("first StreamLogs() error = %v", err)
	}
	second := &mockLogStream{}
	if err := server.streamLogs(streamReq(), second); err != nil {
		t.Fatalf("second StreamLogs() error = %v", err)
	}

	if !hasTruncated(first) || !hasTruncated(second) {
		t.Fatal("each over-cap request must include its own truncated signal")
	}
	if got := collectedData(first); !strings.HasPrefix(got, "0123") {
		t.Errorf("first request data = %q, want prefix 0123", got)
	}
	if got := collectedData(second); !strings.HasPrefix(got, "0123") {
		t.Errorf("second request is a new budget, data = %q, want prefix 0123", got)
	}
	if collectedData(first) == "0123456789" || collectedData(second) == "0123456789" {
		t.Error("neither request may exceed the per-request cap")
	}
}

func TestStreamLogsFilterThenCap(t *testing.T) {
	// Resource probe + 20 epoch ticks. Filter keeps probe + first + last epoch;
	// the cap then applies to that reduced stream (not the raw 20 ticks).
	var chunks [][]byte
	chunks = append(chunks, []byte("[zenthros-resources] host CPU=2 RAM=1.00 GiB torch.cuda=False gpu=none\n"))
	chunks = append(chunks, []byte("[RAY CLUSTER RESOURCES]\n"))
	chunks = append(chunks, []byte("[synth] Starting training pipeline\n"))
	for i := 0; i < 20; i++ {
		chunks = append(chunks, []byte("[synth-train] Epoch "+strconv.Itoa(i)+" loss=1.39\n"))
	}
	chunks = append(chunks, []byte("[synth] Training job failed: boom\n"))

	exec := &scriptedLogExecutor{chunks: chunks}
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	// Small cap: middle epochs must not consume budget (filter first).
	server.maxLogBytes = 220
	server.leftoverHint = leftoverHintFor("k8s")

	stream := &mockLogStream{}
	if err := server.streamLogs(summaryReq(), stream); err != nil {
		t.Fatalf("StreamLogs() error = %v", err)
	}
	got := collectedData(stream)
	if !strings.Contains(got, "[zenthros-resources]") {
		t.Errorf("filtered stream must keep resource probe, got %q", got)
	}
	if !strings.Contains(got, "[RAY CLUSTER RESOURCES]") {
		t.Errorf("filtered stream must keep cluster resources, got %q", got)
	}
	if !strings.Contains(got, "Epoch 0") {
		t.Errorf("filtered stream must keep first epoch, got %q", got)
	}
	if strings.Contains(got, "Epoch 10") {
		t.Errorf("filtered stream must collapse middle epochs, got %q", got)
	}
	if !hasTruncated(stream) && !strings.Contains(got, "Training job failed") {
		t.Errorf("filter-then-cap must still surface the error or the truncated signal, got %q", got)
	}
}

func TestStreamLogsSecondRequestNewFilterWindow(t *testing.T) {
	chunks := [][]byte{
		[]byte("[synth-train] Epoch 0 loss=1.0\n"),
		[]byte("[synth-train] Epoch 1 loss=0.9\n"),
		[]byte("[synth-train] Epoch 2 loss=0.8\n"),
	}
	exec := &scriptedLogExecutor{chunks: chunks}
	server := NewLogServer(exec, newJobTracker(), logr.Discard())
	server.maxLogBytes = 8 << 20

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
	server := NewLogServer(&scriptedLogExecutor{chunks: chunks}, newJobTracker(), logr.Discard())
	stream := &mockLogStream{}
	if err := server.streamLogs(streamReq(), stream); err != nil {
		t.Fatal(err)
	}
	if got, want := collectedData(stream), string(bytes.Join(chunks, nil)); got != want {
		t.Fatalf("raw logs = %q, want every line %q", got, want)
	}
}
