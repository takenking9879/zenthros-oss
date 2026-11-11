// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// sentLogs is a LogChannel that keeps what the agent sent on it.
type sentLogs struct {
	agentpb.Agent_LogChannelClient
	mu   sync.Mutex
	msgs []*agentpb.LogChannelFromAgent
	fail error
}

func (s *sentLogs) Send(msg *agentpb.LogChannelFromAgent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *sentLogs) sent() []*agentpb.LogChannelFromAgent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentpb.LogChannelFromAgent(nil), s.msgs...)
}

// data is every byte sent, in order: what a reader writes.
func (s *sentLogs) data() string {
	var b strings.Builder
	for _, msg := range s.sent() {
		b.Write(msg.GetChunk().GetData())
	}
	return b.String()
}

// batchSender is a sender over a kept LogChannel. A window of an hour never
// ends by itself within a test.
func batchSender(window time.Duration) (*channelLogSender, *sentLogs) {
	stream := &sentLogs{}
	return newChannelLogSender(context.Background(), &logChannelClient{stream: stream}, "pull-1", window), stream
}

func line(stream string, i int) *agentpb.LogChunk {
	return &agentpb.LogChunk{Stream: stream, Data: []byte(fmt.Sprintf("line %03d\n", i))}
}

// The lines of a burst reach the Gateway in one message, not one each: a
// message per line was all of a Gateway's CPU with 900 readers
// (scale lab, 2026-10-10). The reader gets the same bytes in the same
// order.
func TestLogLinesOfABurstTravelTogether(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	var want strings.Builder
	for i := range 200 {
		chunk := line("stdout", i)
		want.Write(chunk.GetData())
		if err := sender.Send(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := sender.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := len(stream.sent()); n != 2 {
		t.Fatalf("200 lines of a burst went in %d messages, want 2: the first line at once, the rest together", n)
	}
	if got := stream.data(); got != want.String() {
		t.Fatalf("the reader gets %d bytes, want the %d sent, in order", len(got), want.Len())
	}
	for _, msg := range stream.sent() {
		if msg.GetRequestId() != "pull-1" || msg.GetChunk().GetStream() != "stdout" {
			t.Fatalf("message %+v, want pull-1 / stdout", msg)
		}
	}
}

// A line after a quiet moment is not held back: the first line of a log, and
// a job that prints once in a while, arrive as before.
func TestLogLineAfterAQuietMomentGoesAtOnce(t *testing.T) {
	sender, stream := batchSender(20 * time.Millisecond)
	for i := range 3 {
		if err := sender.Send(line("stdout", i)); err != nil {
			t.Fatal(err)
		}
		if n := len(stream.sent()); n != i+1 {
			t.Fatalf("line %d: %d messages sent right after it, want %d", i, n, i+1)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

// Lines that wait go when the window ends, with nobody sending another.
func TestWaitingLogLinesGoWhenTheWindowEnds(t *testing.T) {
	sender, stream := batchSender(30 * time.Millisecond)
	for i := range 3 {
		if err := sender.Send(line("stdout", i)); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(stream.sent()); n != 1 {
		t.Fatalf("%d messages right after the burst, want only the first line", n)
	}
	deadline := time.After(5 * time.Second)
	for len(stream.sent()) < 2 {
		select {
		case <-deadline:
			t.Fatal("the lines waiting were never sent")
		case <-time.After(2 * time.Millisecond):
		}
	}
	if got := stream.data(); got != "line 000\nline 001\nline 002\n" {
		t.Fatalf("the reader got %q", got)
	}
}

// A batch is cut at its size: heavy lines do not pile up for the whole
// window, and no message is much larger than the cut.
func TestLogBatchIsCutAtItsSize(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	heavy := &agentpb.LogChunk{Stream: "stdout", Data: []byte(strings.Repeat("x", 5<<10-1) + "\n")}
	for range 9 {
		if err := sender.Send(heavy); err != nil {
			t.Fatal(err)
		}
	}
	// The first at once, then 4 lines (20 KiB, past the cut) twice.
	if n := len(stream.sent()); n != 3 {
		t.Fatalf("%d messages for 9 lines of 5 KiB before any flush, want 3", n)
	}
	for _, msg := range stream.sent() {
		if n := len(msg.GetChunk().GetData()); n > logBatchBytes+5<<10 {
			t.Fatalf("a message of %d bytes, want at most the cut plus one line", n)
		}
	}
}

// stderr is not joined to stdout, and the truncation notice goes alone,
// after everything that was waiting: order is kept across all of them.
func TestLogStreamsAndTheTruncationNoticeAreNotJoined(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	notice := &agentpb.LogChunk{Stream: "stdout", Data: []byte("[truncated]\n"), Truncated: true}
	for _, chunk := range []*agentpb.LogChunk{line("stdout", 0), line("stdout", 1), line("stdout", 2), line("stderr", 3), line("stderr", 4), notice} {
		if err := sender.Send(chunk); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	for _, msg := range stream.sent() {
		c := msg.GetChunk()
		got = append(got, fmt.Sprintf("%s truncated=%v %q", c.GetStream(), c.GetTruncated(), c.GetData()))
	}
	want := []string{
		`stdout truncated=false "line 000\n"`,
		`stdout truncated=false "line 001\nline 002\n"`,
		`stderr truncated=false "line 003\nline 004\n"`,
		`stdout truncated=true "[truncated]\n"`,
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("sent:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// A send that fails is the pull's error from then on: lines are not
// accepted into a batch that will never leave.
func TestLogSendErrorStopsThePull(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	if err := sender.Send(line("stdout", 0)); err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(line("stdout", 1)); err != nil {
		t.Fatal(err)
	}
	stream.mu.Lock()
	stream.fail = errors.New("channel closed")
	stream.mu.Unlock()
	if err := sender.Flush(); err == nil {
		t.Fatal("a failed flush reported nothing")
	}
	if err := sender.Send(line("stdout", 2)); err == nil {
		t.Fatal("a line was accepted after the channel failed")
	}
}

// The order that keeps a log whole: the lines waiting leave before the pull
// says it ended. Without the flush the reader's stream closed with the last
// lines of the log still in the agent.
func TestWaitingLogLinesLeaveBeforeThePullEnds(t *testing.T) {
	var lines [][]byte
	var want strings.Builder
	for i := range 50 {
		data := fmt.Sprintf("line %03d\n", i)
		want.WriteString(data)
		lines = append(lines, []byte(data))
	}
	logs := NewLogServer(&scriptedLogExecutor{chunks: lines}, newJobTracker(), logr.Discard())
	stream := &sentLogs{}
	client := &logChannelClient{stream: stream}
	(&Agent{}).handleLogPull(context.Background(), client, logs, "pull-1", streamReq())

	sent := stream.sent()
	if len(sent) == 0 || !sent[len(sent)-1].GetEof() {
		t.Fatalf("the last of %d messages is not the end of the pull", len(sent))
	}
	for _, msg := range sent[:len(sent)-1] {
		if msg.GetEof() || msg.GetChunk() == nil {
			t.Fatalf("a message before the end carries no lines: %+v", msg)
		}
	}
	if got := stream.data(); got != want.String() {
		t.Fatalf("the reader got %d bytes before the end, want all %d", len(got), want.Len())
	}
	if n := len(sent) - 1; n > 3 {
		t.Fatalf("50 lines read at once went in %d messages, want them batched", n)
	}
}

// A window of zero joins nothing: every line is its own message, as before
// batching. It is the base the windows are measured against.
func TestLogLinesAreNotJoinedWithoutAWindow(t *testing.T) {
	sender, stream := batchSender(0)
	for i := range 200 {
		if err := sender.Send(line("stdout", i)); err != nil {
			t.Fatal(err)
		}
	}
	sent := stream.sent()
	if len(sent) != 200 {
		t.Fatalf("200 lines went in %d messages before any flush, want one each", len(sent))
	}
	for i, msg := range sent {
		if got, want := string(msg.GetChunk().GetData()), fmt.Sprintf("line %03d\n", i); got != want {
			t.Fatalf("message %d carries %q, want %q", i, got, want)
		}
	}
}

// The window is the agent's option, and a pull batches with it: unset it is
// the production window, negative it joins nothing, anything else is used as
// given.
func TestLogBatchWindowIsTheAgentsOption(t *testing.T) {
	for _, tc := range []struct {
		set, want time.Duration
	}{
		{0, time.Second},
		{-1, 0},
		{500 * time.Millisecond, 500 * time.Millisecond},
		{time.Second, time.Second},
	} {
		if got := (Config{LogBatchWindow: tc.set}).logBatchWindow(); got != tc.want {
			t.Fatalf("LogBatchWindow %s batches with %s, want %s", tc.set, got, tc.want)
		}
	}

	pull := func(window time.Duration) int {
		var lines [][]byte
		for i := range 50 {
			lines = append(lines, []byte(fmt.Sprintf("line %03d\n", i)))
		}
		logs := NewLogServer(&scriptedLogExecutor{chunks: lines}, newJobTracker(), logr.Discard())
		stream := &sentLogs{}
		a := &Agent{cfg: Config{LogBatchWindow: window}}
		a.handleLogPull(context.Background(), &logChannelClient{stream: stream}, logs, "pull-1", streamReq())
		chunks := 0
		for _, msg := range stream.sent() {
			if msg.GetChunk() != nil {
				chunks++
			}
		}
		return chunks
	}
	if n := pull(-1); n != 50 {
		t.Fatalf("an agent with no window sent 50 lines in %d messages, want one each", n)
	}
	// The first line at once, the rest when the pull ends.
	if n := pull(time.Hour); n != 2 {
		t.Fatalf("an agent with a window of an hour sent 50 lines in %d messages, want 2", n)
	}
}

// The installation sets the window once, at the Gateway, which names it when
// it accepts a machine's session: every machine batches with it, whatever
// its agent was installed with. An agent given its own window keeps it, and
// the hub's word lasts its session: a Gateway that says nothing leaves the
// agent with its default again.
func TestRegisterTakesTheHubsLogWindow(t *testing.T) {
	register := func(t *testing.T, a *Agent, hubMs int64) {
		t.Helper()
		stream := newRecordingStream()
		stream.recv <- &agentpb.RegisterResponse{Accepted: true, LogBatchWindowMs: hubMs}
		if err := a.Register(context.Background(), stream); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name  string
		own   time.Duration
		hubMs int64
		want  time.Duration
	}{
		{"the hub's window", 0, 250, 250 * time.Millisecond},
		{"the hub asks for none", 0, -1, 0},
		{"a hub that names none", 0, 0, defaultLogBatchWindow},
		{"the agent's own wins", 2 * time.Second, 250, 2 * time.Second},
		{"the agent's own none wins", -1, 250, 0},
	} {
		a := NewWithExecutor(Config{BackendID: "vm-1", BackendType: "vm", Network: NetworkTailscaleUserspace, LogBatchWindow: tc.own}, SystemdExecutor{}, logr.Discard())
		register(t, a, tc.hubMs)
		if got := a.logBatchWindow(); got != tc.want {
			t.Fatalf("%s: batches with %s, want %s", tc.name, got, tc.want)
		}
	}

	a := NewWithExecutor(Config{BackendID: "vm-1", BackendType: "vm", Network: NetworkTailscaleUserspace}, SystemdExecutor{}, logr.Discard())
	register(t, a, 250)
	register(t, a, 0)
	if got := a.logBatchWindow(); got != defaultLogBatchWindow {
		t.Fatalf("after a session whose hub named no window the agent batches with %s, want its default %s", got, defaultLogBatchWindow)
	}
}
