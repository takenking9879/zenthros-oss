// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"

	agentpb "github.com/takenking9879/zenthros-oss/operator/internal/grpc/gen"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
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

// data is every byte of log sent, in order: what a reader writes.
func (s *sentLogs) data() string {
	var b strings.Builder
	for _, msg := range s.sent() {
		b.Write(unpacked(msg.GetChunk()))
	}
	return b.String()
}

var testDecoder, _ = zstd.NewReader(nil)

// unpacked is chunk's bytes of log, as a reader unpacks them.
func unpacked(chunk *agentpb.LogChunk) []byte {
	if chunk.GetEncoding() != agentpb.LogEncoding_LOG_ZSTD {
		return chunk.GetData()
	}
	out, err := testDecoder.DecodeAll(chunk.GetData(), nil)
	if err != nil || int64(len(out)) != chunk.GetRawLen() {
		panic(fmt.Sprintf("a chunk that unpacks to %d bytes of %d: %v", len(out), chunk.GetRawLen(), err))
	}
	return out
}

// batchSender is a sender over a kept LogChannel. A window of an hour never
// ends by itself within a test.
func batchSender(window time.Duration) (*channelLogSender, *sentLogs) {
	stream := &sentLogs{}
	return newChannelLogSender(context.Background(), &logChannelClient{stream: stream}, "pull-1", window, newHourlyBudget(0)), stream
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
	if n := len(stream.sent()); n != 3 {
		t.Fatalf("200 lines of a burst went in %d messages, want 3: the first two lines at once, the rest together", n)
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

// The runner's first line and the function's right after it, then a line each
// window: each leaves when it is read. Counted from the last message sent, the
// function's first line waited the rest of the window behind the runner's, and
// from then on each line came just after a send and waited a whole window:
// a job that printed a line a second had every line 1 s late, and each
// message still carried one line (This computer, 2026-11-18).
func TestALineEachWindowLeavesWhenItIsRead(t *testing.T) {
	const window = 200 * time.Millisecond
	sender, stream := batchSender(window)
	if err := sender.Send(line("stdout", 0)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	time.Sleep(5 * time.Millisecond)
	for i := 1; i <= 5; i++ {
		time.Sleep(time.Until(start.Add(time.Duration(i-1) * window)))
		if err := sender.Send(line("stdout", i)); err != nil {
			t.Fatal(err)
		}
		if n := len(stream.sent()); n != i+1 {
			t.Fatalf("line %d: %d messages sent right after it, want %d: it waited for the window", i, n, i+1)
		}
	}
}

// A log busier than the window still leaves one message a window, plus the
// two a quiet moment lets go: what halved the Gateways' CPU with 900 readers
// (scale lab, 2026-10-12).
func TestABusyLogLeavesOneMessageAWindow(t *testing.T) {
	const window = 50 * time.Millisecond
	sender, stream := batchSender(window)
	start := time.Now()
	for i := 0; time.Since(start) < 20*window; i++ {
		if err := sender.Send(line("stdout", i)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(window / 10)
	}
	if n := len(stream.sent()); n > 20+2 {
		t.Fatalf("%d messages in 20 windows, want at most one a window and the two after a quiet moment", n)
	}
}

// Lines that wait go when the window ends, with nobody sending another.
func TestWaitingLogLinesGoWhenTheWindowEnds(t *testing.T) {
	sender, stream := batchSender(30 * time.Millisecond)
	for i := range 4 {
		if err := sender.Send(line("stdout", i)); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(stream.sent()); n != 2 {
		t.Fatalf("%d messages right after the burst, want only the first two lines", n)
	}
	deadline := time.After(5 * time.Second)
	for len(stream.sent()) < 3 {
		select {
		case <-deadline:
			t.Fatal("the lines waiting were never sent")
		case <-time.After(2 * time.Millisecond):
		}
	}
	if got := stream.data(); got != "line 000\nline 001\nline 002\nline 003\n" {
		t.Fatalf("the reader got %q", got)
	}
}

// A batch is cut at its size: heavy lines do not pile up for the whole
// window, and no message is much larger than the cut.
func TestLogBatchIsCutAtItsSize(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	for range 9 {
		heavy := &agentpb.LogChunk{Stream: "stdout", Data: []byte(strings.Repeat("x", 5<<10-1) + "\n")}
		if err := sender.Send(heavy); err != nil {
			t.Fatal(err)
		}
	}
	// The first two at once, then 4 lines (20 KiB, past the cut), and 3
	// waiting.
	if n := len(stream.sent()); n != 3 {
		t.Fatalf("%d messages for 9 lines of 5 KiB before any flush, want 3", n)
	}
	for _, msg := range stream.sent() {
		if n := len(unpacked(msg.GetChunk())); n > logBatchBytes+5<<10 {
			t.Fatalf("a message of %d bytes, want at most the cut plus one line", n)
		}
	}
}

// A read that fills a batch alone leaves as it came. The window was made
// for lines (scale lab, 2026-10-11) and copied a batch's first chunk into
// room for four; since a chunk is a whole read of the job's file (W2), a
// read of up to 128 KiB was copied into 512 KiB and sent at once all the
// same: the agent spent 9 % more CPU at 1,000 lines a second and 29 % more
// at 10,000 with the window than without (agent lab, 2026-11-15).
func TestAReadThatFillsABatchLeavesUncopied(t *testing.T) {
	read := []byte(strings.Repeat("a line of a busy log, 1,000 a second\n", 1000)) // ~36 KiB
	allocs := func(window time.Duration) float64 {
		sender, _ := batchSender(window)
		send := func() {
			if err := sender.Send(&agentpb.LogChunk{Stream: "stdout", Data: bytes.Clone(read)}); err != nil {
				t.Fatal(err)
			}
		}
		send() // the first, after a quiet moment, goes at once with any window
		return testing.AllocsPerRun(50, send)
	}
	if with, without := allocs(time.Hour), allocs(0); with > without {
		t.Fatalf("a read of %d bytes takes %.0f allocations with a window and %.0f without: the window copied it", len(read), with, without)
	}
}

// A batch's first chunk is not copied, and its bytes may share an array
// with a later chunk's: the pieces of a line past backend.MaxLogLine are
// slices of the line (k8sagent's pod log). Joining what follows must not
// write over them.
func TestABatchNeverWritesOverAChunkThatFollows(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	for i := range logBatchBurst { // the window is open from here
		if err := sender.Send(line("stdout", i)); err != nil {
			t.Fatal(err)
		}
	}
	whole := []byte(strings.Repeat("a", 100) + strings.Repeat("c", 100))
	for _, data := range [][]byte{whole[:100], []byte(strings.Repeat("b", 100)), whole[100:]} {
		if err := sender.Send(&agentpb.LogChunk{Stream: "stdout", Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sender.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "line 000\nline 001\n" + strings.Repeat("a", 100) + strings.Repeat("b", 100) + strings.Repeat("c", 100)
	if got := stream.data(); got != want {
		t.Fatalf("the reader gets %q, want %q", got, want)
	}
}

// stderr is not joined to stdout, and a chunk after a gap goes alone, after
// everything that was waiting: order is kept across all of them. A batch
// carries the cursor of its last chunk, so a reader that goes on from it
// gets the next byte.
func TestLogStreamsAndAGapAreNotJoined(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	chunks := []*agentpb.LogChunk{line("stdout", 0), line("stdout", 1), line("stdout", 2), line("stdout", 3),
		line("stderr", 4), line("stderr", 5), line("stderr", 6)}
	for i, c := range chunks {
		c.Cursor = fmt.Sprint(i + 1)
	}
	chunks[6].Gap = 100
	for _, chunk := range chunks {
		if err := sender.Send(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := sender.Flush(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, msg := range stream.sent() {
		c := msg.GetChunk()
		got = append(got, fmt.Sprintf("%s cursor=%s gap=%d %q", c.GetStream(), c.GetCursor(), c.GetGap(), unpacked(c)))
	}
	want := []string{
		`stdout cursor=1 gap=0 "line 000\n"`,
		`stdout cursor=2 gap=0 "line 001\n"`,
		`stdout cursor=4 gap=0 "line 002\nline 003\n"`,
		`stderr cursor=6 gap=0 "line 004\nline 005\n"`,
		`stderr cursor=7 gap=100 "line 006\n"`,
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("sent:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// A send that fails is the pull's error from then on: lines are not
// accepted into a batch that will never leave.
func TestLogSendErrorStopsThePull(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	for i := range logBatchBurst + 1 { // the last one waits
		if err := sender.Send(line("stdout", i)); err != nil {
			t.Fatal(err)
		}
	}
	stream.mu.Lock()
	stream.fail = errors.New("channel closed")
	stream.mu.Unlock()
	if err := sender.Flush(); err == nil {
		t.Fatal("a failed flush reported nothing")
	}
	if err := sender.Send(line("stdout", logBatchBurst+1)); err == nil {
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
	logs := testLogServer(&scriptedLogExecutor{chunks: lines})
	stream := &sentLogs{}
	client := &logChannelClient{stream: stream}
	(&Agent{hourlyLogs: newHourlyBudget(0)}).handleLogPull(context.Background(), client, logs, "pull-1", streamReq())

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
		if got, want := string(unpacked(msg.GetChunk())), fmt.Sprintf("line %03d\n", i); got != want {
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
		logs := testLogServer(&scriptedLogExecutor{chunks: lines})
		stream := &sentLogs{}
		a := &Agent{cfg: Config{LogBatchWindow: window}, hourlyLogs: newHourlyBudget(0)}
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
	// The first two lines at once, the rest when the pull ends.
	if n := pull(time.Hour); n != 3 {
		t.Fatalf("an agent with a window of an hour sent 50 lines in %d messages, want 3", n)
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

// A batch never grows past what the Gateway takes: the lines waiting go
// first when the next one would not fit with them. Lines waiting plus a long
// line's piece were past it, and the Gateway ended the read.
func TestABatchNeverPassesTheGatewaysChunk(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	var want strings.Builder
	send := func(data string) {
		want.WriteString(data)
		if err := sender.Send(&agentpb.LogChunk{Stream: "stdout", Data: []byte(data)}); err != nil {
			t.Fatal(err)
		}
	}
	send("first\n")
	send("second\n")
	send(strings.Repeat("a", 10<<10) + "\n")
	send(strings.Repeat("b", backend.MaxLogLine) + "\n")
	send("last\n")
	if err := sender.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, msg := range stream.sent() {
		if n, raw := len(msg.GetChunk().GetData()), len(unpacked(msg.GetChunk())); n > backend.MaxLogChunk || raw > backend.MaxLogChunk {
			t.Errorf("a message of %d bytes (%d unpacked), past the %d the Gateway takes", n, raw, backend.MaxLogChunk)
		}
	}
	if stream.data() != want.String() {
		t.Fatal("the reader did not get the same bytes in the same order")
	}
}

// Each chunk leaves packed, in a zstd frame of its own that unpacks to the
// bytes it carried (raw_len); one that packs no smaller, a short line,
// leaves as it is.
func TestALogChunkLeavesPacked(t *testing.T) {
	sender, stream := batchSender(0)
	block := []byte(strings.Repeat("epoch 3 step 120 loss=0.4312 lr=3e-4\n", 3000))
	for _, data := range [][]byte{block, []byte("x\n")} {
		if err := sender.Send(&agentpb.LogChunk{Stream: "stdout", Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	sent := stream.sent()
	if len(sent) != 2 {
		t.Fatalf("%d messages", len(sent))
	}
	big, small := sent[0].GetChunk(), sent[1].GetChunk()
	if big.GetEncoding() != agentpb.LogEncoding_LOG_ZSTD || big.GetRawLen() != int64(len(block)) || len(big.GetData()) > len(block)/5 {
		t.Errorf("a %d-byte block left as %v of %d bytes, raw_len %d", len(block), big.GetEncoding(), len(big.GetData()), big.GetRawLen())
	}
	if small.GetEncoding() != agentpb.LogEncoding_LOG_RAW || string(small.GetData()) != "x\n" {
		t.Errorf("a short line left as %v %q", small.GetEncoding(), small.GetData())
	}
	if stream.data() != string(block)+"x\n" {
		t.Error("the reader did not get the same bytes")
	}
}

// The hourly budget counts what leaves the machine: the packed bytes. Past
// it a read does not end: it skips what it cannot send, and the first chunk
// after it says how many bytes of log, and why.
func TestTheHourlyBudgetCountsWhatLeaves(t *testing.T) {
	budget := newHourlyBudget(4 << 10)
	asked := 0
	budget.now = func() time.Time {
		// The budget comes back before the third chunk.
		if asked++; asked == 3 {
			return time.Unix(0, 0).Add(time.Hour)
		}
		return time.Unix(0, 0)
	}
	stream := &sentLogs{}
	sender := newChannelLogSender(context.Background(), &logChannelClient{stream: stream}, "pull-1", 0, budget)
	// 64 KiB of training output packs well under the 4 KiB budget; 6 KiB
	// of noise do not pack, and are past what is left.
	packs := []byte(strings.Repeat("epoch 3 step 120 loss=0.4312 lr=3e-4\n", 1800))
	noise := make([]byte, 6<<10)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range noise {
		noise[i] = byte(rng.Uint32())
	}
	for _, data := range [][]byte{packs, noise, []byte("three\n")} {
		if err := sender.Send(&agentpb.LogChunk{Stream: "stdout", Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	sent := stream.sent()
	if len(sent) != 2 || string(unpacked(sent[0].GetChunk())) != string(packs) || string(unpacked(sent[1].GetChunk())) != "three\n" {
		t.Fatalf("sent %d chunks", len(sent))
	}
	last := sent[1].GetChunk()
	if last.GetGap() != int64(len(noise)) || last.GetGapReason() != agentpb.GapReason_GAP_BUDGET {
		t.Fatalf("the chunk after the budget says gap %d (%v), want the %d bytes of log skipped", last.GetGap(), last.GetGapReason(), len(noise))
	}
}

// A gap or a history no one can count (-1) is said like any other: its chunk
// is never joined to the lines waiting, and a gap of it skipped past the
// budget is said as one no one can count.
func TestAnUncountedGapIsSaid(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	for _, chunk := range []*agentpb.LogChunk{
		line("stdout", 0), line("stdout", 1),
		{Stream: "stdout", Data: []byte("after a file the cluster removed\n"), Gap: backend.Uncounted},
	} {
		if err := sender.Send(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := sender.Flush(); err != nil {
		t.Fatal(err)
	}
	sent := stream.sent()
	if len(sent) != 3 || sent[2].GetChunk().GetGap() != backend.Uncounted || string(unpacked(sent[2].GetChunk())) != "after a file the cluster removed\n" {
		t.Fatalf("sent %v", sent)
	}

	budget := newHourlyBudget(6)
	asked := 0
	budget.now = func() time.Time {
		if asked++; asked == 3 {
			return time.Unix(0, 0).Add(time.Hour)
		}
		return time.Unix(0, 0)
	}
	stream = &sentLogs{}
	sender = newChannelLogSender(context.Background(), &logChannelClient{stream: stream}, "pull-1", 0, budget)
	for _, chunk := range []*agentpb.LogChunk{
		{Data: []byte("one\n")}, {Data: []byte("two\n"), Gap: backend.Uncounted}, {Data: []byte("three\n")},
	} {
		if err := sender.Send(chunk); err != nil {
			t.Fatal(err)
		}
	}
	sent = stream.sent()
	if len(sent) != 2 || sent[1].GetChunk().GetGap() != backend.Uncounted || sent[1].GetChunk().GetGapReason() != agentpb.GapReason_GAP_BUDGET {
		t.Fatalf("sent %v", sent)
	}
}

// What the budget skipped at a read's end is said too: a chunk of no bytes
// before the read ends, with the gap and the place after it, so a reader
// that comes back goes on after what was dropped.
func TestWhatTheBudgetSkippedAtTheEndIsSaid(t *testing.T) {
	budget := newHourlyBudget(6)
	budget.now = func() time.Time { return time.Unix(0, 0) }
	stream := &sentLogs{}
	sender := newChannelLogSender(context.Background(), &logChannelClient{stream: stream}, "pull-1", 0, budget)
	for _, chunk := range []*agentpb.LogChunk{
		{Data: []byte("one\n"), Cursor: "c1"},
		{Data: []byte("two two two\n"), Cursor: "c2"},
		{Data: []byte("three three\n"), Cursor: "c3"},
	} {
		if err := sender.Send(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := sender.Flush(); err != nil {
		t.Fatal(err)
	}
	sent := stream.sent()
	if len(sent) != 2 {
		t.Fatalf("sent %v", sent)
	}
	last := sent[1].GetChunk()
	if len(last.GetData()) != 0 || last.GetGap() != int64(len("two two two\nthree three\n")) ||
		last.GetGapReason() != agentpb.GapReason_GAP_BUDGET || last.GetCursor() != "c3" {
		t.Fatalf("the read's last chunk %+v, want the bytes skipped and the place after them", last)
	}
}

// A piece of a long line has no place of its own: joined to a batch, the
// batch keeps the place of the last chunk that had one.
func TestABatchKeepsItsPlaceThroughAPieceOfALine(t *testing.T) {
	sender, stream := batchSender(time.Hour)
	for _, chunk := range []*agentpb.LogChunk{
		{Stream: "stdout", Data: []byte("zeroth\n"), Cursor: "c0"},
		{Stream: "stdout", Data: []byte("first\n"), Cursor: "c1"},
		{Stream: "stdout", Data: []byte("second\n"), Cursor: "c2"},
		{Stream: "stdout", Data: []byte("a long line's first piece")},
	} {
		if err := sender.Send(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := sender.Flush(); err != nil {
		t.Fatal(err)
	}
	sent := stream.sent()
	if got := sent[len(sent)-1].GetChunk().GetCursor(); got != "c2" {
		t.Fatalf("the batch's place is %q, want c2", got)
	}
}

// goneLogExecutor is a machine that no longer has the job's log: it keeps
// the logs of its last 20 jobs.
type goneLogExecutor struct{ mockExecutor }

func (*goneLogExecutor) Logs(context.Context, string, backend.LogRequest) (<-chan backend.LogChunk, error) {
	return nil, backend.LogGone(20)
}

// A log the machine no longer keeps ends the pull with its code, NotFound,
// and the reason in words: the lab's reader got "rpc error: code =
// Unavailable desc = open logs for zenthros-....service: open
// /var/lib/zenthros/logs/...: no such file or directory", as a 502 (27).
func TestALogTheMachineNoLongerKeepsEndsThePullAsNotFound(t *testing.T) {
	logs := testLogServer(&goneLogExecutor{})
	stream := &sentLogs{}
	(&Agent{hourlyLogs: newHourlyBudget(0)}).handleLogPull(context.Background(), &logChannelClient{stream: stream}, logs, "pull-1", streamReq())
	sent := stream.sent()
	if len(sent) != 2 || !sent[1].GetEof() {
		t.Fatalf("the pull sent %+v, want its error then its end", sent)
	}
	got := sent[0]
	if codes.Code(got.GetCode()) != codes.NotFound {
		t.Fatalf("the pull ended with code %v (%q), want NotFound", codes.Code(got.GetCode()), got.GetError())
	}
	if got.GetError() != "this job's log is no longer kept: the machine keeps the logs of its last 20 jobs" {
		t.Fatalf("the pull ended with %q, want the reason alone, without the agent's paths or gRPC's words", got.GetError())
	}
}
