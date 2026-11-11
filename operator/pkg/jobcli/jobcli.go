// SPDX-License-Identifier: Apache-2.0

// Package jobcli is `zenthros-agent job ...`: the job side of the agent's
// report, checkpoint and interruption-notice protocol, for programs in any
// language. It sends the same bytes the Python SDK sends (a test compares
// both against one fixture), so a shell script and a Python trainer are the
// same to the agent. docs/interfaces/job-protocol.md describes the wire.
//
// Exit codes: 0 done; 1 refused or failed (the agent's words on stderr);
// 2 usage; 3 the checkpoint cursor is frozen because a replacement was
// already submitted, so this run must stop, not retry.
package jobcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/checkpoint"
	"github.com/zenthros/operator/pkg/executionreport"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/jobspec"
)

// Env the agent gives every job, named where the agent sets it.
const (
	EnvReportEndpoint = jobspec.EnvReportEndpoint
	EnvReportToken    = jobspec.EnvReportToken
	EnvJobID          = jobspec.EnvJobID
	EnvPreemptFile    = jobspec.EnvPreemptFile
)

// Env the job's author may set.
const (
	// EnvProducerID names who sends the job's reports; the job's
	// hostname when unset.
	EnvProducerID = "ZENTHROS_PRODUCER_ID"
	// EnvStateDir is where the CLI keeps what one run already said (the
	// report payload, the last seq per phase) between invocations.
	EnvStateDir = "ZENTHROS_JOB_STATE_DIR"
)

const (
	ExitOK     = 0
	ExitFailed = 1
	ExitUsage  = 2
	ExitFrozen = 3
)

// CLI is one invocation. Zero fields take the process's own.
type CLI struct {
	Env    func(string) string
	Stdout io.Writer
	Stderr io.Writer
	Now    func() time.Time
	NewID  func() string
	// Attempts and Backoff bound retries on a transient answer (connection
	// refused, 502/503/504). Zero is the SDK's schedule (retryWait); a
	// Backoff waits that long before every new try.
	Attempts int
	Backoff  time.Duration
	// sleep waits between tries; nil waits on the clock.
	sleep func(ctx context.Context, wait time.Duration) error
}

// A job insists with its agent for as long as the controller waits for the
// machine itself (liveness.DefaultJobGrace, 45 s): an agent restarting or a
// hub that answers 502/503/504 for less than that costs the job nothing. Ten
// tries 2 s apart, to be back at once after a blip, and five more 5.4 s
// apart: the fifteenth leaves 45 s after the first. The SDK waits the same
// (sdk/python/zenthros/core/agent.py); testdata/wire.json holds the table
// both are tested against.
const (
	retryAttempts      = 15
	retryQuickAttempts = 10
	retryQuickWait     = 2 * time.Second
	retrySlowWait      = 5400 * time.Millisecond
)

// retryWait is the wait before try number attempt, counted from zero.
func retryWait(attempt int) time.Duration {
	if attempt < retryQuickAttempts {
		return retryQuickWait
	}
	return retrySlowWait
}

func (c *CLI) defaults() {
	if c.Env == nil {
		c.Env = os.Getenv
	}
	if c.Stdout == nil {
		c.Stdout = os.Stdout
	}
	if c.Stderr == nil {
		c.Stderr = os.Stderr
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.NewID == nil {
		c.NewID = uuid.NewString
	}
	if c.Attempts <= 0 {
		c.Attempts = retryAttempts
	}
	if c.sleep == nil {
		c.sleep = func(ctx context.Context, wait time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
				return nil
			}
		}
	}
}

const usage = `usage: zenthros-agent job <command>

  report set KEY=VALUE... [--final] [--artifact URI[=NAME]]...
      Publish the whole report: what this run set before, plus these keys.
      A VALUE that parses as JSON is sent as JSON; anything else as a string.
  checkpoint next --phase P
      Print {"seq","uri"} of the next checkpoint of phase P.
  checkpoint commit --phase P --file F [--uri U] [--seq N] [--complete]
      Ask the controller to take F (already at U) as phase P's cursor.
      Without --uri, F is copied to the next URI under a local
      ZENTHROS_CHECKPOINT_PREFIX; an object-store prefix needs --uri.
  checkpoint cursor --phase P
      Print phase P's acked cursor {"seq","uri","sha256","complete"};
      exit 1 when there is none.
  preempted
      Exit 0 and print the notice when this machine is being taken back;
      exit 1 when it is not.
  task
      Print the job's task env (TASK_INDEX, TASK_COUNT, ...) as KEY=VALUE lines.
  spawn --name G --count N (--same | --cmd ARG...) [--env K=V]... [--wait] [-o json]
      Create N child jobs <job>-G-<i>, each with TASK_INDEX=i of TASK_COUNT=N.
      They inherit this job's image, runtime, compute, target and placement;
      --cmd (last: everything after it is the argv) or --same says what they
      run. Spawning G again creates nothing twice. --wait returns when all
      have ended and fails, with each failed child's words, if one failed.
  children [--name G] [--wait] [-o json]
      Print the children of G (or of every group) and their phases.
`

// Run executes args (without "job") and returns the exit code.
func (c *CLI) Run(ctx context.Context, args []string) int {
	c.defaults()
	if len(args) == 0 {
		fmt.Fprint(c.Stderr, usage)
		return ExitUsage
	}
	switch args[0] {
	case "report":
		if len(args) < 2 || args[1] != "set" {
			fmt.Fprint(c.Stderr, usage)
			return ExitUsage
		}
		return c.reportSet(ctx, args[2:])
	case "checkpoint":
		if len(args) < 2 {
			fmt.Fprint(c.Stderr, usage)
			return ExitUsage
		}
		switch args[1] {
		case "next":
			return c.checkpointNext(args[2:])
		case "commit":
			return c.checkpointCommit(ctx, args[2:])
		case "cursor":
			return c.checkpointCursor(args[2:])
		}
	case "preempted":
		return c.preempted()
	case "task":
		return c.task()
	case "spawn":
		return c.spawn(ctx, args[1:])
	case "children":
		return c.childrenCmd(ctx, args[1:])
	case "-h", "--help", "help":
		fmt.Fprint(c.Stdout, usage)
		return ExitOK
	}
	fmt.Fprint(c.Stderr, usage)
	return ExitUsage
}

// --- report ---------------------------------------------------------------

type artifactFlags []executionreport.ArtifactRef

func (a *artifactFlags) String() string { return "" }

func (a *artifactFlags) Set(v string) error {
	uri, name, _ := strings.Cut(v, "=")
	if uri == "" {
		return errors.New("--artifact needs a URI")
	}
	*a = append(*a, executionreport.ArtifactRef{URI: uri, Name: name})
	return nil
}

// reportBody is the snapshot exactly as the SDK writes it: every key always
// present, artifacts as [] when there are none.
type reportBody struct {
	APIVersion string                        `json:"apiVersion"`
	Kind       string                        `json:"kind"`
	JobID      string                        `json:"job_id"`
	ProducerID string                        `json:"producer_id"`
	ReportID   string                        `json:"report_id"`
	ProducedAt string                        `json:"produced_at"`
	Payload    map[string]json.RawMessage    `json:"payload"`
	Artifacts  []executionreport.ArtifactRef `json:"artifacts"`
	Final      bool                          `json:"final"`
}

func (c *CLI) reportSet(ctx context.Context, args []string) int {
	fs := c.flags("report set")
	final := fs.Bool("final", false, "the last report of this run")
	var artifacts artifactFlags
	fs.Var(&artifacts, "artifact", "URI[=NAME] of an output (repeatable)")
	pairs, err := parseInterleaved(fs, args)
	if err != nil {
		return c.usageErr(err)
	}
	if len(pairs) == 0 {
		return c.usageErr(errors.New("report set needs at least one KEY=VALUE"))
	}
	st, err := c.loadState()
	if err != nil {
		return c.fail(err)
	}
	if st.Payload == nil {
		st.Payload = map[string]json.RawMessage{}
	}
	for _, p := range pairs {
		key, value, ok := strings.Cut(p, "=")
		if !ok || key == "" {
			return c.usageErr(fmt.Errorf("%q is not KEY=VALUE", p))
		}
		st.Payload[key] = jsonValue(value)
	}
	st.Artifacts = append(st.Artifacts, artifacts...)
	body := reportBody{
		APIVersion: executionreport.APIVersion,
		Kind:       executionreport.Kind,
		JobID:      c.Env(EnvJobID),
		ProducerID: c.producer(),
		ReportID:   c.NewID(),
		ProducedAt: c.Now().UTC().Format("2006-01-02T15:04:05Z"),
		Payload:    st.Payload,
		Artifacts:  st.Artifacts,
		Final:      *final,
	}
	if body.Artifacts == nil {
		body.Artifacts = []executionreport.ArtifactRef{}
	}
	raw, _ := json.Marshal(body)
	if len(raw) > executionreport.MaxPayloadBytes {
		return c.fail(fmt.Errorf("PAYLOAD_TOO_LARGE: payload exceeds %d bytes", executionreport.MaxPayloadBytes))
	}
	reply, err := c.post(ctx, "/v1/reports", raw)
	if err != nil {
		return c.fail(err)
	}
	// The agent took it: remember it, so the next set sends it again.
	if err := c.saveState(st); err != nil {
		return c.fail(err)
	}
	c.Stdout.Write(reply)
	return ExitOK
}

// jsonValue is value as JSON when it is JSON, else as a JSON string.
func jsonValue(value string) json.RawMessage {
	if json.Valid([]byte(value)) {
		var compact bytes.Buffer
		if json.Compact(&compact, []byte(value)) == nil {
			return compact.Bytes()
		}
	}
	quoted, _ := json.Marshal(value)
	return quoted
}

func (c *CLI) producer() string {
	if p := c.Env(EnvProducerID); p != "" {
		return p
	}
	host, _ := os.Hostname()
	return host
}

// --- checkpoint -----------------------------------------------------------

// commitBody is the CheckpointCommit as the SDK writes it.
type commitBody struct {
	JobID      string `json:"job_id"`
	Generation int64  `json:"generation"`
	Phase      string `json:"phase"`
	Seq        int64  `json:"seq"`
	URI        string `json:"uri"`
	SHA256     string `json:"sha256"`
	Complete   bool   `json:"complete"`
}

// Cursor is one phase's acked checkpoint, as ZENTHROS_CHECKPOINT_CURSORS
// carries it.
type Cursor struct {
	Seq      int64  `json:"seq"`
	URI      string `json:"uri"`
	SHA256   string `json:"sha256,omitempty"`
	Complete bool   `json:"complete"`
}

// generation is the job's lease generation (ZENTHROS_GENERATION): its
// checkpoints are written under it and its spawns name it. 1 before the
// first lease, as the controller's LeaseGeneration.
func (c *CLI) generation() int64 {
	g, err := strconv.ParseInt(c.Env(checkpoint.EnvGeneration), 10, 64)
	if err != nil || g <= 0 {
		return 1
	}
	return g
}

// cursor is the acked cursor of phase from env: every phase in
// ZENTHROS_CHECKPOINT_CURSORS, else the latest in ZENTHROS_CHECKPOINT_*.
// This is the SDK's _cursor.
func (c *CLI) cursor(phase string) (Cursor, bool) {
	if raw := c.Env(checkpoint.EnvCursors); raw != "" {
		var all map[string]Cursor
		if json.Unmarshal([]byte(raw), &all) == nil {
			if cur, ok := all[phase]; ok && cur.URI != "" {
				return cur, true
			}
		}
	}
	if c.Env(checkpoint.EnvPhase) == phase && c.Env(checkpoint.EnvURI) != "" {
		seq, _ := strconv.ParseInt(c.Env(checkpoint.EnvSeq), 10, 64)
		return Cursor{
			Seq:      seq,
			URI:      c.Env(checkpoint.EnvURI),
			SHA256:   c.Env(checkpoint.EnvSHA256),
			Complete: c.Env(checkpoint.EnvComplete) == "true",
		}, true
	}
	return Cursor{}, false
}

// nextSeq is one past the highest seq phase has: acked before this run
// (env) or committed by it (state).
func (c *CLI) nextSeq(phase string, st state) int64 {
	var seq int64
	if cur, ok := c.cursor(phase); ok {
		seq = cur.Seq
	}
	if s := st.Seqs[phase]; s > seq {
		seq = s
	}
	return seq + 1
}

func (c *CLI) nextURI(phase string, seq int64) (string, error) {
	prefix := c.Env(checkpoint.EnvPrefix)
	if prefix == "" {
		prefix = c.Env("ZENTHROS_DATA_OUTPUT")
	}
	if prefix == "" {
		return "", errors.New("ZENTHROS_CHECKPOINT_PREFIX is not set")
	}
	jobID := c.Env(EnvJobID)
	if jobID == "" {
		return "", errors.New("ZENTHROS_JOB_ID is not set")
	}
	return checkpoint.URI(prefix, jobID, c.generation(), phase, seq), nil
}

func (c *CLI) checkpointNext(args []string) int {
	fs := c.flags("checkpoint next")
	phase := fs.String("phase", "", "phase name")
	if err := fs.Parse(args); err != nil || *phase == "" {
		return c.usageErr(errors.New("checkpoint next needs --phase"))
	}
	st, err := c.loadState()
	if err != nil {
		return c.fail(err)
	}
	seq := c.nextSeq(*phase, st)
	uri, err := c.nextURI(*phase, seq)
	if err != nil {
		return c.fail(err)
	}
	return c.printJSON(map[string]any{"seq": seq, "uri": uri})
}

func (c *CLI) checkpointCursor(args []string) int {
	fs := c.flags("checkpoint cursor")
	phase := fs.String("phase", "", "phase name")
	if err := fs.Parse(args); err != nil || *phase == "" {
		return c.usageErr(errors.New("checkpoint cursor needs --phase"))
	}
	cur, ok := c.cursor(*phase)
	if !ok {
		fmt.Fprintf(c.Stderr, "phase %s has no acked checkpoint\n", *phase)
		return ExitFailed
	}
	return c.printJSON(cur)
}

func (c *CLI) checkpointCommit(ctx context.Context, args []string) int {
	fs := c.flags("checkpoint commit")
	phase := fs.String("phase", "", "phase name")
	file := fs.String("file", "", "the checkpoint's bytes")
	uri := fs.String("uri", "", "where the bytes are (default: the next URI under a local prefix)")
	seqFlag := fs.Int64("seq", 0, "the seq to commit (default: one past the last)")
	complete := fs.Bool("complete", false, "the phase is done")
	if err := fs.Parse(args); err != nil {
		return c.usageErr(err)
	}
	if *phase == "" || *file == "" {
		return c.usageErr(errors.New("checkpoint commit needs --phase and --file"))
	}
	jobID := c.Env(EnvJobID)
	if jobID == "" {
		return c.fail(errors.New("ZENTHROS_JOB_ID is not set"))
	}
	payload, err := os.ReadFile(*file)
	if err != nil {
		return c.fail(err)
	}
	st, err := c.loadState()
	if err != nil {
		return c.fail(err)
	}
	seq := *seqFlag
	if seq <= 0 {
		seq = c.nextSeq(*phase, st)
	}
	target := *uri
	if target == "" {
		if target, err = c.nextURI(*phase, seq); err != nil {
			return c.fail(err)
		}
		if err := copyLocal(target, payload); err != nil {
			return c.fail(err)
		}
	}
	sum := sha256.Sum256(payload)
	body, _ := json.Marshal(commitBody{
		JobID: jobID, Generation: c.generation(), Phase: *phase, Seq: seq,
		URI: target, SHA256: hex.EncodeToString(sum[:]), Complete: *complete,
	})
	reply, err := c.post(ctx, "/v1/checkpoints", body)
	if err != nil {
		if !refused(err) {
			// No answer is not a refusal: the controller may hold this seq,
			// its ack lost with the link. The seq is spent, as the SDK's
			// is, so the next commit takes the one after and leaves these
			// bytes where the acked cursor may point.
			if st.Seqs == nil {
				st.Seqs = map[string]int64{}
			}
			st.Seqs[*phase] = seq
			_ = c.saveState(st)
		}
		if frozen(err) {
			fmt.Fprintf(c.Stderr, "CHECKPOINT_FROZEN: phase %s seq %d was not acked: %v\n", *phase, seq, err)
			return ExitFrozen
		}
		return c.fail(err)
	}
	if st.Seqs == nil {
		st.Seqs = map[string]int64{}
	}
	st.Seqs[*phase] = seq
	if err := c.saveState(st); err != nil {
		return c.fail(err)
	}
	c.Stdout.Write(reply)
	return ExitOK
}

// frozen is the controller saying a replacement already took its cursor:
// no retry of this commit can land.
func frozen(err error) bool {
	var reply *replyError
	return errors.As(err, &reply) && reply.Code == executionreport.CodeCheckpointFrozen
}

// refused is the controller, or the agent, turning the request down: it
// was answered, and nothing of it was kept. A link that failed, or an agent
// that could not reach the controller, answered nothing.
func refused(err error) bool {
	var reply *replyError
	if !errors.As(err, &reply) {
		return false
	}
	switch reply.Status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return false
	}
	return reply.Status >= 400 && reply.Status < 500
}

// copyLocal writes payload at a file:// URI or a plain path. Object storage
// is the job's own tool's to write; the CLI never holds cloud credentials.
func copyLocal(uri string, payload []byte) error {
	path := uri
	if strings.HasPrefix(uri, "file://") {
		u, err := url.Parse(uri)
		if err != nil {
			return err
		}
		path = u.Path
	} else if strings.Contains(uri, "://") {
		return fmt.Errorf("checkpoint prefix %s is object storage: upload the file there yourself and pass --uri (see: checkpoint next)", uri)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, payload, 0o644)
}

// --- preempted, task --------------------------------------------------------

func (c *CLI) preempted() int {
	path := c.Env(EnvPreemptFile)
	if path == "" {
		return ExitFailed
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return ExitFailed
	}
	c.Stdout.Write(raw)
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		fmt.Fprintln(c.Stdout)
	}
	return ExitOK
}

var taskEnv = []string{
	v1.EnvTaskIndex, v1.EnvTaskCount, EnvJobID, checkpoint.EnvGeneration,
}

func (c *CLI) task() int {
	for _, name := range taskEnv {
		if v := c.Env(name); v != "" {
			fmt.Fprintf(c.Stdout, "%s=%s\n", name, v)
		}
	}
	return ExitOK
}

// --- wire -----------------------------------------------------------------

// replyError is the agent's refusal, word for word.
type replyError struct {
	Status int
	Code   string
	Text   string
}

func (e *replyError) Error() string {
	if e.Code == "" {
		return e.Text
	}
	return e.Code + ": " + e.Text
}

func (c *CLI) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	endpoint := c.Env(EnvReportEndpoint)
	if endpoint == "" {
		return nil, errors.New("ZENTHROS_REPORT_ENDPOINT is not set")
	}
	timeout := 10 * time.Second
	if path == "/v1/children" {
		// The controller answers once every child exists.
		timeout = 90 * time.Second
	}
	httpClient, target := &http.Client{Timeout: timeout}, strings.TrimRight(endpoint, "/")+path
	if sock, ok := strings.CutPrefix(endpoint, "unix://"); ok {
		httpClient.Transport = &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}}
		target = "http://localhost" + path
	}
	var lastErr error
	for attempt := 0; attempt < c.Attempts; attempt++ {
		if attempt > 0 {
			wait := c.Backoff
			if wait <= 0 {
				wait = retryWait(attempt)
			}
			if err := c.sleep(ctx, wait); err != nil {
				return nil, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.Env(EnvReportToken))
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			if fault.Retryable(err) {
				continue
			}
			return nil, err
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if triedAgain(resp.StatusCode) {
			lastErr = decodeReply(resp.StatusCode, raw)
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, decodeReply(resp.StatusCode, raw)
		}
		return raw, nil
	}
	// Only transient answers, or no agent: what was sent may have been
	// kept, and a caller that waits on (waitChildren) asks again.
	return nil, fault.Wrap(fault.Transient, "", lastErr)
}

func decodeReply(status int, raw []byte) error {
	var body struct {
		Code string `json:"error_code"`
		Text string `json:"error"`
	}
	if json.Unmarshal(raw, &body) != nil || body.Text == "" {
		text := strings.TrimSpace(string(raw))
		if text == "" {
			text = fmt.Sprintf("http %d", status)
		}
		return &replyError{Status: status, Text: text}
	}
	return &replyError{Status: status, Code: body.Code, Text: body.Text}
}

// triedAgain reports an answer of the agent a call sends again: a transient
// one (fault.TransientHTTP: 408, 429, any 5xx; a 507 SPOOL_FULL is the spool
// waiting on the controller). Every route may be sent again, so none is a
// create the server may have carried out. A transport error is sent again
// unless the caller gave up (fault.Retryable): whatever its text, the agent
// is restarting or not up yet. The SDK's table is the same
// (testdata/wire.json).
func triedAgain(status int) bool {
	return fault.TransientHTTP(status)
}

// --- state ----------------------------------------------------------------

// state is what this run already told the agent.
type state struct {
	Payload   map[string]json.RawMessage    `json:"payload,omitempty"`
	Artifacts []executionreport.ArtifactRef `json:"artifacts,omitempty"`
	Seqs      map[string]int64              `json:"seqs,omitempty"`
}

// statePath is per job and generation: a replacement starts a new report
// and takes its seqs from the acked cursors.
func (c *CLI) statePath() string {
	dir := c.Env(EnvStateDir)
	if dir == "" {
		dir = os.TempDir()
	}
	job := c.Env(EnvJobID)
	if job == "" {
		job = "local"
	}
	return filepath.Join(dir, fmt.Sprintf("zenthros-job-%s-g%d.json", job, c.generation()))
}

func (c *CLI) loadState() (state, error) {
	var st state
	raw, err := os.ReadFile(c.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return st, fmt.Errorf("%s: %w", c.statePath(), err)
	}
	return st, nil
}

func (c *CLI) saveState(st state) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	path := c.statePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// --- helpers --------------------------------------------------------------

func (c *CLI) flags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	return fs
}

// parseInterleaved lets flags and KEY=VALUE words come in any order.
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var words []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return words, nil
		}
		words = append(words, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func (c *CLI) printJSON(v any) int {
	raw, _ := json.Marshal(v)
	fmt.Fprintln(c.Stdout, string(raw))
	return ExitOK
}

func (c *CLI) usageErr(err error) int {
	fmt.Fprintln(c.Stderr, err)
	return ExitUsage
}

func (c *CLI) fail(err error) int {
	fmt.Fprintln(c.Stderr, err)
	return ExitFailed
}
