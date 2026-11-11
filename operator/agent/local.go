// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/checkpoint"
	"github.com/zenthros/operator/pkg/executionreport"
)

// The local mode: the agent with no hub. It enrols with nobody, dials
// nobody and opens no port. It takes the same commands through the same
// Dispatch as a gateway's, one per connection on a Unix socket under its state
// directory, whoever reaches the socket being whoever already controls the
// container (`docker exec`) or the namespace (`kubectl exec`): the channel
// adds no authority. What the controller does for a job (admission, acks of
// checkpoints, retries) is either not there or is decided here by the same
// rule (checkpoint.Advance).

// EnvLocal, set, makes the agent local. It never enrols, whatever else is set.
const EnvLocal = "ZENTHROS_LOCAL"

// On a person's own cluster: the folder its nodes share, and the uid:gid a
// job's processes run as (the chart's agent.local).
const (
	EnvLocalShared = "ZENTHROS_LOCAL_SHARED"
	EnvLocalUser   = "ZENTHROS_LOCAL_USER"
)

// LocalSharedMark is the file the SDK writes in the shared folder. The agent
// says what it reads there: the same words is the only proof the cluster's
// nodes have that folder, and not an empty one of the same name.
const LocalSharedMark = ".zenthros-shared"

// LocalOptions is how a local agent serves.
type LocalOptions struct {
	// Socket is the path of the command socket.
	Socket string
	// StateDir keeps what the controller would keep: the acked checkpoints.
	StateDir string
	// IdleExit ends the agent after this long with no work and no command;
	// zero never does. A stopped container is reused by the next command.
	IdleExit time.Duration
	// Busy says whether a job still runs; nil is never.
	Busy func() bool
	// Shared is the folder the nodes of a person's own cluster share.
	Shared string
}

// localRequest is one command on the socket: exactly one field.
type localRequest struct {
	// Command is an agentpb.RegisterResponse as protojson: what a gateway
	// sends, handled by Dispatch.
	Command json.RawMessage `json:"command,omitempty"`
	// What the hub would have answered, since only the agent has it here.
	Logs     *localLogs `json:"logs,omitempty"`
	Reports  string     `json:"reports,omitempty"`
	Progress string     `json:"progress,omitempty"`
	Forget   string     `json:"forget,omitempty"`
	Info     bool       `json:"info,omitempty"`
	// StopAfter keeps a job's time limit, which no controller watches here.
	StopAfter *localStopAfter `json:"stopAfter,omitempty"`
}

type localStopAfter struct {
	Seconds int64       `json:"seconds"`
	Tasks   []LocalTask `json:"tasks"`
}

type localLogs struct {
	Handle string `json:"handle"`
	Follow bool   `json:"follow,omitempty"`
	// Tail is the last lines to start from; negative is the whole log.
	Tail int `json:"tail"`
}

// localLine is one line of a logs answer.
type localLine struct {
	Data  string `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

// localInfo is what an SDK asks of an agent it just reached.
type localInfo struct {
	Version       string   `json:"version"`
	BackendType   string   `json:"backendType"`
	AgeRecipients []string `json:"ageRecipients,omitempty"`
	SharedMark    string   `json:"sharedMark,omitempty"`
}

// readSharedMark is what the shared folder's mark says here: none with no
// folder, or on a node that does not have it.
func readSharedMark(folder string) string {
	if folder == "" {
		return ""
	}
	mark, err := os.ReadFile(filepath.Join(folder, LocalSharedMark))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(mark))
}

// maxLocalLine bounds a command: a job's own limit, its inline code, plus the
// JSON around it.
const maxLocalLine = 16 << 20

// RunLocal serves the socket until ctx ends or the agent has been idle for
// IdleExit. A job's reports stay in the spool for the SDK; its checkpoints are
// acked by Advance and kept under StateDir.
func (a *Agent) RunLocal(ctx context.Context, opts LocalOptions) error {
	a.rebuildTracker(ctx)
	a.commit = (&localProgress{dir: filepath.Join(opts.StateDir, "progress")}).Commit
	a.spawnRefused = "a job's children need a hub: run this on a machine of your Workspace, or use zenthros.map here"
	a.serveReports(ctx)
	go a.noticeOnSignal(ctx)
	listener, err := listenLocal(opts.Socket)
	if err != nil {
		return err
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	idle := newLocalIdle(opts, stop)
	go idle.watch(ctx)
	a.logger.Info("local agent listening", "socket", opts.Socket)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("local socket: %w", err)
		}
		idle.touch()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			a.serveLocal(ctx, conn, opts)
			idle.touch()
		}()
	}
}

// listenLocal listens on socket, only its owner reaching it: the directory
// is the owner's alone before the socket exists, so there is no moment when
// another user may connect.
func listenLocal(socket string) (net.Listener, error) {
	dir := filepath.Dir(socket)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("local socket directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("local socket directory: %w", err)
	}
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, fmt.Errorf("local socket: %w", err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("local socket: %w", err)
	}
	return listener, nil
}

// noticeOnSignal tells the jobs when the container is told to stop: on a
// hub's stream the notice watcher does.
func (a *Agent) noticeOnSignal(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-a.interruptCh:
		if notice := a.interrupted.Load(); notice != nil {
			a.deliverNotice(ctx, *notice)
		}
	}
}

// serveLocal answers one request. Only a logs answer takes more than one line.
func (a *Agent) serveLocal(ctx context.Context, conn net.Conn, opts LocalOptions) {
	reader := bufio.NewReaderSize(conn, 64<<10)
	line, err := readLocalLine(reader)
	if err != nil {
		a.answerLocal(conn, nil, fmt.Errorf("read the command: %w", err))
		return
	}
	var req localRequest
	if err := json.Unmarshal(line, &req); err != nil {
		a.answerLocal(conn, nil, fmt.Errorf("the command is not JSON: %w", err))
		return
	}
	switch {
	case len(req.Command) > 0:
		var command agentpb.RegisterResponse
		if err := protojson.Unmarshal(req.Command, &command); err != nil {
			a.answerLocal(conn, nil, fmt.Errorf("the command does not read: %w", err))
			return
		}
		answer := a.Dispatch(ctx, &command)
		if answer == nil {
			answer = &agentpb.RegisterRequest{}
		}
		raw, err := protojson.Marshal(answer)
		if err != nil {
			a.answerLocal(conn, nil, err)
			return
		}
		a.answerLocal(conn, json.RawMessage(raw), nil)
	case req.Logs != nil:
		a.serveLocalLogs(ctx, conn, *req.Logs)
	case req.Reports != "":
		a.answerReports(ctx, conn, req.Reports)
	case req.Progress != "":
		progress, err := (&localProgress{dir: filepath.Join(opts.StateDir, "progress")}).read(req.Progress)
		a.answerLocal(conn, progress, err)
	case req.Forget != "":
		a.answerLocal(conn, map[string]bool{"forgotten": true}, a.forgetLocal(ctx, req.Forget, opts))
	case req.StopAfter != nil:
		a.stopAfter(ctx, *req.StopAfter)
		a.answerLocal(conn, map[string]bool{"set": true}, nil)
	case req.Info:
		a.answerLocal(conn, localInfo{Version: a.cfg.Version, BackendType: a.cfg.BackendType, AgeRecipients: a.cfg.ageRecipients(), SharedMark: readSharedMark(opts.Shared)}, nil)
	default:
		a.answerLocal(conn, nil, errors.New("an empty command"))
	}
}

// stopAfter cancels the tasks once their time is up. It lives as long as the
// agent, as they do: a container's jobs never outlive it.
func (a *Agent) stopAfter(ctx context.Context, stop localStopAfter) {
	timer := time.NewTimer(time.Duration(stop.Seconds) * time.Second)
	go func() {
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
			for _, task := range stop.Tasks {
				a.Dispatch(ctx, &agentpb.RegisterResponse{Cancel: cancelOf(task)})
			}
		}
	}()
}

// readLocalLine reads one line of at most maxLocalLine.
func readLocalLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > maxLocalLine {
			return nil, fmt.Errorf("a command of more than %d bytes", maxLocalLine)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil && len(line) == 0 {
			return nil, err
		}
		return line, nil
	}
}

// answerLocal writes the one line of an answer: {"ok": value} or {"error": why}.
func (a *Agent) answerLocal(conn net.Conn, value any, err error) {
	out := map[string]any{"ok": value}
	if err != nil {
		out = map[string]any{"error": err.Error()}
	}
	raw, marshalErr := json.Marshal(out)
	if marshalErr != nil {
		raw = []byte(`{"error":"the answer does not encode"}`)
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		a.logger.V(1).Info("the local command went before its answer", "error", err.Error())
	}
}

func (a *Agent) serveLocalLogs(ctx context.Context, conn net.Conn, req localLogs) {
	// A follower that goes (its docker exec killed) ends the stream, which
	// nothing else would until the job ends.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer cancel()
		_, _ = conn.Read(make([]byte, 1))
	}()
	encoder := json.NewEncoder(conn)
	chunks, err := a.executor.Logs(ctx, req.Handle, req.Follow, req.Tail)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return // no log: nothing to read, as a job that wrote none
		}
		_ = encoder.Encode(localLine{Error: err.Error()})
		return
	}
	for chunk := range chunks {
		if err := encoder.Encode(localLine{Data: string(chunk.Data)}); err != nil {
			cancel()
			for range chunks {
			}
			return
		}
	}
}

func (a *Agent) answerReports(ctx context.Context, conn net.Conn, jobID string) {
	if a.spool == nil {
		a.answerLocal(conn, []executionreport.Snapshot{}, nil)
		return
	}
	reports, err := a.spool.Reports(ctx, jobID)
	if err == nil && reports == nil {
		reports = []executionreport.Snapshot{}
	}
	a.answerLocal(conn, reports, err)
}

// forgetLocal drops what the agent keeps of a job the SDK has collected.
func (a *Agent) forgetLocal(ctx context.Context, jobID string, opts LocalOptions) error {
	if a.spool != nil {
		if err := a.spool.DropReports(ctx, jobID); err != nil {
			return err
		}
		if err := a.spool.EndJob(ctx, jobID); err != nil {
			return err
		}
	}
	return (&localProgress{dir: filepath.Join(opts.StateDir, "progress")}).forget(jobID)
}

// localIdle ends the agent once nothing has happened for opts.IdleExit.
type localIdle struct {
	opts LocalOptions
	stop func()
	mu   sync.Mutex
	last time.Time
	// tick is how often it looks.
	tick time.Duration
}

func newLocalIdle(opts LocalOptions, stop func()) *localIdle {
	return &localIdle{opts: opts, stop: stop, last: time.Now(), tick: max(opts.IdleExit/10, time.Second)}
}

func (i *localIdle) touch() {
	i.mu.Lock()
	i.last = time.Now()
	i.mu.Unlock()
}

func (i *localIdle) watch(ctx context.Context) {
	if i.opts.IdleExit <= 0 {
		return
	}
	ticker := time.NewTicker(i.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if i.opts.Busy != nil && i.opts.Busy() {
			i.touch()
			continue
		}
		i.mu.Lock()
		quiet := time.Since(i.last)
		i.mu.Unlock()
		if quiet >= i.opts.IdleExit {
			i.stop()
			return
		}
	}
}

// localProgress is the checkpoint cursor a hub would keep, a file per job.
type localProgress struct {
	dir string
	mu  sync.Mutex
}

var localJobIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func (p *localProgress) path(jobID string) (string, error) {
	if !localJobIDPattern.MatchString(jobID) {
		return "", fmt.Errorf("job id %q", jobID)
	}
	return filepath.Join(p.dir, jobID+".json"), nil
}

// Commit acks a job's checkpoint commit by the rule the controller uses.
func (p *localProgress) Commit(_ context.Context, commit *agentpb.CheckpointCommit) (*agentpb.CheckpointAck, error) {
	ack := &agentpb.CheckpointAck{JobId: commit.GetJobId(), Phase: commit.GetPhase(), Seq: commit.GetSeq()}
	asked := checkpoint.Commit{URI: commit.GetUri(), Phase: commit.GetPhase(), Seq: commit.GetSeq(), SHA256: commit.GetSha256(), Complete: commit.GetComplete()}
	if why := asked.Valid(); why != "" {
		ack.Error, ack.ErrorCode = why, executionreport.CodeRejected
		return ack, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, err := p.read(commit.GetJobId())
	if err != nil {
		return nil, err
	}
	next, dirty, refused := checkpoint.Advance(current, asked)
	if refused != "" {
		ack.Error, ack.ErrorCode = refused, executionreport.CodeRejected
		return ack, nil
	}
	if dirty {
		if err := p.write(commit.GetJobId(), next); err != nil {
			return nil, err
		}
	}
	ack.Accepted = true
	return ack, nil
}

func (p *localProgress) read(jobID string) (*v1.JobProgress, error) {
	path, err := p.path(jobID)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var progress v1.JobProgress
	if err := json.Unmarshal(raw, &progress); err != nil {
		return nil, fmt.Errorf("the cursor of %s: %w", jobID, err)
	}
	return &progress, nil
}

// write replaces the cursor whole: a crash leaves the old one or the new one.
func (p *localProgress) write(jobID string, progress *v1.JobProgress) error {
	path, err := p.path(jobID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(p.dir, 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(p.dir, ".cursor-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func (p *localProgress) forget(jobID string) error {
	path, err := p.path(jobID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
