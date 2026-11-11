// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/distribution"
	"github.com/zenthros/operator/pkg/jobspec"
)

// LocalWorkspace is the Workspace of a job with no hub: the agent's own.
const LocalWorkspace = "local"

// LocalNamespace is where a local Kubernetes agent runs its jobs.
const LocalNamespace = "zenthros-local"

// LocalJob is what `local submit` says of what it started, and what every
// later verb takes back: the agent keeps no list of its own, the SDK does.
type LocalJob struct {
	ID string `json:"id"`
	// Tasks are the job's processes: one, or one per rank of a map on a
	// backend that has no native form of it. Rank is -1 on a job with no
	// tasks of its own.
	Tasks []LocalTask `json:"tasks"`
	// Count is the map's size, 0 on any other job.
	Count int32 `json:"count,omitempty"`
	// FailurePolicy is the map's: ContinueOnFailure keeps the other tasks going
	// past one that fails.
	FailurePolicy string `json:"failurePolicy,omitempty"`
}

// LocalTask is one process of a job.
type LocalTask struct {
	Rank   int32  `json:"rank"`
	ID     string `json:"id"`
	Handle string `json:"handle"`
}

// LocalStatus is the job as the hub would state it: the fields the SDK reads
// of a ZenthrosJob's status.
type LocalStatus struct {
	Phase           v1.Phase           `json:"phase"`
	Reason          string             `json:"reason,omitempty"`
	Message         string             `json:"message,omitempty"`
	StartTime       *metav1.Time       `json:"startTime,omitempty"`
	CompletionTime  *metav1.Time       `json:"completionTime,omitempty"`
	ExitCode        *int32             `json:"exitCode,omitempty"`
	ResolvedImage   string             `json:"resolvedImage,omitempty"`
	Endpoints       []v1.Endpoint      `json:"endpoints,omitempty"`
	EndpointProblem string             `json:"endpointProblem,omitempty"`
	Conditions      []metav1.Condition `json:"conditions,omitempty"`
	Reasons         []v1.Reason        `json:"reasons,omitempty"`
	Progress        *v1.JobProgress    `json:"progress,omitempty"`
}

// LocalSpecs are the job specs a ZenthrosJob becomes with no controller:
// the same FromZenthrosJob, the same data references, a map split into its
// ranks by the same names a controller gives them where the backend has no
// native form of it. Everything the controller derives from what it stored
// comes from job.status, which the SDK fills from what it waited for.
func LocalSpecs(job *v1.ZenthrosJob, backendType string) ([]jobspec.JobSpec, error) {
	if job.JobID() == "" {
		return nil, errors.New("a local job needs its id: metadata.uid")
	}
	namespace := job.Namespace
	if namespace == "" {
		namespace = LocalNamespace
	}
	spec := jobspec.FromZenthrosJob(job, backend.TypeAgent, namespace)
	spec.TargetBackend = LocalWorkspace
	spec.Workspace, spec.WorkspaceUID, spec.Tenant = LocalWorkspace, LocalWorkspace, LocalWorkspace
	spec.Env = backend.WithDataReferences(spec.Env, spec.Artifacts)
	switch backendType {
	case backend.TypeContainer:
		spec.Runtime = v1.RuntimeProcess
	default:
		spec.Runtime = ""
	}
	dist := job.Spec.Workload.Template.Distribution
	if dist == nil || dist.Count <= 1 || backendType != backend.TypeContainer {
		return []jobspec.JobSpec{spec}, nil
	}
	specs := make([]jobspec.JobSpec, 0, dist.Count)
	for rank := int32(0); rank < dist.Count; rank++ {
		task := spec
		task.Distribution = nil
		task.ID = distribution.MemberName(spec.ID, rank)
		task.Env = make(map[string]string, len(spec.Env)+2)
		for name, value := range spec.Env {
			task.Env[name] = value
		}
		for name, value := range distribution.RankEnv(dist, rank) {
			task.Env[name] = value
		}
		specs = append(specs, task)
	}
	return specs, nil
}

// RunLocalCLI is `zenthros-agent local <verb>`: the client of the socket.
// It prints JSON, exits 0, or says why on stderr and exits 1 (2 for usage).
func RunLocalCLI(ctx context.Context, args []string, socket string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: zenthros-agent local info|submit|status|cancel|logs|reports|progress|forget")
		return 2
	}
	client := localClient{socket: socket}
	err := client.run(ctx, args[0], args[1:], stdin, stdout)
	if err == nil {
		return 0
	}
	if errors.Is(err, errLocalUsage) {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprintln(stderr, err)
	return 1
}

var errLocalUsage = errors.New("usage")

type localClient struct {
	socket string
	// wait is how long to retry a socket that is not there yet.
	wait time.Duration
}

func (c localClient) run(ctx context.Context, verb string, args []string, stdin io.Reader, stdout io.Writer) error {
	switch verb {
	case "info":
		flags := flag.NewFlagSet("info", flag.ContinueOnError)
		wait := flags.Duration("wait", 0, "how long to wait for the agent")
		if err := flags.Parse(args); err != nil {
			return fmt.Errorf("%w: %w", errLocalUsage, err)
		}
		c.wait = *wait
		return c.relay(ctx, localRequest{Info: true}, stdout)
	case "submit":
		return c.submit(ctx, stdin, stdout)
	case "status":
		return c.status(ctx, stdin, stdout)
	case "cancel":
		return c.cancel(ctx, stdin, stdout)
	case "logs":
		return c.logs(ctx, args, stdout)
	case "reports":
		id, err := oneArg(verb, args)
		if err != nil {
			return err
		}
		return c.relay(ctx, localRequest{Reports: id}, stdout)
	case "progress":
		id, err := oneArg(verb, args)
		if err != nil {
			return err
		}
		return c.relay(ctx, localRequest{Progress: id}, stdout)
	case "forget":
		id, err := oneArg(verb, args)
		if err != nil {
			return err
		}
		return c.relay(ctx, localRequest{Forget: id}, stdout)
	default:
		return fmt.Errorf("%w: no verb %q", errLocalUsage, verb)
	}
}

func oneArg(verb string, args []string) (string, error) {
	if len(args) != 1 || args[0] == "" {
		return "", fmt.Errorf("%w: zenthros-agent local %s JOB-ID", errLocalUsage, verb)
	}
	return args[0], nil
}

// submit reads the ZenthrosJob the SDK built, turns it into its specs and
// gives each to Agent.Submit. A task refused after others started cancels
// them: a job starts whole or not at all.
func (c localClient) submit(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	decoder := json.NewDecoder(stdin)
	decoder.DisallowUnknownFields()
	var job v1.ZenthrosJob
	if err := decoder.Decode(&job); err != nil {
		return fmt.Errorf("the job does not read: %w", err)
	}
	var info localInfo
	if err := c.ask(ctx, localRequest{Info: true}, &info); err != nil {
		return err
	}
	specs, err := LocalSpecs(&job, info.BackendType)
	if err != nil {
		return err
	}
	out := LocalJob{ID: job.JobID()}
	if dist := job.Spec.Workload.Template.Distribution; dist != nil && len(specs) > 1 {
		out.Count, out.FailurePolicy = dist.Count, dist.FailurePolicy
	}
	for rank, spec := range specs {
		wire, err := envelope.Encode(spec)
		if err != nil {
			return err
		}
		var answer agentpb.RegisterRequest
		if err := c.command(ctx, &agentpb.RegisterResponse{Submit: &agentpb.SubmitRequest{BackendId: LocalWorkspace, Job: wire}}, &answer); err != nil {
			c.cancelTasks(ctx, out)
			return err
		}
		resp := answer.GetSubmitResponse()
		if resp.GetError() != "" {
			c.cancelTasks(ctx, out)
			return errors.New(resp.GetError())
		}
		task := LocalTask{Rank: -1, ID: spec.ID, Handle: resp.GetHandleName()}
		if out.Count > 0 {
			task.Rank = int32(rank)
		}
		out.Tasks = append(out.Tasks, task)
	}
	if seconds, _ := job.Wall(); seconds > 0 {
		var set map[string]bool
		if err := c.ask(ctx, localRequest{StopAfter: &localStopAfter{Seconds: seconds, Tasks: out.Tasks}}, &set); err != nil {
			c.cancelTasks(ctx, out)
			return err
		}
	}
	return json.NewEncoder(stdout).Encode(out)
}

func (c localClient) cancelTasks(ctx context.Context, job LocalJob) {
	for _, task := range job.Tasks {
		var answer agentpb.RegisterRequest
		_ = c.command(ctx, &agentpb.RegisterResponse{Cancel: &agentpb.CancelRequest{BackendId: LocalWorkspace, Id: task.ID, HandleName: task.Handle}}, &answer)
	}
}

func readLocalJob(stdin io.Reader) (LocalJob, error) {
	var job LocalJob
	decoder := json.NewDecoder(stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&job); err != nil {
		return job, fmt.Errorf("the job does not read: %w", err)
	}
	return job, nil
}

// status asks Agent.Status for each task and adds them up as the controller
// does (distribution.Aggregate).
func (c localClient) status(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	job, err := readLocalJob(stdin)
	if err != nil {
		return err
	}
	states := make([]*agentpb.JobStatus, len(job.Tasks))
	for i, task := range job.Tasks {
		var answer agentpb.RegisterRequest
		if err := c.command(ctx, &agentpb.RegisterResponse{Status: &agentpb.StatusRequest{BackendId: LocalWorkspace, Id: task.ID, HandleName: task.Handle}}, &answer); err != nil {
			return err
		}
		resp := answer.GetStatusResponse()
		if resp.GetError() != "" {
			states[i] = &agentpb.JobStatus{Phase: string(v1.PhaseFailed), Message: resp.GetError(),
				Reasons: []*agentpb.Reason{{Code: v1.ReasonWorkerLost, Detail: resp.GetError()}}}
			continue
		}
		states[i] = resp.GetStatus()
	}
	out := localStatusOf(job, states)
	var progress v1.JobProgress
	if len(job.Tasks) == 1 {
		if err := c.ask(ctx, localRequest{Progress: job.Tasks[0].ID}, &progress); err != nil {
			return err
		}
		if progress.URI != "" {
			out.Progress = &progress
		}
	}
	return json.NewEncoder(stdout).Encode(out)
}

func localStatusOf(job LocalJob, states []*agentpb.JobStatus) LocalStatus {
	if job.Count == 0 && len(states) == 1 {
		return localStatusOne(states[0])
	}
	dist := &v1.Distribution{Count: job.Count, FailurePolicy: job.FailurePolicy}
	members := make([]distribution.Member, len(states))
	var out LocalStatus
	for i, state := range states {
		one := localStatusOne(state)
		members[i] = distribution.Member{Rank: job.Tasks[i].Rank, Present: true, Phase: one.Phase, Reason: one.Reason, Message: one.Message}
		if one.StartTime != nil && (out.StartTime == nil || one.StartTime.Before(out.StartTime)) {
			out.StartTime = one.StartTime
		}
		if one.CompletionTime != nil && (out.CompletionTime == nil || out.CompletionTime.Before(one.CompletionTime)) {
			out.CompletionTime = one.CompletionTime
		}
	}
	outcome := distribution.Aggregate(dist, members)
	out.Phase, out.Reason, out.Message = outcome.Phase, outcome.Reason, outcome.Message
	if !out.Phase.Terminal() {
		out.CompletionTime = nil
	}
	return out
}

func localStatusOne(state *agentpb.JobStatus) LocalStatus {
	out := LocalStatus{
		Phase:           v1.Phase(state.GetPhase()),
		Message:         state.GetMessage(),
		ExitCode:        state.ExitCode,
		ResolvedImage:   state.GetResolvedImage(),
		EndpointProblem: state.GetEndpointProblem(),
	}
	for _, reason := range state.GetReasons() {
		out.Reasons = append(out.Reasons, v1.Reason{Code: reason.GetCode(), Detail: reason.GetDetail()})
	}
	if len(out.Reasons) > 0 {
		out.Reason = out.Reasons[0].Code
	}
	if state.GetEndpointProblem() != "" || len(state.GetEndpoints()) > 0 {
		out.Conditions = []metav1.Condition{serviceReady(state)}
	}
	for _, e := range state.GetEndpoints() {
		out.Endpoints = append(out.Endpoints, v1.Endpoint{Name: e.GetName(), Port: e.GetPort(), URL: e.GetUrl(), Scope: e.GetScope()})
	}
	if unix := state.GetStartTimeUnix(); unix > 0 {
		at := metav1.Unix(unix, 0)
		out.StartTime = &at
	}
	if unix := state.GetCompletionTimeUnix(); unix > 0 {
		at := metav1.Unix(unix, 0)
		out.CompletionTime = &at
	}
	return out
}

// serviceReady is the condition a controller keeps for a Service.
func serviceReady(state *agentpb.JobStatus) metav1.Condition {
	if problem := state.GetEndpointProblem(); problem != "" {
		return metav1.Condition{Type: v1.ConditionServiceReady, Status: metav1.ConditionFalse, Reason: "WaitingForPort", Message: problem}
	}
	return metav1.Condition{Type: v1.ConditionServiceReady, Status: metav1.ConditionTrue, Reason: "EndpointsPublished"}
}

func cancelOf(task LocalTask) *agentpb.CancelRequest {
	return &agentpb.CancelRequest{BackendId: LocalWorkspace, Id: task.ID, HandleName: task.Handle}
}

func (c localClient) cancel(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	job, err := readLocalJob(stdin)
	if err != nil {
		return err
	}
	for _, task := range job.Tasks {
		var answer agentpb.RegisterRequest
		if err := c.command(ctx, &agentpb.RegisterResponse{Cancel: &agentpb.CancelRequest{BackendId: LocalWorkspace, Id: task.ID, HandleName: task.Handle}}, &answer); err != nil {
			return err
		}
		if resp := answer.GetCancelResponse(); resp.GetError() != "" {
			return errors.New(resp.GetError())
		}
	}
	return json.NewEncoder(stdout).Encode(map[string]bool{"cancelled": true})
}

func (c localClient) logs(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("logs", flag.ContinueOnError)
	tail := flags.Int("tail", -1, "the last lines; negative is the whole log")
	follow := flags.Bool("follow", false, "keep reading until the job ends")
	if err := flags.Parse(args); err != nil || flags.NArg() != 1 {
		return fmt.Errorf("%w: zenthros-agent local logs [--tail N] [--follow] HANDLE", errLocalUsage)
	}
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := writeRequest(conn, localRequest{Logs: &localLogs{Handle: flags.Arg(0), Tail: *tail, Follow: *follow}}); err != nil {
		return err
	}
	decoder := json.NewDecoder(bufio.NewReader(conn))
	for {
		var line localLine
		if err := decoder.Decode(&line); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read the log: %w", err)
		}
		if line.Error != "" {
			return errors.New(line.Error)
		}
		if _, err := io.WriteString(stdout, line.Data); err != nil {
			return err
		}
	}
}

// relay prints the answer's value as it came.
func (c localClient) relay(ctx context.Context, req localRequest, stdout io.Writer) error {
	var value json.RawMessage
	if err := c.ask(ctx, req, &value); err != nil {
		return err
	}
	_, err := stdout.Write(append(value, '\n'))
	return err
}

// command is one gateway command through Dispatch.
func (c localClient) command(ctx context.Context, command *agentpb.RegisterResponse, answer *agentpb.RegisterRequest) error {
	raw, err := protojson.Marshal(command)
	if err != nil {
		return err
	}
	var value json.RawMessage
	if err := c.ask(ctx, localRequest{Command: raw}, &value); err != nil {
		return err
	}
	return protojson.Unmarshal(value, answer)
}

// ask sends req and reads the one-line answer into value.
func (c localClient) ask(ctx context.Context, req localRequest, value any) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := writeRequest(conn, req); err != nil {
		return err
	}
	line, err := readLocalLine(bufio.NewReaderSize(conn, 64<<10))
	if err != nil {
		return fmt.Errorf("the agent did not answer: %w", err)
	}
	var answer struct {
		OK    json.RawMessage `json:"ok"`
		Error string          `json:"error"`
	}
	if err := json.Unmarshal(line, &answer); err != nil {
		return fmt.Errorf("the agent's answer does not read: %w", err)
	}
	if answer.Error != "" {
		return errors.New(answer.Error)
	}
	if len(answer.OK) == 0 || string(answer.OK) == "null" {
		if raw, ok := value.(*json.RawMessage); ok {
			*raw = json.RawMessage("null")
		}
		return nil
	}
	return json.Unmarshal(answer.OK, value)
}

func writeRequest(conn net.Conn, req localRequest) error {
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("the agent went: %w", err)
	}
	return nil
}

func (c localClient) dial(ctx context.Context) (net.Conn, error) {
	deadline := time.Now().Add(c.wait)
	var dialer net.Dialer
	for {
		conn, err := dialer.DialContext(ctx, "unix", c.socket)
		if err == nil {
			return conn, nil
		}
		if time.Now().After(deadline) {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
				return nil, fmt.Errorf("no local agent at %s: %w", c.socket, err)
			}
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
