// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
)

func httpService(ports ...int32) *v1.ServiceSpec {
	spec := &v1.ServiceSpec{}
	for i, p := range ports {
		name := "http"
		if i > 0 {
			name = "http" + string(rune('0'+i))
		}
		spec.Ports = append(spec.Ports, v1.ServicePort{Name: name, Port: p, Protocol: v1.ProtocolHTTP})
	}
	return spec
}

// resolver answers every port as open, with the env and addresses given.
func resolver(template string, env map[string]string, publicIP, peer string) *endpointResolver {
	return &endpointResolver{
		template:   template,
		getenv:     func(name string) string { return env[name] },
		publicIPv4: func(context.Context) string { return publicIP },
		peerAddr:   func() string { return peer },
		dial:       func(context.Context, int32) error { return nil },
	}
}

func TestEndpointTemplateMustMakeAnAbsoluteURL(t *testing.T) {
	for _, template := range []string{"{port}", "{env:HOST};scope=Nowhere"} {
		r := resolver(template, map[string]string{"HOST": "h"}, "", "")
		if endpoints, problem := r.Resolve(context.Background(), httpService(8000)); endpoints != nil || !strings.HasPrefix(problem, EndpointUnresolved) {
			t.Fatalf("%q: Resolve = %v, %q", template, endpoints, problem)
		}
	}
}

// Every port or none: a Service half ready is not ready.
func TestEndpointsWaitForEveryPort(t *testing.T) {
	r := resolver("", nil, "198.51.100.4", "")
	r.dial = func(_ context.Context, port int32) error {
		if port == 9000 {
			return errors.New("connection refused")
		}
		return nil
	}
	endpoints, problem := r.Resolve(context.Background(), httpService(8000, 9000))
	if endpoints != nil || !strings.HasPrefix(problem, EndpointWaitingForPort+": port 9000") {
		t.Fatalf("Resolve = %v, %q", endpoints, problem)
	}
}

func TestDialLocalSeesAListeningPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := int32(ln.Addr().(*net.TCPAddr).Port)
	if err := dialLocal(context.Background(), port); err != nil {
		t.Fatalf("dial a listening port: %v", err)
	}
	ln.Close()
	if err := dialLocal(context.Background(), port); err == nil {
		t.Fatal("a closed port must not read as open")
	}
}

// A systemd unit outlives the agent: its Service's ports are read back.
func TestServicesSurviveAnAgentRestart(t *testing.T) {
	dir := t.TempDir()
	if err := newServices(dir).Put("zenthros-job-1.service", httpService(8000)); err != nil {
		t.Fatal(err)
	}
	restarted := newServices(dir)
	got := restarted.Get("zenthros-job-1.service")
	if got == nil || got.Ports[0].Port != 8000 {
		t.Fatalf("Get after restart = %+v", got)
	}
	restarted.Delete("zenthros-job-1.service")
	if newServices(dir).Get("zenthros-job-1.service") != nil {
		t.Fatal("a deleted Service must not come back")
	}
}

func TestWithEndpointsOnlyForARunningService(t *testing.T) {
	a := &Agent{services: newServices(""), endpoints: resolver("", nil, "198.51.100.4", "")}
	ctx := context.Background()
	if state := a.withEndpoints(ctx, "plain", backend.JobState{Phase: v1.PhaseRunning}); state.Endpoints != nil || state.EndpointProblem != "" {
		t.Fatalf("a job that is no Service got %+v", state)
	}
	_ = a.services.Put("svc", httpService(8000))
	if state := a.withEndpoints(ctx, "svc", backend.JobState{Phase: v1.PhasePending}); state.Endpoints != nil {
		t.Fatalf("a Service that does not run got %+v", state.Endpoints)
	}
	if state := a.withEndpoints(ctx, "svc", backend.JobState{Phase: v1.PhaseRunning}); len(state.Endpoints) != 1 {
		t.Fatalf("a running Service got %+v", state)
	}
	// What the executor published itself (a Manifest's endpointFrom) stands.
	own := []v1.Endpoint{{Name: "http", URL: "https://model.example"}}
	if state := a.withEndpoints(ctx, "svc", backend.JobState{Phase: v1.PhaseRunning, Endpoints: own}); state.Endpoints[0].URL != own[0].URL {
		t.Fatalf("executor's endpoints replaced: %+v", state.Endpoints)
	}
}

// A port that opens is pushed although the phase stays Running: that is
// when the Service becomes reachable.
func TestStatusWatcherPushesWhenEndpointsChange(t *testing.T) {
	exec := newMockExecutor()
	tracker := newJobTracker()
	stream := newRecordingStream()
	exec.setPhase("handle-1", v1.PhaseRunning)
	tracker.Add("job-1", "handle-1")

	open := make(chan struct{})
	watcher := newStatusWatcher(exec, tracker, newStreamSender(stream), "vm-test", logr.Discard())
	watcher.interval = 20 * time.Millisecond
	watcher.decorate = func(_ context.Context, _ string, state backend.JobState) backend.JobState {
		select {
		case <-open:
			state.Endpoints = []v1.Endpoint{{Name: "http", Port: 8000, URL: "http://198.51.100.4:8000", Scope: v1.EndpointScopePublic}}
		default:
			state.EndpointProblem = EndpointWaitingForPort + ": port 8000 (http) does not accept connections yet"
		}
		return state
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watcher.Run(ctx)

	waitFor := func(n int) []*agentpb.JobStatus {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if updates := stream.statusUpdates(); len(updates) >= n {
				out := make([]*agentpb.JobStatus, 0, len(updates))
				for _, u := range updates {
					out = append(out, u.GetStatus())
				}
				return out
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %d status updates", n)
		return nil
	}
	first := waitFor(1)[0]
	if first.GetPhase() != string(v1.PhaseRunning) || len(first.GetEndpoints()) != 0 || !strings.HasPrefix(first.GetEndpointProblem(), EndpointWaitingForPort) {
		t.Fatalf("first push = %v", first)
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(stream.statusUpdates()); n != 1 {
		t.Fatalf("%d pushes while nothing changed, want 1", n)
	}
	close(open)
	second := waitFor(2)[1]
	if second.GetPhase() != string(v1.PhaseRunning) || len(second.GetEndpoints()) != 1 || second.GetEndpoints()[0].GetUrl() != "http://198.51.100.4:8000" {
		t.Fatalf("second push = %v", second)
	}
}

// Submit remembers a Service's ports, Status publishes its endpoints, and
// Cancel forgets them: a later job on the same handle is no Service.
func TestAgentPublishesAServiceFromSubmitToCancel(t *testing.T) {
	exec := newMockExecutor()
	a := NewWithExecutor(Config{BackendID: "vm-test", BackendType: "vm", ServiceDir: t.TempDir()}, exec, logr.Discard())
	a.endpoints = resolver("", nil, "198.51.100.4", "")
	spec := jobspec.JobSpec{ID: "uid-svc", Name: "svc", Namespace: "tenant-default", Kind: v1.WorkloadKindJob,
		Command: []string{"python3", "-m", "http.server"}, Service: httpService(8000)}
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	resp := a.Submit(ctx, &agentpb.SubmitRequest{RequestId: "r", Job: job})
	if resp.GetError() != "" {
		t.Fatal(resp.GetError())
	}
	handle := resp.GetHandleName()
	exec.setPhase(handle, v1.PhaseRunning)
	status := a.Status(ctx, &agentpb.StatusRequest{Id: "uid-svc", HandleName: handle}).GetStatus()
	if len(status.GetEndpoints()) != 1 || status.GetEndpoints()[0].GetUrl() != "http://198.51.100.4:8000" {
		t.Fatalf("status endpoints = %v", status.GetEndpoints())
	}
	a.Cancel(ctx, &agentpb.CancelRequest{Id: "uid-svc", HandleName: handle})
	exec.setPhase(handle, v1.PhaseRunning)
	if got := a.Status(ctx, &agentpb.StatusRequest{Id: "uid-svc", HandleName: handle}).GetStatus().GetEndpoints(); len(got) != 0 {
		t.Fatalf("endpoints after cancel = %v", got)
	}
}
