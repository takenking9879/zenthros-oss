// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/agent/agepayload"
	"github.com/zenthros/operator/internal/grpc/envelope"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/internal/testutil/jobenv"
	"github.com/zenthros/operator/pkg/prewarm/cache"
	"github.com/zenthros/operator/pkg/reportspool"
	"github.com/zenthros/operator/pkg/systemd"
)

// The agent adds to a job it runs in a container, with a file secret, on a
// machine with a warm cache and interruption notices, exactly the variables
// wire.json's job_env says the agent sets: one renamed here and not in the
// SDK or the job CLI, or dropped, fails without a machine.
func TestTheAgentAddsTheJobEnvItSets(t *testing.T) {
	bundle, err := agepayload.Generate()
	if err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	exec := SystemdExecutor{
		Manager:  systemd.NewManager(systemd.Config{Scope: systemd.ScopeSystem}, runner, logr.Discard()),
		Age:      agepayload.Config{Root: t.TempDir(), Bundle: bundle},
		AgentBin: "/usr/local/bin/zenthros-agent",
		Cache:    &cache.Manager{Root: t.TempDir()},
	}
	a := NewWithExecutor(Config{BackendID: "vm-a", BackendType: "vm", MaxJobs: 1,
		ReportEndpoint: "unix:///var/run/zenthros/agent/reports.sock", PreemptDir: "/var/run/zenthros/preempt"}, exec, logr.Discard())
	spool, err := reportspool.Open(context.Background(), reportspool.Config{Path: filepath.Join(t.TempDir(), "reports.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spool.Close() })
	a.spool = spool

	spec := containerdSpec()
	spec.SecretSources = append(spec.SecretSources, sealedFile(t, bundle, ""))
	job, err := envelope.Encode(spec)
	if err != nil {
		t.Fatal(err)
	}
	if resp := a.Submit(context.Background(), &agentpb.SubmitRequest{RequestId: "r", Job: job}); resp.GetError() != "" {
		t.Fatal(resp.GetError())
	}
	env := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(writtenTeeBody(t, runner.commands, "ctr-env")), "\n") {
		name, value, _ := strings.Cut(line, "=")
		env[name] = value
	}
	if got, want := jobenv.Owned(env), jobenv.Of(t, jobenv.Agent); !slices.Equal(got, want) {
		t.Fatalf("the agent adds %v\nwire.json job_env says it sets %v", got, want)
	}
}
