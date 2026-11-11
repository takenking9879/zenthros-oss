// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/agent/agepayload"
	"github.com/zenthros/operator/agent/awssecret"
	v1 "github.com/zenthros/operator/api/v1"
	ctrlage "github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/owner"
	"github.com/zenthros/operator/pkg/prewarm/cache"
	"github.com/zenthros/operator/pkg/systemd"
)

// mismatchRunner returns a pre-existing unit file (different spec) for the Install
// idempotency read, driving SystemdExecutor.Submit into the spec-mismatch path.
type mismatchRunner struct{ unitContent string }

func (r mismatchRunner) Run(_ context.Context, cmd string) (string, error) {
	if strings.Contains(cmd, "cat ") {
		return r.unitContent, nil
	}
	return "", nil
}

// TestSystemdExecutorSubmitMapsSpecMismatch ensures the VM execution path surfaces
// a resubmit-with-changed-spec as backend.ErrSpecMismatch, so the controller maps
// it to a terminal spec-conflict exactly like the k8s backend does.
func TestSystemdExecutorSubmitMapsSpecMismatch(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	spec := jobspec.JobSpec{
		ID:         "uid-x",
		Name:       "job",
		Namespace:  "tenant-default",
		Kind:       v1.WorkloadKindJob,
		Entrypoint: "echo a",
		Image:      "img:latest",
	}

	other := spec
	other.Entrypoint = "echo b"
	runner := mismatchRunner{unitContent: systemd.RenderUnit(other, nil, cfg, "")}

	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}
	if _, err := exec.Submit(context.Background(), spec, nil); !errors.Is(err, backend.ErrSpecMismatch) {
		t.Fatalf("err = %v, want backend.ErrSpecMismatch", err)
	}
}

// A machine with a warm cache gives each job ZENTHROS_CACHE_DIR, and the
// unit's spec hash covers it; a resubmit of the same spec (the controller
// re-drives, the agent restarted) must hash the same env and adopt the unit.
// E2E TestResubmitAfterCheckpointAckAdopts, TestExitZeroWithoutCompleteFails
// and TestJobRunsOnHostWithoutImage on Floci, where machines have a cache.
func TestSystemdExecutorResubmitWithCacheAdopts(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	spec := vmTestSpec()
	spec.Runtime = "systemd"
	store := &cache.Manager{Root: t.TempDir()}
	installed := systemd.RenderUnit(spec, map[string]string{jobspec.EnvCacheDir: store.PathsDir()}, cfg, "")
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, mismatchRunner{unitContent: installed}, logr.Discard()), Cache: store}
	if _, err := exec.Submit(context.Background(), spec, nil); errors.Is(err, backend.ErrSpecMismatch) {
		t.Fatalf("resubmit on a machine with a cache: %v", err)
	}
}

// The VM runs Job only; a Ray kind reaching it is refused naming what it runs,
// before anything touches the host.
func TestSystemdExecutorRunsJobOnly(t *testing.T) {
	for _, kind := range []string{v1.WorkloadKindRayJob, v1.WorkloadKindRayCluster, v1.WorkloadKindSparkApplication} {
		spec := vmTestSpec()
		spec.Kind = kind
		exec := SystemdExecutor{}
		_, err := exec.Submit(context.Background(), spec, nil)
		if !errors.Is(err, backend.ErrUnsupportedKind) || !strings.Contains(err.Error(), "it runs Job") {
			t.Fatalf("%s: err = %v, want ErrUnsupportedKind naming Job", kind, err)
		}
	}
	spec := vmTestSpec()
	spec.Runtime = "kubernetes"
	if _, err := (SystemdExecutor{}).Submit(context.Background(), spec, nil); !errors.Is(err, backend.ErrUnsupportedBackend) {
		t.Fatalf("kubernetes runtime on a VM: err = %v", err)
	}
}

// recordingRunner remembers every command and serves canned unit-file content for
// `cat`, so Cancel tests can observe the cleanup sequence.
type recordingRunner struct {
	commands    []string
	unitContent string
	// markerExists controls the retain-marker probe (`test -e`); false fails it.
	markerExists bool
}

func (r *recordingRunner) Run(_ context.Context, cmd string) (string, error) {
	r.commands = append(r.commands, cmd)
	if strings.Contains(cmd, "test -e") {
		if r.markerExists {
			return "", nil
		}
		return "", errors.New("test: file does not exist")
	}
	if strings.Contains(cmd, "cat ") {
		if r.unitContent == "" {
			return "", errors.New("cat: no such file or directory")
		}
		return r.unitContent, nil
	}
	return "", nil
}

func (r *recordingRunner) ran(substr string) bool {
	for _, c := range r.commands {
		if strings.Contains(c, substr) {
			return true
		}
	}
	return false
}

func vmTestSpec() jobspec.JobSpec {
	return jobspec.JobSpec{
		ID:         "uid-vm-1",
		Name:       "vm-job",
		Namespace:  "tenant-default",
		Kind:       v1.WorkloadKindJob,
		Entrypoint: "echo hi",
		Image:      "img:latest",
	}
}

func containerdSpec() jobspec.JobSpec {
	spec := vmTestSpec()
	spec.Runtime = v1.RuntimeContainerd
	return spec
}

// The runtime comes from the spec the controller resolved. A host that was
// installed with containerd as its default still runs a systemd job as a
// plain process: runtime-default only feeds the declaration.
func TestSystemdExecutorIgnoresTheContainerdDefaultWhenRuntimeSystemd(t *testing.T) {
	dir := t.TempDir()
	old := stateDir
	stateDir = dir
	t.Cleanup(func() { stateDir = old })
	if got := declaredDefaultRuntime(); got != v1.RuntimeSystemd {
		t.Fatalf("default with no runtime-default = %q, want systemd", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "runtime-default"), []byte("containerd\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, runtime := range []string{v1.RuntimeSystemd, ""} {
		cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
		runner := &recordingRunner{}
		exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}
		spec := vmTestSpec()
		spec.Image = ""
		spec.Runtime = runtime
		if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
			t.Fatalf("runtime %q: Submit: %v", runtime, err)
		}
		for _, cmd := range runner.commands {
			if strings.Contains(cmd, "ctr ") || strings.Contains(cmd, "ctr-run") {
				t.Fatalf("runtime %q ran a container command: %s", runtime, cmd)
			}
		}
	}
	if got := declaredDefaultRuntime(); got != v1.RuntimeContainerd {
		t.Fatalf("declared default = %q, want runtime-default's containerd", got)
	}
}

// A container runtime without an image is refused before anything is
// installed, with the field the user must set.
func TestSystemdExecutorContainerdNeedsImage(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &recordingRunner{}
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}
	spec := containerdSpec()
	spec.Image = ""
	_, err := exec.Submit(context.Background(), spec, nil)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), "runtime containerd requires workload.template.image") {
		t.Fatalf("Submit = %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("ran %v before refusing", runner.commands)
	}
}

// TestSystemdExecutorCancelRemovesWorkdir covers the VM half of Phase L: cleaning
// up a finished job removes the unit AND its work directory.
func TestSystemdExecutorCancelRemovesWorkdir(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &recordingRunner{unitContent: systemd.RenderUnit(vmTestSpec(), nil, cfg, "")}
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}

	if err := exec.Cancel(context.Background(), systemd.UnitName(vmTestSpec().ID), owner.Ref{}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !runner.ran("disable --now") {
		t.Error("Cancel must uninstall the unit")
	}
	if !runner.ran(`rm -rf "/var/lib/zenthros/jobs/uid-vm-1"`) {
		t.Errorf("Cancel must remove the work dir; ran %v", runner.commands)
	}
}

// TestSystemdExecutorCancelHonorsRetainWorkdir proves the GC flag shipped in the
// unit file keeps the work directory (debugging opt-in).
func TestSystemdExecutorCancelHonorsRetainWorkdir(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	spec := vmTestSpec()
	spec.RetainWorkdir = true
	runner := &recordingRunner{unitContent: systemd.RenderUnit(spec, nil, cfg, "")}
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}

	if err := exec.Cancel(context.Background(), systemd.UnitName(spec.ID), owner.Ref{}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !runner.ran("disable --now") {
		t.Error("Cancel must uninstall the unit even when retaining the workdir")
	}
	if runner.ran("rm -rf") {
		t.Errorf("Cancel must keep the work dir when the retain flag is set; ran %v", runner.commands)
	}
}

// TestSystemdExecutorRepeatCancelHonorsRetainWorkdir covers the double-Cancel race:
// the GC retention pass cancels first (removing the unit but keeping the workdir),
// then the finalizer cancels again at TTL. With the unit gone the second Cancel
// must recover the retain decision from the marker inside the job dir instead of
// deleting the retained workdir.
func TestSystemdExecutorRepeatCancelHonorsRetainWorkdir(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &recordingRunner{unitContent: "", markerExists: true} // unit already uninstalled
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}

	if err := exec.Cancel(context.Background(), systemd.UnitName(vmTestSpec().ID), owner.Ref{}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if runner.ran("rm -rf") {
		t.Errorf("repeat Cancel must keep the retained work dir; ran %v", runner.commands)
	}
}

func TestSystemdExecutorMaterializesAWSSecretWithoutPuttingValueInUnit(t *testing.T) {
	const plaintext = "executor-must-not-embed-this"
	root := t.TempDir()
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &recordingRunner{}
	spec := vmTestSpec()
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeAWSSecretsManager,
		AWSSecretsManager: &v1.AWSSecretsManagerRef{
			SecretID: "workload/token", Region: "us-east-2", JSONKey: "token",
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T14_SECRET"},
	}}
	exec := SystemdExecutor{
		Manager: systemd.NewManager(cfg, runner, logr.Discard()),
		Secrets: awssecret.Config{
			Root:     root,
			Identity: func(context.Context) (bool, error) { return true, nil },
			Factory: func(context.Context, string) (awssecret.Getter, error) {
				return getterStub(func(context.Context, awssecret.GetRequest) (string, error) {
					return `{"token":"` + plaintext + `"}`, nil
				}), nil
			},
		},
	}
	if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	if strings.Contains(joined, plaintext) {
		t.Fatal("systemd commands contained plaintext secret")
	}
	if err := exec.Cancel(context.Background(), systemd.UnitName(spec.ID), owner.Ref{}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, spec.ID)); !os.IsNotExist(err) {
		t.Fatal("secret directory remained after Cancel")
	}
}

func TestSystemdExecutorMaterializesEncryptedPayloadWithoutPuttingValueInUnit(t *testing.T) {
	const plaintext = "executor-must-not-embed-this"
	root := t.TempDir()
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	runner := &recordingRunner{}
	bundle, err := agepayload.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := ctrlage.Encrypt([]byte(plaintext), bundle.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	spec := vmTestSpec()
	spec.SecretSources = []v1.SecretSourceRef{{
		Type: v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{
			Ciphertext: ciphertext, Recipients: bundle.Recipients(),
			KeyID: ctrlage.KeyID(bundle.Recipients()[0]),
		},
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T15_SECRET"},
	}}
	exec := SystemdExecutor{
		Manager: systemd.NewManager(cfg, runner, logr.Discard()),
		Age:     agepayload.Config{Root: root, Bundle: bundle},
	}
	if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	if strings.Contains(joined, plaintext) {
		t.Fatal("systemd commands contained plaintext secret")
	}
	if err := exec.Cancel(context.Background(), systemd.UnitName(spec.ID), owner.Ref{}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, spec.ID)); !os.IsNotExist(err) {
		t.Fatal("secret directory remained after Cancel")
	}
}

type getterStub func(context.Context, awssecret.GetRequest) (string, error)

func (f getterStub) Get(ctx context.Context, req awssecret.GetRequest) (string, error) {
	return f(ctx, req)
}

// A re-run is its own unit: attempt n installs zenthros-<id>-a<n>, never the
// failed attempt's, and the process reads which run it is.
func TestAttemptUnitNameDistinct(t *testing.T) {
	names := map[int32]string{}
	for _, attempt := range []int32{0, 1, 2} {
		cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
		runner := &recordingRunner{}
		exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard())}
		spec := vmTestSpec()
		spec.Image = ""
		spec.Runtime = v1.RuntimeSystemd
		spec.Attempt = attempt
		name, err := exec.Submit(context.Background(), spec, map[string]string{"A": "b"})
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		names[attempt] = name
		env := writtenTeeBody(t, runner.commands, ".service.env")
		if !strings.Contains(env, fmt.Sprintf("ZENTHROS_ATTEMPT=\"%d\"\n", attempt)) {
			t.Fatalf("attempt %d: the job's environment does not set ZENTHROS_ATTEMPT:\n%s", attempt, env)
		}
	}
	if names[0] != systemd.UnitName(vmTestSpec().ID) || names[1] != systemd.UnitName(vmTestSpec().ID+"-a1") || names[1] == names[2] {
		t.Fatalf("unit names %v", names)
	}
	spec, env := attemptScoped(vmTestSpec(), map[string]string{"A": "b"})
	if spec.ID != vmTestSpec().ID || env[EnvAttempt] != "0" || env["A"] != "b" {
		t.Fatalf("first attempt scoped to %q env %v", spec.ID, env)
	}
}

// installedUnits decodes the unit files the executor wrote (printf '<b64>' |
// base64 -d | tee).
func installedUnits(commands []string) string {
	var out strings.Builder
	for _, cmd := range commands {
		rest, ok := strings.CutPrefix(cmd, "printf '%s' '")
		if !ok {
			continue
		}
		encoded, _, _ := strings.Cut(rest, "'")
		if raw, err := base64.StdEncoding.DecodeString(encoded); err == nil {
			out.Write(raw)
		}
	}
	return out.String()
}
