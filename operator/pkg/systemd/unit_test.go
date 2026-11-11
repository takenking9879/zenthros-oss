// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"encoding/base64"
	"strings"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

func gitSourceSpec() jobspec.JobSpec {
	spec := testSpec()
	spec.Source = v1.Source{
		Git:          "git@github.com:o/r.git",
		GitRef:       "master",
		GitSecretRef: "zenthros-git-ssh",
	}
	return spec
}

func TestSpecHashStableForIdenticalSpecs(t *testing.T) {
	if a, b := specHash(gitSourceSpec(), nil), specHash(gitSourceSpec(), nil); a != b {
		t.Errorf("identical specs hashed differently: %s vs %s", a, b)
	}
}

func TestSpecHashChangesWithGitRef(t *testing.T) {
	base := gitSourceSpec()
	changed := gitSourceSpec()
	changed.Source.GitRef = "main"
	if specHash(base, nil) == specHash(changed, nil) {
		t.Error("spec hash must change when gitRef changes")
	}
}

func TestSpecHashChangesWithGitSecretRef(t *testing.T) {
	base := gitSourceSpec()
	changed := gitSourceSpec()
	changed.Source.GitSecretRef = "other-secret"
	if specHash(base, nil) == specHash(changed, nil) {
		t.Error("spec hash must change when gitSecretRef changes")
	}
}

func TestSpecHashChangesWithInline(t *testing.T) {
	base := testSpec()
	base.Source = v1.Source{Inline: "abc123"}
	changed := testSpec()
	changed.Source = v1.Source{Inline: "different"}
	if specHash(base, nil) == specHash(changed, nil) {
		t.Error("spec hash must change when inline content changes")
	}
}

func TestSpecHashDiffersBetweenGitAndInline(t *testing.T) {
	git := testSpec()
	git.Source = v1.Source{Git: "https://github.com/o/r.git"}
	inline := testSpec()
	inline.Source = v1.Source{Inline: "abc123"}
	if specHash(git, nil) == specHash(inline, nil) {
		t.Error("spec hash must differ between git and inline sources")
	}
}

func TestExecStartRoundTripsMultilineEntrypoint(t *testing.T) {
	entrypoint := "echo \"out\"\necho \"err\" >&2\nexit 1"
	line := execStartShell(entrypoint)

	if strings.Contains(line, "\n") {
		t.Errorf("ExecStart must occupy a single unit line, got %q", line)
	}

	// The literal '%' is systemd-escaped to '%%' so the unit file survives systemd's
	// specifier expansion (see execStart); the marker therefore carries the doubled '%'.
	const marker = "printf %%s "
	idx := strings.Index(line, marker)
	if idx < 0 {
		t.Fatalf("ExecStart missing encoded payload: %q", line)
	}
	rest := line[idx+len(marker):]
	payload := rest[:strings.Index(rest, " ")]
	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if string(decoded) != entrypoint {
		t.Errorf("round-trip entrypoint = %q, want %q", decoded, entrypoint)
	}
}

func TestRenderUnitRunsJobOnceWithoutStartLimit(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := testSpec()
	spec.BackoffLimit = 0
	unit := renderUnit(spec, nil, cfg, "", "", nil)
	for _, want := range []string{"Restart=no\n", "StartLimitIntervalSec=0\n"} {
		if !strings.Contains(unit, want) {
			t.Errorf("unit missing %q:\n%s", want, unit)
		}
	}
	for _, banned := range []string{"Restart=on-failure", "RestartSec=", "StartLimitBurst="} {
		if strings.Contains(unit, banned) {
			t.Errorf("one-shot job must not contain %q:\n%s", banned, unit)
		}
	}
}

func TestRenderUnitWaitsForContainerdWhenRuntimeImageSet(t *testing.T) {
	cfg := Config{Scope: ScopeSystem, RuntimeImage: "example.com/runtime:1"}
	unit := renderUnit(testSpec(), nil, cfg, "", "", nil)
	if !strings.Contains(unit, "After=network-online.target containerd.service") {
		t.Fatalf("job unit must wait for containerd:\n%s", unit)
	}
}

func TestRenderUnitStructuredCommandRunsWithoutShell(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := testSpec()
	spec.Entrypoint = ""
	spec.Command = []string{"python", "train.py"}
	spec.Args = []string{"--epochs", "10"}

	unit := renderUnit(spec, nil, cfg, "", "", nil)

	want := `ExecStart=/usr/bin/env "python" "train.py" "--epochs" "10"`
	if !strings.Contains(unit, want) {
		t.Errorf("unit missing %q:\n%s", want, unit)
	}
	// The point of the structured spec: no shell in between.
	for _, banned := range []string{"/bin/bash", "base64"} {
		if strings.Contains(unit, banned) {
			t.Errorf("structured ExecStart must not involve %q:\n%s", banned, unit)
		}
	}
}

// systemd resolves a bare ExecStart name on its own compile-time path, not
// the unit's PATH: `python3 prep.py` ran /usr/bin/python3 on a stock Ubuntu
// machine whose python toolchain (with the SDK) is /opt/zenthros/bin/python3,
// first on the PATH every unit gets. A bare name goes through env(1), which
// uses that PATH; a path, or a name env would misread, is left as it is.
func TestRenderUnitBareCommandIsFoundOnTheUnitPath(t *testing.T) {
	for command, want := range map[string]string{
		"python3":          `ExecStart=/usr/bin/env "python3" "prep.py"`,
		"/usr/bin/python3": `ExecStart="/usr/bin/python3" "prep.py"`,
		"./run.sh":         `ExecStart="./run.sh" "prep.py"`,
		"bin/tool":         `ExecStart="bin/tool" "prep.py"`,
		"-weird":           `ExecStart="-weird" "prep.py"`,
		"NAME=value":       `ExecStart="NAME=value" "prep.py"`,
	} {
		spec := testSpec()
		spec.Entrypoint = ""
		spec.Command = []string{command}
		spec.Args = []string{"prep.py"}
		unit := renderUnit(spec, nil, Config{Scope: ScopeSystem}, "", "", nil)
		if !strings.Contains(unit, want+"\n") {
			t.Errorf("command %q: unit missing %q:\n%s", command, want, unit)
		}
	}
	// The wrapper (the sandbox runner) still comes first; env runs inside it.
	spec := testSpec()
	spec.Entrypoint = ""
	spec.Command = []string{"python3"}
	if line := execStart(spec, "/usr/lib/zenthros/wrap"); line != `/bin/sh "/usr/lib/zenthros/wrap" /usr/bin/env "python3"` {
		t.Errorf("wrapped ExecStart = %s", line)
	}
}

func TestRenderUnitStructuredCommandEscapesSpecials(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := testSpec()
	spec.Entrypoint = ""
	spec.Command = []string{"/usr/bin/run"}
	spec.Args = []string{"100% done", `say "hi"`, `back\slash`}

	unit := renderUnit(spec, nil, cfg, "", "", nil)

	// '%' doubled (systemd specifier), quotes and backslashes escaped for systemd's
	// double-quote parser.
	want := `ExecStart="/usr/bin/run" "100%% done" "say \"hi\"" "back\\slash"`
	if !strings.Contains(unit, want) {
		t.Errorf("unit missing %q:\n%s", want, unit)
	}
}

func TestRenderUnitWorkingDirOverridesPreparedDir(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := testSpec()
	spec.WorkingDir = "/data/run"

	unit := renderUnit(spec, nil, cfg, "/var/lib/zenthros/jobs/uid-123/work", "", nil)

	if !strings.Contains(unit, "WorkingDirectory=/data/run\n") {
		t.Errorf("explicit workingDir must win over the prepared dir:\n%s", unit)
	}
}

func TestSpecHashChangesWithStructuredExecution(t *testing.T) {
	base := testSpec()
	base.Entrypoint = ""
	base.Command = []string{"python", "train.py"}

	cases := []struct {
		name   string
		change func(*jobspec.JobSpec)
	}{
		{"command", func(s *jobspec.JobSpec) { s.Command = []string{"python3", "train.py"} }},
		{"args", func(s *jobspec.JobSpec) { s.Args = []string{"--fast"} }},
		{"workingDir", func(s *jobspec.JobSpec) { s.WorkingDir = "/data" }},
	}
	for _, tc := range cases {
		changed := base
		tc.change(&changed)
		if specHash(base, nil) == specHash(changed, nil) {
			t.Errorf("spec hash must change when %s changes", tc.name)
		}
	}
}

func TestRenderUnitCarriesRetainWorkdirFlag(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := testSpec()
	if parseRetainWorkdir(renderUnit(spec, nil, cfg, "", "", nil)) {
		t.Error("flag must be absent when RetainWorkdir is false")
	}
	spec.RetainWorkdir = true
	if !parseRetainWorkdir(renderUnit(spec, nil, cfg, "", "", nil)) {
		t.Error("flag must be present when RetainWorkdir is true")
	}
}

func TestJobIDFromUnitRoundTrips(t *testing.T) {
	cases := []struct {
		jobID string
		want  string
	}{
		{"abc-123", "abc-123"}, // typical k8s UID shape
		{"f47ac10b-58cc-4372-a567-0e02b2c3d479", "f47ac10b-58cc-4372-a567-0e02b2c3d479"},
	}
	for _, tc := range cases {
		unit := UnitName(tc.jobID)
		if got := JobIDFromUnit(unit); got != tc.want {
			t.Errorf("JobIDFromUnit(%q) = %q, want %q", unit, got, tc.want)
		}
	}
	for _, foreign := range []string{"other.service", "zenthros-.service", ""} {
		if got := JobIDFromUnit(foreign); got != "" {
			t.Errorf("JobIDFromUnit(%q) = %q, want empty", foreign, got)
		}
	}
}

func TestSpecHashChangesWithAWSSecretReference(t *testing.T) {
	base := testSpec()
	base.SecretSources = []v1.SecretSourceRef{{
		Type:              v1.SecretSourceTypeAWSSecretsManager,
		AWSSecretsManager: &v1.AWSSecretsManagerRef{SecretID: "workload/token", Region: "us-east-2"},
		Materialization:   v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T14_SECRET"},
	}}
	changed := testSpec()
	changed.SecretSources = []v1.SecretSourceRef{{
		Type:              v1.SecretSourceTypeAWSSecretsManager,
		AWSSecretsManager: &v1.AWSSecretsManagerRef{SecretID: "workload/other", Region: "us-east-2"},
		Materialization:   v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T14_SECRET"},
	}}
	if specHash(base, nil) == specHash(changed, nil) {
		t.Error("spec hash must change when the AWS secret reference changes")
	}
}

func TestRenderUnitWrapsExecStartWithoutSecretValue(t *testing.T) {
	cfg := Config{Scope: ScopeSystem}
	spec := testSpec()
	spec.Command = []string{"python", "-c"}
	spec.Args = []string{"print('ok')"}
	unit := renderUnit(spec, nil, cfg, "", "/dev/shm/zenthros/secrets/job/wrapper", nil)
	if !strings.Contains(unit, `ExecStart=/bin/sh "/dev/shm/zenthros/secrets/job/wrapper" /usr/bin/env "python" "-c" "print('ok')"`) {
		t.Fatalf("wrapped ExecStart missing:\n%s", unit)
	}
	if strings.Contains(unit, "s3cret") {
		t.Fatal("unit file contained a secret value")
	}
}

// Which run this is does not change what runs: ZENTHROS_ATTEMPT stays out of
// the spec hash, so a unit installed before an upgrade is still adopted.
// What the control plane sets per run (attempt, generation, report token,
// the acked checkpoint cursor) never changes the hash: a resubmit after a
// checkpoint ack adopts the running unit instead of failing SPEC_MISMATCH.
// The user's env and the checkpoint prefix do.
func TestSpecHashExcludesControllerDerivedEnv(t *testing.T) {
	spec := jobspec.JobSpec{ID: "j", Entrypoint: "echo hi"}
	user := map[string]string{"A": "b", "ZENTHROS_CHECKPOINT_PREFIX": "s3://b/p"}
	base := specHash(spec, user)
	derived := map[string]string{
		"ZENTHROS_ATTEMPT": "2", "ZENTHROS_GENERATION": "3", "ZENTHROS_REPORT_TOKEN": "t",
		"ZENTHROS_CHECKPOINT_URI": "s3://b/p/jobs/j/gen/1/train/4", "ZENTHROS_CHECKPOINT_PHASE": "train",
		"ZENTHROS_CHECKPOINT_SEQ": "4", "ZENTHROS_CHECKPOINT_SHA256": "h", "ZENTHROS_CHECKPOINT_COMPLETE": "false",
		"ZENTHROS_CHECKPOINT_CURSORS": `{"train":{"seq":4}}`,
	}
	for name, value := range derived {
		env := map[string]string{name: value}
		for k, v := range user {
			env[k] = v
		}
		if got := specHash(spec, env); got != base {
			t.Errorf("%s changed the spec hash", name)
		}
	}
	if specHash(spec, map[string]string{"A": "c", "ZENTHROS_CHECKPOINT_PREFIX": "s3://b/p"}) == base {
		t.Fatal("a user env change did not change the spec hash")
	}
	if specHash(spec, map[string]string{"A": "b", "ZENTHROS_CHECKPOINT_PREFIX": "s3://b/other"}) == base {
		t.Fatal("a checkpoint prefix change did not change the spec hash")
	}
}
