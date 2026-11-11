// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

func gitSpec() jobspec.JobSpec {
	spec := testSpec()
	spec.Source = v1.Source{Git: "https://github.com/o/r.git", GitRef: "main"}
	return spec
}

func inlineSpec() jobspec.JobSpec {
	spec := testSpec()
	spec.Source = v1.Source{Inline: inlineTarball(map[string]string{"main.py": "print(1)"}, nil)}
	return spec
}

// inlineTarball is a base64 gzipped tarball of files and symlinks, as zctl
// sends inline code.
func inlineTarball(files, links map[string]string) string {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg})
		_, _ = tw.Write([]byte(body))
	}
	for name, target := range links {
		_ = tw.WriteHeader(&tar.Header{Name: name, Linkname: target, Typeflag: tar.TypeSymlink})
	}
	_ = tw.Close()
	_ = gz.Close()
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func TestPrepareSourceGitClone(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: true}

	workDir, err := PrepareSource(context.Background(), runner, gitSpec(), cfg)
	if err != nil {
		t.Fatalf("PrepareSource: %v", err)
	}
	if workDir != "/var/lib/zenthros/jobs/uid-123/work" {
		t.Errorf("workDir = %q", workDir)
	}
	if dir := TmpDir(gitSpec().ID, cfg); dir != "/var/lib/zenthros/jobs/uid-123/tmp" {
		t.Errorf("TmpDir = %q", dir)
	}
	if got := IsolatedJobDir("research", "uid-123", cfg); got != "/var/lib/zenthros/workspaces/research/jobs/uid-123" {
		t.Errorf("IsolatedJobDir = %q", got)
	}
	if got := IsolatedJobDir("", "uid-123", cfg); got != JobDir("uid-123", cfg) {
		t.Errorf("empty workspace IsolatedJobDir = %q", got)
	}
	if !runner.ran("git clone --depth 1") {
		t.Errorf("expected git clone command; ran %v", runner.commands)
	}
	if !runner.ran("--branch 'main'") {
		t.Errorf("expected --branch flag; ran %v", runner.commands)
	}
	if !runner.ran("'https://github.com/o/r.git'") {
		t.Errorf("expected quoted repo URL; ran %v", runner.commands)
	}
}

func TestPrepareSourceGitCloneNoRef(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: false}
	spec := gitSpec()
	spec.Source.GitRef = ""

	_, err := PrepareSource(context.Background(), runner, spec, cfg)
	if err != nil {
		t.Fatalf("PrepareSource: %v", err)
	}
	if runner.ran("--branch") {
		t.Errorf("no ref means no --branch; ran %v", runner.commands)
	}
	if runner.ran("sudo ") {
		t.Errorf("UseSudo=false must not use sudo; ran %v", runner.commands)
	}
}

func TestPrepareSourceInlineWorkspaceUsesIsolatedDir(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: true}
	spec := inlineSpec()
	spec.Workspace = "default"

	workDir, err := PrepareSource(context.Background(), runner, spec, cfg)
	if err != nil {
		t.Fatalf("PrepareSource: %v", err)
	}
	want := "/var/lib/zenthros/workspaces/default/jobs/uid-123/work"
	if workDir != want {
		t.Fatalf("workDir = %q, want %q", workDir, want)
	}
}

func TestPrepareSourceInlineExtract(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: true}

	workDir, err := PrepareSource(context.Background(), runner, inlineSpec(), cfg)
	if err != nil {
		t.Fatalf("PrepareSource: %v", err)
	}
	if workDir != "/var/lib/zenthros/jobs/uid-123/work" {
		t.Errorf("workDir = %q", workDir)
	}
	if !runner.ran("cp -a ") {
		t.Errorf("expected the staged tree copied in; ran %v", runner.commands)
	}
	if runner.ran("tar ") {
		t.Errorf("the job's archive reached tar as root; ran %v", runner.commands)
	}
}

// The job's archive is unpacked confined, as the agent, never by tar run as
// root, which wrote where an entry said: ../x, or a file through a link
// out (19#3). One that tries is refused and nothing is copied in.
func TestAnInlineArchiveThatEscapesIsRefused(t *testing.T) {
	for name, archive := range map[string]string{
		"a path out":         inlineTarball(map[string]string{"../../etc/cron.d/x": "* * * * * root id"}, nil),
		"a link out":         inlineTarball(nil, map[string]string{"etc": "/etc"}),
		"a file through one": inlineTarball(map[string]string{"s/t/x": "x"}, map[string]string{"s": ".", "s/t": ".."}),
	} {
		t.Run(name, func(t *testing.T) {
			runner := &fakeRunner{}
			spec := inlineSpec()
			spec.Source.Inline = archive
			if _, err := PrepareSource(context.Background(), runner, spec, Config{Scope: ScopeSystem, UseSudo: true}); err == nil {
				t.Fatal("an archive escaping its work directory was accepted")
			}
			if runner.ran("cp -a ") || runner.ran("tar ") {
				t.Fatalf("something of it was written as root: %v", runner.commands)
			}
		})
	}
}

func TestPrepareSourceUserScope(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeUser, UseSudo: false}

	workDir, err := PrepareSource(context.Background(), runner, gitSpec(), cfg)
	if err != nil {
		t.Fatalf("PrepareSource: %v", err)
	}
	if workDir != "$HOME/.local/share/zenthros/jobs/uid-123/work" {
		t.Errorf("workDir = %q", workDir)
	}
}

func TestPrepareSourceNoSource(t *testing.T) {
	runner := &fakeRunner{}
	cfg := Config{Scope: ScopeSystem, UseSudo: true}

	workDir, err := PrepareSource(context.Background(), runner, testSpec(), cfg)
	if err != nil {
		t.Fatalf("PrepareSource: %v", err)
	}
	if workDir != "" {
		t.Errorf("expected empty workDir for no source, got %q", workDir)
	}
	if len(runner.commands) != 0 {
		t.Errorf("no commands expected for no source; ran %v", runner.commands)
	}
}

func TestInstallWithGitSourceSetsWorkingDirectory(t *testing.T) {
	runner := &fakeRunner{}
	m := NewManager(Config{Scope: ScopeSystem, UseSudo: true}, runner, logr.Discard())

	name, err := m.Install(context.Background(), gitSpec(), nil)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if name != "zenthros-uid-123.service" {
		t.Errorf("unit name = %q", name)
	}
	if !runner.ran("git clone --depth 1") {
		t.Errorf("expected git clone before unit install; ran %v", runner.commands)
	}
}

// Inline source never rides the command line: bash gets the command as one
// argument, which Linux caps at 128 KiB, while the controller admits 512
// KiB of inline code. At that size the extract failed with "argument list
// too long" on every machine.
func TestInlineSourceNeverRidesTheCommandLine(t *testing.T) {
	runner := &fakeRunner{}
	spec := inlineSpec()
	spec.Source.Inline = inlineTarball(map[string]string{"big.bin": strings.Repeat("A", 512<<10)}, nil)

	if _, err := PrepareSource(context.Background(), runner, spec, Config{Scope: ScopeSystem, UseSudo: true}); err != nil {
		t.Fatalf("PrepareSource: %v", err)
	}
	for _, cmd := range runner.commands {
		if len(cmd) > 4096 {
			t.Fatalf("a %d-byte command line carries the source", len(cmd))
		}
	}
}

// A git URL carrying a token keeps it out of a failed clone's error: the
// command line the runner quotes names it.
func TestAFailedGitCloneQuotesNoToken(t *testing.T) {
	spec := gitSpec()
	spec.Source.Git = "https://user:ghp-secret-token@github.com/o/r.git"
	runner := RunnerFunc(func(_ context.Context, cmd string) (string, error) {
		if strings.Contains(cmd, "git clone") {
			return "", &ExitError{Command: cmd, Stderr: "fatal: could not read from " + spec.Source.Git, Code: 128}
		}
		return "", nil
	})
	_, err := PrepareSource(context.Background(), runner, spec, Config{Scope: ScopeSystem})
	if err == nil || strings.Contains(err.Error(), "ghp-secret-token") {
		t.Fatalf("err = %v", err)
	}
}
