// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"fmt"
	"github.com/zenthros/operator/pkg/shellword"
	"os"
	"strings"

	"github.com/zenthros/operator/pkg/jobsource"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/urlredact"
)

// JobDir returns the per-job state directory (the prepared source lives inside it).
// GC removes the whole directory when a job is cleaned up.
func JobDir(jobID string, cfg Config) string {
	if cfg.Scope == ScopeUser {
		return "$HOME/.local/share/zenthros/jobs/" + jobID
	}
	return "/var/lib/zenthros/jobs/" + jobID
}

// IsolatedJobDir is the per-workspace JobDir used by the containerd executor
// when a Workspace is set. systemd keeps JobDir().
func IsolatedJobDir(workspace, jobID string, cfg Config) string {
	if workspace == "" {
		return JobDir(jobID, cfg)
	}
	return workspacesRoot(cfg) + "/" + workspace + "/jobs/" + jobID
}

// WorkDir returns the directory where user code is placed for a job. Git clones
// and inline extractions target this path; the unit's WorkingDirectory points here.
func WorkDir(jobID string, cfg Config) string {
	return IsolatedWorkDir("", jobID, cfg)
}

// IsolatedWorkDir is WorkDir inside IsolatedJobDir. Empty workspace matches WorkDir.
func IsolatedWorkDir(workspace, jobID string, cfg Config) string {
	return IsolatedJobDir(workspace, jobID, cfg) + "/work"
}

// TmpDir is the per-job writable temp directory. The systemd executor leaves the
// unit on the host /tmp; the containerd executor bind-mounts this path at /tmp
// inside the container (PrivateTmp equivalent) so generated files stay in JobDir.
func TmpDir(jobID string, cfg Config) string {
	return JobDir(jobID, cfg) + "/tmp"
}

// retainMarker is the flag file whose presence tells cleanup to keep the job
// dir.
const retainMarker = ".zenthros-retain"

// RetainMarkerPath returns the flag file whose presence tells cleanup to keep the
// job dir. It lives inside the job dir itself (the Workspace's, where the
// source is) so it survives the unit removal the first Cancel performs: a
// repeat Cancel (the finalizer's, after the GC retention pass already
// uninstalled the unit) must reach the same retain decision. The job
// directory is root's, never the job's: a job could plant a link there.
func RetainMarkerPath(workspace, jobID string, cfg Config) string {
	return IsolatedJobDir(workspace, jobID, cfg) + "/" + retainMarker
}

// retainMarkerProbe succeeds when a job's marker exists, found without its
// Workspace (a unit's name carries none): in its directory of any Workspace,
// or of none.
func retainMarkerProbe(jobID string, cfg Config) string {
	return fmt.Sprintf(`for f in "%s" %s/*/jobs/%s/%s; do test -e "$f" && exit 0; done; exit 1`,
		RetainMarkerPath("", jobID, cfg), workspacesRoot(cfg), jobID, retainMarker)
}

// workspacesRoot holds the Workspaces' job directories (IsolatedJobDir).
func workspacesRoot(cfg Config) string {
	if cfg.Scope == ScopeUser {
		return "$HOME/.local/share/zenthros/workspaces"
	}
	return "/var/lib/zenthros/workspaces"
}

// PrepareSource materializes user code on the host before the unit starts. For git
// sources it clones; for inline sources it decodes and extracts the tarball. An S3
// URI is refused: ignoring it would start the job with no code. Returns the working
// directory path on success, or "" when no source preparation is needed.
func PrepareSource(ctx context.Context, runner Runner, spec jobspec.JobSpec, cfg Config) (string, error) {
	switch {
	case spec.Source.Git != "":
		return prepareGit(ctx, runner, spec, cfg)
	case spec.Source.Inline != "":
		return prepareInline(ctx, runner, spec, cfg)
	case spec.Source.S3 != "":
		return "", fmt.Errorf("s3 source %q is not prepared", spec.Source.S3)
	default:
		return "", nil
	}
}

func sourceWorkDir(spec jobspec.JobSpec, cfg Config) string {
	return IsolatedWorkDir(spec.Workspace, spec.ID, cfg)
}

func prepareGit(ctx context.Context, runner Runner, spec jobspec.JobSpec, cfg Config) (string, error) {
	workDir := sourceWorkDir(spec, cfg)
	sudo := cfg.SudoPrefix()

	if _, err := runner.Run(ctx, fmt.Sprintf(`%smkdir -p "%s"`, sudo, workDir)); err != nil {
		return "", fmt.Errorf("create work dir for git source: %w", err)
	}

	cmd := fmt.Sprintf(`%sgit clone --depth 1`, sudo)
	if spec.Source.GitRef != "" {
		cmd += " --branch " + shellword.Quote(spec.Source.GitRef)
	}
	cmd += " " + shellword.Quote(spec.Source.Git) + " " + shellword.Quote(workDir)

	if _, err := runner.Run(ctx, cmd); err != nil {
		// The URL may carry a token, and the command line and git's output
		// quote it: neither reaches the job's status.
		return "", urlredact.Quoted(fmt.Errorf("git clone %s: %w", spec.Source.Git, err), spec.Source.Git)
	}
	return workDir, nil
}

func prepareInline(ctx context.Context, runner Runner, spec jobspec.JobSpec, cfg Config) (string, error) {
	workDir := sourceWorkDir(spec, cfg)
	sudo := cfg.SudoPrefix()

	// The archive is the job's: tar run as root writes where its entries
	// say (../x, a link out, then a file through it). It is unpacked as the
	// agent, confined to a directory of its own, by the extractor every
	// agent uses (19#3), and that tree is copied in.
	staged, err := os.MkdirTemp("", "zenthros-inline-")
	if err != nil {
		return "", fmt.Errorf("stage inline source: %w", err)
	}
	defer os.RemoveAll(staged)
	if err := jobsource.ExtractInline(strings.NewReader(spec.Source.Inline), staged); err != nil {
		return "", fmt.Errorf("extract inline source: %w", err)
	}
	// What mkdir -p gives the work directory, which cp -a copies onto it.
	if err := os.Chmod(staged, 0o755); err != nil {
		return "", fmt.Errorf("stage inline source: %w", err)
	}
	if _, err := runner.Run(ctx, fmt.Sprintf(`%smkdir -p "%s" && %scp -a %s/. "%s"`, sudo, workDir, sudo, shellword.Quote(staged), workDir)); err != nil {
		return "", fmt.Errorf("copy inline source into %s: %w", workDir, err)
	}
	return workDir, nil
}
