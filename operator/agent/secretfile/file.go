// SPDX-License-Identifier: Apache-2.0

// Package secretfile writes destination-local env/file secret materialization.
// Callers resolve or decrypt values before calling Apply; this package never
// fetches a secret manager or decrypts ciphertext.
package secretfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/secretsource"
)

const (
	FileMode    = 0o600
	DirMode     = 0o700
	WrapperMode = 0o700
	// RootMode is the root's and the directories above it: a job's user
	// passes through to its own directory (Dir, given to it) and lists
	// none.
	RootMode      = 0o711
	wrapperName   = "wrapper"
	secretFileDir = "files"
	mountDir      = "mount"

	// EnvSecretsDir tells a job where its file secrets without an explicit
	// path are: secretsource.DefaultFileRoot in a container, its secret root
	// on a host.
	EnvSecretsDir = jobspec.EnvSecretsDir
)

// Binding is one destination-local value ready to materialize.
type Binding struct {
	Ref   v1.SecretSourceRef
	Value string
}

// Result is destination-local materialization. Values never leave this host.
type Result struct {
	Root        string
	WrapperPath string
	Files       []string
	// Mounts are the file secrets: the job must see Source at Target.
	Mounts []Mount
}

// Mount is one file secret, kept at Source under the job's root, which the
// job sees at Target.
type Mount struct {
	Source, Target string
	// Explicit: Target is a path of the job's own, not the default place
	// (no path, or one under the default root: secretsource.AtDefaultPlace).
	Explicit bool
}

// rootCandidates are the default secret roots, best first, each under the
// shared directory it is made in.
var rootCandidates = []struct{ base, path string }{
	{"/dev/shm", "/dev/shm/zenthros/secrets"},
	{"/run", "/run/zenthros/secrets"},
}

// DefaultRoot prefers tmpfs when writable and the agent's own.
func DefaultRoot() string {
	for _, candidate := range rootCandidates {
		if canUseRoot(candidate.base, candidate.path) {
			return candidate.path
		}
	}
	return filepath.Join(os.TempDir(), "zenthros-secrets")
}

// canUseRoot makes path under base and says whether it is usable: every
// directory from base down is the agent's and writable by no one else. A
// local user who made /dev/shm/zenthros first owned the parent and could
// swap a job's wrapper in under it.
func canUseRoot(base, path string) bool {
	if err := os.MkdirAll(path, RootMode); err != nil {
		return false
	}
	if private(base, path) != nil {
		return false
	}
	probe := filepath.Join(path, ".probe")
	if err := os.WriteFile(probe, []byte("1"), FileMode); err != nil {
		return false
	}
	_ = os.Remove(probe)
	return true
}

// private refuses path unless it and each directory between base and it is
// a directory (not a link) of this process's user that no one else may write.
func private(base, path string) error {
	rel, err := filepath.Rel(base, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return fmt.Errorf("secret root %s is not under %s", path, base)
	}
	dir := base
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "." {
			continue
		}
		dir = filepath.Join(dir, part)
		info, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("secret root: %s is not a directory", dir)
		}
		if info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("secret root: %s is writable by others (%v)", dir, info.Mode().Perm())
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Geteuid() {
			return fmt.Errorf("secret root: %s belongs to uid %d, not %d", dir, st.Uid, os.Geteuid())
		}
	}
	return nil
}

func jobDir(root, jobID string) string {
	return filepath.Join(root, jobID)
}

// Dir is a job's directory in root: everything Apply wrote for it, which
// its user is given (the job reads it as that user).
func Dir(root, jobID string) string {
	return jobDir(root, jobID)
}

// validJobID refuses an id that is not one path segment, so a job's root
// can never be another directory.
func validJobID(jobID string) error {
	if jobID == "" || jobID == "." || jobID == ".." || strings.ContainsAny(jobID, "/\\\x00") {
		return fmt.Errorf("secret root: invalid job id %q", jobID)
	}
	return nil
}

// SecretsDir is where a job's file secrets without an explicit path live in
// its root: the job reads them as $ZENTHROS_SECRETS_DIR/<name>/<key> when no
// mount puts them at secretsource.DefaultFileRoot.
func SecretsDir(root, jobID string) string {
	return mountSource(jobDir(root, jobID), secretsource.DefaultFileRoot)
}

// mountSource is where the file the job sees at target is kept in dir.
func mountSource(dir, target string) string {
	return filepath.Join(dir, mountDir, filepath.Clean("/"+target))
}

// CleanupRoot removes a job's plaintext: its directory under root, and
// nothing outside it. An empty job ID is refused, not skipped: the
// plaintext it was to remove would stay unknown to anyone (07#9).
func CleanupRoot(root, jobID string) error {
	if root == "" {
		return errors.New("secret root: empty")
	}
	if err := validJobID(jobID); err != nil {
		return err
	}
	return os.RemoveAll(jobDir(root, jobID))
}

// Apply writes env/file bindings for one job, all under <root>/<jobID>
// (tmpfs by default). Nothing is written where the job will see a file:
// Result.Mounts says which file in the root the job must see at which path,
// and the runtime mounts it (a container) or links it (Link). Empty bindings
// are a no-op. On error nothing is left behind.
func Apply(root, jobID string, bindings []Binding) (Result, error) {
	if len(bindings) == 0 {
		return Result{}, nil
	}
	if err := validJobID(jobID); err != nil {
		return Result{}, err
	}
	if err := os.MkdirAll(root, RootMode); err != nil {
		return Result{}, fmt.Errorf("create secret root: %w", err)
	}
	// The root itself is the agent's: the fallback in /tmp is a name any
	// local user could take first.
	if err := private(filepath.Dir(root), root); err != nil {
		return Result{}, err
	}
	if err := os.Chmod(root, RootMode); err != nil {
		return Result{}, fmt.Errorf("secret root: %w", err)
	}
	dir := jobDir(root, jobID)
	if err := os.MkdirAll(filepath.Join(dir, secretFileDir), DirMode); err != nil {
		return Result{}, fmt.Errorf("create secret directory: %w", err)
	}
	fail := func(err error) (Result, error) {
		_ = os.RemoveAll(dir)
		return Result{}, err
	}
	result := Result{Root: root}
	envExports := make([]string, 0)
	for _, binding := range bindings {
		if binding.Ref.Materialization.Mode == v1.SecretMaterializationFile {
			target := secretsource.FilePath(binding.Ref)
			source := mountSource(dir, target)
			if err := WriteFile(source, binding.Value); err != nil {
				return fail(err)
			}
			result.Files = append(result.Files, source)
			result.Mounts = append(result.Mounts, Mount{Source: source, Target: target, Explicit: !secretsource.AtDefaultPlace(binding.Ref)})
			continue
		}
		stored := filepath.Join(dir, secretFileDir, secretsource.EnvName(binding.Ref))
		if err := WriteFile(stored, binding.Value); err != nil {
			return fail(err)
		}
		result.Files = append(result.Files, stored)
		envExports = append(envExports, fmt.Sprintf("%s:%s", secretsource.EnvName(binding.Ref), stored))
	}
	if len(envExports) > 0 {
		wrapperPath := filepath.Join(dir, wrapperName)
		if err := os.WriteFile(wrapperPath, []byte(wrapperScript(envExports)), WrapperMode); err != nil {
			return fail(fmt.Errorf("write secret wrapper: %w", err))
		}
		result.WrapperPath = wrapperPath
		result.Files = append(result.Files, wrapperPath)
	}
	return result, nil
}

// Link places a job's explicit-path file secrets when the job has no mount
// namespace of its own (systemd, the container agent) and so sees the host:
// each path must be under one of dirs (the job's own directories), where a
// symlink to the file in the root stands. A path anywhere else is refused
// before anything is created, and an existing file is never replaced. A file
// without an explicit path stays in the root (SecretsDir).
func Link(result Result, dirs []string) error {
	for _, m := range result.Mounts {
		if !m.Explicit {
			continue
		}
		if !underAny(m.Target, dirs) {
			return fmt.Errorf("file secret path %q is outside the job's working directory (%s): "+
				"without a container runtime a file secret must live under it, or omit path and read $%s",
				m.Target, strings.Join(dirs, ", "), EnvSecretsDir)
		}
	}
	for _, m := range result.Mounts {
		if !m.Explicit {
			continue
		}
		current, err := os.Readlink(m.Target)
		if err == nil && current == m.Source {
			continue
		}
		// 0755: it holds links only, and the job's user goes through it to
		// the file (its own, 0600).
		if err := os.MkdirAll(filepath.Dir(m.Target), 0o755); err != nil {
			return fmt.Errorf("create secret parent: %w", err)
		}
		if err == nil && earlierLink(result.Root, current, m.Target) {
			// An earlier attempt's link to this path (its secrets are
			// attempt-scoped): swap it in one rename.
			if err := replaceLink(m.Source, m.Target); err != nil {
				return fmt.Errorf("link file secret %q: %w", m.Target, err)
			}
			continue
		}
		if err := os.Symlink(m.Source, m.Target); err != nil {
			return fmt.Errorf("link file secret %q: %w", m.Target, err)
		}
	}
	return nil
}

// earlierLink reports a link to target's secret in another job directory
// of root: <root>/<id>-a<N>/mount/<target>, which Link wrote for an earlier
// attempt of the job.
func earlierLink(root, dest, target string) bool {
	if root == "" || !filepath.IsAbs(dest) {
		return false
	}
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(dest))
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return false
	}
	jobDir, rest, ok := strings.Cut(rel, string(filepath.Separator))
	return ok && validJobID(jobDir) == nil && filepath.Join(string(filepath.Separator), rest) ==
		filepath.Join(string(filepath.Separator), mountDir, filepath.Clean("/"+target))
}

func replaceLink(source, target string) error {
	tmp := target + ".zenthros-link"
	_ = os.Remove(tmp)
	if err := os.Symlink(source, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func underAny(path string, dirs []string) bool {
	path = filepath.Clean(path)
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		dir = filepath.Clean(dir)
		if path != dir && strings.HasPrefix(path, strings.TrimSuffix(dir, "/")+"/") {
			return true
		}
	}
	return false
}

// WriteFile creates parent directories and writes mode 0600 contents. The
// file itself is opened without following a symlink.
func WriteFile(path, value string) error {
	if err := os.MkdirAll(filepath.Dir(path), DirMode); err != nil {
		return fmt.Errorf("create secret parent: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, FileMode)
	if err != nil {
		return fmt.Errorf("write secret file: %w", err)
	}
	if _, err := f.WriteString(value); err != nil {
		_ = f.Close()
		return fmt.Errorf("write secret file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write secret file: %w", err)
	}
	return os.Chmod(path, FileMode)
}

func wrapperScript(exports []string) string {
	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	for _, export := range exports {
		name, file, ok := strings.Cut(export, ":")
		if !ok {
			continue
		}
		fmt.Fprintf(&b, "export %s=$(cat -- %q)\n", name, file)
	}
	b.WriteString("exec \"$@\"\n")
	return b.String()
}
