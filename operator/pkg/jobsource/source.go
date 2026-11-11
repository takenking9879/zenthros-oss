// SPDX-License-Identifier: Apache-2.0

// Package jobsource puts a job's code in its work directory without a shell:
// an inline tarball is unpacked in Go, and a git source runs the git binary
// directly, when the environment has one. The container agent uses it; it
// assumes nothing about the image beyond a writable work directory.
package jobsource

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/urlredact"
)

// ErrUnsupported is a source this environment cannot prepare. The message
// says which and why; the job is refused rather than started without code.
var ErrUnsupported = errors.New("source not supported here")

// Prepare materializes spec's source under workDir and returns the directory
// the job runs in, or "" when the job has no source to prepare.
func Prepare(ctx context.Context, spec jobspec.JobSpec, workDir string) (string, error) {
	src := spec.Source
	switch {
	case src.HostPath != "":
		return "", fmt.Errorf("%w: source.hostPath %q names a directory on a host; a container sees only its own image", ErrUnsupported, src.HostPath)
	case src.S3 != "":
		return "", fmt.Errorf("%w: s3 source %q is not prepared", ErrUnsupported, src.S3)
	case src.Inline != "":
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			return "", fmt.Errorf("create work dir for inline source: %w", err)
		}
		if err := ExtractInline(strings.NewReader(src.Inline), workDir); err != nil {
			return "", fmt.Errorf("extract inline source: %w", err)
		}
		return workDir, nil
	case src.Git != "":
		return workDir, cloneGit(ctx, spec, workDir)
	default:
		return "", nil
	}
}

func cloneGit(ctx context.Context, spec jobspec.JobSpec, workDir string) error {
	git, err := exec.LookPath("git")
	if err != nil {
		return fmt.Errorf("%w: source.git needs a git binary and this image has none; use an inline source or an image with git", ErrUnsupported)
	}
	if err := os.MkdirAll(filepath.Dir(workDir), 0o755); err != nil {
		return fmt.Errorf("create work dir for git source: %w", err)
	}
	args := []string{"clone", "--depth", "1"}
	if spec.Source.GitRef != "" {
		args = append(args, "--branch", spec.Source.GitRef)
	}
	args = append(args, "--", spec.Source.Git, workDir)
	out, err := exec.CommandContext(ctx, git, args...).CombinedOutput()
	if err != nil {
		// The URL may carry a token (https://user:token@host/...), and git
		// quotes it back: neither reaches the job's status.
		return urlredact.Quoted(fmt.Errorf("git clone %s: %w: %s", spec.Source.Git, err, strings.TrimSpace(string(out))), spec.Source.Git)
	}
	return nil
}

// ExtractInline unpacks a base64-encoded gzipped tarball into dir. Entries
// that would land outside dir (absolute paths, "..", or links pointing out)
// are refused.
func ExtractInline(encoded io.Reader, dir string) error {
	gz, err := gzip.NewReader(base64.NewDecoder(base64.StdEncoding, encoded))
	if err != nil {
		return fmt.Errorf("not a gzipped tarball: %w", err)
	}
	defer gz.Close()
	root, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	// Every write goes through os.Root, which follows no link out of dir. The
	// checks below read one entry at a time: links that each stay inside can
	// chain out of it (s -> ., s/t -> .., then t/x).
	confined, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer confined.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read tarball: %w", err)
		}
		target, err := within(root, hdr.Name)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, target)
		if err != nil {
			return err
		}
		mode := os.FileMode(hdr.Mode).Perm()
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := confined.MkdirAll(rel, mode|0o700); err != nil {
				return entryError(hdr.Name, err)
			}
		case tar.TypeReg:
			if err := confined.MkdirAll(filepath.Dir(rel), 0o755); err != nil {
				return entryError(hdr.Name, err)
			}
			f, err := confined.OpenFile(rel, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode|0o600)
			if err != nil {
				return entryError(hdr.Name, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("write %s: %w", hdr.Name, err)
			}
			if err := f.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			resolved := filepath.Join(filepath.Dir(target), hdr.Linkname)
			if filepath.IsAbs(hdr.Linkname) || !inside(root, resolved) {
				return fmt.Errorf("tarball entry %q links outside the work directory", hdr.Name)
			}
			if err := confined.MkdirAll(filepath.Dir(rel), 0o755); err != nil {
				return entryError(hdr.Name, err)
			}
			if err := confined.Symlink(hdr.Linkname, rel); err != nil {
				return entryError(hdr.Name, err)
			}
		default:
			// Devices, FIFOs and hard links have no place in user code.
			return fmt.Errorf("tarball entry %q has unsupported type %q", hdr.Name, string(hdr.Typeflag))
		}
	}
}

// entryError names the entry an extraction step refused.
func entryError(name string, err error) error {
	return fmt.Errorf("tarball entry %q: %w", name, err)
}

// within resolves name under root and refuses one that escapes it.
func within(root, name string) (string, error) {
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("tarball entry %q is an absolute path", name)
	}
	target := filepath.Join(root, name)
	if !inside(root, target) {
		return "", fmt.Errorf("tarball entry %q escapes the work directory", name)
	}
	return target, nil
}

func inside(root, path string) bool {
	return path == root || strings.HasPrefix(path, root+string(os.PathSeparator))
}
