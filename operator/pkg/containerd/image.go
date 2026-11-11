// SPDX-License-Identifier: Apache-2.0

// Package containerd pulls images and renders a ctr-run wrapper for VM jobs.
// Job files still live in the systemd JobDir/WorkDir/TmpDir layout; this package
// does not replace the systemd executor.
package containerd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/capability"
	"github.com/zenthros/operator/pkg/shellword"
	"github.com/zenthros/operator/pkg/systemd"
)

const (
	// Namespace isolates Zenthros job containers from other content on the host.
	Namespace = "zenthros"
)

// NormalizeImageRef expands a Docker-style short name to a registry reference
// `ctr images pull` accepts. It does not pin a tag or digest.
func NormalizeImageRef(ref string) string {
	return capability.NormalizeImage(ref)
}

// ImageExists reports whether ref is already in the containerd namespace.
func ImageExists(ctx context.Context, runner systemd.Runner, sudo, ref string) (bool, error) {
	ref = NormalizeImageRef(ref)
	if ref == "" {
		return false, fmt.Errorf("image ref is empty")
	}
	out, err := runner.Run(ctx, sudo+"ctr -n "+Namespace+" images ls -q")
	if err != nil {
		if isAbsent(err, out) {
			return false, nil
		}
		return false, err
	}
	return imageListed(out, ref), nil
}

func imageListed(listing, ref string) bool {
	alt := strings.TrimPrefix(ref, "docker.io/")
	for _, line := range strings.Split(listing, "\n") {
		name := strings.TrimSpace(line)
		if name == ref || name == alt {
			return true
		}
	}
	return false
}

// EnsureImage pulls ref with `ctr images pull` when it is not already present.
// pulled is true only when a registry fetch ran.
func EnsureImage(ctx context.Context, runner systemd.Runner, sudo, ref string) (pulled bool, err error) {
	return EnsureImageWith(ctx, runner, sudo, ref, nil)
}

// EnsureImageWith is EnsureImage with a registry login from auth for the
// hosts it knows (each provider's registry through the machine's own
// identity, see DefaultRegistryAuth). The password
// reaches ctr through a private file the command reads, never through the
// command text, which an error would carry into the job's status. It is in
// ctr's argv for the pull: ctr reads a password only from a terminal, and
// its hosts.toml headers would replace the token exchange the registries
// need. Who could read that argv already has the token: a container job
// does not see the host's processes, and a host process asks the instance
// metadata for the machine's identity itself (07#6, decided 2026-10-21).
func EnsureImageWith(ctx context.Context, runner systemd.Runner, sudo, ref string, auth RegistryAuth) (pulled bool, err error) {
	ref = NormalizeImageRef(ref)
	if ref == "" {
		return false, fmt.Errorf("image ref is empty")
	}
	ok, err := ImageExists(ctx, runner, sudo, ref)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	userFile := ""
	if auth != nil {
		user, password, known, err := auth.Login(ctx, imageHost(ref))
		if err != nil {
			return false, err
		}
		if known {
			dir, err := os.MkdirTemp("", "zenthros-pull-")
			if err != nil {
				return false, err
			}
			defer os.RemoveAll(dir)
			userFile = filepath.Join(dir, "user")
			if err := os.WriteFile(userFile, []byte(user+":"+password), 0o600); err != nil {
				return false, err
			}
		}
	}
	for attempt := 0; ; attempt++ {
		_, err := runner.Run(ctx, pullCommand(sudo, ref, userFile))
		if err == nil {
			return true, nil
		}
		if attempt >= len(pullRetryWaits) || backend.PullRefused(err.Error()) || pullThrottled(err) {
			return false, fmt.Errorf("ctr images pull %s: %w", ref, pullClass(err))
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("ctr images pull %s: %w", ref, err)
		case <-time.After(pullRetryWaits[attempt]):
		}
	}
}

// pullRetryWaits is how long a pull the registry did not refuse waits before
// each new try: a registry's name that does not resolve for an instant, or
// a connection that drops, must not fail the job (local suite, 2026-10-12).
var pullRetryWaits = []time.Duration{2 * time.Second, 5 * time.Second, 10 * time.Second}

// pullClass marks a failed pull for the controller's retry policy: only
// the registry's refusal (no such image, no access) is final; its 429 is a
// throttle, and everything else (the network however it broke, a registry's
// 5xx) is transient, within the submit budget (c4-A7). A list of the
// network's failures left out a connection cut a way nobody listed, and the
// job failed for good (R4 ronda 3, G1).
func pullClass(err error) error {
	switch {
	case backend.PullRefused(err.Error()):
		return backend.Classified(backend.ErrImagePull, err)
	case pullThrottled(err):
		return backend.Classified(backend.ErrRateLimited, err)
	default:
		return backend.Classified(backend.ErrDestinationUnavailable, err)
	}
}

// pullThrottled reports a registry's "too many requests" (Docker Hub's pull
// limit among them).
func pullThrottled(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") || strings.Contains(msg, "too many requests") || strings.Contains(msg, "toomanyrequests")
}

// imageHost is the registry host of a normalized ref.
func imageHost(ref string) string {
	host, _, _ := strings.Cut(ref, "/")
	return host
}

// pullCommand pulls ref, with the registry login EnsureImageWith wrote to
// userFile when there is one (GARAuth reads the GCE token on Google Cloud),
// else anonymously. No --platform: ctr pulls the host architecture.
// linux/amd64 on an arm64 VM is the wrong image.
func pullCommand(sudo, ref, userFile string) string {
	pull := sudo + "ctr -n " + Namespace + " images pull "
	if userFile != "" {
		return pull + `--user "$(cat ` + shellword.Quote(userFile) + `)" ` + shellword.Quote(ref)
	}
	return pull + shellword.Quote(ref)
}

func isAbsent(err error, stdout string) bool {
	msg := strings.ToLower(err.Error() + " " + stdout)
	if strings.Contains(msg, "command not found") {
		return false
	}
	return strings.Contains(msg, "not found") || strings.Contains(msg, "notfound") || strings.Contains(msg, "no such image")
}

// ImageDigest is ref pinned by the digest it resolved to in the containerd
// namespace (repo@sha256:...), or "" when ref is not there or ctr does not
// say. A ref already pinned by digest is returned as is.
func ImageDigest(ctx context.Context, runner systemd.Runner, sudo, ref string) (string, error) {
	ref = NormalizeImageRef(ref)
	if ref == "" {
		return "", nil
	}
	if strings.Contains(ref, "@sha256:") {
		return ref, nil
	}
	out, err := runner.Run(ctx, sudo+"ctr -n "+Namespace+" images ls "+shellword.Quote("name=="+ref))
	if err != nil {
		if isAbsent(err, out) {
			return "", nil
		}
		return "", err
	}
	return PinnedRef(ref, listedDigest(out, ref)), nil
}

// listedDigest reads ref's digest from a `ctr images ls` table
// (REF TYPE DIGEST ...).
func listedDigest(listing, ref string) string {
	alt := strings.TrimPrefix(ref, "docker.io/")
	for _, line := range strings.Split(listing, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || (fields[0] != ref && fields[0] != alt) {
			continue
		}
		if strings.HasPrefix(fields[2], "sha256:") {
			return fields[2]
		}
	}
	return ""
}

// PinnedRef is ref's repository at digest: tag dropped, digest kept. "" when
// digest is.
func PinnedRef(ref, digest string) string {
	if digest == "" {
		return ""
	}
	if at := strings.Index(ref, "@"); at >= 0 {
		ref = ref[:at]
	}
	if colon := strings.LastIndex(ref, ":"); colon > strings.LastIndex(ref, "/") {
		ref = ref[:colon]
	}
	return ref + "@" + digest
}

// Root is where containerd keeps the images (install-agent.sh's config).
const Root = "/var/lib/containerd"

// ListImages is every image ref in the namespace.
func ListImages(ctx context.Context, runner systemd.Runner, sudo string) ([]string, error) {
	out, err := runner.Run(ctx, sudo+"ctr -n "+Namespace+" images ls -q")
	if err != nil {
		return nil, fmt.Errorf("ctr images ls: %w", err)
	}
	return strings.Fields(out), nil
}

// ContainerImages is the image of every container in the namespace, running
// or not, as containerd says now (CONTAINER IMAGE RUNTIME).
func ContainerImages(ctx context.Context, runner systemd.Runner, sudo string) (map[string]bool, error) {
	out, err := runner.Run(ctx, sudo+"ctr -n "+Namespace+" containers ls")
	if err != nil {
		return nil, fmt.Errorf("ctr containers ls: %w", err)
	}
	images := map[string]bool{}
	for i, line := range strings.Split(out, "\n") {
		if fields := strings.Fields(line); i > 0 && len(fields) >= 2 {
			images[fields[1]] = true
		}
	}
	return images, nil
}

// RemoveImage removes ref from the namespace and waits until what only it
// held is gone from the disk (--sync); one already gone is no error.
func RemoveImage(ctx context.Context, runner systemd.Runner, sudo, ref string) error {
	out, err := runner.Run(ctx, sudo+"ctr -n "+Namespace+" images rm --sync "+shellword.Quote(ref))
	if err != nil && !isAbsent(err, out) {
		return fmt.Errorf("ctr images rm %s: %w", ref, err)
	}
	return nil
}

// ImageSpace is the bytes containerd's store takes on its disk, and that
// disk's size.
func ImageSpace(ctx context.Context, runner systemd.Runner, sudo string) (used, disk int64, err error) {
	out, err := runner.Run(ctx, sudo+"du -sxb "+Root+" | cut -f1; "+sudo+"df -B1 --output=size "+Root+" | tail -n 1")
	if err != nil {
		return 0, 0, fmt.Errorf("measure %s: %w", Root, err)
	}
	fields := strings.Fields(out)
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("measure %s: %q", Root, out)
	}
	if used, err = strconv.ParseInt(fields[0], 10, 64); err == nil {
		disk, err = strconv.ParseInt(fields[1], 10, 64)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("measure %s: %q", Root, out)
	}
	return used, disk, nil
}
