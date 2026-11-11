// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	ctd "github.com/zenthros/operator/pkg/containerd"
	"github.com/zenthros/operator/pkg/prewarm/cache"
	"github.com/zenthros/operator/pkg/shellword"
	"github.com/zenthros/operator/pkg/systemd"
)

// KeptInstallerDir is where install-agent.sh keeps a verified copy of itself
// and the pins it installed from (INSTALL_BASE, the SHA256SUMS digest).
const KeptInstallerDir = "/var/lib/zenthros/installer"

// NewVMCache is a machine's warm cache: images go to containerd, the same
// store a job's image is pulled into, and toolchains through the installer
// the machine was installed with.
func NewVMCache(manager *systemd.Manager, registry ctd.RegistryAuth, root string, maxBytes int64, images *ImageGC) (*cache.Manager, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create cache %s: %w", root, err)
	}
	cfg := manager.Config()
	sudo := cfg.SudoPrefix()
	runner := manager.Runner()
	return &cache.Manager{
		Root:     root,
		MaxBytes: maxBytes,
		Pull: func(ctx context.Context, ref string) (bool, error) {
			if probeRuntimeClass() != runtimeClassContainerd {
				return false, errors.New("containerd is not running on this machine; images are warmed in containerd (VMPool spec.runtime.container.engine: containerd)")
			}
			images.Note(ref)
			return ctd.EnsureImageWith(ctx, runner, sudo, ref, registry)
		},
		Install: func(ctx context.Context, name, version string, packages []string) error {
			script := KeptInstallerDir + "/install-agent.sh"
			if _, err := os.Stat(script); err != nil {
				return fmt.Errorf("this machine keeps no installer (%s): it was installed before toolchain Prewarms, or its installer could not be kept", script)
			}
			spec := name
			if version != "" {
				spec += "@" + version
			}
			cmd := fmt.Sprintf(`%senv $(cat %s/env) bash %s --only-toolchains --toolchain %s`,
				sudo, KeptInstallerDir, script, shellword.Quote(spec))
			if len(packages) > 0 {
				cmd += " --toolchain-packages " + shellword.Quote(strings.Join(packages, " "))
			}
			_, err := runner.Run(ctx, cmd)
			return installerError(err)
		},
	}, nil
}

// installerError is the installer's own refusal: its "install-agent.sh:"
// lines, or the end of what it wrote when it wrote none.
func installerError(err error) error {
	var exit *systemd.ExitError
	if !errors.As(err, &exit) {
		return err
	}
	var own []string
	lines := strings.Split(strings.TrimSpace(exit.Stderr), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "install-agent.sh: ") {
			own = append(own, strings.TrimPrefix(line, "install-agent.sh: "))
		}
	}
	if len(own) > 0 {
		return errors.New(strings.Join(own, "; "))
	}
	if len(lines) > 10 {
		lines = lines[len(lines)-10:]
	}
	return fmt.Errorf("installer exited %d: %s", exit.Code, strings.Join(lines, "\n"))
}
