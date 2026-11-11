// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
)

// powerOffTimeout bounds one try at powering the machine off.
const powerOffTimeout = time.Minute

// powerOff obeys the hub's ask that this machine power itself off
// (RegisterResponse.power_off). The hub asks when the machine's cloud account
// no longer lets the provisioner terminate it: such a machine was launched to
// terminate when it powers off, so this is its termination.
//
// Only a machine of the installation's own obeys: whoever joined a machine of
// theirs never handed the hub its power switch. The hub asks again until the
// machine is gone, so a try that fails is tried again at the next ask.
func (a *Agent) powerOff() {
	if infrav1alpha1.NormalizeOwnership(a.cfg.Ownership) != infrav1alpha1.OwnershipManaged {
		a.logger.Info("ignoring the hub's ask to power off: this machine is not the installation's own")
		return
	}
	if a.cfg.PowerOff == nil {
		a.logger.Info("ignoring the hub's ask to power off: this agent cannot power its machine off")
		return
	}
	if !a.poweringOff.CompareAndSwap(false, true) {
		return
	}
	a.logger.Info("powering the machine off: the hub asks, its cloud account no longer lets it be terminated from outside")
	go func() {
		// Not the session's context: the session closes as the machine goes.
		ctx, cancel := context.WithTimeout(context.Background(), powerOffTimeout)
		defer cancel()
		if err := a.cfg.PowerOff(ctx); err != nil {
			a.logger.Error(err, "power off failed; tried again when the hub asks again")
			a.poweringOff.Store(false)
		}
	}()
}

// SystemPowerOff powers the machine off through systemd, which stops its
// units in order first.
func SystemPowerOff(useSudo bool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		args := []string{"systemctl", "poweroff"}
		if useSudo {
			args = append([]string{"sudo", "-n"}, args...)
		}
		out, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
		return nil
	}
}
