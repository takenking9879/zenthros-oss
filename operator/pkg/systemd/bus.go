// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"
)

// privateSocket is the manager's own socket: systemd answers on it without
// a D-Bus daemon, to root for the system's manager and to its user for a
// user manager.
func (c Config) privateSocket() string {
	if c.Scope == ScopeUser {
		return filepath.Join(os.Getenv("XDG_RUNTIME_DIR"), "systemd", "private")
	}
	return "/run/systemd/private"
}

// busAuthTimeout bounds how long a TerminalWatch waits for the manager to
// take a connection: the watch reads no unit meanwhile.
var busAuthTimeout = 5 * time.Second

// unitChanges hears the manager on its socket at path. The channel says
// each unit whose state changed (its org.freedesktop.systemd1.Unit
// properties, where ActiveState is) or that was unloaded, and closes when
// ctx ends or the connection drops.
//
// Nothing here waits on the manager past the authentication, itself given
// up after busAuthTimeout. The manager left the Subscribe call unanswered
// for minutes in the VM E2E (2026-11-16, twice in seven runs of the logs
// E2E) while its signals came all the same, and the watch, waiting on that
// answer, told no stream its job ended. The manager signals every client of
// its own socket; the call is still made, without waiting for its answer.
func unitChanges(path string) func(context.Context) (<-chan string, error) {
	return func(ctx context.Context) (<-chan string, error) {
		conn, err := dbus.Dial("unix:path="+path, dbus.WithContext(ctx))
		if err != nil {
			return nil, err
		}
		signals := make(chan *dbus.Signal, 64)
		conn.Signal(signals)
		// Closing the connection ends an authentication the manager does not
		// answer.
		attempt, cancel := context.WithTimeout(ctx, busAuthTimeout)
		defer cancel()
		giveUp := context.AfterFunc(attempt, func() { conn.Close() })
		// No Hello on the manager's own socket: there is no bus daemon to
		// greet.
		err = conn.Auth([]dbus.Auth{dbus.AuthExternal(strconv.Itoa(os.Getuid()))})
		if !giveUp() && err == nil {
			err = fmt.Errorf("the manager did not authenticate the connection in %s", busAuthTimeout)
		}
		if err != nil {
			conn.Close()
			return nil, err
		}
		conn.Object("org.freedesktop.systemd1", "/org/freedesktop/systemd1").
			Go("org.freedesktop.systemd1.Manager.Subscribe", dbus.FlagNoReplyExpected, nil)
		changes := make(chan string, 64)
		go func() {
			defer close(changes)
			defer conn.Close()
			// Closed when the connection is, which ctx's end closes.
			for signal := range signals {
				unit, ok := changedUnit(signal)
				if !ok {
					continue
				}
				select {
				case changes <- unit:
				case <-ctx.Done():
					return
				}
			}
		}()
		return changes, nil
	}
}

// changedUnit is the unit a signal of the manager says changed.
func changedUnit(signal *dbus.Signal) (string, bool) {
	switch signal.Name {
	case "org.freedesktop.DBus.Properties.PropertiesChanged":
		if len(signal.Body) == 0 || signal.Body[0] != "org.freedesktop.systemd1.Unit" {
			return "", false
		}
		return unitOfPath(signal.Path)
	case "org.freedesktop.systemd1.Manager.UnitRemoved":
		if len(signal.Body) == 0 {
			return "", false
		}
		unit, ok := signal.Body[0].(string)
		return unit, ok
	}
	return "", false
}

// unitOfPath is the unit a manager's object path names: the name escaped
// with each byte outside [A-Za-z0-9] as _ and two hex digits
// (zenthros-j1.service is .../unit/zenthros_2dj1_2eservice).
func unitOfPath(path dbus.ObjectPath) (string, bool) {
	label, ok := strings.CutPrefix(string(path), "/org/freedesktop/systemd1/unit/")
	if !ok || label == "" || strings.Contains(label, "/") {
		return "", false
	}
	var b strings.Builder
	for i := 0; i < len(label); i++ {
		if label[i] != '_' {
			b.WriteByte(label[i])
			continue
		}
		if i+2 >= len(label) {
			return "", false
		}
		v, err := strconv.ParseUint(label[i+1:i+3], 16, 8)
		if err != nil {
			return "", false
		}
		b.WriteByte(byte(v))
		i += 2
	}
	return b.String(), true
}
