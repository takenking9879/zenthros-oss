// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"
)

// The manager names a unit's object by its name escaped: each byte outside
// [A-Za-z0-9] as _ and two hex digits.
func TestUnitOfPath(t *testing.T) {
	for path, want := range map[dbus.ObjectPath]string{
		"/org/freedesktop/systemd1/unit/zenthros_2dj001_2eservice":                             "zenthros-j001.service",
		"/org/freedesktop/systemd1/unit/zenthros_2d6f1c0e2a_2d1b2c_2eservice":                  "zenthros-6f1c0e2a-1b2c.service",
		"/org/freedesktop/systemd1/unit/ssh_2eservice":                                         "ssh.service",
		"/org/freedesktop/systemd1/unit/_31_2dfirst_5fdigit_2eservice":                         "1-first_digit.service",
		"/org/freedesktop/systemd1/unit/user_40_31000_2eservice":                               "user@1000.service",
		"/org/freedesktop/systemd1/unit/zenthros_2dbus_2dprobe_2d12_2d34_2eservice":            "zenthros-bus-probe-12-34.service",
		"/org/freedesktop/systemd1/unit/system_2dgetty_2eslice":                                "system-getty.slice",
		"/org/freedesktop/systemd1/unit/zenthros_2d0123456789abcdef0123456789abcdef_2eservice": "zenthros-0123456789abcdef0123456789abcdef.service",
	} {
		if got, ok := unitOfPath(path); !ok || got != want {
			t.Errorf("unitOfPath(%s) = %q, %v; want %q", path, got, ok, want)
		}
	}
	for _, path := range []dbus.ObjectPath{
		"/org/freedesktop/systemd1",
		"/org/freedesktop/systemd1/job/42",
		"/org/freedesktop/systemd1/unit/",
		"/org/freedesktop/systemd1/unit/a_2",
		"/org/freedesktop/systemd1/unit/a_zz",
		"/org/freedesktop/systemd1/unit/a/b",
	} {
		if got, ok := unitOfPath(path); ok {
			t.Errorf("unitOfPath(%s) = %q, want no unit", path, got)
		}
	}
}

// fakeManager listens where a manager's own socket would be and plays its
// side: it takes EXTERNAL authentication (mute: it never answers it), then
// answers no call, as the system's manager left the agent's Subscribe
// unanswered in the VM E2E (2026-11-16). Each connection that authenticated
// is handed to the test, to write the manager's signals on.
func fakeManager(t *testing.T, mute bool) (string, <-chan net.Conn) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	ready := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
			go func() {
				in := bufio.NewReader(conn)
				if _, err := in.ReadByte(); err != nil { // the client's NUL byte
					return
				}
				for {
					line, err := in.ReadString('\n')
					if err != nil {
						return
					}
					if mute {
						continue
					}
					var answer string
					switch command := strings.TrimSpace(line); {
					case command == "AUTH":
						answer = "REJECTED EXTERNAL"
					case strings.HasPrefix(command, "AUTH EXTERNAL"):
						answer = "OK 0123456789abcdef0123456789abcdef"
					case command == "NEGOTIATE_UNIX_FD":
						answer = "AGREE_UNIX_FD"
					case command == "BEGIN":
						ready <- conn
						_, _ = io.Copy(io.Discard, in) // every call, unanswered
						return
					default:
						answer = "ERROR"
					}
					if _, err := conn.Write([]byte(answer + "\r\n")); err != nil {
						return
					}
				}
			}()
		}
	}()
	return path, ready
}

// sayChanged writes the manager's PropertiesChanged of unit on conn.
func sayChanged(t *testing.T, conn net.Conn, unit string) {
	t.Helper()
	var label strings.Builder
	for i := 0; i < len(unit); i++ {
		if c := unit[i]; c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' && i > 0 {
			label.WriteByte(c)
		} else {
			fmt.Fprintf(&label, "_%02x", c)
		}
	}
	msg := &dbus.Message{
		Type: dbus.TypeSignal,
		Headers: map[dbus.HeaderField]dbus.Variant{
			dbus.FieldPath:      dbus.MakeVariant(dbus.ObjectPath("/org/freedesktop/systemd1/unit/" + label.String())),
			dbus.FieldInterface: dbus.MakeVariant("org.freedesktop.DBus.Properties"),
			dbus.FieldMember:    dbus.MakeVariant("PropertiesChanged"),
			dbus.FieldSignature: dbus.MakeVariant(dbus.SignatureOf("", map[string]dbus.Variant{}, []string{})),
		},
		Body: []interface{}{"org.freedesktop.systemd1.Unit", map[string]dbus.Variant{"ActiveState": dbus.MakeVariant("inactive")}, []string{}},
	}
	if err := msg.EncodeTo(conn, binary.LittleEndian); err != nil {
		t.Fatal(err)
	}
}

// hear runs unitChanges on path in the background: what it returns, or
// nothing after 5 s.
func hear(t *testing.T, ctx context.Context, path string) (<-chan string, error) {
	t.Helper()
	type result struct {
		changes <-chan string
		err     error
	}
	done := make(chan result, 1)
	go func() {
		changes, err := unitChanges(path)(ctx)
		done <- result{changes, err}
	}()
	select {
	case r := <-done:
		return r.changes, r.err
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting on the manager after 5 s: the watch would wait with it, and tell no stream its job ended")
		return nil, nil
	}
}

// The manager left the agent's Subscribe unanswered for minutes in the VM
// E2E (2026-11-16, twice in seven runs of the logs E2E), and the watch,
// waiting on it, told no stream its job ended: a log saved to its bucket
// never got END.json. The manager signals its own socket's clients without
// it, so the call is sent without waiting for an answer, and signals come
// as before.
func TestHearingTheManagerNeverWaitsForAnAnswer(t *testing.T) {
	path, ready := fakeManager(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes, err := hear(t, ctx, path)
	if err != nil {
		t.Fatalf("hear a manager that answers no call: %v", err)
	}
	conn := <-ready
	sayChanged(t, conn, "zenthros-j001.service")
	select {
	case got := <-changes:
		if got != "zenthros-j001.service" {
			t.Fatalf("heard %q, want zenthros-j001.service", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the manager said a unit changed and nothing was heard")
	}
}

// A manager that takes a connection and never authenticates it is given up
// after busAuthTimeout: the watch asks every followPollInterval meanwhile,
// and dials again.
func TestHearingTheManagerGivesUpOnAConnectionItNeverTakes(t *testing.T) {
	defer func(old time.Duration) { busAuthTimeout = old }(busAuthTimeout)
	busAuthTimeout = 200 * time.Millisecond
	path, _ := fakeManager(t, true)
	if _, err := hear(t, context.Background(), path); err == nil {
		t.Fatal("a manager that never authenticated the connection was heard")
	}
}

// The agent hears the manager on its own socket, without a D-Bus daemon.
// A user manager answers the same protocol on its socket as the system's
// does to root: a unit it runs is said when it starts and when it ends, and
// the end of ctx closes the connection. Run where a user manager runs (a
// developer's machine); the VM E2E hears the system's manager.
func TestUnitChangesHearsTheManager(t *testing.T) {
	socket := Config{Scope: ScopeUser}.privateSocket()
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("no user manager here: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	changes, err := unitChanges(socket)(ctx)
	if err != nil {
		t.Fatalf("hear the user manager: %v", err)
	}
	unit := fmt.Sprintf("zenthros-bus-probe-%d-%d.service", os.Getpid(), time.Now().UnixNano())
	// --collect: the manager unloads it once it ends, nothing is left.
	if out, err := exec.CommandContext(ctx, "systemd-run", "--user", "--collect", "--unit="+unit, "--", "sleep", "0.2").CombinedOutput(); err != nil {
		t.Fatalf("systemd-run: %v: %s", err, out)
	}
	for heard := 0; heard < 2; {
		select {
		case got, ok := <-changes:
			if !ok {
				t.Fatal("the connection closed while the unit ran")
			}
			if got == unit {
				heard++
			}
		case <-ctx.Done():
			t.Fatalf("heard %s %d times in 30 s, want its start and its end", unit, heard)
		}
	}

	cancel()
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the connection outlived its context")
		}
	}
}

// The whole watch on a real manager: a follow of a unit that ends is told
// by what systemd says, with no interval or fallback within the test. The
// unit fails, so the manager keeps it to be read (a job's unit has its file
// and stays loaded).
func TestATerminalWatchHearsAUnitEnd(t *testing.T) {
	cfg := Config{Scope: ScopeUser}
	if _, err := os.Stat(cfg.privateSocket()); err != nil {
		t.Skipf("no user manager here: %v", err)
	}
	watch := NewTerminalWatch(cfg, LocalRunner{})
	watch.interval, watch.fallback = time.Hour, time.Hour
	unit := fmt.Sprintf("zenthros-bus-end-%d-%d.service", os.Getpid(), time.Now().UnixNano())
	t.Cleanup(func() { _ = exec.Command("systemctl", "--user", "reset-failed", unit).Run() })
	if out, err := exec.Command("systemd-run", "--user", "--unit="+unit, "--", "sh", "-c", "sleep 1; exit 3").CombinedOutput(); err != nil {
		t.Fatalf("systemd-run: %v: %s", err, out)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	select {
	case <-watch.Ended(ctx, unit):
		if waited := time.Since(start); waited < 500*time.Millisecond {
			t.Fatalf("told after %s, before the unit could end", waited)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the unit ended and its stream was never told")
	}
}
