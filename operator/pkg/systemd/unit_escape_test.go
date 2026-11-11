// SPDX-License-Identifier: Apache-2.0

package systemd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// hardValues are what broke units before: a space (systemd cut the
// assignment there and dropped the rest), a newline (a unit line cannot
// hold one: "Unbalanced quoting ... unit will not be started"), `$` (systemd
// expanded $NAME in ExecStart before the program saw it), and the quote,
// backslash and '%' its parsers read specially.
var hardValues = []string{
	"two words",
	"line one\nline two\r\n",
	"$HOME ${PATH} $$ cost $5",
	`say "hi" and back\slash`,
	"100% done %n",
	"",
}

func escapeSpec() (map[string]string, []string) {
	env := map[string]string{}
	for i, v := range hardValues {
		env[string(rune('A'+i))+"_VALUE"] = v
	}
	script := "import os\nprint('E2E', os.environ['A_VALUE'])\nprint(\"$HOME stays\")"
	return env, []string{"/bin/sh", "-c", script, "--", hardValues[0], hardValues[1], hardValues[2], hardValues[3], hardValues[4]}
}

// Every Environment value and ExecStart argument reads back, through
// systemd's own quoting rules, as exactly what the job asked for. E2E
// TestMachineRunsJobsToCompletion (unit escaping subtest).
func TestUnitEscapingRoundTrips(t *testing.T) {
	env, argv := escapeSpec()
	spec := testSpec()
	spec.Entrypoint = ""
	spec.Command, spec.Args = argv[:1], argv[1:]
	// The system manager's unit reads its environment from a file; a user
	// manager's has it in Environment= lines.
	unit := renderUnit(spec, env, Config{Scope: ScopeUser}, "", "", nil)
	fromFile, err := systemdEnvFile(formatEnvFile(env))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range env {
		if fromFile[name] != want {
			t.Errorf("EnvironmentFile %s = %q, want %q", name, fromFile[name], want)
		}
	}
	if len(fromFile) != len(env) {
		t.Errorf("EnvironmentFile has %d variables, want %d: %q", len(fromFile), len(env), fromFile)
	}

	gotEnv := map[string]string{}
	var gotArgv []string
	for _, line := range strings.Split(unit, "\n") {
		switch {
		case strings.HasPrefix(line, "Environment="):
			words, err := systemdWords(strings.TrimPrefix(line, "Environment="), false)
			if err != nil || len(words) != 1 {
				t.Fatalf("%q: %v %q", line, err, words)
			}
			name, value, _ := strings.Cut(words[0], "=")
			gotEnv[name] = value
		case strings.HasPrefix(line, "ExecStart="):
			words, err := systemdWords(strings.TrimPrefix(line, "ExecStart="), true)
			if err != nil {
				t.Fatalf("%q: %v", line, err)
			}
			gotArgv = words
		}
	}
	for name, want := range env {
		if gotEnv[name] != want {
			t.Errorf("Environment %s = %q, want %q", name, gotEnv[name], want)
		}
	}
	if strings.Join(gotArgv, "\x00") != strings.Join(argv, "\x00") {
		t.Errorf("ExecStart argv\n got %q\nwant %q", gotArgv, argv)
	}
}

// systemd itself accepts the unit: no "Invalid environment assignment", no
// "Unbalanced quoting", no fatal error. Skipped where systemd-analyze is
// not installed.
func TestUnitPassesSystemdAnalyzeVerify(t *testing.T) {
	analyze, err := exec.LookPath("systemd-analyze")
	if err != nil {
		t.Skip("systemd-analyze not installed")
	}
	env, argv := escapeSpec()
	spec := testSpec()
	spec.Entrypoint = ""
	spec.Command, spec.Args = argv[:1], argv[1:]
	dir := t.TempDir()
	path := filepath.Join(dir, unitName(spec.ID))
	if err := os.WriteFile(path, []byte(renderUnit(spec, env, Config{Scope: ScopeSystem}, "", "", nil)), 0o644); err != nil {
		t.Fatal(err)
	}
	out, _ := exec.Command(analyze, "verify", "--man=no", path).CombinedOutput()
	for _, line := range strings.Split(string(out), "\n") {
		// Lines about this file's own settings are parse errors; a missing
		// ExecStopPost binary on the test machine is not.
		if strings.HasPrefix(line, path+":") || strings.Contains(line, "fatal error") || strings.Contains(line, "bad unit file setting") {
			t.Errorf("systemd-analyze verify: %s", line)
		}
	}
}

// systemdEnvFile reads an EnvironmentFile the way systemd's env-file.c does
// for the lines formatEnvFile writes (NAME="value"): in double quotes a
// backslash before one of \ " ` $ is dropped, one before a newline eats
// both, one before anything else stays, and a newline is kept.
func systemdEnvFile(text string) (map[string]string, error) {
	env := map[string]string{}
	for text != "" {
		name, rest, ok := strings.Cut(text, "=\"")
		if !ok || !envNamePattern.MatchString(name) {
			return nil, fmt.Errorf("not NAME=\"value\": %q", text)
		}
		var value strings.Builder
		closed := false
		for i := 0; i < len(rest); i++ {
			c := rest[i]
			switch {
			case c == '\\' && i+1 < len(rest):
				i++
				switch n := rest[i]; {
				case strings.IndexByte("\"\\`$", n) >= 0:
					value.WriteByte(n)
				case n == '\n':
				default:
					value.WriteByte('\\')
					value.WriteByte(n)
				}
			case c == '"':
				closed = true
				rest = rest[i+1:]
			default:
				value.WriteByte(c)
			}
			if closed {
				break
			}
		}
		if !closed || !strings.HasPrefix(rest, "\n") {
			return nil, fmt.Errorf("%s: unclosed or trailing text: %q", name, rest)
		}
		env[name] = value.String()
		text = rest[1:]
	}
	return env, nil
}

// systemdWords splits a unit value the way systemd reads Environment= and
// ExecStart=: words separated by blanks, double quotes grouping, C escapes
// inside them, "%%" a literal '%'; in ExecStart "$$" is a literal '$'. It
// refuses what systemd refuses: an unbalanced quote or a raw line break.
func systemdWords(value string, exec bool) ([]string, error) {
	if strings.ContainsAny(value, "\n\r") {
		return nil, errUnbalanced
	}
	var words []string
	i := 0
	for i < len(value) {
		for i < len(value) && (value[i] == ' ' || value[i] == '\t') {
			i++
		}
		if i >= len(value) {
			break
		}
		var b strings.Builder
		quoted := false
		for i < len(value) {
			c := value[i]
			switch {
			case c == '"':
				quoted = !quoted
				i++
				continue
			case !quoted && (c == ' ' || c == '\t'):
			case c == '\\' && i+1 < len(value):
				switch value[i+1] {
				case 'n':
					b.WriteByte('\n')
				case 'r':
					b.WriteByte('\r')
				case 't':
					b.WriteByte('\t')
				default:
					b.WriteByte(value[i+1])
				}
				i += 2
				continue
			case c == '%' && i+1 < len(value) && value[i+1] == '%':
				b.WriteByte('%')
				i += 2
				continue
			case c == '%':
				return nil, errSpecifier
			case exec && c == '$' && i+1 < len(value) && value[i+1] == '$':
				b.WriteByte('$')
				i += 2
				continue
			case exec && c == '$':
				return nil, errExpands
			default:
				b.WriteByte(c)
				i++
				continue
			}
			break
		}
		if quoted {
			return nil, errUnbalanced
		}
		words = append(words, b.String())
	}
	return words, nil
}

type parseError string

func (e parseError) Error() string { return string(e) }

const (
	errUnbalanced = parseError("unbalanced quoting")
	errSpecifier  = parseError("a lone % is a specifier")
	errExpands    = parseError("a lone $ expands a variable")
)
