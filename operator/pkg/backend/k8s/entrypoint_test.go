// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"fmt"
	"github.com/zenthros/operator/pkg/shellword"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

// list2cmdline is Python's subprocess.list2cmdline, which Ray's
// `ray job submit -- ARGS...` uses to turn the submitter's words back into
// the entrypoint string (ray 2.53, dashboard/modules/job/cli.py).
func list2cmdline(args []string) string {
	var b strings.Builder
	for i, arg := range args {
		if i > 0 {
			b.WriteByte(' ')
		}
		quote := arg == "" || strings.ContainsAny(arg, " \t")
		if quote {
			b.WriteByte('"')
		}
		backslashes := 0
		for _, c := range arg {
			switch c {
			case '\\':
				backslashes++
			case '"':
				b.WriteString(strings.Repeat(`\`, backslashes*2) + `\"`)
				backslashes = 0
			default:
				b.WriteString(strings.Repeat(`\`, backslashes))
				backslashes = 0
				b.WriteRune(c)
			}
		}
		b.WriteString(strings.Repeat(`\`, backslashes))
		if quote {
			b.WriteString(strings.Repeat(`\`, backslashes))
			b.WriteByte('"')
		}
	}
	return b.String()
}

func shellOutput(t *testing.T, shell string, script string, home string) string {
	t.Helper()
	cmd := exec.Command(shell, "-c", script)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home}
	cmd.Dir = t.TempDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "error: " + err.Error() + ": " + string(out)
	}
	return string(out)
}

// throughSubmitter runs an entrypoint the way a dedicated cluster does: the
// submitter's bash reads the line KubeRay splices it into, Ray rejoins the
// words with list2cmdline, and Ray's job shell runs the result.
func throughSubmitter(t *testing.T, entrypoint string) string {
	t.Helper()
	words := shellOutput(t, "bash", `printf '%s\0' -- `+entrypoint, "/submitter")
	if strings.HasPrefix(words, "error: ") {
		return words
	}
	argv := strings.Split(strings.TrimSuffix(words, "\x00"), "\x00")[1:]
	return shellOutput(t, "sh", list2cmdline(argv), "/ray")
}

func dedicatedSpec(t *testing.T) jobspec.JobSpec {
	spec := testSpec()
	spec.ID = fmt.Sprintf("test-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = os.Remove("/tmp/zenthros-entrypoint-" + spec.ID) })
	return spec
}

// Found in the local E2E: `python -c "print('ok')"` reached Ray as
// `python -c print('ok')`. Whatever the user wrote runs as if one shell read
// it, on a dedicated cluster too.
func TestDedicatedClusterRunsEntrypointAsWritten(t *testing.T) {
	for _, entrypoint := range []string{
		`printf '%s\n' "print('E2E_RAY_OK')"`,
		`printf '%s|' "a;b" '$HOME' "$HOME" "two  spaces"`,
		`x=kept; echo "x=$x"; echo /tmp/$x`,
		`echo plain words --lr=0.1`,
	} {
		t.Run(entrypoint, func(t *testing.T) {
			spec := dedicatedSpec(t)
			spec.Entrypoint = entrypoint
			want := shellOutput(t, "sh", entrypoint, "/ray")
			if got := throughSubmitter(t, renderEntrypoint(spec)); got != want {
				t.Fatalf("ran %q, want %q (entrypoint %q)", got, want, renderEntrypoint(spec))
			}
		})
	}
}

// The structured form means exactly these argv words. A word the submitter's
// shells would expand must not be: `x=kept; echo /tmp/$x` expanded early is
// `echo /tmp/`.
func TestDedicatedClusterRunsStructuredCommandAsWritten(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command []string
	}{
		{"metachars without blanks", []string{"printf", "%s|", "print('x')", "a;b", "$HOME", "it's"}},
		{"shell script with expansions", []string{"sh", "-c", "x=kept; echo $(echo sub) /tmp/$x", "arg0"}},
		{"plain words with blanks", []string{"echo", "--message", "hello  world"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := dedicatedSpec(t)
			spec.Entrypoint = ""
			spec.Command, spec.Args = tc.command[:1], tc.command[1:]
			quoted := make([]string, len(tc.command))
			for i, word := range tc.command {
				quoted[i] = shellword.Quote(word)
			}
			want := shellOutput(t, "sh", strings.Join(quoted, " "), "/ray")
			if got := throughSubmitter(t, renderEntrypoint(spec)); got != want {
				t.Fatalf("ran %q, want %q (entrypoint %q)", got, want, renderEntrypoint(spec))
			}
		})
	}
}

// Entrypoints that already come through the submitter unchanged stay
// readable, and existing RayJobs keep matching their spec.
func TestEntrypointEncodedOnlyWhenNeeded(t *testing.T) {
	spec := dedicatedSpec(t)
	spec.Entrypoint = "python train.py --epochs 3 --lr=0.1"
	if got := renderEntrypoint(spec); got != spec.Entrypoint {
		t.Errorf("plain entrypoint rendered %q", got)
	}
	structured := structuredSpec()
	if got, want := renderEntrypoint(structured), `'python' 'train.py' '--message' 'hello world'`; got != want {
		t.Errorf("plain structured command rendered %q, want %q", got, want)
	}
	standing := dedicatedSpec(t)
	standing.Entrypoint = `python -c "print('x')"`
	standing.RayCluster = &v1.RayClusterReference{Name: "standing", Namespace: "tenant-default"}
	if got := renderEntrypoint(standing); got != standing.Entrypoint {
		t.Errorf("standing cluster entrypoint rendered %q; HTTP submission has one shell", got)
	}
}
