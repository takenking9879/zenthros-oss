// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/takenking9879/zenthros-oss/operator/api/v1"
	"github.com/takenking9879/zenthros-oss/operator/pkg/jobspec"
	"github.com/takenking9879/zenthros-oss/operator/pkg/systemd"
)

// The unit's ExecStop runs `ctr-run --zenthros-stop`: it stops the job's
// task itself, SIGTERM, StopGrace, then SIGKILL to every process of the
// task, and deletes the task (its shim goes with it) and, unless the job
// keeps it, the container. systemd's own stop reached only ctr, which
// passes SIGTERM on to the task's PID 1, which ignores it without a handler:
// a time.sleep job cut by its timeout ran on for minutes and wrote its
// result after the hub said it failed (38). Nothing of the job's setup runs.
func TestStopKillsTheTaskThatIgnoresSIGTERM(t *testing.T) {
	for _, tc := range []struct {
		name       string
		task       string // RUNNING, or "" for none
		leavesOn   string // the signal the task exits on
		lifespan   string
		wantCalls  []string
		wantAbsent []string
	}{
		{"ignores SIGTERM", "RUNNING", "SIGKILL", "", []string{
			"tasks kill zj-name", "tasks kill -a -s SIGKILL zj-name", "tasks delete -f zj-name", "containers delete zj-name"}, nil},
		{"leaves on SIGTERM", "RUNNING", "SIGTERM", "", []string{
			"tasks kill zj-name", "tasks delete -f zj-name", "containers delete zj-name"}, []string{"SIGKILL"}},
		{"kept", "RUNNING", "SIGTERM", v1.LifespanRetain, []string{"tasks kill zj-name", "tasks delete -f zj-name"}, []string{"containers delete"}},
		{"no task", "", "", "", nil, []string{"kill", "delete"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			if err := os.MkdirAll(bin, 0o755); err != nil {
				t.Fatal(err)
			}
			calls, state := filepath.Join(dir, "calls"), filepath.Join(dir, "state")
			if err := os.WriteFile(state, []byte(tc.task), 0o600); err != nil {
				t.Fatal(err)
			}
			// A ctr whose task is RUNNING until the signal it leaves on.
			ctr := `#!/bin/bash
shift 2  # -n zenthros
echo "$*" >> ` + calls + `
case "$1 $2" in
"tasks ls") [ "$(cat ` + state + `)" = RUNNING ] && echo "zj-name 42 RUNNING"; exit 0 ;;
"tasks kill") sig=SIGTERM; [ "$3" = -a ] && sig=$5; [ "$sig" = "` + tc.leavesOn + `" ] && echo STOPPED > ` + state + `; exit 0 ;;
esac
exit 0
`
			for name, body := range map[string]string{
				"ctr":   ctr,
				"sleep": "#!/bin/sh\necho sleep >> " + calls + "\n",
				"mkdir": "#!/bin/sh\necho mkdir \"$@\" >> " + calls + "\n",
				"ip":    "#!/bin/sh\necho ip \"$@\" >> " + calls + "\n",
			} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			opts := RunOptsFromSpec(jobspec.JobSpec{ID: "job-1", Image: "img", IsolatedNetwork: true, Lifespan: tc.lifespan},
				systemd.Config{Scope: systemd.ScopeSystem}, nil, "")
			opts.Container = "zj-name"
			script := strings.ReplaceAll(RenderRunScript(opts), "/usr/local/bin/ctr", "ctr")
			path := filepath.Join(dir, "ctr-run")
			if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", path, StopArg)
			cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin"}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("stop: %v\n%s", err, out)
			}
			raw, _ := os.ReadFile(calls)
			got := string(raw)
			at := 0
			for _, want := range tc.wantCalls {
				i := strings.Index(got[at:], want+"\n")
				if i < 0 {
					t.Fatalf("stop did not run %q after the calls before it:\n%s", want, got)
				}
				at += i + len(want)
			}
			for _, absent := range append(tc.wantAbsent, "mkdir", "ip ", "run ") {
				if strings.Contains(got, absent) {
					t.Fatalf("stop ran %q:\n%s", absent, got)
				}
			}
			if tc.leavesOn == "SIGKILL" && strings.Count(got, "sleep\n") < StopGrace {
				t.Fatalf("SIGKILL came before the %d s grace:\n%s", StopGrace, got)
			}
		})
	}
}
