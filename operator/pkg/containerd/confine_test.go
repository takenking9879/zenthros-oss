// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zenthros/operator/pkg/backend"
)

// runWrapper executes a rendered ctr-run with ctr replaced by printf, the way
// systemd would, and returns the argv ctr would have received.
func runWrapper(t *testing.T, opts RunOpts) []string {
	t.Helper()
	scratch := t.TempDir()
	script := strings.Replace(RenderRunScript(opts), "/usr/local/bin/ctr", "printf '%s\\n'", 1)
	script = strings.ReplaceAll(script, "mkdir -p '"+PreemptDir+"'", "mkdir -p '"+scratch+"/run/preempt'")
	script = strings.ReplaceAll(script, "mkdir -p '/var/run/zenthros'", "mkdir -p '"+scratch+"/run'")
	path := filepath.Join(scratch, "ctr-run")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("bash", path).CombinedOutput()
	if err != nil {
		t.Fatalf("wrapper failed: %v\n%s\n--- script:\n%s", err, out, script)
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

// Host paths come from the job spec. The root-run wrapper must pass them to
// ctr as data, never run them.
func TestRunScriptNeverExecutesSpecPaths(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "pwned")
	evil := filepath.Join(dir, "src$(touch "+marker+")")
	argv := runWrapper(t, RunOpts{
		Image:     "docker.io/library/python:3.13",
		Container: "zenthros-j",
		TmpDir:    filepath.Join(dir, "tmp"),
		HostPath:  evil,
		Cwd:       evil,
	})
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("a spec path ran as a command in the wrapper")
	}
	want := "type=bind,src=" + evil + ",dst=" + evil + ",options=rbind:rw"
	found := false
	for _, arg := range argv {
		found = found || arg == want
	}
	if !found {
		t.Fatalf("mount argument not passed verbatim; argv:\n%s", strings.Join(argv, "\n"))
	}
}

func TestConfineHostMounts(t *testing.T) {
	roots := []string{"/srv/data"}
	identity := func(p string) (string, error) { return p, nil }
	jobDir := "/var/lib/zenthros/jobs/j"

	t.Run("hostPath under a root is mounted", func(t *testing.T) {
		opts := RunOpts{HostPath: "/srv/data/set", Cwd: "/srv/data/set"}
		if err := opts.ConfineHostMounts(jobDir, roots, identity); err != nil {
			t.Fatal(err)
		}
	})
	for _, hostPath := range []string{"/", "/etc", "/srv/database", "/srv/data/../../root", "relative/dir"} {
		t.Run("hostPath "+hostPath+" is refused", func(t *testing.T) {
			opts := RunOpts{HostPath: hostPath}
			err := opts.ConfineHostMounts(jobDir, roots, identity)
			if !errors.Is(err, backend.ErrRejected) {
				t.Fatalf("err = %v, want ErrRejected", err)
			}
		})
	}
	t.Run("no roots means no hostPath", func(t *testing.T) {
		opts := RunOpts{HostPath: "/srv/data/set"}
		if err := opts.ConfineHostMounts(jobDir, nil, identity); !errors.Is(err, backend.ErrRejected) {
			t.Fatalf("err = %v, want ErrRejected", err)
		}
	})
	t.Run("a symlink out of the root is refused", func(t *testing.T) {
		escape := func(p string) (string, error) {
			return strings.Replace(p, "/srv/data/link", "/etc", 1), nil
		}
		opts := RunOpts{HostPath: "/srv/data/link"}
		if err := opts.ConfineHostMounts(jobDir, roots, escape); !errors.Is(err, backend.ErrRejected) {
			t.Fatalf("err = %v, want ErrRejected", err)
		}
	})
	t.Run("workingDir outside the job is backed by a private dir", func(t *testing.T) {
		opts := RunOpts{Cwd: "/root", TmpDir: jobDir + "/tmp"}
		if err := opts.ConfineHostMounts(jobDir, roots, identity); err != nil {
			t.Fatal(err)
		}
		for _, m := range mounts(opts) {
			if m.Dest == "/root" && m.Source != jobDir+"/cwd/root" {
				t.Fatalf("workingDir bound from host %q, want the job's private dir", m.Source)
			}
		}
	})
}

// The container binds the path that was checked, not the name that was
// given: the name is a symlink the job's owner can point elsewhere between
// the check and the mount (07#1).
func TestTheCheckedPathIsTheOneMounted(t *testing.T) {
	opts := RunOpts{TmpDir: "/var/lib/zenthros/j/tmp", HostPath: "/srv/data/link"}
	resolve := func(string) (string, error) { return "/srv/data/real", nil }
	if err := opts.ConfineHostMounts("/var/lib/zenthros/j", []string{"/srv/data"}, resolve); err != nil {
		t.Fatal(err)
	}
	script := RenderRunScript(opts)
	if !strings.Contains(script, "src=/srv/data/real,dst=/srv/data/link") {
		t.Fatalf("the script does not bind the resolved path at the given one:\n%s", script)
	}
}

// A mount whose path holds a comma or a newline is refused, not left out:
// ctr reads its mount argument comma-separated, so the path would add
// options (",options=rbind:rw"), and leaving it out ran the job without
// the directory it asked for (07#3).
func TestAMountThatCtrCannotTakeIsRefused(t *testing.T) {
	resolve := func(path string) (string, error) { return path, nil }
	for _, path := range []string{"/srv/data/a,options=rbind:rw", "/srv/data/a\nb"} {
		opts := RunOpts{TmpDir: "/var/lib/zenthros/j/tmp", HostPath: path}
		if err := opts.ConfineHostMounts("/var/lib/zenthros/j", []string{"/srv/data"}, resolve); !errors.Is(err, backend.ErrRejected) {
			t.Errorf("hostPath %q: %v, want refused", path, err)
		}
	}
	working := RunOpts{TmpDir: "/var/lib/zenthros/j/tmp", Cwd: "/opt/a,b"}
	if err := working.ConfineHostMounts("/var/lib/zenthros/j", nil, resolve); !errors.Is(err, backend.ErrRejected) {
		t.Errorf("a working directory with a comma: %v, want refused", err)
	}
}
