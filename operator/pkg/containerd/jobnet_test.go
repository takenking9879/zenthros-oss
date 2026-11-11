// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/systemd"
)

// On a backend other Workspaces share, a container job runs in its own
// network namespace, never the host's: --net-host gave every tenant the
// instance metadata (the machine's role, the join token) and the others'
// ports. Its DNS comes from a resolv.conf bound read-only, and the
// metadata range, the host's addresses and the other jobs are dropped. A
// private backend keeps the host's network.
func TestSharedBackendJobRunsInItsOwnNetwork(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem}
	spec := jobspec.JobSpec{ID: "job-1", Image: "img", IsolatedNetwork: true}
	opts := RunOptsFromSpec(spec, cfg, nil, "")
	script := RenderRunScript(opts)
	ns := JobNetName("job-1")
	if opts.NetNS != ns {
		t.Fatalf("NetNS = %q, want %q", opts.NetNS, ns)
	}
	if strings.Contains(script, "--net-host") {
		t.Fatalf("isolated job keeps the host's network:\n%s", script)
	}
	for _, want := range []string{
		`--with-ns "network:/var/run/netns/$zj_ns"`,
		"zj_ns=" + ns,
		"ip netns add",
		"-d 169.254.0.0/16 -j DROP",
		"-d " + JobNetCIDR + " -j DROP",
		"ZENTHROS-JOBNET-IN -i \"$zj_host\" -j DROP",
		"MASQUERADE",
		"disable_ipv6=1",
		"src=" + opts.ResolvConf + ",dst=/etc/resolv.conf,options=rbind:ro",
		"--dport 53 -j ACCEPT",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q", want)
		}
	}
	// DNS is allowed before the metadata range is dropped: on GCP the
	// resolver is 169.254.169.254 itself.
	if strings.Index(script, "--dport 53 -j ACCEPT") > strings.Index(script, "-d 169.254.0.0/16 -j DROP") {
		t.Error("the metadata drop comes before the DNS accept")
	}

	// Replies to a connection the host opened come back in; nothing the job
	// opens toward the host does. Without the accept before the drop the
	// host could not reach a job's port at all (seen on a real machine).
	established := strings.Index(script, "ZENTHROS-JOBNET-IN -i \"$zj_host\" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT")
	if established < 0 || established > strings.Index(script, "ZENTHROS-JOBNET-IN -i \"$zj_host\" -j DROP") {
		t.Error("replies to the host's own connections are not accepted before the inbound drop")
	}

	private := RenderRunScript(RunOptsFromSpec(jobspec.JobSpec{ID: "job-2", Image: "img"}, cfg, nil, ""))
	if !strings.Contains(private, "--net-host") || strings.Contains(private, "ip netns add") {
		t.Fatalf("private job:\n%s", private)
	}
}

// The unit's ExecStopPost runs `ctr-run --zenthros-net-down`: it removes the
// job's network and exits, before any of the job's own setup or ctr run, and
// is a no-op for a job that never had one.
func TestNetDownRunsNothingElse(t *testing.T) {
	for _, isolated := range []bool{true, false} {
		dir := t.TempDir()
		bin := filepath.Join(dir, "bin")
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatal(err)
		}
		log := filepath.Join(dir, "calls")
		for _, tool := range []string{"ip", "iptables", "ctr", "flock", "sysctl", "mkdir"} {
			stub := "#!/bin/sh\necho " + tool + " \"$@\" >> " + log + "\n"
			if err := os.WriteFile(filepath.Join(bin, tool), []byte(stub), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		opts := RunOptsFromSpec(jobspec.JobSpec{ID: "job-1", Image: "img", IsolatedNetwork: isolated}, systemd.Config{Scope: systemd.ScopeSystem}, nil, "")
		script := strings.ReplaceAll(RenderRunScript(opts), "/usr/local/bin/ctr", "ctr")
		path := filepath.Join(dir, "ctr-run")
		if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("bash", path, NetDownArg)
		cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin"}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated=%v: %v\n%s", isolated, err, out)
		}
		calls, _ := os.ReadFile(log)
		if strings.Contains(string(calls), "ctr ") || strings.Contains(string(calls), "netns add") || strings.Contains(string(calls), "mkdir") {
			t.Fatalf("isolated=%v: net-down ran the job's setup:\n%s", isolated, calls)
		}
		if isolated && !strings.Contains(string(calls), "ip netns del "+JobNetName("job-1")) {
			t.Fatalf("net-down did not remove the namespace:\n%s", calls)
		}
		if !isolated && len(calls) != 0 {
			t.Fatalf("net-down of a host-network job ran:\n%s", calls)
		}
	}
}
