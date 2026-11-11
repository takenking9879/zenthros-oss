// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"context"
	"errors"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/systemd"
)

// idMap runs the run script's zj_idmap for id: the ranges ctr maps, as
// inside:outside:count.
func idMap(t *testing.T, id int) []string {
	t.Helper()
	out, err := exec.Command("bash", "-c", idMapFunction+`zj_idmap "$1"`, "bash", strconv.Itoa(id)).Output()
	if err != nil {
		t.Fatalf("zj_idmap %d: %v", id, err)
	}
	return strings.Fields(string(out))
}

// In a user namespace a job's image users are its Workspace's own range on
// the machine: the image's root and every other user of it are 2^30 + uid
// * 2^16 onward, never a user of the host, and two Workspaces' ranges never
// meet. The job's own user is itself on both sides: its directories, its
// secrets and its report socket's peer are the user's, as without one.
func TestAJobsImageUsersAreItsWorkspacesRange(t *testing.T) {
	got := idMap(t, 997)
	want := []string{"0:1139081216:997", "997:997:1", "998:1139082214:64538"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("zj_idmap 997 = %v, want %v", got, want)
	}
	covered := map[int]bool{}
	outside := map[int][2]int{}
	for _, uid := range []int{997, 998} {
		lo, hi := 1<<62, 0
		for _, r := range idMap(t, uid) {
			f := strings.Split(r, ":")
			in, _ := strconv.Atoi(f[0])
			out, _ := strconv.Atoi(f[1])
			n, _ := strconv.Atoi(f[2])
			if in == uid {
				if out != uid || n != 1 {
					t.Errorf("uid %d: its own user maps to %d (%d), want itself", uid, out, n)
				}
				continue
			}
			if out < 1<<30 {
				t.Errorf("uid %d: %d..%d inside is the host's %d..: a user of the host", uid, in, in+n-1, out)
			}
			lo, hi = min(lo, out), max(hi, out+n-1)
			if uid == 997 {
				for i := in; i < in+n; i++ {
					covered[i] = true
				}
			}
		}
		outside[uid] = [2]int{lo, hi}
	}
	if len(covered) != 65535 {
		t.Errorf("uid 997 maps %d of the image's 65536 users besides itself, want 65535", len(covered))
	}
	if a, b := outside[997], outside[998]; a[1] >= b[0] {
		t.Errorf("two Workspaces' ranges meet: %v and %v", a, b)
	}
}

// A job with the host's network gets the user namespace from runc, and the
// host's /sys bound read-only where containerd would mount a sysfs: the
// kernel mounts one only for the user namespace that owns the network,
// here the host's. The flags come before the image. A job with GPUs keeps
// no user namespace (the NVIDIA hook is not tested in one), nor does one
// without a job user (the user scope).
func TestAHostNetworkJobRunsInItsWorkspacesUserNamespace(t *testing.T) {
	opts := RunOpts{Image: "i", Container: "c", User: "zj-0a1b2c3d", UserNS: true}
	script := RenderRunScript(opts)
	for _, want := range []string{
		`job_uidmap=$(zj_idmap "$job_uid")`,
		`for zj_m in $job_uidmap; do args+=(--uidmap "$zj_m"); done`,
		`for zj_m in $job_gidmap; do args+=(--gidmap "$zj_m"); done`,
		"args+=(--remap-labels)\n",
		"args+=(--mount 'type=bind,src=/sys,dst=/sys,options=rbind:rro:nosuid:nodev:noexec')\n",
		"args+=(--user \"$job_uid:$job_gid\")\n",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("run script lacks %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "--with-ns \"user:") {
		t.Errorf("a host network job joins a user namespace of its own making:\n%s", script)
	}
	image := strings.Index(script, `args+=("$image" "$name")`)
	for _, flag := range []string{"--uidmap", "--remap-labels", "src=/sys"} {
		if i := strings.Index(script, flag); i < 0 || i > image {
			t.Errorf("%s comes after the image:\n%s", flag, script)
		}
	}
	if out, err := exec.Command("bash", "-n", "-c", script).CombinedOutput(); err != nil {
		t.Fatalf("run script is not valid bash: %v\n%s", err, out)
	}
	for name, o := range map[string]RunOpts{
		"gpu":     {Image: "i", Container: "c", User: "zj-0a1b2c3d", UserNS: true, GPUs: 1},
		"no user": {Image: "i", Container: "c", UserNS: true},
		"off":     {Image: "i", Container: "c", User: "zj-0a1b2c3d"},
	} {
		if s := RenderRunScript(o); strings.Contains(s, "--uidmap") || strings.Contains(s, "src=/sys") {
			t.Errorf("%s: a user namespace:\n%s", name, s)
		}
	}
}

// The host's /sys, bound recursively, brings the host's cgroup tree with it:
// every unit's processes and memory, read-only. A job's own sysfs showed none
// (containerd mounts no cgroup in a job), and neither does the bound one: an
// empty read-only tmpfs covers it, after the bind, so that it lands on top
// (independent review of A.5, 2026-11-10; seen in a containerd sandbox: 69
// cgroup2 entries).
func TestAHostNetworkJobSeesNoCgroupOfTheHost(t *testing.T) {
	script := RenderRunScript(RunOpts{Image: "i", Container: "c", User: "zj-0a1b2c3d", UserNS: true})
	bind := strings.Index(script, "src=/sys,dst=/sys,")
	mask := strings.Index(script, "args+=(--mount 'type=tmpfs,src=tmpfs,dst=/sys/fs/cgroup,options=ro:nosuid:nodev:noexec')\n")
	image := strings.Index(script, `args+=("$image" "$name")`)
	if bind < 0 || mask < bind || mask > image {
		t.Fatalf("the host's cgroups are not covered after its /sys is bound (bind at %d, cover at %d, image at %d):\n%s", bind, mask, image, script)
	}
}

// On a shared backend the job's network is made inside its user namespace,
// which then owns it: runc can mount the job's own /sys (the kernel refuses
// a sysfs for a network another user namespace owns), and the job sees its
// own interfaces. Both namespaces are kept as files; the map is written
// before either is kept, the job's uid known before the network, and the
// network is gone with the user namespace's file at the end (zj_down, and
// the agent's sweep).
func TestASharedJobsNetworkIsMadeInsideItsUserNamespace(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem}
	opts := RunOptsFromSpec(jobspec.JobSpec{ID: "job-1", Workspace: "ws", Image: "img", IsolatedNetwork: true}, cfg, nil, "")
	opts.User, opts.UserNS = "zj-0a1b2c3d", true
	script := RenderRunScript(opts)
	order := []string{
		"job_uid=$(id -u 'zj-0a1b2c3d')\n",
		"zj_down\n",
		"coproc zj_holder { exec unshare --user --net sh -c 'read -r _'; }\n",
		`cat > "/proc/$zj_pid/uid_map" <<<"$(zj_ranges "$job_uidmap")"`,
		`cat > "/proc/$zj_pid/gid_map" <<<"$(zj_ranges "$job_gidmap")"`,
		`mount --bind "/proc/$zj_pid/ns/user" "$zj_userns"`,
		`ip netns attach "$zj_ns" "$zj_pid"`,
		`ip link set "$zj_peer" netns "$zj_ns"`,
		`args=(/usr/local/bin/ctr -n zenthros run --rm --read-only --with-ns "user:$zj_userns" --with-ns "network:/var/run/netns/$zj_ns")`,
		`args+=("$image" "$name")`,
	}
	at := -1
	for _, want := range order {
		i := strings.Index(script[at+1:], want)
		if i < 0 {
			t.Fatalf("after %d, run script lacks %q:\n%s", at, want, script)
		}
		at += 1 + i
	}
	// The kernel takes the first write as the whole map: bash's printf
	// wrote it a line at a time, and the second line was refused
	// ("Invalid argument", in a containerd sandbox).
	if regexp.MustCompile(`(printf|echo)[^\n]*> "/proc/\$zj_pid/[ug]id_map"`).MatchString(script) {
		t.Errorf("a map written by a builtin that writes a line at a time:\n%s", script)
	}
	if strings.Contains(script, `ip netns add "$zj_ns"`) {
		t.Errorf("the job's network is made outside its user namespace:\n%s", script)
	}
	if strings.Contains(script, "src=/sys") {
		t.Errorf("a job with a network of its own gets the host's /sys:\n%s", script)
	}
	for _, s := range []string{script, JobNetDownScript(opts.NetNS)} {
		if !strings.Contains(s, `umount "$zj_userns" 2>/dev/null || true`) || !strings.Contains(s, `rm -f "$zj_userns"`) {
			t.Errorf("zj_down leaves the user namespace's file:\n%s", s)
		}
	}
	if out, err := exec.Command("bash", "-n", "-c", script).CombinedOutput(); err != nil {
		t.Fatalf("run script is not valid bash: %v\n%s", err, out)
	}
	// Without a user namespace the network is made as before.
	opts.UserNS = false
	if s := RenderRunScript(opts); !strings.Contains(s, `ip netns add "$zj_ns"`) || strings.Contains(s, "unshare") {
		t.Errorf("without a user namespace:\n%s", s)
	}
}

type probeRunner struct {
	out map[string]string
	err map[string]error
	ran []string
}

func (r *probeRunner) Run(_ context.Context, cmd string) (string, error) {
	r.ran = append(r.ran, cmd)
	for prefix, err := range r.err {
		if strings.Contains(cmd, prefix) {
			return "", err
		}
	}
	for prefix, out := range r.out {
		if strings.Contains(cmd, prefix) {
			return out, nil
		}
	}
	return "", nil
}

// A machine runs container jobs in user namespaces when containerd's
// overlayfs snapshotter remaps ids (remap-ids: containerd's own test of the
// kernel's idmapped mounts on overlayfs, Linux 5.19 and newer) and unshare
// is there to make a job's network inside one. Anything else, a containerd
// that does not answer included, keeps them without, with the reason.
func TestUserNamespacesNeedContainerdToRemapIDs(t *testing.T) {
	const plugins = "Type:          io.containerd.snapshotter.v1\nID:            overlayfs\nCapabilities:  remap-ids,only-remap-ids,rebase\nStatus:        ok\n"
	for name, c := range map[string]struct {
		r    probeRunner
		ok   bool
		says string
	}{
		"remaps":         {r: probeRunner{out: map[string]string{"plugins ls": plugins, "command -v unshare": "/usr/bin/unshare\n"}}, ok: true},
		"an old kernel":  {r: probeRunner{out: map[string]string{"plugins ls": strings.Replace(plugins, "remap-ids,only-remap-ids,", "", 1), "command -v unshare": "/usr/bin/unshare\n"}}, says: "remap"},
		"no unshare":     {r: probeRunner{out: map[string]string{"plugins ls": plugins}, err: map[string]error{"command -v unshare": errors.New("exit status 1")}}, says: "unshare"},
		"no containerd":  {r: probeRunner{err: map[string]error{"plugins ls": errors.New("ctr: not found")}}, says: "containerd"},
		"no capabilitie": {r: probeRunner{out: map[string]string{"plugins ls": "Type: io.containerd.snapshotter.v1\nID: overlayfs\n", "command -v unshare": "/usr/bin/unshare\n"}}, says: "remap"},
	} {
		t.Run(name, func(t *testing.T) {
			ok, why := UserNamespaces(context.Background(), &c.r, "sudo ")
			if ok != c.ok || !strings.Contains(why, c.says) {
				t.Fatalf("UserNamespaces = %v, %q; want %v, saying %q", ok, why, c.ok, c.says)
			}
			if !strings.HasPrefix(c.r.ran[0], "sudo ctr plugins ls -d id==overlayfs") {
				t.Errorf("asked %q", c.r.ran[0])
			}
		})
	}
}
