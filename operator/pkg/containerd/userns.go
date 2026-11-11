// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"context"
	"strings"

	"github.com/zenthros/operator/pkg/systemd"
)

// A container job runs in a user namespace of its Workspace's (RunOpts.UserNS)
// when the machine can: the image's users, its root first, are a range of
// uids no user of the host has, one range per Workspace on the machine, and
// the job's own user (systemd.JobUser) is itself on both sides, so its
// directories, secrets and report socket stay its own. ctr maps the image's
// layers with --remap-labels (an idmapped mount, no copy per range).
//
// The kernel mounts a sysfs only for the user namespace that owns the
// network: a job with the host's network gets the host's /sys bound
// read-only (recursively: a user namespace may not bind it without its
// submounts, and rro makes them read-only too), and a job with a network
// of its own (jobnet.go) has that network made inside its user namespace,
// both kept as files for ctr to join. GPU jobs keep none: the NVIDIA hook
// is not tested in one.

// UserNSDir holds the kept user namespaces of jobs with a network of their
// own, one file per network, named as it.
const UserNSDir = "/run/zenthros/userns"

// idMapFunction is the run script's zj_idmap ID: the inside:outside:count
// ranges of uid (or gid) ID's user namespace. Inside 0..65535; ID is itself,
// every other id is 2^30 + ID*2^16 + itself, so two ids' ranges never meet
// and none is below 2^30, where a host's users are. ID is a job user's, a
// system user (useradd --system, under 1000): the ranges fit below 2^32 for
// any ID under 49151.
const idMapFunction = `zj_idmap() {
  local id="$1" base=$(( (1 << 30) + ($1 << 16) ))
  echo "0:${base}:${id} ${id}:${id}:1 $(( id + 1 )):$(( base + id + 1 )):$(( 65535 - id ))"
}
`

// UserNamespaces reports whether this machine can run container jobs in user
// namespaces, and when not, why: containerd's overlayfs snapshotter must
// remap ids (remap-ids, containerd's own test of the kernel's idmapped
// mounts on overlayfs, Linux 5.19 and newer), and unshare must be there to
// make a job's network inside one.
func UserNamespaces(ctx context.Context, runner systemd.Runner, sudo string) (bool, string) {
	out, err := runner.Run(ctx, sudo+"ctr plugins ls -d id==overlayfs")
	if err != nil {
		return false, "containerd does not answer: " + err.Error()
	}
	if !remapsIDs(out) {
		return false, "containerd's overlayfs cannot remap ids (it needs Linux 5.19 or newer)"
	}
	if _, err := runner.Run(ctx, "command -v unshare"); err != nil {
		return false, "no unshare to make a job's network inside one"
	}
	return true, ""
}

// remapsIDs reads `ctr plugins ls -d` output for the remap-ids capability.
func remapsIDs(plugins string) bool {
	for _, line := range strings.Split(plugins, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(key) != "Capabilities" {
			continue
		}
		for _, c := range strings.Split(value, ",") {
			if strings.TrimSpace(c) == "remap-ids" {
				return true
			}
		}
	}
	return false
}

// userNS reports whether the job runs in a user namespace.
func (o RunOpts) userNS() bool {
	return o.UserNS && o.User != "" && o.GPUs == 0
}

// userNSNetUp makes the job's network namespace inside a new user namespace
// with the job's map, and keeps both: the user namespace at $zj_userns, the
// network where `ip netns` keeps its own. The map is written before either
// is kept, by the parent (root), in one write each: the kernel takes the
// first write as the whole map, and bash's printf and echo write a line at
// a time; cat writes what it read at once.
const userNSNetUp = `mkdir -p ` + UserNSDir + `
touch "$zj_userns"
coproc zj_holder { exec unshare --user --net sh -c 'read -r _'; }
zj_pid=$zj_holder_PID
until [ "$(readlink "/proc/$zj_pid/ns/user")" != "$(readlink /proc/self/ns/user)" ]; do sleep 0.05; done
zj_ranges() { local r; for r in $1; do echo "${r//:/ }"; done; }
cat > "/proc/$zj_pid/uid_map" <<<"$(zj_ranges "$job_uidmap")"
cat > "/proc/$zj_pid/gid_map" <<<"$(zj_ranges "$job_gidmap")"
mount --bind "/proc/$zj_pid/ns/user" "$zj_userns"
ip netns attach "$zj_ns" "$zj_pid"
echo >&"${zj_holder[1]}"
wait "$zj_pid" || true
`
