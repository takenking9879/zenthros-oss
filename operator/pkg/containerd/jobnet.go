// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/zenthros/operator/pkg/shellword"
	"strings"

	"github.com/zenthros/operator/pkg/systemd"
)

// A container job on a backend several Workspaces share gets a network
// namespace of its own instead of the host's: a veth
// pair to the host, NAT out, and nothing else. From inside it the instance
// metadata (169.254.0.0/16: the machine's role and its user-data), the
// host's own addresses and every other job's network are unreachable; DNS
// goes to the host's upstream servers, which a resolv.conf names because the
// host's 127.0.0.53 stub is not in the namespace. IPv6 is off in it, which
// also takes fd00:ec2::254 away. No CNI: the plugins would have to be on
// every image.
//
// The run script sets it up before `ctr run --with-ns network:...`;
// `ctr-run --zenthros-net-down` (the unit's ExecStopPost) and the agent's
// sweep at start remove it.

// JobNetCIDR is where each job's /30 comes from.
const JobNetCIDR = "10.213.0.0/16"

// NetDownArg makes the run script remove the job's network and exit.
const NetDownArg = systemd.ContainerNetDownArg

// JobNetPrefix starts every job network's namespace name.
const JobNetPrefix = "zj-"

// JobNetName is the job's network namespace: zj-<8 hex of its id>. Its veth
// pair is zjh<hex> (host) and zjp<hex> (peer), 11 characters, under the
// kernel's 15.
func JobNetName(jobID string) string {
	sum := sha256.Sum256([]byte(jobID))
	return JobNetPrefix + hex.EncodeToString(sum[:4])
}

// jobNetFunctions are the bash functions both directions need.
func jobNetFunctions(ns string) string {
	id := strings.TrimPrefix(ns, JobNetPrefix)
	var b strings.Builder
	fmt.Fprintf(&b, "zj_ns=%s\nzj_host=zjh%s\nzj_peer=zjp%s\nzj_comment=zenthros-jobnet:%s\nzj_userns=%s/%s\n", ns, id, id, ns, UserNSDir, ns)
	b.WriteString(`zj_flush() {
  local table="$1" chain="$2" rule
  iptables -t "$table" -S "$chain" 2>/dev/null | grep -F -- "$zj_comment" | sed 's/^-A /-D /' |
    while IFS= read -r rule; do eval "iptables -t $table $rule" || true; done || true
}
zj_down() {
  ip netns del "$zj_ns" 2>/dev/null || true
  umount "$zj_userns" 2>/dev/null || true
  rm -f "$zj_userns"
  ip link del "$zj_host" 2>/dev/null || true
  zj_flush filter ZENTHROS-JOBNET
  zj_flush filter ZENTHROS-JOBNET-IN
  zj_flush nat ZENTHROS-JOBNET-NAT
}
`)
	return b.String()
}

// JobNetDownScript removes the network of the job whose namespace is ns
// (the agent's sweep of networks no unit owns).
func JobNetDownScript(ns string) string {
	return "set -u\n" + jobNetFunctions(ns) + "zj_down\n"
}

// jobNetUp sets the namespace up, inside the job's user namespace when
// userns (userNSNetUp); resolv is the file it writes for the container's
// /etc/resolv.conf.
func jobNetUp(resolv string, userns bool) string {
	add := "ip netns add \"$zj_ns\"\n"
	if userns {
		add = userNSNetUp
	}
	return `for zj_tool in ip iptables flock sysctl; do
  command -v "$zj_tool" >/dev/null || { echo "zenthros: $zj_tool is needed to give this job a network of its own on a shared machine" >&2; exit 1; }
done
exec 9>/run/zenthros-jobnet.lock
flock 9
zj_down
iptables -N ZENTHROS-JOBNET 2>/dev/null || true
iptables -C FORWARD -j ZENTHROS-JOBNET 2>/dev/null || iptables -I FORWARD 1 -j ZENTHROS-JOBNET
iptables -N ZENTHROS-JOBNET-IN 2>/dev/null || true
iptables -C INPUT -j ZENTHROS-JOBNET-IN 2>/dev/null || iptables -I INPUT 1 -j ZENTHROS-JOBNET-IN
iptables -t nat -N ZENTHROS-JOBNET-NAT 2>/dev/null || true
iptables -t nat -C POSTROUTING -j ZENTHROS-JOBNET-NAT 2>/dev/null || iptables -t nat -I POSTROUTING 1 -j ZENTHROS-JOBNET-NAT
zj_used=" $(ip -4 -o addr show | awk '{print $4}' | tr '\n' ' ') "
zj_n=$(( 0x${zj_ns#` + JobNetPrefix + `} % 16384 ))
zj_try=0
while :; do
  zj_a=$(( zj_n / 64 )); zj_b=$(( (zj_n % 64) * 4 ))
  zj_hostip=10.213.${zj_a}.$(( zj_b + 1 ))
  case "$zj_used" in *" ${zj_hostip}/30 "*) ;; *) break ;; esac
  zj_n=$(( (zj_n + 1) % 16384 )); zj_try=$(( zj_try + 1 ))
  [ "$zj_try" -lt 16384 ] || { echo "zenthros: no free job network in ` + JobNetCIDR + `" >&2; exit 1; }
done
zj_peerip=10.213.${zj_a}.$(( zj_b + 2 ))
` + add + `ip link add "$zj_host" type veth peer name "$zj_peer"
ip link set "$zj_peer" netns "$zj_ns"
ip addr add "${zj_hostip}/30" dev "$zj_host"
ip link set "$zj_host" up
ip netns exec "$zj_ns" ip addr add "${zj_peerip}/30" dev "$zj_peer"
ip netns exec "$zj_ns" ip link set "$zj_peer" up
ip netns exec "$zj_ns" ip link set lo up
ip netns exec "$zj_ns" ip route add default via "$zj_hostip"
ip netns exec "$zj_ns" sysctl -qw net.ipv6.conf.all.disable_ipv6=1 >/dev/null 2>&1 || true
ip netns exec "$zj_ns" sysctl -qw net.ipv6.conf.default.disable_ipv6=1 >/dev/null 2>&1 || true
sysctl -qw net.ipv4.ip_forward=1
zj_src=/run/systemd/resolve/resolv.conf
[ -s "$zj_src" ] || zj_src=/etc/resolv.conf
grep -E '^[[:space:]]*(nameserver|search|options)[[:space:]]' "$zj_src" 2>/dev/null |
  grep -vE '^[[:space:]]*nameserver[[:space:]]+(127\.|::1|0\.0\.0\.0)' > ` + shellword.Quote(resolv) + ` || true
zj_rule() { iptables "$@" -m comment --comment "$zj_comment"; }
for zj_dns in $(awk '$1 == "nameserver" && $2 ~ /^[0-9.]+$/ {print $2}' ` + shellword.Quote(resolv) + `); do
  for zj_proto in udp tcp; do
    zj_rule -A ZENTHROS-JOBNET -i "$zj_host" -d "$zj_dns" -p "$zj_proto" --dport 53 -j ACCEPT
    zj_rule -A ZENTHROS-JOBNET-IN -i "$zj_host" -d "$zj_dns" -p "$zj_proto" --dport 53 -j ACCEPT
  done
done
zj_rule -A ZENTHROS-JOBNET -i "$zj_host" -d 169.254.0.0/16 -j DROP
zj_rule -A ZENTHROS-JOBNET -i "$zj_host" -d ` + JobNetCIDR + ` -j DROP
zj_rule -A ZENTHROS-JOBNET -i "$zj_host" -j ACCEPT
zj_rule -A ZENTHROS-JOBNET -o "$zj_host" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
zj_rule -A ZENTHROS-JOBNET -o "$zj_host" -j DROP
zj_rule -A ZENTHROS-JOBNET-IN -i "$zj_host" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
zj_rule -A ZENTHROS-JOBNET-IN -i "$zj_host" -j DROP
zj_rule -t nat -A ZENTHROS-JOBNET-NAT -s "${zj_peerip}/32" ! -d ` + JobNetCIDR + ` -j MASQUERADE
flock -u 9
exec 9>&-
`
}
