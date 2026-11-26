// SPDX-License-Identifier: Apache-2.0

// Package containerboot starts the container agent in an image that does not
// carry it: a provider's cached image (runpod/pytorch, say) used as it is. The
// container's command is a short POSIX sh script that prefers an agent baked
// into the image, and otherwise downloads one, verifies it through the pinned
// SHA256SUMS and execs it as PID 1. A restart reuses the download only after
// verifying it again.
//
// It assumes sh (any POSIX one: busybox on Alpine) and one of curl, wget or
// python3 to download with, and sha256sum or python3 to verify with. The
// agent comes zstd-compressed, with zenthros-unzstd, a static unpacker
// listed in SHA256SUMS: no zstd, gzip or root needed. Nothing runs before it
// is verified. Its arguments, if any, go to the agent. Artifacts come from the base, or each from its own URL in
// ZENTHROS_ARTIFACT_URLS (presigned URLs of a private bucket), the
// installer's convention.
package containerboot

// Env names the script reads; the controller's ManagedBootstrapper or the
// user sets them with the join env.
const (
	EnvInstallBase  = "ZENTHROS_INSTALL_BASE"
	EnvSumsSHA256   = "ZENTHROS_SHA256SUMS_SHA256"
	EnvArtifactURLs = "ZENTHROS_ARTIFACT_URLS"
	// EnvBakedAgent is where an agent baked into the image would be.
	EnvBakedAgent = "ZENTHROS_BAKED_AGENT"
)

// BakedAgent is where an image carries the agent (test/cluster/container).
const BakedAgent = "/usr/local/bin/zenthros-agent"

// AgentArtifact is the agent the script downloads for arch, zstd-compressed
// as every artifact is (install/stage.sh).
func AgentArtifact(arch string) string { return "zenthros-agent-linux-" + arch + ".zst" }

// UnzstdArtifact is the static unpacker the script downloads for arch and
// unpacks the agent with (cmd/unzstd).
func UnzstdArtifact(arch string) string { return "zenthros-unzstd-linux-" + arch }

// Stalled starts the boot's failure on a download that stopped moving and
// did again on each try: the host's, not Zenthros's (Vast.ai, 2026-10-28:
// hosts 45390 and 146320 stalled an agent download past 5 minutes, the
// second after a good one hours before). The harness reads it.
const Stalled = "the download stalled:"

// Fetch leaves in ${agent} an agent to run: the image's own, else one it
// downloads and verifies through the pinned SHA256SUMS (cached in the state
// dir, and verified again when reused). It runs nothing. Without a pin it
// refuses to download, unless the shell variable zenthros_trust_sums is set:
// then it trusts the SHA256SUMS it downloads, as an installer fetched over
// HTTPS trusts itself, and pins it from then on. A download that stops
// moving for a minute is cut and tried again, three times at most, then the
// boot fails with Stalled.
const Fetch = `set -eu
fail() { echo "zenthros-boot: $*" >&2; exit 1; }
stalled="` + Stalled + `"
zenthros_stall="${zenthros_stall:-60}"
state="${ZENTHROS_STATE_DIR:-/tmp/zenthros}"
dir="${state}/bin"
baked="${ZENTHROS_BAKED_AGENT:-/usr/local/bin/zenthros-agent}"
have() { command -v "$1" >/dev/null 2>&1; }
# fetch downloads $1 to $2 and gives up on a download that stopped moving:
# under 1 KiB/s (curl) or no byte (wget, python3) for ${zenthros_stall}
# seconds, or no connection in as long; it exits 28 then. One that moves at
# 1 KiB/s or more is never cut, however long it takes. Only
# -t and -T are wget's on busybox too; GNU wget's network failure (4) is
# taken for a stall, and busybox's (1) for a failure. curl's URL comes
# first, before its flags: a process list cut short still shows what it
# downloads.
fetch() {
  if have curl; then curl "$1" -fsSL --connect-timeout "${zenthros_stall}" --speed-limit 1024 --speed-time "${zenthros_stall}" -o "$2"
  elif have wget; then
    rc=0
    wget -q -t 1 -T "${zenthros_stall}" -O "$2" "$1" || rc=$?
    [ "${rc}" -ne 4 ] || rc=28
    return "${rc}"
  elif have python3; then python3 -c '
import shutil, socket, sys, urllib.request
try:
    with urllib.request.urlopen(sys.argv[1], timeout=float(sys.argv[3])) as r, open(sys.argv[2], "wb") as f:
        shutil.copyfileobj(r, f)
except (socket.timeout, TimeoutError):
    sys.exit(28)
' "$1" "$2" "${zenthros_stall}"
  else fail "no curl, wget or python3 to download the agent with"
  fi
}
# get downloads $1 to $2, three times at most. Its log names each download
# before it starts, so a boot that hangs shows where.
get() {
  echo "zenthros-boot downloading $(from "$1")" >&2
  try=1
  while :; do
    rc=0
    fetch "$1" "$2" || rc=$?
    [ "${rc}" -ne 0 ] || return 0
    [ "${try}" -lt 3 ] || break
    sleep "${try}"
    try=$((try + 1))
  done
  [ "${rc}" -ne 28 ] || fail "${stalled} $(from "$1")"
  fail "cannot download $(from "$1")"
}
sum() {
  if have sha256sum; then sha256sum "$1" | cut -d' ' -f1
  elif have python3; then python3 -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$1"
  else fail "no sha256sum or python3 to verify the agent with"
  fi
}
# url is where artifact $1 comes from: its own URL, else the base. No
# globbing: a presigned URL has a '?'.
url() (
  set -f
  for pair in ${urls}; do
    case "${pair}" in "$1="*) printf '%s' "${pair#*=}"; exit 0 ;; esac
  done
  [ -n "${base}" ] || fail "ZENTHROS_ARTIFACT_URLS has no $1 and ZENTHROS_INSTALL_BASE is not set"
  printf '%s/%s' "${base}" "$1"
)
# from names a URL in messages without its query, which may be a signature.
from() { printf '%s' "${1%%\?*}"; }
# listed prints the checksum SHA256SUMS file $1 gives file $2.
listed() { sed -n "s/^\([0-9a-f]\{64\}\)  \*\{0,1\}$2\$/\1/p" "$1"; }
# verified: the files cached in dir are the pinned SHA256SUMS, and the agent
# and the unpacker it lists. Jobs can write the state dir, so an earlier
# start's download is checked again before it runs.
verified() {
  [ -f "${dir}/SHA256SUMS" ] && [ -f "${dir}/${name}" ] && [ -f "${dir}/${unz}" ] || return 1
  [ "$(sum "${dir}/SHA256SUMS")" = "${pin}" ] || return 1
  for f in "${name}" "${unz}"; do
    want="$(listed "${dir}/SHA256SUMS" "${f}")"
    [ -n "${want}" ] && [ "$(sum "${dir}/${f}")" = "${want}" ] || return 1
  done
}
# download leaves the verified agent, unpacked, at ${dir}/zenthros-agent.
download() {
  base="${ZENTHROS_INSTALL_BASE:-}"
  base="${base%/}"
  urls="${ZENTHROS_ARTIFACT_URLS:-}"
  pin="${ZENTHROS_SHA256SUMS_SHA256:-}"
  [ -n "${pin}" ] || [ -n "${zenthros_trust_sums:-}" ] || fail "ZENTHROS_SHA256SUMS_SHA256 is not set; refusing an unpinned download"
  case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64) arch=arm64 ;;
    *) fail "unsupported machine $(uname -m)" ;;
  esac
  name="zenthros-agent-linux-${arch}.zst"
  unz="zenthros-unzstd-linux-${arch}"
  work="${dir}/.boot"
  rm -rf "${work}"
  mkdir -p "${work}"
  if ! verified; then
    [ -n "${base}${urls}" ] || fail "the image has no agent and neither ZENTHROS_INSTALL_BASE nor ZENTHROS_ARTIFACT_URLS is set"
    src="$(url SHA256SUMS)"
    get "${src}" "${work}/SHA256SUMS"
    [ -n "${pin}" ] || pin="$(sum "${work}/SHA256SUMS")"
    [ "$(sum "${work}/SHA256SUMS")" = "${pin}" ] || fail "checksum mismatch for SHA256SUMS from $(from "${src}")"
    for f in "${unz}" "${name}"; do
      want="$(listed "${work}/SHA256SUMS" "${f}")"
      [ -n "${want}" ] || fail "SHA256SUMS lists no ${f}"
      src="$(url "${f}")"
      get "${src}" "${work}/${f}"
      [ "$(sum "${work}/${f}")" = "${want}" ] || fail "checksum mismatch for ${f} from $(from "${src}")"
    done
    mv "${work}/SHA256SUMS" "${dir}/SHA256SUMS"
    mv "${work}/${unz}" "${dir}/${unz}"
    mv "${work}/${name}" "${dir}/${name}"
  fi
  chmod 0755 "${dir}/${unz}"
  "${dir}/${unz}" < "${dir}/${name}" > "${work}/zenthros-agent" || fail "cannot unpack ${name}"
  chmod 0755 "${work}/zenthros-agent"
  mv "${work}/zenthros-agent" "${dir}/zenthros-agent"
  rm -rf "${work}"
}
if [ -x "${baked}" ]; then
  agent="${baked}"
else
  download
  agent="${dir}/zenthros-agent"
fi
`

// Script is the container's command after sh -c: Fetch, then the agent. It
// always needs the pin: nothing in its environment makes it trust.
const Script = "zenthros_trust_sums=\n" + Fetch + `exec "${agent}" ${1+"$@"}
`

// Command is the container command that runs Script.
func Command() []string { return []string{"/bin/sh", "-c", Script} }

// The script's arguments are the agent's. In a container that is already
// running, the zenadm that install-container.sh writes runs Fetch, then
// `zenthros-agent attach` (zenadm token create --attach).
