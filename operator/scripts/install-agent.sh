#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0

# Official curl-able Zenthros host installer. No secrets. No join.
#
# Default: the Zenthros base only (Tailscale, zenadm, the agent). Every
# runtime is opt-in and none is tied to a language:
#
#   curl -fsSL "${INSTALL_BASE}/install-agent.sh" | INSTALL_BASE="${INSTALL_BASE}" bash   # the attach line your hub prints
#   bash install-agent.sh --container-engine containerd          # OCI images: any language
#   bash install-agent.sh --container-engine containerd --gpu auto
#   bash install-agent.sh --toolchain node@24 --toolchain python@3.12 --toolchain-packages "polars pyarrow"
#
# Order:
#   1. SHA256SUMS and upstream.lock (tiny)
#   2. every download at once: zenthros-unzstd, zenadm, the agent, Tailscale,
#      containerd and runc, toolchain archives, system and GPU packages
#      (download only)
#   3. sha256sum -c against SHA256SUMS or upstream.lock, then every .zst
#      unpacked in parallel with zenthros-unzstd (static: no zstd, no dpkg)
#   4. zenadm, the agent, Tailscale: the machine can join now
#   5. containerd, GPU, packages, toolchains; in the background behind
#      runtime-pending/runtime-ready when ZENTHROS_RUNTIME_BACKGROUND=1
#
# Every upstream download (Tailscale, uv, Node, Go, Java) is pinned by
# upstream.lock, which SHA256SUMS lists; a bootstrap that pins SHA256SUMS
# (ZENTHROS_SHA256SUMS_SHA256) pins every file this script installs.
#
# Env (flags win):
#   INSTALL_BASE / ZENTHROS_INSTALL_BASE   where the Zenthros artifacts live
#   ZENTHROS_ARTIFACT_URLS                 "name=url ..." per-artifact URLs
#                                          (presigned); the name is the file
#                                          name in SHA256SUMS
#   ZENTHROS_SHA256SUMS_SHA256             expected SHA-256 of SHA256SUMS
#   ZENTHROS_NETWORK                       tailscale (default) or direct
#   ZENTHROS_CONTAINER_ENGINE              containerd or none (default)
#   ZENTHROS_GPU                           auto, nvidia or none (default)
#   ZENTHROS_DEFAULT_JOB_RUNTIME           systemd or containerd
#   ZENTHROS_RUNTIME_BACKGROUND=1          step 5 in the background
#   ZENTHROS_PLAN_ONLY=1                   print the plan and exit
#   ZENTHROS_PREFLIGHT=1                   check the host before anything is
#                                          downloaded (the line a person pastes)
#   ZENTHROS_HUB_ADDR                      the hub's host:port list (a,b); the preflight must reach one
#   ZENTHROS_RUNTIME_LOG                   background runtime log (/var/log/zenthros-runtime.log)
set -euo pipefail
# What it installs is the machine's, run by every job as its Workspace's
# user: the caller's umask (the managed bootstrap's 077, which guards the join
# token it holds; a hardened host's) must not leave a toolchain root's only.
# It writes no secret; its temporary files are mktemp's, 0700.
umask 022

INSTALL_BASE="${INSTALL_BASE:-${ZENTHROS_INSTALL_BASE:-}}"
ZENADM_DEST="${ZENTHROS_ZENADM_DEST:-/usr/local/bin/zenadm}"
UNZSTD_DEST="${ZENTHROS_UNZSTD_DEST:-/usr/local/lib/zenthros/zenthros-unzstd}"
AGENT_DEST="${ZENTHROS_AGENT_DEST:-/usr/local/bin/zenthros-agent}"
STATE_DIR="${ZENTHROS_STATE_DIR:-/var/lib/zenthros}"
TOOLCHAIN_ROOT="${ZENTHROS_TOOLCHAIN_ROOT:-/opt/zenthros}"
UV_TOOL="${STATE_DIR}/tools/uv"
TOOLCHAIN_BIN="${TOOLCHAIN_ROOT}/bin"
NVIDIA_BRANCH="${ZENTHROS_NVIDIA_BRANCH:-580}"
NVIDIA_CTK_VERSION="${ZENTHROS_NVIDIA_CTK_VERSION:-1.20.1}"
# libnvidia-container signing keys (https://nvidia.github.io/libnvidia-container/gpgkey).
NVIDIA_KEY_FPRS="C95B321B61E88C1809C4F759DDCAE044F796ECB0 2041629BF5352FAC178A9B1F6ED91CA3AC1160CD"

# Host toolchain modules. Each has the same interface:
#   module_<name>_fetch VERSION   prints "<file> lock:<key>" lines to download
#   module_<name>_install VERSION PACKAGES
TOOLCHAIN_MODULES="python node go java"

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  SUDO="sudo"
fi

NETWORK="${ZENTHROS_NETWORK:-tailscale}"
CONTAINER_ENGINE="${ZENTHROS_CONTAINER_ENGINE:-none}"
GPU="${ZENTHROS_GPU:-none}"
DEFAULT_JOB_RUNTIME="${ZENTHROS_DEFAULT_JOB_RUNTIME:-}"
TOOLCHAINS=()
TOOLCHAIN_PACKAGES=()
PACKAGES=()
ONLY_TOOLCHAINS=0
# The agent runs the kept installer to install toolchains a Prewarm asks for.
KEPT_DIR="${STATE_DIR}/installer"

info() { echo ">> $*" >&2; }
ok() { echo "OK  $*" >&2; }
event() {
  local line
  line="ZENTHROS_EVENT event=$1 ts=$(date -u +%Y-%m-%dT%H:%M:%SZ) ${2:-}"
  echo "${line}" >&2
  # The serial console is what a cold-start harness reads without SSH.
  echo "${line}" 2>/dev/null >/dev/console || true
}
fail() {
  echo "install-agent.sh: $*" >&2
  echo "install-agent.sh: $*" 2>/dev/null >/dev/console || true
  event "install_fail" "msg=$*"
  exit 1
}

# refuse is a preflight's failure: one line a person reads, ending with
# what to do, before anything was installed.
refuse() {
  echo "✗ $*" >&2
  event "install_fail" "msg=$*"
  exit 3
}

# preflight checks what a machine a person attaches must have, before
# anything is downloaded (usability E-02, E-03, E-07): systemd, as the agent
# is a unit; the disk the jobs' default needs, containerd keeping images; and
# the hub, which the join reaches next. A machine the hub made needs none of
# it, so only ZENTHROS_PREFLIGHT=1 runs it. ZENTHROS_PLAN_ROOT and
# ZENTHROS_PLAN_FREE_GB stand in for the host in tests.
preflight() {
  local root="${ZENTHROS_PLAN_ROOT:-}" need=2 what="the agent" free addr host port tried="" ports=""
  local -a addrs=()
  [ -d "${root}/run/systemd/system" ] ||
    refuse "systemd is not running here. Looks like a container: attach it with --type container"
  if [ "${DEFAULT_JOB_RUNTIME:-systemd}" = containerd ]; then
    need=10 what="containerd"
  fi
  free="${ZENTHROS_PLAN_FREE_GB:-$(df -Pk / | awk 'NR==2 {print int($4 / 1048576)}')}"
  [ "${free}" -ge "${need}" ] ||
    refuse "${free} GB free on /; ${what} needs ${need} GB. Free some space or grow the disk, then run this again"
  # The hub's addresses in the order the join tries them (the Gateway's
  # port, then 443 through Traefik): one that answers is enough.
  IFS=, read -r -a addrs <<<"${ZENTHROS_HUB_ADDR:-}"
  [ "${#addrs[@]}" -gt 0 ] || return 0
  for addr in "${addrs[@]}"; do
    host="${addr%:*}" port="${addr##*:}"
    [ "${host}" != "${addr}" ] || port=9443
    host="${host#[}" host="${host%]}"
    case "${host}" in
      *.ts.net) return 0 ;; # a tailnet address is reachable once the join brings Tailscale
    esac
    if timeout 5 bash -c ': </dev/tcp/"$1"/"$2"' _ "${host}" "${port}" 2>/dev/null; then
      ok "Preflight: systemd, ${free} GB free, hub reachable at ${addr}"
      return 0
    fi
    tried="${tried:+${tried} or }${addr}" ports="${ports:+${ports} or }${port}"
  done
  refuse "cannot reach the hub at ${tried} from here. Allow outbound TCP ${ports} to it, then run this again"
}

usage() {
  cat >&2 <<'EOF'
install-agent.sh — Zenthros host installer

  bash install-agent.sh [flags]

Flags:
  --network tailscale|direct          how the machine reaches the controller
  --container-engine containerd|none  containerd + runc: jobs in OCI images, any language
  --gpu auto|nvidia|none              NVIDIA driver + container toolkit (auto: an NVIDIA PCI device)
  --default-job-runtime systemd|containerd
                                      what a job that names no runtime runs under
                                      (containerd with --container-engine containerd)
  --toolchain NAME[@VERSION]          a host toolchain: python, node, go, java
  --toolchain-packages "A B"          packages for the last --toolchain
  --package NAME                      a system package (apt or dnf), repeatable
  --only-toolchains                   install the --toolchain flags only, on a
                                      host already installed (Prewarm)
  --help                             this message

Without flags this installs only Tailscale, zenadm and the agent.
EOF
}

add_toolchain() {
  TOOLCHAINS+=("$1")
  TOOLCHAIN_PACKAGES+=("")
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --network|--container-engine|--gpu|--default-job-runtime|--toolchain|--toolchain-packages|--package)
        [ "$#" -ge 2 ] || fail "$1 requires a value"
        set -- "$1=$2" "${@:3}"
        continue
        ;;
      --network=*) NETWORK="${1#*=}" ;;
      --container-engine=*) CONTAINER_ENGINE="${1#*=}" ;;
      --gpu=*) GPU="${1#*=}" ;;
      --default-job-runtime=*) DEFAULT_JOB_RUNTIME="${1#*=}" ;;
      --toolchain=*) add_toolchain "${1#*=}" ;;
      --toolchain-packages=*)
        [ "${#TOOLCHAINS[@]}" -gt 0 ] || fail "--toolchain-packages must follow a --toolchain"
        TOOLCHAIN_PACKAGES[${#TOOLCHAINS[@]}-1]="${1#*=}"
        ;;
      --package=*) PACKAGES+=("${1#*=}") ;;
      --only-toolchains) ONLY_TOOLCHAINS=1 ;;
      --help|-h) usage; exit 0 ;;
      --) shift; break ;;
      -*) fail "unknown flag: $1 (see --help)" ;;
      *) fail "unexpected argument: $1 (see --help)" ;;
    esac
    shift
  done

  case "${NETWORK}" in tailscale|direct) ;; *) fail "unknown network: ${NETWORK} (tailscale or direct)" ;; esac
  case "${CONTAINER_ENGINE}" in containerd|none) ;; *) fail "unknown container engine: ${CONTAINER_ENGINE} (containerd or none)" ;; esac
  case "${GPU}" in auto|nvidia|none) ;; *) fail "unknown gpu: ${GPU} (auto, nvidia or none)" ;; esac
  if [ -z "${DEFAULT_JOB_RUNTIME}" ] && [ "${CONTAINER_ENGINE}" = containerd ]; then
    # Asked for containerd, a person means the jobs run in it: the engine
    # alone left them under systemd (U-06). Say systemd to keep that.
    DEFAULT_JOB_RUNTIME=containerd
  fi
  case "${DEFAULT_JOB_RUNTIME}" in
    ""|systemd) ;;
    containerd) [ "${CONTAINER_ENGINE}" = containerd ] || fail "--default-job-runtime containerd needs --container-engine containerd" ;;
    *) fail "unknown default job runtime: ${DEFAULT_JOB_RUNTIME} (systemd or containerd)" ;;
  esac
  local tc name
  for tc in "${TOOLCHAINS[@]}"; do
    name="${tc%%@*}"
    case " ${TOOLCHAIN_MODULES} " in
      *" ${name} "*) ;;
      *) fail "unknown toolchain: ${name} (modules: ${TOOLCHAIN_MODULES})" ;;
    esac
  done
}

detect_host() {
  local machine="${ZENTHROS_PLAN_MACHINE:-$(uname -m)}"
  case "${machine}" in
    x86_64|amd64) ARCH=amd64 ;;
    aarch64|arm64) ARCH=arm64 ;;
    *) fail "unsupported CPU ${machine}: Zenthros binaries are built for amd64 and arm64" ;;
  esac
  if [ -n "${ZENTHROS_PLAN_PKG:-}" ]; then
    PKG="${ZENTHROS_PLAN_PKG}"
  elif command -v apt-get >/dev/null 2>&1; then
    PKG=apt
  elif command -v dnf >/dev/null 2>&1; then
    PKG=dnf
  else
    fail "no apt-get or dnf on this host; supported: Ubuntu/Debian (apt) and Amazon Linux 2023 or another dnf distribution"
  fi
  GPU_ACTIVE=0
  case "${GPU}" in
    nvidia) GPU_ACTIVE=1 ;;
    auto) nvidia_pci_present && GPU_ACTIVE=1 || true ;;
  esac
}

nvidia_pci_present() {
  if [ -n "${ZENTHROS_PLAN_NVIDIA_PCI:-}" ]; then
    [ "${ZENTHROS_PLAN_NVIDIA_PCI}" = 1 ]
    return
  fi
  grep -qsx 0x10de /sys/bus/pci/devices/*/vendor
}

# ---------------------------------------------------------------- the plan
#
# The plan is what runs: FETCH_* feed the parallel download group, STEPS the
# installs in order, BACKGROUND the runtime steps behind runtime-ready.
FETCH_FILES=()
FETCH_SOURCES=()
STEPS=()
BACKGROUND=()

# plan_fetch plans a download once: a file two steps need (uv for containerd
# jobs and for the python toolchain) is fetched once and read by both.
plan_fetch() {
  local planned
  for planned in "${FETCH_FILES[@]}"; do
    [ "${planned}" = "$1" ] && return 0
  done
  FETCH_FILES+=("$1")
  FETCH_SOURCES+=("$2")
}

tailscale_installed() {
  [ -z "${ZENTHROS_PLAN_ONLY:-}" ] && command -v tailscale >/dev/null 2>&1
}

system_packages() {
  local pkgs=("${PACKAGES[@]}")
  if [ "${GPU_ACTIVE}" = 1 ]; then
    pkgs+=($(gpu_packages))
  fi
  printf '%s\n' "${pkgs[@]}"
}

build_plan() {
  if [ "${ONLY_TOOLCHAINS}" = 1 ]; then
    build_toolchain_plan
    return
  fi
  plan_fetch "zenthros-unzstd-linux-${ARCH}" artifact
  plan_fetch "zenadm-linux-${ARCH}.zst" artifact
  plan_fetch "zenthros-agent-linux-${ARCH}.zst" artifact
  if [ "${NETWORK}" = tailscale ] && ! tailscale_installed; then
    if [ "${PKG}" = apt ]; then
      plan_fetch tailscale.deb "lock:tailscale-deb@"
      plan_fetch tailscale-keyring.deb "lock:tailscale-keyring-deb@"
    else
      plan_fetch tailscale.rpm "lock:tailscale-rpm@"
    fi
  fi
  if [ "${CONTAINER_ENGINE}" = containerd ]; then
    plan_fetch "containerd-linux-${ARCH}.tar.zst" artifact
    plan_fetch "runc-linux-${ARCH}.zst" artifact
    plan_fetch uv.tar.gz "lock:uv@"
  fi
  local i name version line
  for i in "${!TOOLCHAINS[@]}"; do
    name="${TOOLCHAINS[$i]%%@*}"
    version=""
    [ "${TOOLCHAINS[$i]}" = "${name}" ] || version="${TOOLCHAINS[$i]#*@}"
    while read -r line; do
      [ -n "${line}" ] && plan_fetch "${line%% *}" "${line#* }"
    done < <("module_${name}_fetch" "${version}")
  done
  if [ -n "$(system_packages)" ]; then
    plan_fetch system-packages "pkg:$(system_packages | tr '\n' ' ')"
  fi

  STEPS=(binaries)
  [ "${NETWORK}" = tailscale ] && STEPS+=(tailscale)
  STEPS+=(scaffold)
  # The default first: until runtime-default is written the agent declares
  # systemd as the default.
  BACKGROUND=(runtime-default)
  [ "${CONTAINER_ENGINE}" = containerd ] && BACKGROUND+=(containerd)
  [ "${GPU_ACTIVE}" = 1 ] && BACKGROUND+=(gpu)
  [ "${#PACKAGES[@]}" -gt 0 ] && BACKGROUND+=(packages)
  for i in "${!TOOLCHAINS[@]}"; do
    BACKGROUND+=("toolchain:${i}")
  done
  BACKGROUND+=(runtime-ready)
}

# build_toolchain_plan is the plan of --only-toolchains: the toolchains'
# downloads, in one parallel group, then their installs.
build_toolchain_plan() {
  [ "${#TOOLCHAINS[@]}" -gt 0 ] || fail "--only-toolchains needs at least one --toolchain"
  local i name version line
  for i in "${!TOOLCHAINS[@]}"; do
    name="${TOOLCHAINS[$i]%%@*}"
    version=""
    [ "${TOOLCHAINS[$i]}" = "${name}" ] || version="${TOOLCHAINS[$i]#*@}"
    while read -r line; do
      [ -n "${line}" ] && plan_fetch "${line%% *}" "${line#* }"
    done < <("module_${name}_fetch" "${version}")
  done
  STEPS=()
  BACKGROUND=()
  for i in "${!TOOLCHAINS[@]}"; do
    BACKGROUND+=("toolchain:${i}")
  done
}

print_plan() {
  local i
  for i in "${!FETCH_FILES[@]}"; do
    echo "fetch ${FETCH_FILES[$i]} ${FETCH_SOURCES[$i]}"
  done
  for i in "${STEPS[@]}"; do
    echo "install ${i}"
  done
  echo "jobs-default ${DEFAULT_JOB_RUNTIME:-systemd}"
  for i in "${BACKGROUND[@]}"; do
    case "${i}" in
      toolchain:*) echo "runtime toolchain ${TOOLCHAINS[${i#toolchain:}]}" ;;
      *) echo "runtime ${i}" ;;
    esac
  done
}

# ---------------------------------------------------------------- downloads

# curl_get rides out a host's moment: curl tries again on a timeout, a refused
# connection, 408, 429 or 5xx, waiting 1, 2, 4... 64 s, about two minutes in
# all (GitHub answered uv with 504 for 24 s, full suite, 2026-11-05). Any other
# answer, such as a 404, ends the download at once.
curl_get() {
  local url="$1" dest="$2"
  shift 2
  local patient=(--connect-timeout 15 --max-time 900 --retry 7 --retry-max-time 120 --retry-connrefused -fsSL)
  if curl -4 "${patient[@]}" "$@" -o "${dest}" "${url}"; then
    return 0
  fi
  info "IPv4 download failed; retrying ${url%%\?*}"
  curl "${patient[@]}" "$@" -o "${dest}" "${url}" || fail "download failed: ${url%%\?*}"
}

# artifact_url is where a Zenthros artifact comes from: its own URL in
# ZENTHROS_ARTIFACT_URLS (presigned), else INSTALL_BASE.
artifact_url() {
  local pair pairs=()
  read -r -a pairs <<<"${ZENTHROS_ARTIFACT_URLS:-}" || true
  for pair in "${pairs[@]}"; do
    if [ "${pair%%=*}" = "$1" ]; then
      printf '%s' "${pair#*=}"
      return
    fi
  done
  printf '%s/%s' "${INSTALL_BASE%/}" "$1"
}

# lock_row prints "sha256 url" for the first upstream.lock row whose key is
# want (or, for want "name@", any version; for "name@1.2", any 1.2.x) on ARCH.
lock_row() {
  awk -v want="$1" -v arch="${ARCH}" '
    function matches(key) {
      if (key == want) return 1
      if (substr(want, length(want)) == "@") return index(key, want) == 1
      return index(key, want ".") == 1 || index(key, want "+") == 1
    }
    !/^#/ && NF == 4 && matches($1) && ($2 == arch || $2 == "all") { print $3, $4; exit }
  ' "${WORK}/upstream.lock"
}

fetch_one() {
  local file="$1" source="$2" sha url
  event "fetch_start" "artifact=${file}"
  case "${source}" in
    artifact)
      curl_get "$(artifact_url "${file}")" "${WORK}/${file}"
      ;;
    lock:*)
      read -r sha url < <(lock_row "${source#lock:}") || true
      [ -n "${url:-}" ] || fail "upstream.lock has no ${source#lock:} for ${ARCH}"
      curl_get "${url}" "${WORK}/${file}"
      echo "${sha}  ${WORK}/${file}" | sha256sum -c --quiet - >/dev/null 2>&1 ||
        fail "checksum mismatch for ${file} from ${url}; aborting"
      ;;
    pkg:*)
      pkg_download ${source#pkg:}
      ;;
  esac
  event "fetch_end" "artifact=${file}"
}

# fetch_all downloads every planned file at once.
fetch_all() {
  local i pids=() rc=0
  for i in "${!FETCH_FILES[@]}"; do
    fetch_one "${FETCH_FILES[$i]}" "${FETCH_SOURCES[$i]}" &
    pids+=($!)
  done
  for i in "${pids[@]}"; do
    wait "${i}" || rc=1
  done
  [ "${rc}" -eq 0 ] || fail "a download failed; nothing was installed"
}

# fetch_manifest gets SHA256SUMS and upstream.lock before anything else and
# checks SHA256SUMS against the digest the bootstrap pinned.
fetch_manifest() {
  curl_get "$(artifact_url SHA256SUMS)" "${WORK}/SHA256SUMS"
  if [ -n "${ZENTHROS_SHA256SUMS_SHA256:-}" ]; then
    echo "${ZENTHROS_SHA256SUMS_SHA256}  ${WORK}/SHA256SUMS" | sha256sum -c --quiet - >/dev/null 2>&1 ||
      fail "SHA256SUMS does not match the pinned digest; aborting before any install"
  fi
  curl_get "$(artifact_url upstream.lock)" "${WORK}/upstream.lock"
  verify_artifacts upstream.lock
}

# verify_artifacts checks named Zenthros artifacts against SHA256SUMS; a name
# SHA256SUMS does not list fails.
verify_artifacts() {
  local name
  for name in "$@"; do
    awk -v f="${name}" '$2 == f || $2 == "*"f { found = 1 } END { exit !found }' "${WORK}/SHA256SUMS" ||
      fail "${name} is not listed in SHA256SUMS"
  done
  (
    cd "${WORK}"
    for name in "$@"; do
      awk -v f="${name}" '$2 == f || $2 == "*"f { print $1 "  " f }' SHA256SUMS
    done | sha256sum -c --quiet -
  ) >/dev/null 2>&1 || fail "checksum mismatch in $*; aborting"
}

# verify_and_unpack checks every artifact against SHA256SUMS, the unpacker
# among them, and only then unpacks each .zst with it.
verify_and_unpack() {
  local i artifacts=() pids=() rc=0 unzstd="${WORK}/zenthros-unzstd-linux-${ARCH}"
  for i in "${!FETCH_FILES[@]}"; do
    [ "${FETCH_SOURCES[$i]}" = artifact ] && artifacts+=("${FETCH_FILES[$i]}")
  done
  [ "${#artifacts[@]}" -eq 0 ] || verify_artifacts "${artifacts[@]}"
  if [ -f "${unzstd}" ]; then
    chmod 0755 "${unzstd}"
    # A hardened host mounts /tmp noexec (126): the verified unpacker runs
    # from a root-owned directory there.
    if ! "${unzstd}" </dev/null >/dev/null 2>&1; then
      ${SUDO} install -D -m 0755 "${unzstd}" "${UNZSTD_DEST}" || fail "install ${UNZSTD_DEST}"
      unzstd="${UNZSTD_DEST}"
      "${unzstd}" </dev/null >/dev/null 2>&1 || fail "zenthros-unzstd does not run here, not even from ${UNZSTD_DEST%/*}"
    fi
  fi
  for i in "${artifacts[@]}"; do
    case "${i}" in
      *.zst)
        [ -f "${unzstd}" ] || fail "zenthros-unzstd-linux-${ARCH} was not downloaded to unpack ${i} with"
        "${unzstd}" < "${WORK}/${i}" > "${WORK}/${i%.zst}" & pids+=($!)
        ;;
    esac
  done
  for i in "${pids[@]}"; do
    wait "${i}" || rc=1
  done
  [ "${rc}" -eq 0 ] || fail "unpacking a .zst failed"
}

# ---------------------------------------------------------------- packages

pkg_install() {
  if [ "${PKG}" = apt ]; then
    ${SUDO} env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "$@" >&2
  else
    ${SUDO} dnf install -y "$@" >&2
  fi
}

# pkg_download fetches packages without installing them: it runs in the
# download group, the install later reads the package cache.
pkg_download() {
  [ "$#" -gt 0 ] || return 0
  [ "${GPU_ACTIVE}" = 1 ] && gpu_repositories
  if [ "${PKG}" = apt ]; then
    ${SUDO} apt-get update -y >&2 || fail "apt-get update"
    ${SUDO} env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends --download-only "$@" >&2 ||
      fail "download packages: $*"
  else
    ${SUDO} dnf install -y --downloadonly "$@" >&2 || fail "download packages: $*"
  fi
}

# ensure_commands installs the named commands a minimal image may lack (the
# package has the command's name on apt and dnf).
ensure_commands() {
  local cmd missing=()
  for cmd in "$@"; do
    command -v "${cmd}" >/dev/null 2>&1 || missing+=("${cmd}")
  done
  [ "${#missing[@]}" -gt 0 ] || return 0
  [ "${PKG}" != apt ] || ${SUDO} apt-get update -y >&2 || fail "apt-get update"
  case " ${missing[*]} " in *" curl "*) missing+=(ca-certificates) ;; esac
  pkg_install "${missing[@]}" || fail "install ${missing[*]}"
}

# ---------------------------------------------------------------- base

install_binaries() {
  ${SUDO} install -m 0755 "${WORK}/zenadm-linux-${ARCH}" "${ZENADM_DEST}"
  ${SUDO} install -m 0755 "${WORK}/zenthros-agent-linux-${ARCH}" "${AGENT_DEST}"
  ${SUDO} ln -sfn "${AGENT_DEST}" /usr/local/bin/agent
  ok "zenadm ${ZENADM_DEST}, agent ${AGENT_DEST} (${ARCH})"
}

install_tailscale() {
  if tailscale_installed; then
    ok "tailscale already installed"
    return 0
  fi
  if [ "${PKG}" = apt ]; then
    ${SUDO} dpkg -i "${WORK}/tailscale.deb" "${WORK}/tailscale-keyring.deb" >&2 || fail "dpkg tailscale"
  else
    ${SUDO} rpm -Uvh --replacepkgs "${WORK}/tailscale.rpm" >&2 || fail "rpm tailscale"
  fi
  command -v tailscale >/dev/null 2>&1 || fail "tailscale not on PATH after install"
  ${SUDO} systemctl enable --now tailscaled >/dev/null 2>&1 || true
  ok "tailscale installed"
}

scaffold() {
  ${SUDO} mkdir -p "${STATE_DIR}"
  ${SUDO} chmod 0755 "${STATE_DIR}"
  keep_installer
}

# keep_installer leaves a verified copy of this installer, and where its
# artifacts come from, for the agent to install toolchains later (Prewarm).
# Presigned artifact URLs expire, so only INSTALL_BASE and the SHA256SUMS
# pin are kept. Best effort: a host without it refuses toolchain Prewarms.
keep_installer() {
  awk '$2 == "install-agent.sh" || $2 == "*install-agent.sh" { found = 1 } END { exit !found }' "${WORK}/SHA256SUMS" || return 0
  curl -fsSL --connect-timeout 15 --max-time 60 -o "${WORK}/install-agent.sh" "$(artifact_url install-agent.sh)" 2>/dev/null || {
    info "could not keep the installer; toolchain Prewarms will be refused"
    return 0
  }
  (verify_artifacts install-agent.sh) || return 0
  ${SUDO} mkdir -p "${KEPT_DIR}"
  ${SUDO} install -m 0755 "${WORK}/install-agent.sh" "${KEPT_DIR}/install-agent.sh"
  printf 'INSTALL_BASE=%s\nZENTHROS_SHA256SUMS_SHA256=%s\n' "${INSTALL_BASE}" "$(sha256sum "${WORK}/SHA256SUMS" | cut -d' ' -f1)" |
    ${SUDO} tee "${KEPT_DIR}/env" >/dev/null
}

mark_runtime_pending() {
  ${SUDO} rm -f "${STATE_DIR}/runtime-ready"
  echo "steps=${BACKGROUND[*]}" | ${SUDO} tee "${STATE_DIR}/runtime-pending" >/dev/null
}

mark_runtime_ready() {
  echo "ready=1" | ${SUDO} tee "${STATE_DIR}/runtime-ready" >/dev/null
  ${SUDO} rm -f "${STATE_DIR}/runtime-pending"
  event "runtime_ready"
  # Re-open the control stream so the next heartbeat carries the runtime.
  ${SUDO} systemctl try-restart zenthros-agent >/dev/null 2>&1 || true
}

write_runtime_default() {
  echo "${DEFAULT_JOB_RUNTIME:-systemd}" | ${SUDO} tee "${STATE_DIR}/runtime-default" >/dev/null
}

# ---------------------------------------------------------------- containerd

install_containerd() {
  if command -v containerd >/dev/null 2>&1 && command -v runc >/dev/null 2>&1; then
    ok "containerd already installed"
  else
    event "containerd_install_start"
    local unpack
    unpack="$(mktemp -d)"
    tar -C "${unpack}" -xf "${WORK}/containerd-linux-${ARCH}.tar"
    ${SUDO} mkdir -p /usr/local/bin /usr/local/sbin
    ${SUDO} install -m 0755 "${unpack}"/bin/* /usr/local/bin/
    ${SUDO} install -m 0755 "${WORK}/runc-linux-${ARCH}" /usr/local/sbin/runc
    ${SUDO} ln -sfn /usr/local/sbin/runc /usr/local/bin/runc
    rm -rf "${unpack}"
    ${SUDO} mkdir -p /etc/containerd /var/lib/containerd /etc/systemd/system
    if [ ! -f /etc/containerd/config.toml ]; then
      ${SUDO} tee /etc/containerd/config.toml >/dev/null <<'EOF'
version = 2
root = "/var/lib/containerd"
state = "/run/containerd"
[grpc]
  address = "/run/containerd/containerd.sock"
EOF
    fi
    ${SUDO} tee /etc/systemd/system/containerd.service >/dev/null <<'EOF'
[Unit]
Description=containerd
After=network.target

[Service]
ExecStart=/usr/local/bin/containerd
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF
    ${SUDO} systemctl daemon-reload
    ${SUDO} systemctl enable --now containerd >&2
    event "containerd_install_end"
  fi
  local i
  for i in $(seq 1 30); do
    ${SUDO} ctr version >/dev/null 2>&1 && break
    sleep 1
  done
  ${SUDO} ctr version >/dev/null 2>&1 || fail "containerd did not become ready"
  install_uv_tool
  ok "containerd ready"
}

# install_uv_tool puts the pinned uv where the agent binds it, read-only, into
# every container job (agent.HostUV): an image without uv installs a
# function's packages with it. The static build runs on any libc.
install_uv_tool() {
  local unpack
  unpack="$(mktemp -d)"
  tar -C "${unpack}" --strip-components=1 -xzf "${WORK}/uv.tar.gz"
  ${SUDO} install -D -m 0755 "${unpack}/uv" "${UV_TOOL}"
  rm -rf "${unpack}"
}

# ---------------------------------------------------------------- GPU

gpu_packages() {
  if [ "${PKG}" = apt ]; then
    echo "linux-modules-nvidia-${NVIDIA_BRANCH}-server-$(uname -r)"
    echo "nvidia-headless-no-dkms-${NVIDIA_BRANCH}-server"
    echo "nvidia-utils-${NVIDIA_BRANCH}-server"
    [ "${CONTAINER_ENGINE}" = containerd ] && echo "nvidia-container-toolkit=${NVIDIA_CTK_VERSION}-1"
  else
    echo "kernel-modules-extra"
    echo "nvidia-driver"
    echo "nvidia-driver-cuda"
    [ "${CONTAINER_ENGINE}" = containerd ] && echo "nvidia-container-toolkit-${NVIDIA_CTK_VERSION}"
  fi
  return 0
}

# gpu_repositories adds NVIDIA's container toolkit repository after checking
# its signing key against the pinned fingerprints, and on dnf the CUDA
# repository that carries the driver.
gpu_repositories() {
  [ "${CONTAINER_ENGINE}" = containerd ] || [ "${PKG}" = dnf ] || return 0
  local key="${WORK}/nvidia.gpg" fpr
  curl_get https://nvidia.github.io/libnvidia-container/gpgkey "${key}"
  for fpr in $(gpg --show-keys --with-colons "${key}" 2>/dev/null | awk -F: '$1 == "fpr" { print $10 }'); do
    case " ${NVIDIA_KEY_FPRS} " in
      *" ${fpr} "*) ;;
      *) fail "NVIDIA repository key ${fpr} is not a pinned fingerprint; aborting" ;;
    esac
  done
  if [ "${PKG}" = apt ]; then
    ${SUDO} gpg --batch --yes --dearmor -o /usr/share/keyrings/nvidia-container-toolkit.gpg "${key}"
    echo "deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit.gpg] https://nvidia.github.io/libnvidia-container/stable/deb/\$(ARCH) /" |
      ${SUDO} tee /etc/apt/sources.list.d/nvidia-container-toolkit.list >/dev/null
  else
    ${SUDO} rpm --import "${key}"
    ${SUDO} curl -fsSL -o /etc/yum.repos.d/nvidia-container-toolkit.repo \
      https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo
    local cuda_arch=x86_64
    [ "${ARCH}" = arm64 ] && cuda_arch=sbsa
    ${SUDO} curl -fsSL -o /etc/yum.repos.d/cuda-amzn2023.repo \
      "https://developer.download.nvidia.com/compute/cuda/repos/amzn2023/${cuda_arch}/cuda-amzn2023.repo"
  fi
}

install_gpu() {
  event "gpu_install_start" "branch=${NVIDIA_BRANCH}"
  pkg_install $(gpu_packages)
  ${SUDO} modprobe nvidia >/dev/null 2>&1 || true
  nvidia-smi -L >&2 || fail "nvidia-smi found no GPU after installing driver ${NVIDIA_BRANCH}"
  if [ "${CONTAINER_ENGINE}" = containerd ]; then
    ${SUDO} nvidia-ctk runtime configure --runtime=containerd --config=/etc/containerd/config.toml >&2 ||
      fail "nvidia-ctk runtime configure --runtime=containerd"
    ${SUDO} systemctl restart containerd
  fi
  echo nvidia | ${SUDO} tee "${STATE_DIR}/gpu" >/dev/null
  event "gpu_install_end"
}

# ---------------------------------------------------------------- toolchains

# Every toolchain puts its commands in TOOLCHAIN_BIN, which jobs find on PATH.
link_tool() {
  ${SUDO} mkdir -p "${TOOLCHAIN_BIN}"
  local target
  for target in "$@"; do
    ${SUDO} ln -sfn "${target}" "${TOOLCHAIN_BIN}/$(basename "${target}")"
  done
}

toolchain_path() {
  ${SUDO} tee /etc/profile.d/zenthros.sh >/dev/null <<EOF
# Zenthros host toolchains
export PATH="${TOOLCHAIN_BIN}:\$PATH"
EOF
  ${SUDO} mkdir -p /etc/systemd/system.conf.d
  ${SUDO} tee /etc/systemd/system.conf.d/zenthros-path.conf >/dev/null <<EOF
[Manager]
DefaultEnvironment=PATH=${TOOLCHAIN_BIN}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
EOF
  ${SUDO} systemctl daemon-reload >/dev/null 2>&1 || true
}

unpack_to() {
  local archive="$1" dest="$2"
  ${SUDO} rm -rf "${dest}.next"
  ${SUDO} mkdir -p "${dest}.next"
  ${SUDO} tar -C "${dest}.next" --strip-components=1 -xzf "${archive}"
  ${SUDO} rm -rf "${dest}"
  ${SUDO} mv "${dest}.next" "${dest}"
}

module_python_fetch() { echo "uv.tar.gz lock:uv@"; }
module_python_install() {
  local version="${1:-3.12}" packages="$2" venv="${TOOLCHAIN_ROOT}/python"
  unpack_to "${WORK}/uv.tar.gz" "${TOOLCHAIN_ROOT}/uv"
  ${SUDO} ln -sfn "${TOOLCHAIN_ROOT}/uv/uv" /usr/local/bin/uv
  export UV_PYTHON_INSTALL_DIR="${TOOLCHAIN_ROOT}/uv-python"
  ${SUDO} env UV_PYTHON_INSTALL_DIR="${UV_PYTHON_INSTALL_DIR}" uv python install "${version}" >&2 ||
    fail "uv python install ${version}"
  ${SUDO} rm -rf "${venv}.next"
  ${SUDO} env UV_PYTHON_INSTALL_DIR="${UV_PYTHON_INSTALL_DIR}" uv venv "${venv}.next" --python "${version}" >&2 ||
    fail "uv venv --python ${version}"
  if [ -n "${packages}" ]; then
    # Copied into the venv and never read again: uv's cache would stay on
    # the disk (2.4 GiB for examples/pipeline, more than the venv).
    # shellcheck disable=SC2086
    ${SUDO} uv pip install --python "${venv}.next/bin/python" --no-cache --link-mode=copy ${packages} >&2 ||
      fail "uv pip install ${packages}"
  fi
  ${SUDO} rm -rf "${venv}"
  ${SUDO} mv "${venv}.next" "${venv}"
  # Wrappers, not links: a venv is found from the path its python runs at.
  local tool
  ${SUDO} mkdir -p "${TOOLCHAIN_BIN}"
  for tool in python python3 pip; do
    printf '#!/bin/sh\nexec %s/bin/%s "$@"\n' "${venv}" "${tool}" | ${SUDO} tee "${TOOLCHAIN_BIN}/${tool}" >/dev/null
    ${SUDO} chmod 0755 "${TOOLCHAIN_BIN}/${tool}"
  done
  "${TOOLCHAIN_BIN}/python3" --version >&2 || fail "python ${version} does not run"
}

module_node_fetch() { echo "node.tar.gz lock:node@${1}"; }
module_node_install() {
  local packages="$2" dest="${TOOLCHAIN_ROOT}/node"
  unpack_to "${WORK}/node.tar.gz" "${dest}"
  link_tool "${dest}/bin/node" "${dest}/bin/npm" "${dest}/bin/npx"
  if [ -n "${packages}" ]; then
    # shellcheck disable=SC2086
    ${SUDO} env PATH="${dest}/bin:${PATH}" npm install -g --prefix "${dest}" ${packages} >&2 ||
      fail "npm install -g ${packages}"
  fi
  "${TOOLCHAIN_BIN}/node" --version >&2 || fail "node does not run"
}

module_go_fetch() { echo "go.tar.gz lock:go@${1}"; }
module_go_install() {
  local packages="$2" dest="${TOOLCHAIN_ROOT}/go" pkg
  unpack_to "${WORK}/go.tar.gz" "${dest}"
  link_tool "${dest}/bin/go" "${dest}/bin/gofmt"
  for pkg in ${packages}; do
    case "${pkg}" in
      *@*) ;;
      *) fail "go toolchain package ${pkg} needs a version (${pkg}@vX.Y.Z)" ;;
    esac
  done
  # A private module cache, made and removed by the user that builds: the
  # fixed /tmp/zenthros-gopath could be made first by any local user, who
  # then owned the sources root compiled (audit 2026-10-16 NEW-008).
  if [ -n "${packages}" ]; then
    local gotmp
    gotmp="$(${SUDO} mktemp -d /tmp/zenthros-go.XXXXXX)" || fail "mktemp for go install"
    for pkg in ${packages}; do
      ${SUDO} env GOBIN="${TOOLCHAIN_BIN}" GOPATH="${gotmp}/path" GOCACHE="${gotmp}/cache" GOFLAGS=-modcacherw \
        "${dest}/bin/go" install "${pkg}" >&2 || fail "go install ${pkg}"
    done
    ${SUDO} rm -rf "${gotmp}"
  fi
  "${TOOLCHAIN_BIN}/go" version >&2 || fail "go does not run"
}

module_java_fetch() { echo "java.tar.gz lock:java@${1}"; }
module_java_install() {
  local packages="$2" dest="${TOOLCHAIN_ROOT}/java"
  [ -z "${packages}" ] || fail "the java toolchain takes no packages (got: ${packages}); ship dependencies with the job"
  unpack_to "${WORK}/java.tar.gz" "${dest}"
  link_tool "${dest}/bin/java" "${dest}/bin/javac" "${dest}/bin/jar"
  "${TOOLCHAIN_BIN}/java" -version >&2 || fail "java does not run"
}

install_toolchain() {
  local index="$1" spec name version
  spec="${TOOLCHAINS[$index]}"
  name="${spec%%@*}"
  version=""
  [ "${spec}" = "${name}" ] || version="${spec#*@}"
  event "toolchain_install_start" "toolchain=${spec}"
  "module_${name}_install" "${version}" "${TOOLCHAIN_PACKAGES[$index]}"
  toolchain_path
  event "toolchain_install_end" "toolchain=${spec}"
}

# ---------------------------------------------------------------- run

run_step() {
  case "$1" in
    binaries) install_binaries ;;
    tailscale) install_tailscale ;;
    scaffold) scaffold ;;
    containerd) install_containerd ;;
    gpu) install_gpu ;;
    packages) pkg_install "${PACKAGES[@]}" ;;
    toolchain:*) install_toolchain "${1#toolchain:}" ;;
    runtime-default) write_runtime_default ;;
    runtime-ready) mark_runtime_ready ;;
    *) fail "unknown step $1" ;;
  esac
}

run_runtime() {
  local step
  for step in "${BACKGROUND[@]}"; do
    run_step "${step}"
  done
}

main() {
  parse_args "$@"
  detect_host
  build_plan
  if [ "${ZENTHROS_PLAN_ONLY:-}" = 1 ]; then
    print_plan
    return 0
  fi

  # Where the agent comes from is the hub's: it publishes it and its attach
  # line says where. There is no default to fall back on.
  [ -n "${INSTALL_BASE}${ZENTHROS_ARTIFACT_URLS:-}" ] ||
    fail "INSTALL_BASE is not set: run the attach line your hub printed, which sets it"
  if [ "${ZENTHROS_PREFLIGHT:-}" = 1 ] && [ "${ONLY_TOOLCHAINS}" != 1 ]; then
    preflight
  fi
  ensure_commands curl tar gzip
  WORK="$(mktemp -d /tmp/zenthros-install.XXXXXX)"
  if [ "${ZENTHROS_RUNTIME_BACKGROUND:-}" != 1 ]; then
    trap 'rm -rf "${WORK}"' EXIT
  fi

  event "install_start" "arch=${ARCH} pkg=${PKG}"
  # Nothing is installed before SHA256SUMS matches its pin and every download
  # matches SHA256SUMS or upstream.lock.
  fetch_manifest
  fetch_all
  verify_and_unpack
  local step
  for step in "${STEPS[@]}"; do
    run_step "${step}"
  done
  event "base_ready"

  if [ "${ONLY_TOOLCHAINS}" = 1 ]; then
    run_runtime
    ok "Toolchains installed: ${TOOLCHAINS[*]}"
    return 0
  fi
  if [ "${ZENTHROS_RUNTIME_BACKGROUND:-}" = 1 ]; then
    mark_runtime_pending
    (
      trap 'rm -rf "${WORK}"' EXIT
      run_runtime
    ) >"${ZENTHROS_RUNTIME_LOG:-/var/log/zenthros-runtime.log}" 2>&1 &
    echo $! | ${SUDO} tee "${STATE_DIR}/runtime-install.pid" >/dev/null
    ok "Base ready; the runtime installs in the background. The agent may join now."
  else
    run_runtime
    ok "Host ready. Next: paste your one-time 'zenadm join ...' command."
  fi
}

# ZENTHROS_INSTALLER_LIB=1 defines the functions without running (tests).
if [ "${ZENTHROS_INSTALLER_LIB:-}" != 1 ]; then
  main "$@"
fi
