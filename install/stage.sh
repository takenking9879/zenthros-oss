#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0

# Stage everything the official installer downloads from a Zenthros artifact
# base into DIR, and write SHA256SUMS over it:
#
#   bash install/stage.sh DIR
#
#   install-agent.sh                          the installer; the bootstrap pins it
#   upstream.lock                             pins every upstream download
#   zenthros-unzstd-linux-{amd64,arm64}       static zstd unpacker (cmd/unzstd), not
#                                             compressed: the installers and a
#                                             container's boot unpack every .zst with
#                                             it, so a machine needs no zstd or root
#   zenadm-linux-{amd64,arm64}.zst            zstd -3
#   zenthros-agent-linux-{amd64,arm64}.zst    for VMs and containers alike
#   containerd-linux-{amd64,arm64}.tar.zst    bin/ of the pinned containerd release
#   runc-linux-{amd64,arm64}.zst              the pinned runc release
#   install-container.sh                      installs the agent in a running container (zenadm container-installer)
#   zenthros-agent-chart.tgz                  the agent's Helm chart, for a cluster attached to a
#                                             Workspace (helm reads only .tgz: not zstd)
#   SHA256SUMS
#
# The last two lines printed are the digests a managed bootstrap pins:
#   installer_sha256=<hex>
#   sha256sums_sha256=<hex>
#
# publish.sh (S3), publish-gcs.sh (GCS) and the E2E harnesses publish DIR as is.
#
# Env: ZENTHROS_RELEASE, the release the agent and zenadm say they are
# (default dev); ZENTHROS_STAGE_INSTALL_BASE, the URL DIR will be published at, written
# into install-container.sh (default none: the script then needs --install-base);
# ZENTHROS_STAGE_ARCHES (default "amd64 arm64"); ZENTHROS_STAGE_BINARIES,
# a directory with prebuilt zenadm-linux-<arch>, zenthros-agent-linux-<arch>
# and install-container.sh (zenadm container-installer), and optionally
# zenthros-unzstd-linux-<arch> (else built here)
# (default: go build from operator/); UPSTREAM_LOCK; ZENTHROS_STAGE_CACHE, where
# verified upstream downloads are kept between runs, by SHA-256.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${1:-}"
ARCHES="${ZENTHROS_STAGE_ARCHES:-amd64 arm64}"
LOCK="${UPSTREAM_LOCK:-${ROOT}/install/upstream.lock}"
CACHE="${ZENTHROS_STAGE_CACHE:-${XDG_CACHE_HOME:-${HOME}/.cache}/zenthros/upstream}"
# The agent and zenadm name the release they are built for (ZENTHROS_RELEASE).
# shellcheck source=release.sh
. "${ROOT}/install/release.sh"
LDFLAGS="$(release_ldflags "${ZENTHROS_RELEASE:-}")"

fail() { echo "stage.sh: $*" >&2; exit 1; }
[ -n "${DEST}" ] || fail "usage: stage.sh DIR"
command -v zstd >/dev/null 2>&1 || fail "zstd is required"
[ -f "${LOCK}" ] || fail "missing ${LOCK}"

mkdir -p "${DEST}" "${CACHE}"
rm -f "${DEST}"/*.zst "${DEST}"/*.gz "${DEST}"/zenthros-unzstd-linux-* "${DEST}"/*.tgz "${DEST}/SHA256SUMS" "${DEST}/install-agent.sh" "${DEST}/install-container.sh" "${DEST}/upstream.lock"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

# upstream KEY ARCH prints the path of the lock's download for KEY on ARCH,
# fetched once and checked against the lock.
upstream() {
  local sha url file
  read -r sha url < <(awk -v p="$1@" -v arch="$2" \
    '!/^#/ && NF == 4 && index($1, p) == 1 && $2 == arch { print $3, $4; exit }' "${LOCK}") || true
  [ -n "${url:-}" ] || fail "upstream.lock has no $1 for $2"
  file="${CACHE}/${sha}"
  if ! echo "${sha}  ${file}" | sha256sum -c --quiet - >/dev/null 2>&1; then
    curl -fsSL --retry 3 --retry-delay 2 -o "${file}.part" "${url}" || fail "download ${url}"
    echo "${sha}  ${file}.part" | sha256sum -c --quiet - >/dev/null 2>&1 ||
      fail "checksum mismatch for ${url} (upstream.lock says ${sha})"
    mv "${file}.part" "${file}"
  fi
  printf '%s' "${file}"
}

for arch in ${ARCHES}; do
  case "${arch}" in amd64|arm64) ;; *) fail "unsupported arch ${arch} (amd64 or arm64)" ;; esac
  if [ -n "${ZENTHROS_STAGE_BINARIES:-}" ]; then
    cp "${ZENTHROS_STAGE_BINARIES}/zenadm-linux-${arch}" "${ZENTHROS_STAGE_BINARIES}/zenthros-agent-linux-${arch}" "${WORK}/"
  else
    (
      cd "${ROOT}/operator"
      CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go build -trimpath -buildvcs=false -ldflags="${LDFLAGS}" -o "${WORK}/zenadm-linux-${arch}" ./cmd/zenadm
      CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go build -trimpath -buildvcs=false -ldflags="${LDFLAGS}" -o "${WORK}/zenthros-agent-linux-${arch}" ./cmd/agent
    )
  fi
  if [ -n "${ZENTHROS_STAGE_BINARIES:-}" ] && [ -f "${ZENTHROS_STAGE_BINARIES}/zenthros-unzstd-linux-${arch}" ]; then
    cp "${ZENTHROS_STAGE_BINARIES}/zenthros-unzstd-linux-${arch}" "${DEST}/"
  else
    (
      cd "${ROOT}/operator"
      CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go build -trimpath -buildvcs=false -ldflags="-s -w -buildid=" -o "${DEST}/zenthros-unzstd-linux-${arch}" ./cmd/unzstd
    )
  fi
  chmod 0755 "${DEST}/zenthros-unzstd-linux-${arch}"
  zstd -q -3 -T0 -f -o "${DEST}/zenadm-linux-${arch}.zst" "${WORK}/zenadm-linux-${arch}"
  zstd -q -3 -T0 -f -o "${DEST}/zenthros-agent-linux-${arch}.zst" "${WORK}/zenthros-agent-linux-${arch}"

  mkdir -p "${WORK}/containerd-${arch}"
  tar -C "${WORK}/containerd-${arch}" -xzf "$(upstream containerd "${arch}")"
  [ -x "${WORK}/containerd-${arch}/bin/containerd" ] || fail "the containerd release for ${arch} has no bin/containerd"
  tar -C "${WORK}/containerd-${arch}" -cf "${WORK}/containerd-linux-${arch}.tar" bin
  zstd -q -3 -T0 -f -o "${DEST}/containerd-linux-${arch}.tar.zst" "${WORK}/containerd-linux-${arch}.tar"
  zstd -q -3 -T0 -f -o "${DEST}/runc-linux-${arch}.zst" "$(upstream runc "${arch}")"
done

cp "${ROOT}/operator/scripts/install-agent.sh" "${DEST}/install-agent.sh"
chmod 0755 "${DEST}/install-agent.sh"
cp "${LOCK}" "${DEST}/upstream.lock"
# install-container.sh installs the agent in a container that is already
# running, as install-agent.sh does on a VM. zenadm prints it: the build that
# made prebuilt binaries left it next to them, else the zenadm built here for
# this machine does.
host_arch="$(uname -m | sed 's/^x86_64$/amd64/; s/^aarch64$/arm64/')"
if [ -n "${ZENTHROS_STAGE_BINARIES:-}" ]; then
  cp "${ZENTHROS_STAGE_BINARIES}/install-container.sh" "${DEST}/install-container.sh"
elif [ -x "${WORK}/zenadm-linux-${host_arch}" ] && [ "$(uname -s)" = Linux ]; then
  "${WORK}/zenadm-linux-${host_arch}" container-installer > "${DEST}/install-container.sh"
else
  (cd "${ROOT}/operator" && go run ./cmd/zenadm container-installer) > "${DEST}/install-container.sh"
fi
[ -s "${DEST}/install-container.sh" ] || fail "zenadm container-installer printed nothing"
# The container installer downloads from where it is published, so that
# `curl <base>/install-container.sh | sh` needs no flags: write the base in.
if [ -n "${ZENTHROS_STAGE_INSTALL_BASE:-}" ]; then
  base="${ZENTHROS_STAGE_INSTALL_BASE%/}"
  case "${base}" in
    *"'"*|*"|"*|*"&"*|*"\\"*) fail "ZENTHROS_STAGE_INSTALL_BASE ${base} has a character the installer cannot hold" ;;
  esac
  sed "s|^published='[^']*'\$|published='${base}'|" "${DEST}/install-container.sh" > "${WORK}/install-container.sh"
  grep -qx "published='${base}'" "${WORK}/install-container.sh" || fail "install-container.sh has no published line to set"
  mv "${WORK}/install-container.sh" "${DEST}/install-container.sh"
fi
chmod 0755 "${DEST}/install-container.sh"
# The agent chart as helm packages it (one top directory), byte for byte the
# same from the same sources: names sorted, no times, no owners.
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -C "${ROOT}/k3s/helm" -cf - zenthros-agent |
  gzip -9 -n > "${DEST}/zenthros-agent-chart.tgz"
(
  cd "${DEST}"
  sha256sum install-agent.sh install-container.sh upstream.lock ./zenthros-unzstd-linux-* ./*.zst ./*.tgz | sed 's|  \./|  |' > SHA256SUMS
)

echo "installer_sha256=$(sha256sum "${DEST}/install-agent.sh" | cut -d' ' -f1)"
echo "sha256sums_sha256=$(sha256sum "${DEST}/SHA256SUMS" | cut -d' ' -f1)"
