# SPDX-License-Identifier: Apache-2.0

# Sourced by publish.sh and publish-gcs.sh: the name of this publication.
#
# Each publication is a release of its own under the bucket's prefix, and is
# never written over. A hub pins the SHA256SUMS of the release it joins
# machines from (join config sha256sumsSHA256, installBase); a SHA256SUMS
# changed under it would stop every machine it joins. A new release is a new
# directory, and a hub moves to it when its values say so.
#
# ZENTHROS_RELEASE names it (v0.4.0). By default it is the commit
# (git describe), and a tree whose sources changed adds what it ships (the
# git tree of RELEASE_SOURCES as they are on disk): two publications of a
# changing tree never share a name, and the same tree is the same release,
# which a second publication finds there and does not make again.

# RELEASE_SOURCES are what a release is built from (stage.sh,
# publish-agent-image.sh, build-sdk.sh): a change anywhere else (notes, the
# hub's own scripts) is not a release of its own.
RELEASE_SOURCES=(operator install sdk/python k3s/helm)

# shipped_tree prints the git tree of RELEASE_SOURCES as they are on disk
# (changes, and new files git would keep), over the commit's tree, through
# an index of its own: the repository's index is not touched.
shipped_tree() {
  local index rc
  index="$(mktemp)" || return 1
  cp "$(git -C "${ROOT}" rev-parse --path-format=absolute --git-path index)" "${index}" &&
    GIT_INDEX_FILE="${index}" git -C "${ROOT}" add -A -- "${RELEASE_SOURCES[@]}" &&
    GIT_INDEX_FILE="${index}" git -C "${ROOT}" write-tree
  rc=$?
  rm -f "${index}"
  return "${rc}"
}

# release prints the release name, or fails on one that cannot be a path.
release() {
  local name="${ZENTHROS_RELEASE:-}"
  if [ -z "${name}" ]; then
    name="$(git -C "${ROOT}" describe --tags --always 2>/dev/null)" || name=untracked
    if [ -n "$(git -C "${ROOT}" status --porcelain -- "${RELEASE_SOURCES[@]}" 2>/dev/null)" ]; then
      local tree
      tree="$(shipped_tree)" || return 1
      name="${name}-dirty-${tree:0:12}"
    fi
  fi
  case "${name}" in
    ""|.|..|*[!A-Za-z0-9._-]*)
      echo "ZENTHROS_RELEASE=${name}: letters, digits, '.', '_' and '-' only" >&2
      return 1
      ;;
  esac
  printf '%s' "${name}"
}

# release_ldflags prints the go build -ldflags that strip a binary and name
# the release it is built for (operator/internal/buildinfo): the agent tells
# its hub that version, and zctl tells zenthros-api.
release_ldflags() {
  printf '%s' "-s -w -X github.com/zenthros/operator/internal/buildinfo.Release=${1:-dev}"
}

# release_values prints the controller chart values that pin a hub to the
# release published at url with the digests stage.sh printed.
release_values() {
  local url="$1" digests="$2"
  echo "provisioner.managedEnrollment.installBase=${url}"
  echo "provisioner.managedEnrollment.installerURL=${url}/install-agent.sh"
  echo "provisioner.managedEnrollment.installerSHA256=$(printf '%s\n' "${digests}" | sed -n 's/^installer_sha256=//p')"
  echo "provisioner.managedEnrollment.sha256sumsSHA256=$(printf '%s\n' "${digests}" | sed -n 's/^sha256sums_sha256=//p')"
}
