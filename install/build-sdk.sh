#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0

# Build the Python SDK's wheel and sdist, pinned to one release of the agent:
#
#   bash install/build-sdk.sh OUTDIR RELEASE_URL [AGENT_IMAGE]
#
# RELEASE_URL is where SHA256SUMS and the agent are (install/release.sh prints
# it). The pin goes into the wheel, so that a computer with no account verifies
# the agent it downloads against this SDK and nothing else, and can do it with
# no network once it has it. AGENT_IMAGE is the same agent as an image, by
# digest, for on="local-k8s"; without it, that target says it has none. A copy of what the package is made of is built, so
# the build leaves nothing in the tree and takes nothing else (examples, tests,
# a local build/). ZENTHROS_SHA256SUMS: the release's SHA256SUMS as a file,
# for a release that is not published yet (the public repository's
# release.yml builds the wheel before it uploads the release).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-}"
RELEASE="${2:-}"
RELEASE="${RELEASE%/}"
IMAGE="${3:-}"
fail() { echo "build-sdk: $*" >&2; exit 1; }
[ -n "${OUT}" ] && [ -n "${RELEASE}" ] || fail "usage: build-sdk.sh OUTDIR RELEASE_URL"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
if [ -n "${ZENTHROS_SHA256SUMS:-}" ]; then
  cp "${ZENTHROS_SHA256SUMS}" "${work}/SHA256SUMS" || fail "no ${ZENTHROS_SHA256SUMS}"
else
  curl -fsSL "${RELEASE}/SHA256SUMS" -o "${work}/SHA256SUMS" || fail "no SHA256SUMS at ${RELEASE}"
fi
pin="$(sha256sum "${work}/SHA256SUMS" | cut -d' ' -f1)"

mkdir "${work}/src"
for part in pyproject.toml README.md LICENSE MANIFEST.in zenthros; do
  if [ -e "${ROOT}/sdk/python/${part}" ]; then cp -r "${ROOT}/sdk/python/${part}" "${work}/src/"; fi
done
find "${work}/src" -name __pycache__ -type d -prune -exec rm -rf {} +
printf '{"install_base": "%s", "sha256sums_sha256": "%s", "agent_image": "%s"}\n' "${RELEASE}" "${pin}" "${IMAGE}" \
  > "${work}/src/zenthros/_data/agent-release.json"

python3 -m venv "${work}/venv"
"${work}/venv/bin/pip" install -q "$(grep '^build==' "${ROOT}/sdk/python/requirements-dev.txt")"
mkdir -p "${OUT}"
"${work}/venv/bin/python" -m build -q --outdir "${OUT}" "${work}/src" >/dev/null
echo "built $(ls "${OUT}"/*.whl) on the agent of ${RELEASE}"
