#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0

# Publish the agent as a public image, for on="local-k8s" with no account:
#
#   bash install/publish-agent-image.sh <registry>/<repository>/agent:<tag>
#
# It prints the image by digest, what install/publish-sdk-gcs.sh takes as
# ZENTHROS_AGENT_IMAGE. Compiled across, as install/stage.sh does, and only
# copied into the image: the target of production-images/Dockerfile.golang
# compiles the whole module under emulation for arm64 (over an hour). gzip, as
# anyone's runtime reads it. ZENTHROS_RELEASE: the release the agent says it
# is (default dev).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${1:-}"
fail() { echo "publish-agent-image: $*" >&2; exit 1; }
[ -n "${IMAGE}" ] || fail "usage: publish-agent-image.sh <registry>/<repository>/agent:<tag>"

# shellcheck source=release.sh
. "${ROOT}/install/release.sh"
LDFLAGS="$(release_ldflags "${ZENTHROS_RELEASE:-}")"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
for arch in amd64 arm64; do
  for binary in agent zenadm; do
    (cd "${ROOT}/operator" && CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" \
      go build -trimpath -buildvcs=false -ldflags="${LDFLAGS}" -o "${work}/${binary}-${arch}" "./cmd/${binary}")
  done
done
cat > "${work}/Dockerfile" <<'DOCKERFILE'
FROM gcr.io/distroless/static:nonroot
ARG TARGETARCH
WORKDIR /
USER 65532:65532
COPY agent-${TARGETARCH} /agent
COPY zenadm-${TARGETARCH} /zenadm
ENTRYPOINT ["/agent"]
DOCKERFILE
docker buildx build --builder "${BUILDER:-zstd}" --platform linux/amd64,linux/arm64 --provenance=false \
  --metadata-file "${work}/metadata.json" \
  --output "type=image,name=${IMAGE},push=true,compression=gzip,oci-mediatypes=true" "${work}" >&2
digest="$(sed -n 's/.*"containerimage.digest": *"\(sha256:[0-9a-f]*\)".*/\1/p' "${work}/metadata.json" | head -1)"
[ -n "${digest}" ] || fail "the build named no digest"
echo "${IMAGE%:*}@${digest}"
