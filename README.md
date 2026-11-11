# Zenthros: the agent and the SDK

Zenthros runs your Python code on machines of yours, from a notebook or a
script: a computer you attach, a VM, or a GPU pod in a cloud account of yours.
This repository is the part that runs on your side, open source under the
Apache License 2.0:

| Path | What it is |
| ---- | ---------- |
| `sdk/python` | The `zenthros` Python package ([its README](sdk/python/README.md)) |
| `operator/cmd/agent` | The agent on a machine you attach. It connects out to the hub over mutual TLS and runs your workspace's jobs as a user without privileges |
| `operator/cmd/zenadm` | What joins a machine to a hub |
| `operator/scripts/install-agent.sh`, `install/` | The installers, and how a release is built |
| `operator/internal/grpc/proto` | The protocol between the agent and the hub |
| `k3s/helm/zenthros-agent` | The agent's chart, for a Kubernetes cluster you attach |

The hub that coordinates them (`api.zenthros.app`) is not in this repository.
The site: [zenthros.app](https://zenthros.app).

## Build and test

```bash
cd operator && go build ./cmd/agent ./cmd/zenadm ./cmd/unzstd && go test ./...
make -C sdk/python env test lint
```

The Go version is the `toolchain` line of `operator/go.mod`; the SDK's
environment is `sdk/python/.venv`, made with uv by `make env`.

## Check that a release is this code

The agent runs as root on your machine, so you can check that the one you
downloaded comes from this code. `.github/workflows/release.yml` builds each
release from its tag. Build the same binary and compare:

```bash
git checkout v0.1.0
cd operator
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false \
  -ldflags="-s -w -X github.com/zenthros/operator/internal/buildinfo.Release=v0.1.0" \
  -o zenthros-agent ./cmd/agent
sha256sum zenthros-agent
# the release's agent, unpacked:
curl -fsSL https://github.com/takenking9879/zenthros-oss/releases/download/v0.1.0/zenthros-agent-linux-amd64.zst | zstd -d | sha256sum
```

The two digests are the same. `SHA256SUMS` in each release covers every file
the installer downloads, and the hub pins that file's digest.

## License

[Apache License 2.0](LICENSE). Contact: [hola@zenthros.app](mailto:hola@zenthros.app).
