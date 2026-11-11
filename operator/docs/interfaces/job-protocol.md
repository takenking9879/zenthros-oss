# The job protocol: reports, checkpoints, notices, children

A job talks to the agent on its machine for four things:

- publishing a report;
- committing a checkpoint;
- learning that the machine is being taken back;
- spawning child jobs and following them ([fan-out.md](../fan-out.md)).

All four are plain HTTP on the agent's socket, or a file. Nothing here
needs Python.

You can use the protocol three ways, and all three send the same bytes:

- **`$ZENTHROS_AGENT_BIN job ...`**: the agent binary. Use it from shell,
  Node, Go, Java or R, with no library.
- **The Python SDK** (`zenthros.report`, `@zenthros.phase`,
  `zenthros.preemption`, `zenthros.spawn`, `zenthros.map`).
- **Your own HTTP client**, following the wire format below.

`operator/pkg/jobcli/testdata/wire.json` is one job's exact bodies. Two tests
check against it:

- `TestAgentJobCLICheckpointMatchesSDKWire` and
  `TestAgentJobCLISpawnMatchesSDKWire` (Go) check the CLI;
- `sdk/python/tests/test_job_protocol.py` and `test_fanout.py` check the SDK.

If either one drifts, its test fails.

## Env

| Variable | What it is |
|---|---|
| `ZENTHROS_AGENT_BIN` | The agent binary, on machines (process or container). In a container, it is the host's binary bound read-only at `/usr/local/bin/zenthros-agent`. |
| `ZENTHROS_REPORT_ENDPOINT` | `unix:///var/run/zenthros/agent/reports.sock` on a machine; `http://zenthros-agent.zenthros-system.svc.cluster.local:9445` on Kubernetes. |
| `ZENTHROS_REPORT_TOKEN` | The job's capability token, sent as `Authorization: Bearer <token>`. |
| `ZENTHROS_JOB_ID` | The ZenthrosJob UID. |
| `ZENTHROS_GENERATION` | The lease generation that checkpoints are written under. |
| `ZENTHROS_CHECKPOINT_PREFIX` | Where checkpoints go (`workload.template.artifacts.output`). |
| `ZENTHROS_CHECKPOINT_CURSORS` | Every acked phase: `{"<phase>": {"seq", "uri", "sha256", "complete"}}`. |
| `ZENTHROS_CHECKPOINT_PHASE`, `ZENTHROS_CHECKPOINT_URI`, `ZENTHROS_CHECKPOINT_SEQ`, `ZENTHROS_CHECKPOINT_SHA256`, `ZENTHROS_CHECKPOINT_COMPLETE` | The latest acked cursor. |
| `ZENTHROS_PREEMPT_FILE` | Where the interruption notice appears. |
| `ZENTHROS_ATTEMPT` | 0, then n on the n-th re-run after a process failure. |
| `TASK_INDEX`, `TASK_COUNT` | For Independent tasks and for children; see [distributed-vms.md](../distributed-vms.md). |
| `ZENTHROS_PARENT_JOB_ID`, `ZENTHROS_CHILD_GROUP` | In a child: its parent's UID and its group. |
| `ZENTHROS_DATA_OUTPUT` | `workload.template.artifacts.output`. A child's is `<parent output>/children/<group>/<index>`. |
| `ZENTHROS_DATA_INPUT` | `workload.template.artifacts.input`. |
| `ZENTHROS_UPSTREAM_RESULTS` | The result each `spec.dependsOn` job reported, pinned by digest. |
| `ZENTHROS_CACHE_DIR` | Where the artifacts a Prewarm put on the machine are, by the paths it named. |
| `ZENTHROS_SECRETS_DIR` | Where file secrets that name no path are, as `<dir>/<secret-name>/<key>`. |
| `ZENTHROS_ENV_CACHE` | In a container: where the SDK keeps function environments between jobs. |

The job's author may set these:

| Variable | What it is |
|---|---|
| `ZENTHROS_PRODUCER_ID` | Who sends the job's reports; the hostname when unset. |
| `ZENTHROS_JOB_STATE_DIR` | Where the CLI keeps what one run already said between calls. |
| `ZENTHROS_CHECKPOINT_LOCAL_ROOT` | For the SDK: a local directory that `file://` checkpoints map to. |

The SDK sets `ZENTHROS_MAP_ITEMS` on the children of a `map`: where their
items are.

`wire.json`'s `job_env` lists every one of them with who sets it, and the
tests hold each setter (the control plane, the agent, a task, a child, the
SDK) to exactly its variables, and the CLI and the SDK to reading no other.

On Kubernetes the agent binary is not in the pod. Use the Python SDK, your
own HTTP client, or copy the binary into your image.

## The CLI

```sh
A="$ZENTHROS_AGENT_BIN"

# Resume: the acked cursor of phase "train", if there is one.
if cur=$("$A" job checkpoint cursor --phase train); then
  uri=$(printf '%s' "$cur" | sed -n 's/.*"uri":"\([^"]*\)".*/\1/p')
  step=$(cat "${uri#file://}")
fi

# Checkpoint. Under a local prefix the CLI copies the file to the next URI.
"$A" job checkpoint commit --phase train --file state.bin

# Under s3:// or gs://, upload it yourself, then commit that URI.
next=$("$A" job checkpoint next --phase train)   # {"seq":3,"uri":"s3://.../train/3"}
aws s3 cp state.bin "$(printf '%s' "$next" | sed -n 's/.*"uri":"\([^"]*\)".*/\1/p')"
"$A" job checkpoint commit --phase train --file state.bin --uri s3://.../train/3

"$A" job checkpoint commit --phase train --file final.bin --complete

# Report. Each call sends the whole snapshot: earlier keys plus these ones.
"$A" job report set epoch=3 loss=0.25 tags='["a","b"]'
"$A" job report set accuracy=0.91 --final --artifact s3://bucket/model=model

# Has the machine been told it is being taken back? Checkpoint now if so.
if "$A" job preempted >/dev/null; then "$A" job checkpoint commit --phase train --file state.bin; fi

"$A" job task    # TASK_INDEX=1, TASK_COUNT=4, ZENTHROS_JOB_ID=..., one per line

# Fan-out. Everything after --cmd is the children's command.
"$A" job spawn --name shards --count 4 --env MODE=fast --cmd python3 work.py --in s3://b/in
"$A" job spawn --name again --count 2 --same      # children run this job's own command
"$A" job children --name shards                   # train-shards-0 Succeeded, one per line
"$A" job children --wait -o json                  # every group; exit 1 if a child failed
```

### What each command does

- `report set`:
  - a `VALUE` that parses as JSON is sent as JSON (`3`, `0.25`, `true`,
    `["a"]`, `'"text"'`); anything else is sent as a string;
  - the CLI keeps what the run has already set in
    `$ZENTHROS_JOB_STATE_DIR` (default `/tmp`), so every call sends a
    complete snapshot, as the SDK's `Report` does.
- `checkpoint commit`:
  - `--seq` defaults to one past the highest seq of the phase, from the
    acked cursor or from this run's last commit;
  - `sha256` is the hash of `--file`, which must be the bytes stored at the
    URI.
- `checkpoint next` prints the seq and URI of the next checkpoint.
- `checkpoint cursor` prints the acked cursor. It exits 1 when the phase has
  none.
- `spawn`:
  - `--name` is the group, a DNS label. `--count` is at least 1. It takes
    exactly one of `--same` or `--cmd`, and flags go before `--cmd`;
  - it prints the children, `<name> <phase>` per line, or their JSON with
    `-o json`. It returns once every child exists;
  - running it again with the same arguments creates nothing and prints the
    same children. That is what a replacement of this job does.
- `children` prints the children of `--name`, or of every group.
- `--wait`, on either one, polls every 5 seconds until every child has
  ended. A child that ended other than Succeeded prints
  `CHILD_FAILED: <child> ended <phase>: <its message>` on stderr, and the
  exit code is 1.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Done. The agent's answer is on stdout. |
| 1 | Refused or failed. The agent's own words are on stderr, as `CODE: message` (for example `UNAUTHORIZED: job capability token is invalid`). |
| 2 | Usage error. |
| 3 | Checkpoint frozen. A replacement was already submitted from an earlier cursor, so this commit can never land. Stop; the replacement carries on. |

A transient answer is retried, 15 times, 2 seconds apart, as the SDK does:

- connection refused, while the agent restarts;
- 502, 503 or 504.

## Wire

### `POST /v1/reports`

The body is the whole snapshot:

```json
{"apiVersion":"zenthros.ai/v1","kind":"ExecutionReport","job_id":"<uid>",
 "producer_id":"<host>","report_id":"<uuid>","produced_at":"2026-01-02T03:04:05Z",
 "payload":{"epoch":3},"artifacts":[],"final":false}
```

`202` means the report is durable in the agent's spool. See
[execution-report.md](../execution-report.md).

### `POST /v1/checkpoints`

The body:

```json
{"job_id":"<uid>","generation":3,"phase":"train","seq":2,
 "uri":"s3://bucket/p/jobs/<uid>/gen/3/train/2","sha256":"<hex>","complete":false}
```

The URI layout is `<prefix>/jobs/<uid>/gen/<generation>/<phase>/<seq>`.

`generation` is `$ZENTHROS_GENERATION`. A commit on another generation is
refused. A commit without one (or `0`) is judged only by the lease: it is
acked while the job's lease is live on the backend it came from. Send it
always: it is what keeps an older run of the job on the same machine from
moving the cursor.

Answers:

- `202` means the controller persisted the cursor, or the agent holds the
  commit because the controller could not decide it (the link or the
  controller down, its write failed or was cut off). The agent sends a held
  commit again until the controller decides it, in order with the job's
  later commits, so a job that ends during a long controller outage still
  succeeds. The controller's refusal of a held commit reaches only the
  agent's log;
- `409` means it is refused, and nothing of the commit was kept. The
  `error_code` says what to do; the `error` words are for people:
  - `CHECKPOINT_FROZEN`: a replacement already took the cursor
    (`cursor frozen: replacement submitted at T`). No retry lands; stop.
  - `LEASE_NOT_LIVE`: the job's lease is not live. A handover may still
    land: send the same commit again shortly.
  - `REJECTED`: any other refusal (a commit behind the cursor, other bytes
    at an acked seq, a stale generation);
- `503 STORE_UNAVAILABLE` means no decision reached the job and the agent
  could not hold the commit (its spool failed). The commit may have been
  kept. Send the same commit again, or spend the seq: the next commit takes
  the one after. Never commit other bytes at that seq, which is refused when
  the first was kept.

A resumed job loads the acked URI. It never lists the bucket. See
[checkpoints.md](../checkpoints.md).

### `POST /v1/children`

To spawn:

```json
{"job_id":"<uid>","generation":3,"group":"shards","count":4,
 "command":["python3","work.py","--in","s3://b/in"],"env":{"MODE":"fast"}}
```

Or `"same":true` in place of `command`. To list, send `"list":true` and
`"count":0`, and an empty `group` for every group.

The answer, once every child exists:

```json
{"accepted":true,"children":[
 {"group":"shards","index":0,"name":"train-shards-0","phase":"Running","message":""}]}
```

Answers:

- `200` with the children;
- `409` with the controller's refusal, which creates nothing:

  | `error_code` | When |
  |---|---|
  | `SPAWN_QUOTA_EXCEEDED` | Past the per-job or per-workspace limit; the message names the setting. |
  | `LEASE_NOT_LIVE` | The job is over, cancelled, lost its worker, or runs elsewhere or at a later generation. |
  | `NO_GRANDCHILDREN` | A child tried to spawn. |
  | `GROUP_MISMATCH` | The group exists with another count, command or env. |
  | `SPAWN_INVALID` | A bad name or count, neither or both of `command` and `same`, or env Zenthros sets. |
  | `SPAWN_ON_UNTRUSTED_HOST` | The job runs on a host a third party runs (or whose provider is not known) and does not carry `zenthros.ai/spawn-on-untrusted-host`: that host's owner can act as the job. |
  | `PARENT_NOT_FOUND` | No job has that UID. |
  | `CHILD_CREATE_FAILED` | The API server refused a child; its words follow the child's name. |

- `503` is retryable, with either code:
  - `STORE_UNAVAILABLE`: the controller could not be reached;
  - `LEASE_PENDING`: the job asked in the moment between its start and the
    controller recording its lease (a job that spawns first thing, or a
    replacement's first seat). The CLI and the SDK ask again, like any 503;
- `400` for a body with an unknown field (`backend_id` included: the
  gateway sets it) and `401` for a bad token.

### Errors

Every refusal has this body:

```json
{"error_code":"UNAUTHORIZED","error":"job capability token is invalid"}
```

The CLI and the SDK pass both fields to the job unchanged.

### The interruption notice

`$ZENTHROS_PREEMPT_FILE` appears when the provider announces that the
machine is being taken back. It holds JSON:
`{"source","action","deadline","notedAt"}`.

A checkpoint committed before the replacement starts is the one the
replacement resumes from.

## Tests

| E2E | Pins |
|---|---|
| `TestShellJobCheckpointsWithAgentCLI` (local VM: a shell job checkpoints, fails, resumes from the cursor and reports) | `TestAgentJobCLICheckpointMatchesSDKWire`, `TestAgentJobCLIReportMatchesSDKWire`, `TestJobCLIAgainstRealReportServer`, `TestJobSeesAgentBin`, `TestAgentBinOutsideSpecHash`, `test_job_protocol.py` |
| `TestShellJobSpawnsChildren`, `TestSpawnQuotaVerbatim` (local VM) | `TestAgentJobCLISpawnMatchesSDKWire`, `TestSpawnRefusalIsVerbatim`, `TestChildrenEndpointForwardsAfterToken`, `test_fanout.py` |
