# MCP Server

`cowshed-mcp` is a separate binary built on `cowshed-core` + `cowshed-shell` — **never** a wrapper around the CLI. It
exposes warm workspaces to MCP clients as tools, with a capability model that lets an autonomous coordinator run a fleet
of subagents without any subagent being able to widen its own sandbox.

Built on the core crates directly, the server keeps typed grants, streamed exec (the framed protocol of 11_shell.md),
and the `Coordinator`/`WorkspaceHandle` split (07_api.md); shelling out to the CLI would reserialize everything through
argv/JSON and lose exactly those properties.

## v1 scope: tools only

cowshed-mcp v1 is a **tools-only** server. `resources/list` returns an empty array, no resource templates, **no
prompts**, and **no elicitation** (the human-in-the-loop escalation path is rejected — see Tradeoffs). Clients drive
everything through the tool calls below; there is no resource or prompt surface to version yet. This keeps the v1
contract small and the capability model — not a resource ACL — the only thing gating authority.

## Transports

- **stdio** — one server per client, spawned by the MCP client. Coordinator authority is transferred only through a
  controller-created inherited FD or socketpair endpoint. The FD is absent from argv and environment, is never printed
  on stdout/stderr, is marked close-on-exec/non-inheritable immediately after receipt, and is closed after the server
  binds authority. Stdio alone conveys no authority.
- **unix socket** — a shared server at `<runtime dir>/<owner>/<repo>/mcp.sock` for a long-lived coordinator that hands
  scoped connections to many subagents. The controller derives `owner` and `repo` from the primary `repo_id`, validates
  and encodes each as one component, and only then joins the path; an unsplit identifier is never accepted as path text.
  Its parent directory is mode `0700`, socket mode is `0600`, and the server verifies peer uid/platform credentials
  before authentication. A socket connection authenticates with a short-lived, one-use connection descriptor minted by
  `mint_worker`; it binds the connection to exactly one workspace's `WorkspaceHandle` and cannot be replayed. The shared
  MCP socket never accepts the in-volume gateway token.

## Capability model

This is the center of the design. Two capability classes, two authority levels, and one hard invariant: **nothing
readable from inside a sandbox may authorize escalation.** The in-volume `.cowshed/token` is reserved for gateway
request authentication (05_gateway.md); it is not an MCP credential.

### Coordinator token

- A 256-bit capability minted fresh at server start and transferred to the trusted spawning controller only over the
  inherited FD/socketpair above. It is never emitted on stderr, stdout, argv, environment, telemetry, or any filesystem
  path; it is never written into a workspace. The receiving endpoint is non-inheritable and closed after binding.
- Authorizes the full coordinator surface: `workspace_create`, `workspace_destroy`, `fork`, `restore`, `rebase`, `land`,
  `grant`, `revoke`, `slot_assign`, `gc`, `repo_mirror`, checkpoint-quota policy, and minting workspace-scoped
  connections. Checkpoint creation is exercised through the quota-bound one-workspace worker capability.
- Held by the trusted orchestrator. It is the only authority that can change what a workspace may touch or perform
  cross-workspace/project maintenance.

### Worker connection descriptor

- `mint_worker` returns a cryptographically random **256-bit** descriptor for exactly one workspace. The authenticated
  coordinator/orchestrator delivers it to the intended worker; it never passes through a workspace file, argv,
  environment, telemetry, or the reusable in-volume gateway token.
- Descriptors are memory-only, have a fixed **30-second TTL**, are single-use, and are invalidated by server restart.
  Redemption is one atomic consume operation: validation and removal happen under the same lock, so concurrent redeemers
  yield exactly one success. A descriptor is bound to the target workspace, server socket identity, and expected peer
  identity (uid plus available platform peer credentials); a mismatch authorizes nothing and does not consume a
  descriptor intended for the correct peer. Successful consume binds the connection to one `WorkspaceHandle` and
  permanently removes the descriptor.
- The resulting session exposes `bash`, `job_list`, `job_status`, `job_logs`, quota-bound `checkpoint`, and `push` for
  exactly that workspace. It exposes no grant/revoke, restore/destroy/rebase/land/gc/repo-mirror, descriptor minting, or
  access to another workspace.

Descriptors never appear in telemetry or workspace records. The in-volume `.cowshed/token` remains gateway-only.

### The escalation loop (no human, by design)

The point of cowshed is that commands run safely **without asking anyone** — the closed sandbox is what makes unattended
execution acceptable, so the default path involves zero confirmations. When a worker genuinely needs more (a path
outside its mount, an egress host), the flow is:

1. worker's `bash` call hits a sandbox denial → the tool result carries `exitCode 6` and the exact `grant` that would
   resolve it (surfaced from the CLI/error `hint`, 06_cli.md);
2. the worker reports that need to **its coordinator** over normal agent-to-agent traffic (not an MCP prompt to a
   human);
3. the coordinator — applying whatever policy it wants — calls `grant` with its coordinator token;
4. the worker retries; the next exec picks up the new grant revision (11_shell.md propagation).

The coordinator's token is the capability a subagent cannot guess or read, so a subagent cannot grant itself anything
even if compromised — escalation is strictly a decision made one level up.

## Tools

### Coordinator tools (require coordinator authority)

| Tool                | Args (sketch)                                                                                          | Returns                                                  |
| ------------------- | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------- |
| `workspace_create`  | `name`, `ref?`, `from?`, `slot?`                                                                       | mount path, base commit                                  |
| `workspace_list`    | —                                                                                                      | records (name, state, base, age, written/referenced)     |
| `workspace_destroy` | `name`, `force?`                                                                                       | ok                                                       |
| `fork`              | `src`, `dst`                                                                                           | mount path                                               |
| `checkpoint`        | `name`, `label?`                                                                                       | label (generated UTC timestamp if omitted)               |
| `restore`           | `name`, `label` (**required**)                                                                         | mount path                                               |
| `rebase`            | `name`, `onto?`, `fresh?`, `expectedWorkspaceIncarnation?`, `expectedSourceHead?`, `expectedOntoHead?` | new head sha                                             |
| `land`              | `name`, `targetBranch?`, `check?`, `retire?`, `pushOnly?`, source/incarnation/target CAS expectations  | target branch, landed sha, checkout state, retired       |
| `grant` / `revoke`  | `name`, `read[]?`, `write[]?`, `egress[]?`, `repo[]?`, `sim[]?`, `all?`, `expectedRevision?`           | new grant revision                                       |
| `slot_assign`       | `name`, `slot`                                                                                         | ok (recycled mount path for the slot)                    |
| `mint_worker`       | `name`                                                                                                 | 256-bit, 30-second, one-use worker connection descriptor |
| `gc`                | `dryRun?`                                                                                              | reclaimed bytes/report                                   |
| `repo_mirror`       | `name`, `url`                                                                                          | controller-owned read-only mirror path                   |

A coordinator-scoped telemetry query tool (selector/SQL over controller commitments and telemetry, 13_telemetry.md) is
roadmap, not v1. Workers get only one-workspace job status/log tools: lifecycle is reconciled against controller
continuity commitments, while captured bytes resolve from protected in-volume artifacts within their origin boundary.

`restore` requires a label — there is no "restore the latest" default. Worker `checkpoint` accepts an optional label.
`grant`/`revoke` carry `expectedRevision` for compare-and-swap (07_api.md/04_sandbox.md) and a `repo[]` selector for
repo-scoped mirror grants (05_gateway.md). Each `egress[]` entry is
`{ host, ports?, mode?: "intercept" | "opaque", impersonate?: "<profile>" }`, mirroring the grant-file schema
(04_sandbox.md): a coordinator sets a host's interception mode and fingerprint at grant time, `mode` defaulting to
`intercept`. `sim[]` carries personal-session simulator broker verbs (`"openurl"` / `"install"` —
04_sandbox.md/05_gateway.md); `install` remains bound to drop-dir artifacts and the human-gating rule regardless of the
grant (14_nix.md). There are no SSH/Docker grant axes.

`land.targetBranch` defaults to `main`. It names a real local branch in the main workspace repository. If that branch is
checked out in the main workspace, a successful call advances the visible checkout — branch ref, `HEAD`, index, and
working tree — to the exact validated head. If it is not checked out, only `refs/heads/<targetBranch>` advances and the
current checkout is untouched. Dirty checked-out state, an unmanaged linked-worktree checkout, a changed expected value,
or a non-fast-forward returns `Conflict`; the tool never substitutes a hidden ref or silently retries against a new
base. `push` remains the separate worker-scoped preservation primitive and intentionally never advances a branch.

### Worker tools (one-use descriptor; scoped to that workspace)

| Tool         | Args                                                                                                                           | Returns                                                                                    |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `bash`       | `command`, `stdin?`, `timeout?`, `background?`, `session?`, `stdoutCopy?: OutputPublication`, `stderrCopy?: OutputPublication` | `jobId`, state, stdin metadata, `stdout`/`stderr` stream info, exit metadata when terminal |
| `job_list`   | `state?`                                                                                                                       | numeric job ids, state, timings, trace identity, per-stream info                           |
| `job_status` | `jobId` (numeric)                                                                                                              | state, timings, exit metadata, `stdout`/`stderr` stream info, trace identity               |
| `job_logs`   | `jobId` (numeric), `stream?: "out" \| "err"`, `follow?`                                                                        | raw bytes resolved representation-transparently from the protected artifact                |
| `checkpoint` | `label?`                                                                                                                       | label                                                                                      |
| `push`       | `branch?`, `expectedWorkspaceIncarnation?`, `expectedSourceHead?`, `expectedDestinationHead?: { missing: true } \| { oid }`    | source head, non-checked-out destination ref, previous destination head?                   |

`bash` is the workhorse: it runs through the workspace supervisor (warm shell, 11_shell.md), honors `session` for
stateful multi-step work, and returns the workspace-local monotonic numeric `jobId` for **every accepted exec
submission**, including foreground jobs. Auto-backgrounding changes only state, not identity. The result surfaces
`exitCode 6` with the resolving `grant` **only on authoritative denial evidence** (06_cli.md — never synthesized from
stdout, stderr, or their summaries). A spawn failure is a terminal job with the already-allocated id.

`bash.stdin` is a discriminated union with exactly one source: `{ "inlineBase64": "…" }` for opaque inline bytes,
`{ "stream": true }` for subsequent framed channel-1 bytes on socket transport, or `{ "workspaceFile": "rel/path" }` for
a workspace-relative regular file. Omission means empty input for non-interactive tool calls. The server decodes base64
strictly and never embeds bytes or a file expression in `command`. Workspace files are opened by the supervisor
read-only, beneath the workspace, with no-follow component traversal; absolute/escaping paths, symlinks, devices,
sockets, directories, and replacement races fail closed. Stream backpressure propagates to the MCP transport; EOF closes
child stdin once, cancellation closes stdin and records incomplete delivery without implicitly killing the job.
Results/events carry stdin kind, delivered byte count, completion, and normalized relative file path when applicable,
plus normal job/trace identity; inline bytes never enter JSON results or telemetry.

`checkpoint` atomically enforces the coordinator-configured per-workspace checkpoint count and byte quotas. It reaches
the supervisor barrier before cloning: running memory-only stream prefixes promote, protected files and a complete
checkpoint-manifest Arrow batch fsync, and the controller publishes the manifest digest/lineage commitment. Quota
exhaustion is a conflict carrying current usage and limits; it never silently prunes another checkpoint. Restore is
coordinator-only and rejects any manifest/commitment mismatch as `Integrity`.

`push` has the same preservation and compare-and-swap semantics as `WorkspaceHandle::push` (07_api.md): it installs the
exact fetched source head under `refs/cowshed/<ws>/heads/<branch>` in the main workspace repository/object store without
changing the checked-out Git `main` branch, index, or working tree. Any supplied incarnation, source-head, or
destination-head expectation mismatch maps to `Conflict` (`-32002`), leaves the destination unchanged, and retains the
source workspace. Remote publication and workflow policy are not MCP tools.

MCP JSON carries separate `stdout` and `stderr` with frozen `StreamInfo { storage, bytes, sha256, summary }`. A bounded
inline artifact uses `BinaryData` `{encoding:"utf8",data} | {encoding:"base64",data}` exactly as ordinary `JobInfo`;
both branches are decoded-byte bounded. A file path exists only after promotion. Controller commitments carry neither
inline form nor any payload/path. `Redirect.source` is mutable and never used by `job_logs`; the exact combined quota
still applies. `stdoutCopy`/`stderrCopy` are the exact per-stream `{path,policy}` `OutputPublication` objects with
`policy: "createNew"|"replace"`. Publication is independent, post-terminal, never hardlinks, never alters storage, and
never becomes authority.

Every job result/event includes `(repoId, workspaceIncarnation, jobId)` and standard trace identity. Protected in-volume
complete Arrow batches and sealed artifacts own captured content within the origin incarnation/checkpoint boundary.
Compact controller commitments own existence/status/order/lineage and expected counts/hashes, but never output payload
or artifact paths. The server reconciles both: missing/altered committed content, invalid complete batches, and
count/hash/batch/lineage mismatch return `Integrity` (`-32006`), preserving both sides rather than trusting whichever
appears newer. Discarding only an incomplete trailing batch is successful recovery.

`bash` takes shell text for agent ergonomics and converts it explicitly to `["/bin/sh","-c",command]`. A real shell AST
may classify only a proven simple literal `>`/`2>` as `Redirect`, and only when the supervisor controls the actual
writable descriptor and applies identical exact quota accounting. After terminal state it snapshots that source into an
independent protected inline or clone/reflink/copied file artifact. Polling/tailing or reopening the source path is
forbidden. If eligibility/interposition fails, ordinary shell semantics run and the result claims only bytes that
reached captured pipes. This is evidence classification, not the removed hardlink optimization. Structured stdin remains
separate from shell text.

## Error mapping (CowshedError → JSON-RPC)

One table; the server maps the core taxonomy (07_api.md) onto JSON-RPC error codes. `data.hint` carries the same
actionable next step as CLI stderr, and `data.exitCode` carries a child process exit for `bash` results; a child 6 or 7
is not reclassified as a cowshed error.

| CowshedError         | JSON-RPC code           | `data`                                    |
| -------------------- | ----------------------- | ----------------------------------------- |
| `Usage`              | -32602 (invalid params) | `{ code: "usage", hint }`                 |
| `NotFound`           | -32001                  | `{ code: "not-found", hint }`             |
| `Conflict`           | -32002                  | `{ code: "conflict", hint }`              |
| `EnvironmentMissing` | -32003                  | `{ code: "environment-missing", hint }`   |
| `SandboxDenied`      | -32004                  | `{ code: "sandbox-denied", hint, grant }` |
| `Authorization`      | -32005                  | `{ code: "unauthorized", hint }`          |
| `Integrity`          | -32006                  | `{ code: "integrity", hint }`             |
| `Internal`           | -32603 (internal error) | `{ code: "internal" }`                    |

A missing, expired, replayed, peer-mismatched, socket-mismatched, or insufficient capability is the distinct
`Authorization` error **-32005**, returned before tool dispatch. It is never reported as `SandboxDenied` (`-32004`) or
as a process exit code. Authorization failures do not disclose whether the targeted workspace or coordinator operation
exists.

A worker descriptor presented to a coordinator tool is an authorization failure at the transport (`-32005`), returned
before the tool runs, so a subagent never sees a coordinator tool attempt. It is never mapped to `SandboxDenied`.

## Tradeoffs

**Human confirmation / elicitation rejected.** An MCP `elicitation` asking a person to approve each action is the
opposite of the goal: cowshed's sandbox exists precisely so that commands run unattended and safely. Confirmation
prompts don't scale to a subagent fleet and reintroduce a human bottleneck the closed baseline was built to remove. The
one legitimate escalation — widening a sandbox — is authority, not a dialog: it belongs to the coordinator token,
resolved agent-to-agent. (A human orchestrator remains free to _be_ the coordinator and approve grants manually; that is
a policy choice at the top level, not a protocol requirement pushed onto every workspace.)

**One capability type rejected.** A single reusable credential that both scopes a workspace and can grant would let any
sandboxed subagent holding it escalate itself. Coordinator authority and one-use workspace descriptors are distinct; the
gateway's in-volume token has no MCP meaning. This split keeps the sandbox's closed baseline trustworthy under
multi-agent orchestration.

**CLI-wrapping MCP server rejected.** Building the server on the CLI would lose typed grants, streamed stdio, and the
capability split, and would fork a subprocess per tool call. `cowshed-mcp` on the core crates is the same decision as
`cowshed-core` being the API rather than the CLI (07_api.md).
