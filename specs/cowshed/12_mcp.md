# MCP Server

`cowshed-mcp` is a separate binary built on `cowshed-core` + `cowshed-shell` — **never** a wrapper around the CLI. It
exposes cowshed's warm workspaces to MCP clients (Claude Code's "warm worktrees", other agent runtimes) as tools, with a
capability model that lets an autonomous coordinator run a fleet of subagents without a human in the loop and without
any subagent being able to widen its own sandbox.

Built on the core crates directly, the server keeps typed grants, streamed exec (the framed protocol of 11_shell.md),
and the `Coordinator`/`WorkspaceHandle` split (07_api.md); shelling out to the CLI would reserialize everything through
argv/JSON and lose exactly those properties.

## v1 scope: tools only

cowshed-mcp v1 is a **tools-only** server. `resources/list` returns an empty array, no resource templates, **no
prompts**, and **no elicitation** (the human-in-the-loop escalation path is rejected — see Tradeoffs). Clients drive
everything through the tool calls below; there is no resource or prompt surface to version yet. This keeps the v1
contract small and the capability model — not a resource ACL — the only thing gating authority.

## Transports

- **stdio** — one server per client, spawned by the MCP client (the Claude Code pattern). The process **inherits
  coordinator authority**: the coordinator token is delivered to the spawning process (below) and the single stdio
  connection is inherently the coordinator's.
- **unix socket** — a shared server at `<runtime dir>/<project_id>/mcp.sock` for a long-lived coordinator that hands
  scoped connections to many subagents. A socket connection authenticates with a **short-lived, one-use connection
  descriptor** minted by `mint_worker` (coordinator-only); it binds the connection to exactly one workspace's
  `WorkspaceHandle` and cannot be replayed. A bare workspace token on the socket authenticates only as that workspace's
  worker — it is physically incapable of reaching any coordinator tool.

## Capability model

This is the center of the design. Two token types, two authority levels, and one hard invariant: **nothing readable from
inside a sandbox may authorize escalation.**

### Coordinator token

- Minted fresh at server start, delivered **only to the spawning process** — printed once to stderr
  (`cowshed: mcp coordinator token: …`) and/or set in its environment. Never written into any workspace, never into the
  state dir a sandbox can reach, never logged.
- Authorizes the full lifecycle: `workspace_create`, `workspace_destroy`, `fork`, `checkpoint`, `restore`, `rebase`,
  `land`, `grant`, `revoke`, `slot_assign`, and minting workspace-scoped connections for subagents.
- Held by the trusted orchestrator — jcode's swarm coordinator, or a top-level Claude Code session that spawns
  subagents. It is the only authority that can change what a workspace may touch.

### Workspace token

- The in-volume `.cowshed/token` (01_storage.md) — the same identity the gateway already uses for egress policy. A
  subagent working inside a workspace has it by construction.
- Scopes an MCP session to **exactly that one workspace**: `bash`, `job_status`, `job_logs`, `checkpoint`, `push`.
  Nothing else, and critically **no `grant`/`revoke`, no `destroy`, no access to any other workspace.
- Because a sandboxed subagent can read its own workspace token, the token must not authorize anything that could
  escalate the sandbox — hence grants are coordinator-only. This is the same reasoning that puts grant files _outside_
  the volume (01_storage.md, 04_sandbox.md), expressed at the RPC layer: the type a subagent can obtain
  (`WorkspaceHandle`, 07_api.md) simply has no escalation methods.

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

### Coordinator tools (require the coordinator token)

| Tool                | Args (sketch)                                                                      | Returns                                                           |
| ------------------- | ---------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `workspace_create`  | `name`, `ref?`, `from?`, `slot?`                                                   | mount path, base commit                                           |
| `workspace_list`    | —                                                                                  | records (name, state, base, age, written/referenced)              |
| `workspace_destroy` | `name`, `force?`                                                                   | ok                                                                |
| `fork`              | `src`, `dst`                                                                       | mount path                                                        |
| `checkpoint`        | `name`, `label?`                                                                   | label (generated UTC timestamp if omitted)                        |
| `restore`           | `name`, `label` (**required**)                                                     | mount path                                                        |
| `rebase`            | `name`, `fresh?`                                                                   | new head sha                                                      |
| `land`              | `name`, `check?`, `retire?`                                                        | landed sha                                                        |
| `grant` / `revoke`  | `name`, `read[]?`, `write[]?`, `egress[]?`, `repo[]?`, `all?`, `expectedRevision?` | new grant revision                                                |
| `slot_assign`       | `name`, `slot`                                                                     | ok (recycled mount path for the slot)                             |
| `mint_worker`       | `name`                                                                             | a short-lived one-use worker connection descriptor for a subagent |

`checkpoint` and `restore` are split into two tools (not one `label?` row) because `restore`'s label is **required** —
there is no "restore the latest" default — while `checkpoint`'s is optional. `grant`/`revoke` carry `expectedRevision`
for compare-and-swap (07_api.md/04_sandbox.md) and a `repo[]` selector for repo-scoped mirror grants (05_gateway.md);
there are no SSH/Docker grant axes.

### Worker tools (workspace token; scoped to that workspace)

| Tool         | Args                                             | Returns                                      |
| ------------ | ------------------------------------------------ | -------------------------------------------- |
| `bash`       | `command`, `timeout?`, `background?`, `session?` | stdout, stderr, exit, `jobId?` (11_shell.md) |
| `job_status` | `jobId`                                          | state, timings                               |
| `job_logs`   | `jobId`, `stderr?`, `follow?`                    | streamed spool                               |
| `checkpoint` | `label?`                                         | label                                        |
| `push`       | `branch?`                                        | pushed ref                                   |

`bash` is the workhorse: it runs through the workspace supervisor (warm shell, 11_shell.md), honors `session` for
stateful multi-step work, auto-backgrounds on the soft timeout returning a `jobId`, and surfaces `exitCode 6` with the
resolving `grant` **only on authoritative denial evidence** (06_cli.md — never synthesized from output text). Its result
is derived from the same exec record every other client sees.

`bash` takes **shell text** (`command`) for agent ergonomics; the server converts it **explicitly** to the typed core
exec form `["/bin/sh", "-c", command]` before calling `WorkspaceHandle::exec`. The core API is argv-typed throughout
(07_api.md) — the string→argv wrap happens once, at the MCP boundary, visibly, rather than any layer guessing at shell
semantics.

## Error mapping (CowshedError → JSON-RPC)

One table; the server maps the core taxonomy (07_api.md) onto JSON-RPC error codes. `data.hint` carries the same
actionable next step the CLI prints on stderr, and `data.exitCode` carries the process exit for `bash` results (child
code passed through; 6 only on authoritative denial).

| CowshedError    | JSON-RPC code           | `data`                                                         |
| --------------- | ----------------------- | -------------------------------------------------------------- |
| `Usage`         | -32602 (invalid params) | `{ code: "usage", hint }`                                      |
| `NotFound`      | -32001                  | `{ code: "notFound", hint }`                                   |
| `Conflict`      | -32002                  | `{ code: "conflict", hint }` (grant CAS staleness lands here)  |
| `Environment`   | -32003                  | `{ code: "envMissing", hint }`                                 |
| `SandboxDenied` | -32004                  | `{ code: "sandboxDenied", hint, grant }` (the resolving grant) |
| `Internal`      | -32603 (internal error) | `{ code: "internal" }`                                         |

A worker token presented to a coordinator tool is **not** a domain error — it is an authorization failure at the
transport (-32004-class), returned before the tool runs, so a subagent never sees a coordinator tool "attempt and fail."

## Tradeoffs

**Human confirmation / elicitation rejected.** An MCP `elicitation` asking a person to approve each action is the
opposite of the goal: cowshed's sandbox exists precisely so that commands run unattended and safely. Confirmation
prompts don't scale to a subagent fleet and reintroduce a human bottleneck the closed baseline was built to remove. The
one legitimate escalation — widening a sandbox — is authority, not a dialog: it belongs to the coordinator token,
resolved agent-to-agent. (A human orchestrator remains free to _be_ the coordinator and approve grants manually; that is
a policy choice at the top level, not a protocol requirement pushed onto every workspace.)

**One token type rejected.** A single token that both scopes a workspace and can grant would mean any sandboxed subagent
that reads `.cowshed/token` could escalate itself — defeating the entire grants model. Splitting coordinator authority
from workspace scope is what makes the sandbox's closed baseline trustworthy under multi-agent orchestration.

**CLI-wrapping MCP server rejected.** Building the server on the CLI would lose typed grants, streamed stdio, and the
capability split, and would fork a subprocess per tool call. `cowshed-mcp` on the core crates is the same decision as
`cowshed-core` being the API rather than the CLI (07_api.md).
