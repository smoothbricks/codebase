# AGENTS.md

SmoothBricks is the monorepo behind the `@smoothbricks/*` packages: LMAO tracing, arrow-builder and columine (columnar
data), StateBus, cowshed (warm copy-on-write workspaces), and the Nx plugin and `smoo` CLI that build and gate this
repository. This file is the entry card for any agent working in this repository, read with no other context. Rules say
**when** something applies, **what** to do, and **what enforces it** ("review" means the lead who briefed you, or a
maintainer, checks it; no tool does). Mechanics live in the files this card points at. `CLAUDE.md` is a symlink to this
file. `bun`, `nx` and `cargo` come from the repo's devenv: in a shed `cowshed exec` supplies them; on `main`, or under
`cowshed exec --host`, wrap the command in `direnv exec .`. `cowshed` is the host-installed CLI of `packages/cowshed`.

## Words used below

- **Maintainers**: the humans who set direction and approve exceptions. **Lead**: the agent that briefed you; report and
  hand off to it (or to a maintainer if you have no lead).
- **Owner**: whoever was assigned a path or unit in a brief; a path nobody owns belongs to the lead who is working on
  it. Ask the lead when you're unsure.
- **cowshed**: the workspace tool, built in this repository (`packages/cowshed`). A **shed** is one cowshed workspace: a
  warm copy-on-write clone of `main` with its own branch and build volume. `cowshed fork <ws> --project <checkout>`
  creates one, `cowshed path <ws>` prints its directory, `cowshed rm <ws>` retires it; `<ws>` is the shed's name (its
  `.cowshed/workspace.json` `workspace` field, whose `role` says shed or main). **main** is the shared checkout
  (`cowshed path main`); never edit code there. These commands run from the trusted host shell: a sandboxed worker's
  cowshed CLI is refused at its first write, so it hands fork, land and `rm` to its lead.
- **cowshed exec**: `cowshed exec <ws> -- <cmd>` runs a build, test or bench under cowshed's jobserver, which admits
  work by CPU tokens so host load stays schedulable. Work started outside it holds no token and starves everyone. Host
  targets that a sandbox can't run (`host-e2e`) use `cowshed exec <ws> --host -- direnv exec . <cmd>`.
- **Land**: the land command (see Commands). Under the repository's **land turn** (a FIFO queue per repo, holder named),
  it rebases the shed onto `main`, runs the repo's gate (`tooling/land-host.sh`: `nx run-many -t gate`, the root
  aggregate of every project's lint, test and build plus `host-e2e`; then `bun run check:linux`), fast-forwards `main`
  on green and retires the shed.
- **Nx**: the build and test runner. `@smoothbricks/nx-plugin` infers most targets.
- **LMAO spans**: the traces `@smoothbricks/lmao` records. Tests flush them to each package's SQLite sink
  (`<package>/.cache/lmao/trace-results.db`); `TraceQuery` or the `sqlite3` CLI reads them
  (`targets/lmao-docs/src/content/docs/reference/trace-testing.mdx`).
- **Brief**: `agent-todo/<unit>.md` holds a unit's gaps and evidence. **Specs** (`specs/`) state the target design in
  present tense.

## Rules

1. **Shared trees.** When you are about to reset, checkout, stash or commit in a tree another agent uses → coordinate
   with its owner first, and never reset, checkout or stash to "clean" it. Commit one defect or one contract change with
   `git commit --only -- <paths>`, never the whole index. Commit your WIP before a disruptive host operation. _Enforced
   by: review._
2. **Landing.** When you land, retire a shed or claim something is landed → use the land command. "Landed" means the
   commit is an ancestor of `main` (`git merge-base --is-ancestor`). Retire a shed only after its work is landed. When
   `cowshed rm` refuses, never add `--force` or `--abandon`: a shed whose content `main` already holds
   (`cowshed ls --landing` counts its unlanded commits) is reset to `main` and removed with plain `cowshed rm`; a shed
   with unlanded work is landed, or left to its owner. `--abandon` keeps every destroyed tip as a ref in `main`'s
   `.git`; `--force` destroys the uncommitted tree. A failure that blocks more than one shed comes before local polish.
   _Enforced by: the land turn and `tooling/land-host.sh`; `cowshed rm`'s containment refusal; review for the flags._
3. **Edits.** When you are about to edit → work in your own shed (one owner per unit, no git worktrees, never a peer's
   path until released). Use absolute workspace paths in filesystem and edit tools, so a session's cwd cannot redirect
   an edit into `main`. Markdown that belongs to no code unit goes straight to `main` with the doc commit (see
   Commands). _Enforced by: review._
4. **Diagnosis.** When you are about to rerun something, or to name a cause → read the evidence first: the run's LMAO
   spans, the Nx task history and prior verdicts (`.nx/workspace-data/bounded-exec/<task>/verdict.json`). Never decode
   traces by hand, never add a retry, never pass `--skip-nx-cache`. A pass on rerun is not a verdict. _Enforced by:
   review._
5. **Proof.** When you are about to run host work → run it through `cowshed exec` (`--host` for host targets). When you
   choose a test → pick the cheapest check that goes red when this contract is wrong, in the module that owns the
   contract. A decision (ordering, timeouts, retry policy, recovery) gets a table-driven test of a pure function with
   injected events and an injected clock: no real I/O, no sleeps. The I/O shell gets one integration check. A race gets
   a real interleaving, never serialization. A test tears down what it created on every exit path. _Enforced by: cowshed
   tokens; the root `lint` policies `TEST-CLOCK-ORACLE` (deadlines and elapsed-time asserts) and `TEST-ONE-WALK`
   (duplicate walks), on changed tests only (`packages/nx-plugin/README.md`)._
6. **Workarounds.** When a step fails, or you are about to add a sleep used as a wait, a retry, a raised bound, a
   fallback, an env var, a lock or marker file, test serialization, SIGSTOP or manual `nice`, a local copy of another
   owner's table or codec, a compatibility shim, a waiver list or a test-only code path → stop and fix the cause at its
   owner. If a seam, export or generator is missing, add it once at the owner. If you are not the owner, hand off with
   the measured blast radius: files, lines and contract surfaces (wire, codec, spec, another shed), counted with
   `codegraph_explore`, LSP or `rg`. "It would be bigger" without a count is not a reason. Coordination that is allowed:
   one writer, one producer per checkout, the jobserver, the land turn. Anything else needs a maintainer's approval.
   _Enforced by: review._
7. **Done.** When you implement something or call it done → search existing code and specs first, and keep one owner per
   contract (no second copy). Update the spec in the same unit. Done means a production (non-test) call site plus a
   smoke run that exercises it; otherwise wire it or delete it. _Enforced by: review._
8. **Byproducts.** When you choose a data structure, a storage or wire format, a log or trace shape, or a pass over
   data, or when you are about to add a cache, an index, a copy or a second pass → among designs with the same
   performance, pick the one whose necessary work has the most other uses, then thread it through everywhere. That
   removes whole classes of code (extra indexes, sorts, caches, re-encodings, separate profilers) and the later
   optimizations they would need. Examples: a sorted structure serves ordered scans and range queries that a hash table
   plus a sort would pay for twice. LMAO traces write structured columns into preallocated buffers, so monitoring data
   is already columnar for offline analytics, span start and end times are already a profile, and the span result
   already holds the `Ok`/`Err` with its status and error code, so nothing is allocated or computed twice. Before adding
   a cache, name what your structure or pass already proves (order, an index, a checksum, a timing, a result) and use
   it; then measure. Derived state (an index, an envelope, a checkpoint, a cache) is a byproduct, never a second store:
   name its authoritative source and why no duplicate store exists. Refuse these second stores by name: a write ledger
   beside the event log, a duplicate inventory beside the authoritative index, a retraction index kept apart from the
   facts, a derivable-byte cache kept as an authoritative store, a parallel registry of what a manifest lists, a
   checkpoint store duplicating live state, a summary table recomputable from its rows, a canonical copy beside the live
   source, a shadow config beside the declared one. A cache stays basis-keyed, recomputable and disposable; if removing
   the work needs an upstream fork or patch, prefer a boundary cache and record the upstream alternative and its
   maintenance cost in the design record. _Enforced by: review._
9. **Types.** When you are about to write `as`, an `unknown` default or a sentinel value → thread the precise type
   instead. No `as any`, `as unknown as`, or `JSON.parse(...) as T`; `as const` and `satisfies` are fine. _Enforced by:
   ESLint `@typescript-eslint/no-unsafe-type-assertion` on `packages/*/src` (the inferred `lint` target)._
10. **Boundaries.** When you parse bytes, JSON, a fixture or a payload from another process, language or file → validate
    with code generated from the owning schema (Typia `validate`/`createValidate` for TypeScript envelopes and JSON, the
    owning generated codec or schema validator for wire and domain payloads) and return a typed error. Fixture and
    oracle readers reject unknown fields. _Enforced by: review._
11. **Errors.** When a correct caller can cause a failure → return `Err` carrying the operation, an error code, the
    subject (ids, paths, versions), the values involved and the cause. Only programmer bugs throw or panic. Test the
    span as `span-err`. _Enforced by: review._
12. **Portability.** When you write a path, a name, a dependency or a spawn → no home paths, private project names or
    agent/session ids in committed text; an external dependency is pinned by git revision (`rev = "<sha>"`), never by a
    host path (in-repo workspace crates use `path =` as usual); spawn tools by PATH name. _Enforced by:
    `smoo monorepo check-public-denylist` (`tooling/git-hooks`): commit-msg and pre-commit judge the home directory,
    sibling project names and the clone's configured patterns, pre-push the configured patterns; agent and session ids
    need a diff read._

### Conduct

- Finish authorized work; a soft budget is never a reason to stop or to leave work uncommitted.
- A question authorizes no changes: answer it, then continue authorized work.
- While blocked on a dependency, prepare against its current tip; never wait on CI or a push.
- Read the governing spec and this card before editing either; update the owner, not a parallel copy. Add a rule here
  only for a recurring high-loss decision with an actionable scope and a named owner, and never paste vendor-generated
  instruction blocks into this file.
- Before answering a host or tool question, search the session record and the spec for prior findings, and cite them.
- Ask a maintainer once, with the cost per hour of waiting, and staff the root cause in the same step.
- A lead reports land results, installs and blockers to the maintainers unasked.
- With an empty queue and no named dependency, yield after 30 minutes.

## Commands

- **Tools:** Bun only (no npm, npx or bunx). Run Nx as `bun nx` or `nx`.
- **Acceptance (focused):**
  `cowshed exec <ws> -- env NO_COLOR=1 bun nx run-many -t lint test build -p <projects> --outputStyle=static-failures-only`.
  It does not replace the land gate; add `-p @smoothbricks/codebase` to run the root `lint` policies. For
  platform-conditional Rust, run `bun run check:linux` before the final land or push; it proves Linux compilation, not
  Linux test execution.
- **Land:** `<land-command> land <ws>`, where `<land-command>` is the operator-configured host entry that takes the
  repository's land turn; your lead gives you its path, it is not in this repository. It invokes `tooling/land-host.sh`,
  which owns acceptance of the rebased tree; do not invoke its internals directly.
- **Doc commit (Markdown only):** `<land-command> git -C <main> commit --only -m 'docs: summary' -- <paths>` (new files
  first: `<land-command> git -C <main> add -- <paths>`). It takes the land turn briefly and runs no gate of its own; the
  next land re-gates what the Markdown reaches. Conventional commits, no trailers.
- **Build state:** cowshed owns Nx state paths and the Nx daemon. Declare restorable outputs as Nx outputs, one producer
  per checkout; never redirect state paths ad hoc. Infer targets and read their resolved graph rather than adding lint
  or clean stubs.

## Read first

Ownership: arrow-builder owns generic buffers and Arrow conversion; LMAO owns spans, tracing, contexts and span-tree
conversion. The specs below own the rest, not a handwritten API or enum inventory here.

- `specs/lmao/00_package_architecture.md`: package ownership, dependency direction and allocation strategy.
- `specs/lmao/01a_trace_schema_system.md`: storage by schema kind, raw-string lanes and cold-path encoding.
- `specs/lmao/01l_op_context_pattern.md`: inference-preserving contexts and Op authoring.
- `packages/lmao/README.md`: tracer wiring, trace-testing and the TraceQuery/SQLite inspection entry.
- `packages/nx-plugin/README.md`: inferred targets, Cargo closures, runtime inputs and build-state output safety.
- `specs/cowshed/16_build_volumes.md`: workspace-owned build state, warm forks, cache carry and adoption.
- `packages/cowshed/skills/cowshed/SKILL.md`: the shed workflow: fork, exec, land, and what to do when `rm` refuses.
- `packages/cowshed/docs/agents.md`: workspace ownership and the agent-facing CLI workflow.
- `tooling/land-host.sh` and `tooling/land-host.test.ts`: current landing mechanics and refusal evidence.

## Documentation custody

| Unit                                          | Workspace              | Commit                                     |
| --------------------------------------------- | ---------------------- | ------------------------------------------ |
| Markdown unrelated to an active code unit     | `main`, no shed        | Doc commit (above)                         |
| Code plus the docs/specs describing it        | the unit's shed        | One unit through the land command          |
| Unrelated Markdown found while in a code shed | `main` for the doc fix | Doc commit now; the code stays in its shed |
