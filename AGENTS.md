# AGENTS.md

SmoothBricks is the monorepo behind the `@smoothbricks/*` packages: LMAO tracing, arrow-builder and columine (columnar
data), StateBus, cowshed (warm copy-on-write workspaces), and the Nx plugin and `smoo` CLI that build and gate this
repository. Follow the active human task and higher-level instructions. This file is the entry card for any agent
working in this repository, read with no other context. Rules say **when** something applies, **what** to do, and **what
enforces it** ("review" means the lead who briefed you, or a maintainer, checks it; no tool does). Mechanics live in the
files this card points at, and a scoped rule there is more precise than a generic sentence here: name a conflict and fix
it at the owner. `CLAUDE.md` is a symlink to this file. `bun`, `nx` and `cargo` come from the repo's devenv: in a shed
`cowshed exec` supplies them; on `main`, or under `cowshed exec --host`, wrap the command in `direnv exec .`. `cowshed`
is the host-installed CLI of `packages/cowshed`.

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
  cowshed CLI is refused at its first write, so it hands fork, land, `rm` and `gc` to its lead.
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
   with its owner first, and never reset, checkout or stash to "clean" it. Commit each accepted coherent unit (one
   defect or one contract change) promptly with `git commit --only -- <paths>`, never the whole index. Commit your WIP
   before a disruptive host operation. _Enforced by: review._
2. **Landing.** When you land, retire a shed or claim something is landed → use the land command. "Landed" means the
   commit is an ancestor of `main` (`git merge-base --is-ancestor`). Retire every finished shed promptly, and only after
   its work is landed or deliberately dropped: a shed left standing holds its image and build volumes. When `cowshed rm`
   refuses, never add `--force`: a shed whose content `main` already holds (`cowshed ls --landing` counts its unlanded
   commits) is reset to `main` if dirt blocks the removal, then removed with plain `cowshed rm`; a shed whose work was
   deliberately dropped (superseded or obsolete, the reason clear from the brief, the git log or the session JSONL) is
   removed with `cowshed rm --abandon` (clear dirt first: `--abandon` waives none), then `cowshed gc --abandoned`; a
   shed with real unlanded work and a live owner, failed or partly landed, is landed, or left to that owner. `--abandon`
   bundles the unlanded tips it destroys into `sessions/.trash/`, and `gc --abandoned` deletes all of the project's
   bundles, each the only copy of its commits; `--force` destroys the uncommitted tree. Maintainer rulings of 2026-10-18
   and 2026-10-26: cleaning up is as important as creating, if not more, and `--abandon` may be used for it. A fix that
   removes a fleet bottleneck, such as a failure that blocks more than one shed, comes before local polish. Combine
   related fixes into one train, with one lander per project: a second land of the same project loses the cache hits the
   first just wrote. _Enforced by: the land turn and `tooling/land-host.sh`; `cowshed rm`'s containment refusal; review
   for the flags._
3. **Edits.** When you are about to edit → work in your own shed (one owner per unit, no git worktrees, never a peer's
   path until released). Use absolute workspace paths in filesystem and edit tools, so a session's cwd cannot redirect
   an edit into `main`. Markdown that belongs to no code unit goes straight to `main` with the doc commit (see
   Commands). _Enforced by: review._
4. **Diagnosis.** When you are about to rerun something, or to name a cause → read the evidence first: the run's LMAO
   spans, the Nx task history and prior verdicts (`.nx/workspace-data/bounded-exec/<task>/verdict.json`). Never decode
   traces by hand, and never pass `--skip-nx-cache`, not even to diagnose. Never answer a failure with a retry or an
   unchanged rerun: a pass on rerun is not a verdict; the rerun after a fix is the proof. Cancel your own run once its
   result is obsolete or doomed, keeping its evidence; never kill a peer's useful independent work or hide a real
   failure. _Enforced by: review._
5. **Proof.** When you are about to run host work → run it through `cowshed exec` (`--host` for host targets). When you
   choose a test → pick the cheapest check that goes red when this contract is wrong, at the smallest real boundary, in
   the module that owns the contract. Reuse a valid proof until its relevant inputs change; use a directory or owned
   fixture, not a mounted image, when it proves the same contract. Never re-run a dependency's suite in a consumer,
   never assert that the OS behaves like the OS or Nx like Nx, and never fake our own code: use its real in-memory
   implementation. Exercise our real OS, process or network integration where that behavior matters. A decision (event
   handling, ordering, timeouts, retries as policy, recovery) lives in a pure core and gets a table-driven test with
   injected events and an injected clock: no real I/O, no sleeps. The I/O shell holds no decisions: it maps OS events
   into core inputs and executes core outputs; it gets one simple mock integration check plus actual runs, and no test
   drives core logic through real watchers, timers or a load generator. Every test advances on readiness, completion and
   progress, never on a sleep or a per-test deadline. A missing test, an infrastructure error or a timeout is never the
   intended red. A race gets a real interleaving, never serialization. An expensive host fixture stays out of ordinary
   test dependencies; the land gate reaches its dedicated target when its inputs change. A test tears down the fixtures
   it physically owns on every exit path. _Enforced by: cowshed tokens; the root `lint` policies `TEST-CLOCK-ORACLE`
   (deadlines and elapsed-time asserts) and `TEST-ONE-WALK` (duplicate walks), on changed tests only
   (`packages/nx-plugin/README.md`)._
6. **Workarounds.** When a step fails, or you are about to make a failing, slow or blocked step pass without fixing its
   cause (a sleep used as a wait; a retry or rerun around the failure; a bound raised, or a check skipped or waived, to
   get a pass; a fallback; an env var, a lock or marker file, a wrapper protocol or hook; test serialization; SIGSTOP or
   manual `nice`; a quiet window that holds other work for a gate; a local copy of another owner's table or codec; a
   compatibility shim; a waiver list; a test-only code path) → stop and fix the cause at its owner: a tolerated failure
   is a defect with a receipt (`specs/cowshed/05_gateway.md`, "Deadlines": "the failure is the diagnosis"). What a
   contract designs is not a workaround: a transient error retried by its declared policy
   (`specs/lmao/01r_transient_errors_and_retry.md`), a reconnect a spec defines (a cowshed client's one ask after an
   install restart, `05_gateway.md`), a bound that is the product's contract, an additive protocol reader that follows
   its evolution contract. Before settling for a workaround, measure the right change: files, lines and contract
   surfaces (wire, codec, spec, another shed), counted with `codegraph_explore`, LSP or `rg`. A small one is done in the
   unit, coordinated with the path's owner; a large one goes to its owner with the count. "It would be bigger" without a
   count, or an effort guess, is not a reason. If a seam, export or generator is missing, add it once at the owner.
   Coordination that is allowed, and never cover for a failing concurrent test: one writer, one producer per checkout,
   the jobserver, the land turn. Anything else needs a maintainer's approval. _Enforced by: review._
7. **Done.** When you implement something or call it done → search existing code and specs first, and keep one owner per
   contract (no second copy). A field list, key set, constant or count that mirrors another type, schema or manifest is
   generated or derived from its owner, never retyped; a count or ratio is computed from what it counts when it is read,
   never stored beside it (`specs/lmao/01b2_buffer_self_tuning.md`, "Buffer Statistics Logging"). Change an API by
   migrating every caller in the same unit and deleting the old path. Fix the defects you touch; an unrelated code
   defect gets its evidence and an owner, without replacing your goal. Update the spec in the same unit, never by
   dropping a specified capability. Done means a production (non-test) call site plus a smoke run that exercises it;
   otherwise wire it or delete it. _Enforced by: review._
8. **Byproducts.** When you choose a data structure, a storage or wire format, a log or trace shape, or a pass over
   data, or when you are about to add a cache, an index, a copy or a second pass → among designs with the same
   performance, pick the one whose necessary work has the most other uses, then thread it through everywhere. That
   removes whole classes of code (extra indexes, sorts, caches, re-encodings, separate profilers) and the later
   optimizations they would need. Examples: a sorted structure serves ordered scans and range queries that a hash table
   plus a sort would pay for twice. LMAO traces write structured columns into preallocated buffers, so monitoring data
   is already columnar for offline analytics, span start and end times are already a profile, and the span result
   already holds the `Ok`/`Err` with its status and error code, so nothing is allocated or computed twice. Before adding
   a cache, name what your structure or pass already proves (order, an index, a checksum, a timing, a result) and use
   it, and remove redundant passes, checks repeated below a boundary that already checked, re-parsing and duplicate
   representations; then measure the uncached and the cached path. One fact has one authority: derived state (an index,
   an envelope, a checkpoint, a queue, a cache) is a byproduct, never a second store. Before you write a record, ledger,
   sidecar or cache, name the authority that already answers it (a directory listing, the filesystem, git, the kernel's
   mount or process table, the event log) and read that instead (`specs/cowshed/01_storage.md`, "Runtime state: derived,
   never stored" and "Tradeoffs"). Refuse these second stores by name: a write ledger beside the event log, a duplicate
   inventory beside the authoritative index, a record of what a directory listing already holds, a retraction index kept
   apart from the facts, a derivable-byte cache kept as an authoritative store, a parallel registry of what a manifest
   lists, a checkpoint store duplicating live state, a summary table recomputable from its rows, a canonical copy beside
   the live source, a shadow config beside the declared one, a second copy of a cache (move it instead;
   `specs/cowshed/03_caches.md`, "Tradeoffs"). A cache stays basis-keyed, recomputable and disposable: a miss recomputes
   or says why it cannot, never serves a stale copy. If removing the work needs an upstream fork or patch, prefer a
   boundary cache and record the upstream alternative and its maintenance cost in the design record. _Enforced by:
   review._
9. **Types.** When you are about to write `as`, a sentinel value, or an `unknown` default that does not preserve
   inference → thread the precise type from input through storage to the returned value, and fix the source API, not the
   caller. No `as any`, `as unknown as`, or `JSON.parse(...) as T`; `as const`, `satisfies` and an `unknown` generic
   default that preserves inference are fine. _Enforced by: ESLint `@typescript-eslint/no-unsafe-type-assertion` on
   `packages/*/src` (the inferred `lint` target)._
10. **Boundaries.** When you parse bytes, JSON, a fixture or a payload from another process, language or file → validate
    with code generated from the owning schema (Typia `validate`/`createValidate` for TypeScript envelopes and JSON, the
    owning generated codec or schema validator for wire and domain payloads, never a second validator) and return a
    typed error. Data already proven inside the process is not revalidated unless it crosses a trust boundary. Fixture
    and oracle readers reject unknown fields; an additive protocol reader follows its evolution contract. _Enforced by:
    review._
11. **Errors.** When a correct caller can cause a failure → return `Err` carrying the operation, an error code, the
    subject (ids, paths, versions), the values involved and the cause. Only stated programmer bugs and impossible
    invariants throw or panic. Normal wiring carries a tracing context: a root trace at each entry point, child spans
    below it. Test the span as `span-err`, not `span-exception`, with an observable test tracer, never a default no-op
    one; API-comparison and benchmark exceptions are stated where they apply. _Enforced by: review._
12. **Portability.** When you write a path, a name, a dependency or a spawn → no home paths, private project names or
    agent/session ids in committed text (write `~` or `<owner>`, use portable command placeholders, and normalise a
    recorded fixture's root); an external dependency is pinned by git revision (`rev = "<sha>"`), never by a host path
    (in-repo workspace crates use `path =` as usual); spawn tools by PATH name, never a Nix store path. Where platforms
    differ, each platform's mechanism lives in its own file behind one interface, and callers never branch on the
    platform; no test names a BSD-only tool (`lockf`, `pbcopy`, `sw_vers`). _Enforced by:
    `smoo monorepo check-public-denylist` (`tooling/git-hooks`): commit-msg and pre-commit judge the home directory,
    sibling project names and the clone's configured patterns, pre-push the configured patterns; agent and session ids
    need a diff read._
13. **Hot paths.** When you write a warmed path (an LMAO buffer write, a StateBus render or reduce, a columine kernel)
    or claim a performance change → name the phase the claim covers (startup, setup, warmed write, slow path, flush). A
    warmed path allocates nothing and stays monomorphic: capacity is fixed at creation, growth happens only on an
    explicit slow path, and encoding, dictionaries and sorting wait for the flush. A performance claim compares against
    a measured baseline and a reference with that work removed, under realistic input in the phase it names; it counts
    effective memory (heap, external, WASM, Arrow, leases, caches), never `heapUsed` alone. No journal, trace or queue
    is unbounded. Specs: `specs/lmao/00_package_architecture.md` ("Optimization Architecture Contract"),
    `specs/lmao/01b1_buffer_performance_optimizations.md` ("Enforceable Performance and Measurement Contract"),
    `specs/statebus/07_testing_and_performance.md` ("Benchmark matrix", "Allocation checks"). _Enforced by: review._

### Conduct

- Finish authorized work; a soft budget is never a reason to stop, idle, or hand off unfinished or uncommitted work.
- A question authorizes no changes: answer it first, then continue authorized work.
- While blocked on a dependency, prepare useful authorized work against its newest candidate, and never invent another
  owner for it; never wait on CI or a push.
- Read the governing spec and this card before editing either; reuse their terms and update the owner, not a parallel
  copy. Add a rule here only for a recurring high-loss decision with an actionable scope and a named owner, never to
  restate what a tool, template or check enforces. Preserve useful rationale at the existing owner, delete stale recipes
  and API inventories, and never paste vendor-generated instruction blocks into this file.
- Before answering or acting on a host, tool or process problem, search the session JSONL, the governing spec and the
  skill for prior findings, and cite them.
- Decide what you can. A blocker that needs a maintainer is asked once, with its cost per hour of waiting, and its root
  cause and prevention are staffed in the same step; nobody waits on a maintainer untraced. Nobody pauses the host or a
  lane for a measurement that can run alongside; only a benchmark that needs absolute precision gets a quiet host
  (`specs/cowshed/05_gateway.md`, "Nothing else yields to gates").
- Report every state change (a land red or green, an install, a drive result, a blocker) unasked: to your lead, and a
  lead to the maintainers.
- With an empty queue and no named dependency, yield after 30 minutes. A lead waits only on a named dependency and stops
  its idle helpers and editors.

## Commands

- **Tools:** Bun only (no npm, npx or bunx). Run Nx as `bun nx` or `nx`.
- **Acceptance (focused):**
  `cowshed exec <ws> -- env NO_COLOR=1 bun nx run-many -t lint test build -p <projects> --outputStyle=static-failures-only`.
  It does not replace the land gate; read its exit status and evidence, and add `-p @smoothbricks/codebase` to run the
  root `lint` policies. Nx runs the targets in its graph's order; lint first is a local fail-fast, never a serialization
  of the fleet. Bare `cargo` or `bun test` may diagnose a runner and never replaces Nx acceptance. For
  platform-conditional Rust, run `bun run check:linux` before the final land or push; it proves Linux compilation, not
  Linux test execution.
- **Land:** `<land-command> land <ws>`, where `<land-command>` is the operator-configured host entry that takes the
  repository's land turn; your lead gives you its path, it is not in this repository. It invokes `tooling/land-host.sh`,
  which owns acceptance of the rebased tree; do not invoke its internals directly or reconstruct its retry/waiver
  taxonomy in this file.
- **Doc commit (Markdown only):** `<land-command> git -C <main> commit --only -m 'docs: summary' -- <paths>` (new files
  first: `<land-command> git -C <main> add -- <paths>`). It takes the land turn briefly and runs no gate of its own; the
  next land re-gates what the Markdown reaches. Conventional commits, no trailers.
- **Build state:** cowshed owns Nx state paths and the Nx daemon. Declare restorable outputs as Nx outputs, one producer
  per checkout; never redirect state paths ad hoc. Keep caches warm: an unexplained miss is a missing or wrong input,
  output or dependency declaration, fixed where it is found, never by wiping the cache. Infer targets and read their
  resolved graph rather than adding lint or clean stubs. One `.nx/cache`, task database and daemon per checkout, never a
  private second one; use the cache, never avoid it, and keep the task database (the cache's only index) when clearing
  graph state (`specs/cowshed/16_build_volumes.md`, "Every checkout has exactly one Nx state").

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
