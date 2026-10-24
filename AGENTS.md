# AGENTS.md

## Rule card

1. **Preserve others' work.** Coordinate shared files; never reset, checkout or stash a shared tree to clean it. Commit
   each accepted coherent unit promptly with exact paths (`git commit --only -- <paths>`), never the shared index
   wholesale. Commit WIP before disruptive host operations. Confirm main contains the accepted commit before claiming a
   landing; preserve failed or partially landed work for its owner.
2. **Use the cheapest sufficient proof: mount less; wait for the event.** Reuse valid proof until its relevant inputs
   change. Use a directory or owned fixture instead of an image when it proves the same contract. Tests advance on
   readiness, completion and progress, not sleeps or arbitrary total wall-clock ceilings; inject time for time behavior.
   Keep expensive host fixtures out of ordinary test dependencies; the explicit land gate reaches their dedicated proof
   when that boundary or its dependency, fixture, environment or toolchain inputs change. Keep caches warm and fix
   unexplained misses where found. **`--skip-nx-cache` is forbidden, including for diagnosis.** Repair inputs, outputs
   or dependencies; never raise a bound or bypass a cache to manufacture a pass. Clean up physically owned fixtures on
   every exit path.
3. **Inspect evidence before expensive work; stop obsolete work.** Read trace sinks, task history and prior verdicts
   before rerunning. Cancel an owned run whose result is already obsolete or doomed, preserve its evidence, fix the
   cause and run the relevant proof. Do not kill a peer's useful independent work or conceal a real failure.
4. **One owner and one shed per code unit.** Helpers share the unit through explicitly owned files; no git worktrees as
   a substitute. Use absolute workspace paths in filesystem/edit tools so session cwd cannot redirect an edit into main.
   Standalone and unrelated Markdown follow the documentation custody card, not the code-workspace rule.
5. **Land through the repo wrapper.** It owns the rebased-tree gate, cache custody and expected-tip fast-forward.
   Prioritize fixes that remove fleet bottlenecks; combine related fixes into coherent trains. Nx acceptance follows its
   declared DAG: lint-first is focused inner-loop fail-fast advice, not fleet serialization. Bare Cargo or Bun tests may
   diagnose their runners but do not replace Nx acceptance. After success, confirm main's accepted commit and the
   workspace's retirement; remove only a residual completed workspace, never failed or unlanded work.
6. **Choose documentation custody by relationship.** Fix standalone or unrelated Markdown immediately on main through
   its doc lane. Keep a code unit's own docs/specs with that code and land them atomically. The repo card defines the
   local lock and gate behavior; a Markdown-only commit does not imply its contents are irrelevant to later acceptance.
7. **Fix concurrency defects; do not serialize them into passing.** Prove the fix under intended concurrency. Shared
   mutable state needs explicit ownership: single writers, one producer per checkout, land/ref locks and operator
   admission or priority for scarce resources are legitimate. They are not permission to hide a failing concurrent test.
8. **Test at the defect's owner and the smallest real boundary.** Do not repeat dependency suites in consumers or
   re-prove OS internals. Exercise our real OS/process/network integration where that behavior matters; use real
   in-memory implementations for logic, never fake echoes. Choose an oracle that can fail. Missing tests, infrastructure
   errors and timeouts are not intended red. Measure performance claims in comparable regimes and retain the witness.
   Never assert a tautology: a test that the OS behaves like the OS, or Nx like Nx, proves nothing - test our contract
   at the boundary that can actually fail.
9. **Preserve inference.** Thread precise public types and generics from input through storage to returned values; fix
   source APIs instead of adding cast bridges or test-only wrappers. No `as any`, `as unknown as`, or
   `JSON.parse(...) as T` as validation. `as const` and `satisfies` preserve information. An `unknown` generic default
   is valid when it preserves inference, not as a backward-compatibility exemption.
10. **Validate external boundaries from their authoritative contracts.** Generate validation from the owning type or
    schema, translate failures to typed `Err`, and do not revalidate proven internal data without a trust boundary.
    Exact fixture/oracle readers reject unknown fields; additive protocol readers follow their evolution contract. Use
    Typia `validate/createValidate` for TypeScript envelopes/JSON, or translate assert failures at the seam. Wire and
    domain payloads use their owning generated codecs or schema validators; do not maintain a second validator.
11. **Operational failures are observable typed values.** Return `Err`/`Result` with operation, code and cause; throw or
    panic only for stated programmer bugs or impossible invariants. Require tracing context in normal wiring, root
    traces at entry points and child spans below them. Test operational paths as `span-err`, not `span-exception`. Use
    observable test tracers; no default NoOpTracer. API-comparison and benchmark exceptions remain explicit.
12. **Keep private information out of public code and history.** No home paths, private project names or agent/session
    identifiers in public source, comments, docs or commit messages. Use portable command placeholders there. Review
    every surface; a partial guard does not establish complete coverage.
13. **Reuse, simplify and keep one contract owner.** Search existing code/specs before implementing; use available
    search tools and fall back to source when an index is unavailable. Fix touched defects; give unrelated code defects
    evidence and an owner without replacing the goal. Fix unrelated docs immediately under rule 6. No compatibility
    shims, dead APIs or hand-maintained copies of derivable contracts; required protocol evolution remains a contract.
    Avoid needless allocation/copying, pay for computation once and retain what it proves. Specs state the target design
    in present tense, not a diary: implement an agreed better design and update its spec with the same unit, never omit
    the capability. Put implementation gaps and evidence in the owning brief, not status notes in the spec.
14. **Depend on published revisions, never host paths.** Patch a dependency by git source and locked revision
    (`rev = "<sha>"`), never by filesystem path: Cargo serves it from the local git cache, so the same tree builds on
    any host and in CI. A path patch bakes one machine's checkout into the build and fails everywhere else.
15. **Fail early and fix the root cause.** A failure is information: stop the work that depends on it, name the cause
    and fix it there. Never retry around it, tolerate it, widen a bound, skip the check or route around it - a tolerated
    failure is a defect with a receipt. Every error you raise carries the context you already have at the throw site -
    the operation, the subject (ids, paths, versions), the values involved and the cause - in the structured error
    result, so the reader needs no second run to understand it.
16. **No env-var, lock or ceremony fixes.** Never add environment variables, lock files, holder markers, wrapper
    protocols or hooks to work around a tool or coordinate agents; fix the tool itself. Cowshed's jobserver and land
    turn are the coordination. Anything else needs Danny's explicit approval first.

17. **Pay for computation once; keep what it proves.** Prefer work that leaves reusable structure: one structure, many
    consumers; compute order once. Derived state (indexes, envelopes, checkpoints, caches) is a byproduct, never a
    second store; units name their authoritative source and why no duplicate store exists. Assess every unit through
    this byproduct lens. Flag these **second-ledger / second-store** shapes by name:

    - **Write ledger:** a second ledger beside the event log.
    - **Duplicate inventory:** a copy beside the authoritative index.
    - **Retraction index:** a separately kept store instead of a byproduct of the facts.
    - **Derivable-byte cache:** derivable bytes kept as a second durable or authoritative store.
    - **Parallel registry:** another registry of what a manifest already lists.
    - **Checkpoint store:** live state duplicated into a separate store.
    - **Summary table:** a second store recomputable from its rows.
    - **Canonical copy:** another canonical copy beside the live source.
    - **Shadow config:** a copy beside the declared config.

18. **Remove our work first; cache last. Cache over fork.** Measure and optimize our code before memoizing. If removing
    the remaining work requires an upstream fork/patch, prefer a boundary cache; name the upstream alternative and its
    maintenance cost in the design record, and explain why the fork is not worth carrying. Caches stay basis-keyed,
    recomputable and disposable, never second stores.

### Conduct

- **C1:** Finish authorized work; soft budgets never justify stopping, idling or handing off unfinished work.
- **C2:** Prepare useful authorized work against a blocking dependency's newest candidate; do not invent another owner.
- **C3:** Answer the actual question first; questions alone authorize no changes. Continue already authorized work.
- **C4:** Read governing instructions before editing; reuse their terms and update the owner, not a parallel section.

## Repo card

Follow the active human task and higher-level instructions. This sheet gives entry decisions; linked owners define
mechanics and design. A specific scoped rule wins over a generic slogan. Name and repair conflicts at the owner rather
than inventing workarounds. `CLAUDE.md` is a symlink to this file; there is no separate short authority or generated
sheet.

- **Tools:** Bun only; no npm, npx or bunx. Use `bun nx` or the shell's `nx` and other installed tools directly.
- **Acceptance:** `NO_COLOR=1 bun nx run-many -t lint test build -p <projects> --outputStyle=static-failures-only`.
  Focused proof does not replace the land wrapper's acceptance of the rebased candidate. Read its exit status and
  evidence. For platform-conditional Rust, run `bun run check:linux` before the relevant final land/push; it proves
  Linux compilation, not Linux test execution.
- **Land:** `<land-command> land <ws>`, where `<land-command>` is the operator-configured, repository-locking host
  entry. It invokes `tooling/land-host.sh`; that script owns acceptance of the rebased tree and takes no fleet lock
  itself. Do not invoke its unlocked internals directly or reconstruct its retry/waiver taxonomy in this sheet.
- **Markdown commit:** `<land-command> git -C <main> commit --only -m 'docs: summary' -- <paths>`. Stage new files
  through `<land-command> git -C <main> add -- <paths>`. Use conventional commits, no trailers. This configured entry
  takes the repository gate lock, so Markdown can wait behind a land. It runs the Git command, not a separate
  Markdown-validating, ref-lock-only lane. Follow the custody card without claiming a nonwaiting lane.
- **Build state:** shell/cowshed owns paired absolute Nx state paths and daemon lifecycle, preserving explicit absolute
  CI/cowshed overrides. Tasks and agents must not redirect them ad hoc. Declare all restorable deliverables as Nx
  outputs, never mutable tool state; keep one producer per checkout. Infer targets and inspect their resolved graph,
  rather than adding lint/clean stubs. Detailed keying belongs in the linked owners, not another command recipe.
- **Architecture:** arrow-builder owns generic buffers and Arrow conversion; LMAO owns spans, tracing, contexts and
  span-tree conversion. Hot paths use allocation-sensitive column writes with each schema kind's storage, including raw
  strings for dynamic category/text fields. Physical structural columns are eager; sparse system attributes can be lazy.
  The architecture and schema specs own these distinctions, not a handwritten API or enum inventory.
- **Admission:** add entry guidance only for a recurring high-loss decision with an actionable scope and named owner. Do
  not restate what a tool, template or check already enforces. Preserve useful rationale at the existing owner; delete
  stale recipes and API inventories. Do not regenerate vendor instruction blocks into this file.
- **Enforcement:** these rules do not claim universal tool coverage. Source checks are scoped; Markdown instructions and
  fixed-bound runner/waiver behavior are not proved correct by a source lint. Change owner tools/specs coherently.

## Read First

- `specs/lmao/00_package_architecture.md`: package ownership, dependency direction and allocation strategy.
- `specs/lmao/01a_trace_schema_system.md`: storage by schema kind, raw-string lanes and cold-path encoding.
- `specs/lmao/01l_op_context_pattern.md`: inference-preserving contexts and Op authoring.
- `packages/lmao/README.md`: tracer wiring, trace-testing and the TraceQuery/SQLite inspection entry; inspect before
  reruns.
- `packages/nx-plugin/README.md`: inferred targets, Cargo closures, runtime inputs and build-state output safety.
- `specs/cowshed/16_build_volumes.md`: workspace-owned build state, warm forks, cache carry and adoption.
- `packages/cowshed/docs/agents.md`: workspace ownership and agent-facing CLI workflow.
- `tooling/land-host.sh` and `tooling/land-host.test.ts`: current landing mechanics and refusal evidence.

## Documentation custody card

| Unit                                                  | Workspace                                    | Commit and acceptance                                                                    |
| ----------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Only Markdown, unrelated to an active code unit       | Main, no shed                                | Exact-path commit through the configured locking entry; do not start a code gate for it. |
| Code plus docs/specs describing that code             | Unit's shed                                  | One coherent unit through the land wrapper.                                              |
| Unrelated Markdown found while working in a code shed | Main for the doc fix; code stays in its shed | Fix immediately through the doc lane; the next rebase must reuse unaffected task caches. |
