# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important Notes

**ALWAYS use Bun, never npm/npx:** This project uses Bun exclusively. Use `bun` instead of `npm`, and `bun run` instead
of `npm run`. Dev tools like `nx`, `biome`, etc. are available directly on PATH (via node_modules/.bin) so use them
directly without `bunx`.

**Use conventional commits:** Release versioning is derived from commit history. Use subjects like
`feat(statebus-core): add optimistic transactions`, `fix(money): round negative amounts`, or
`feat!: remove deprecated API`.

**Use `npm:public` for publishability:** Every publishable package must have `nx.tags` containing `npm:public`.
Private/internal packages must not have that tag. Release tooling discovers packages from `npm:public`, never hardcoded
package lists.

**Use `smoo` for shared monorepo tooling:** `@smoothbricks/cli` provides the `smoo` command and owns generated CI,
release, and Git hook files. Update generated copies with `smoo monorepo update`; check drift with
`smoo monorepo check`.

**Error handling policy:** Known operational failures must return `Err`/`Result`; reserve `throw` for invariants or
impossible programmer/configuration bugs. For full policy and examples, follow `AGENTS.md`.

**Test tracing policy:** Every package test suite (except `packages/lmao`) must be LMAO-traced and flush to a SQLite
sink (local `.trace-results.db` or worker D1 binding like `TRACE_RESULTS`). Keep preload/setup files wiring-only and
move runner-specific behavior into shared harness modules (`@smoothbricks/lmao/testing/bun`,
`@smoothbricks/lmao/testing/vitest`) plus package-local typed tracer modules.

**Tracing policy — No default `NoOpTracer`:** `NoOpTracer` may exist in `@smoothbricks/lmao` for API proof, comparison,
and overhead benchmarking, but it is not the normal repo pattern. Require tracing context from callers, use child spans,
and use observable suite/test tracers in tests.

If code creates a throwaway tracer to satisfy a `spanContext` parameter, the API is wrong — fix it by:

- **Requiring** spanContext (no optional, no fallback) — callers must be in a traced context
- **Using child spans** (`ctx.span(...)`) not root traces (`tracer.trace(...)`) for nested operations
- **Propagating context** — pass spanContext through the call chain, never create it ad-hoc

Runtime execution that lacks tracing context indicates a broken call graph.

**Agent design policy:** Agents represent domain entities — if something has its own identity and event history
(invoice, credit note, subscription, account), it's an agent. Don't conflate a simple current lifecycle with "shouldn't
be an agent." A credit note that currently processes one event is still a valid agent if credit notes are real domain
entities with their own audit trail. Agent separation follows entity boundaries, not computational complexity.

**Reducer state policy:** State shape reflects domain structure, not signal structure. Don't mirror signal types into
parallel arrays — normalize into unified domain collections. The reducer is the boundary where specific signal payloads
get transformed into canonical state. Use existing discriminators (e.g. `product_type_ord`) for downstream dispatch.

**Signal design policy:** Signals are minimal cross-agent communication protocols. Each signal is a precise command that
carries only what the **receiver** needs to act. For the full rules with code examples, see AGENTS.md.

- **No sender bookkeeping.** Retry counters, backoff schedules, scheduling timestamps, and config values stay in the
  sender's reducer state + `ctx.time.at()`. The receiver should not know or care about the sender's internal strategy.
- **Self-contained for deterministic processing.** When correctness requires exact cross-agent data, embed a
  point-in-time snapshot in the signal. Signals are immutable — data is locked at send time. Don't depend on an index
  read at processing time for correctness-critical operations (e.g. exact invoice amounts for a credit note reversal).
- **Precise commands, no catch-all buckets.** Each signal type has an exact non-optional payload shape. The Arrow
  RecordBatch `type` column already discriminates — separate signal types are free. Junk-drawer signals with untyped
  string discriminators (e.g. `line_type: string`) indicate incomplete domain analysis.
- **Extract common fields as constants.** `S.*` returns reusable marker values. DRY repeated fields and field groups via
  constants + spread.
- **Indexes for visibility and decoupled communication.** Indexes are reactive read-optimized projections of agent state
  (single writer per group, write-through SIEVE cache). Same-group: `ctx.index.{name}.set/get/getWhen`. Cross-group:
  `ctx.peek()` / `ctx.subscribe()` — always stale. Watches fire `$keyChanged` with data, support predicates, TTL with
  `$keyExpired`, and custom notification strategies. Use for dashboards, lookups, routing, and cross-agent wake-ups
  where eventual consistency is acceptable. Not for correctness-critical flows where exact point-in-time values are
  required (use signal snapshots).

  Scenario paths write facts to VM state via the RETE bridge. Do not use JS RETE evaluation in domain tests.

- **Search before implementing.** Reuse existing code and patterns before adding new helpers, APIs, or schema objects.

- **Avoid stupid allocations, especially on hot or frequently hit paths.** Do not excuse needless allocations just
  because a path is "cold" — startup, boundary, serverless, and browser initialization paths still matter. If a
  success/failure wrapper can be an immutable singleton or eliminated entirely, do that instead of allocating fresh
  `{ ok: true }` / tiny throwaway objects. Keep small result objects only when they are truly the right contract and not
  an avoidable churn point.
- **Boundary types must come from real contracts.** At transport/storage/IPC boundaries, prefer Typia or
  `@smoothbricks/validation` over ad-hoc guards/normalizers. Prefer real public types over manually restating
  `Record<string, unknown>` surfaces.

## TypeScript Rules

- **Inference quality is API quality.** Prefer types inferred from `defineAgent()`, `defineState()`, `defineSignals()`,
- **Thread generics end-to-end.** If inference breaks, fix the source generic/signature/return type. Do not patch the
  caller with casts or hand-restated shapes.
  - WHY: once a boundary is validated, the concrete type should flow through the system. That removes internal casts,
    duplicate validation, impossible runtime checks, and makes autocomplete/refactors straightforward.
- **Bundle related generics when possible.** If multiple generic parameters always travel together, prefer one bundled
  type parameter/object over a long generic list.
  - WHY: bundled generics preserve inference and prevent APIs from collapsing into cast-heavy call sites.
- **Proper generic defaults for type inference.** When a generic has a known shape but needs to allow inference:
  - Use `State = unknown` instead of `State = Record<string, unknown>` to allow type inference from init()
  - Use `Def = AnyEventSchema` instead of unconstrained generics to carry schema definitions
  - Use conditional types with `infer` to extract actual types from definition structures
  - Avoid broad constraints that erase type information (e.g., `extends Record<string, unknown>` on defaults)
  - WHY: defaults like `Record<string, unknown>` prevent inference; `unknown` allows the actual type to flow through
- **Typia is the default validation system.** Never hand-write `isRecord`, `typeof` guards, or similar for data that
  crosses a trust boundary — use Typia instead.
  - parsed/runtime value boundary -> `typia.assert/validate/assertEquals/validateEquals`
  - JSON string boundary -> `typia.json.assertParse/validateParse/isParse`
  - repeated boundary -> `typia.create*` / `typia.json.create*Parse`
  - WHY: Typia generates optimal runtime code directly from the type definition. Changing the type changes the validator
    automatically, so validation stays aligned with the real contract with near-zero authoring/runtime overhead.
  - **New package setup:** Every package that validates data needs `typia` in dependencies, `@smoothbricks/validation`
    in devDependencies, and a Typia preload in `bunfig.toml`. Packages with LMAO tracing use
    `"@smoothbricks/lmao/bun/preload"` (which re-exports the Typia preload). Packages without LMAO use
    `"@smoothbricks/validation/bun/preload"` directly.
- **`@smoothbricks/validation` is last resort only.** Use it only for thin shared wrappers around Typia, shared error
  formatting, or tiny utilities Typia truly cannot express.
  - WHY: otherwise it becomes a dumping ground for hand-written validators that drift from the type.
- **Zero cast policy.** No `as any`, no `as unknown as`, no `as never`, no `JSON.parse(...) as T`, no
  `as Record<string, unknown>` fake validation.
  - WHY: casts suppress the exact signal telling you where the type surface is broken.
- **Do not hand-write validators Typia already provides.** New helpers like `isRecord`, `normalizeX`, `parseJsonX`,
  `expectJsonX`, `toRecord`, `coerceX`, `isHttpRequest`, or similar are wrong by default.
  - WHY: they manually duplicate the contract instead of using the contract itself.
- **Use real contract types.** Prefer inferred/public types over manually restating `Record<string, unknown>` surfaces.
- **Preserve WHY comments.** If you refactor a block with a WHY comment, keep or improve that rationale. Replacing it
  with weaker glue or deleting it without preserving intent is a regression.

## Common Development Commands

### Build and Development

- **Build a project**: `nx build <project-name>`
- **Type check**: `nx typecheck <project-name>`
- **Generate a new library**: `nx g @nx/js:lib packages/<name> --publishable --importPath=@my-org/<name>`
- **Sync TypeScript references**: `nx sync`
- **Check TypeScript references**: `nx sync:check`
- **Visualize project graph**: `nx graph`

### Testing

- **Lint (all rules):** `nx lint <project>` — runs biome + eslint (type-checked) + custom checks, cached
- **Lint before tests:** always `nx lint <project>` first
- **Test:** `nx test <project>` — ALWAYS through Nx, never bare `bun test`
- **Test with filter/args:** pass runner args through Nx, e.g. `nx test <project> -- --filter \"test name pattern\"`
- **Typecheck / build:** `nx typecheck <project>`, `nx build <project>`
- **After tsconfig or dependency changes:** `nx sync` and `nx sync:check`
- **Query test results:** Use `.trace-results.db` SQLite databases:
  ```bash
  bun -e \"
    const { Database } = require('bun:sqlite');
    const db = new Database('packages/<project>/.trace-results.db');
    const latest = db.query(\\\"SELECT trace_id FROM spans WHERE parent_span_id = 0 AND row_index = 0 ORDER BY timestamp_ns DESC LIMIT 1\\\").get();
    const failures = db.query(\\\"SELECT s0.message AS name, s1.message AS err FROM spans s0 JOIN spans s1 ON s1.trace_id=s0.trace_id AND s1.span_id=s0.span_id AND s1.row_index=1 WHERE s0.trace_id=? AND s0.row_index=0 AND s1.entry_type IN (3,4)\\\").all(latest.trace_id);
    if (!failures.length) { console.log('All passed'); process.exit(0); }
    for (const f of failures) console.log('[FAIL]', f.name);
  \"
  ```
- **ALWAYS run tests through Nx:** `nx test <project>` — this builds dependencies first, keeps declarations fresh, and
  loads the package's real test config/preloads. Pass extra runner args through Nx instead of switching to `bun test`.
- **Package-local extra preloads still matter.** If a package adds its own extra preload beyond the shared root ones,
  `nx test` handles this automatically.
- **Never skip Nx cache** unless diagnosing cache issues — fix the config instead. If a task only passes with
  `--skip-nx-cache`, treat that as a broken target/input/output/dependency configuration.

### Linting and Formatting

- Code is automatically formatted on commit via Git hooks
- **Format code**: `bun run format`
- **Lint code**: `bun run lint`
- **Fix linting issues**: `bun run lint:fix`

### Package Management

- **Install dependencies**: `bun install`
- **Add dependency**: `bun add <package>`
- **Add dev dependency**: `bun add -d <package>`

## Architecture Overview

This is an Nx-based monorepo using Bun as the package manager, with devenv/direnv for environment management.

### Development Environment

- **Devenv/Direnv** automatically sets up the environment when entering the directory:
  - Installs Node.js (v22 for AWS Lambda compatibility) and Bun via Nix
  - Runs `bun install --no-summary`
  - Adds `node_modules/.bin` to PATH
  - Applies workspace Git configuration
- All dev tools (nx, biome, etc.) are available directly on PATH
- **Important**: If Claude Code is started from within a direnv session, environment changes won't reload automatically.
  To pick up environment changes after `devenv update`:
  - **Option 1**: Exit and re-enter the directory in your terminal, then restart Claude Code
  - **Option 2**: Use the `direnv-run` helper script:
    ```bash
    tooling/direnv-run <your-command>
    ```
- **Devenv command** is available anywhere in the monorepo:
  - `devenv shell` - Enter development shell
  - `devenv update` - Update devenv.lock from devenv.yaml inputs
  - `devenv up` - Start processes in the foreground
  - `devenv processes` - Start or stop processes
  - `devenv tasks` - Run tasks
  - `devenv test` - Run tests
  - `devenv search <package>` - Search for packages in nixpkgs
  - `devenv info` - Print information about the environment
  - `devenv gc` - Delete previous shell generations
  - Configuration files are in `tooling/direnv/`

### Project Structure

```
/
├── modules/        # Shared utilities and components
├── packages/       # Workspace packages (configured in package.json)
└── tooling/        # Development tools and configurations
```

### Code Quality

- **Git hooks** automatically format staged files on commit using:
  - Biome for JS/TS/JSX/TSX/JSON/HTML/CSS/GraphQL
  - ESLint for .astro files
  - Prettier for Markdown
  - Alejandra for Nix files
- **TypeScript** with strict mode and composite projects
- **Code style**: 2 spaces, single quotes, 120 character line width
- **Nx uses inferred tasks** - don't add build/typecheck scripts to package.json (Nx infers these from tsconfig), but DO
  add test scripts
- **Nx `targetDefaults` don't create targets** - they only configure targets that already exist. Targets defined in
  `nx.json` `targetDefaults` (like `lint`, `lint:fix`) must be declared as `"lint": {}` in each package's `"nx".targets`
  in `package.json` for the target to exist. The targetDefault then fills in the executor, options, and dependencies.
  When creating a new package, always add these stub entries.
- **Run `nx sync`** after modifying tsconfig files or adding/removing package dependencies to keep TypeScript project
  references in sync. Verify with `nx sync:check`.

### Testing

Tests use Bun's built-in test runner:

```typescript
import { describe, expect, it } from 'bun:test';
```

**Property-based testing (preferred):** For state machines, rollback/undo semantics, fork graphs, and any \"works for
N\" invariant, default to `fast-check` properties over generated traces rather than only hand-picked examples.

- Prefer property tests for deep fork-of-fork interleavings and reset/rollback correctness.
- Keep at least one focused example test for readability, then scale coverage with properties.
- Use explicit invariants (preservation, reversibility, idempotency where expected) and set `numRuns` high enough to
  stress edge cases.

### Type Inference First (Mandatory)

Integration tests are user-experience tests for public API ergonomics.

- Target outcome: **zero casts in tests** (`as any`, broad assertion casts, non-null workarounds).
- Workflow:
  1. Remove casts in a single test file first.
  2. Run typecheck/lint and read the actual errors.
  3. Fix source/runtime/public typing contracts first (non-test code).
  4. Add shared runtime validators/guards only when boundary patterns repeat.
  5. Re-run checks and keep tests cast-free.
- Do not add test-only wrappers/generics to hide inference problems.
- If a test needs casting to call a public API, treat that as a library typing bug to fix.
- Commit in atomic clusters (e.g., parser/guard + call sites, helper typing upgrades).

### No Type Erasure Policy (Mandatory)

Type inference quality is a primary API quality signal.

- Keep generic type parameters threaded end-to-end (input -> storage -> accessor return type).
- Do not erase specific generic types into broad singleton storage when values are returned to callers.
- If a helper accepts a generic context/tracer, exposed getters/handles must preserve that exact generic.
- Do not patch inference gaps with `as unknown as ...`, `as any`, or broad cast bridges in library code.
- If a cast seems required, treat it as a typing bug in source API design and fix types first.
- Back-compat `unknown` defaults may exist in core public generics, but helper/harness APIs must still preserve concrete
  inferred types at call sites.
