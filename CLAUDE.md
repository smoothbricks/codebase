# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important Notes

**ALWAYS use Bun, never npm/npx:** This project uses Bun exclusively. Use `bun` instead of `npm`, and `bun run` instead
of `npm run`. Dev tools like `nx`, `biome`, etc. are available directly on PATH (via node_modules/.bin) so use them
directly without `bunx`.

**Error handling policy:** Known operational failures must return `Err`/`Result`; reserve `throw` for invariants or
impossible programmer/configuration bugs. For full policy and examples, follow `AGENTS.md`.

**Signal design policy:** Signals are minimal cross-agent communication protocols, not data dumps. Each agent owns its
domain — a signal says "do X now" and carries only what the **receiver** needs to act.

- **No sender state in signals.** Never put the sender's internal state (retry counters, backoff schedules, scheduling
  timestamps, config values) into a signal payload. If the sender needs to track retry attempts, backoff, or cadence,
  that belongs in the sender's reducer state and `ctx.time.at()` for scheduling. The receiver should not know or care
  about the sender's internal retry/escalation strategy.
- **Precise commands, no catch-all buckets.** Each signal type is a specific command with a precise, non-optional
  payload shape. All signals for an agent share one Arrow RecordBatch schema where the `type` column already
  discriminates — separate types are free. Don't create vague catch-all signals like `add_other_lines` with an untyped
  `line_type: string` field. If the domain has tips, donations, and adjustments, either define precise signal types for
  each or unify them under one properly named signal with a typed shape. Junk-drawer signals indicate incomplete domain
  analysis.
- **Indexes for cross-agent visibility.** If an agent needs to read another agent's state for decoupled coordination,
  use indexes — but only when genuinely needed.

## Common Development Commands

### Build and Development

- **Build a project**: `nx build <project-name>`
- **Type check**: `nx typecheck <project-name>`
- **Generate a new library**: `nx g @nx/js:lib packages/<name> --publishable --importPath=@my-org/<name>`
- **Sync TypeScript references**: `nx sync`
- **Check TypeScript references**: `nx sync:check`
- **Visualize project graph**: `nx graph`

### Testing

- **Run tests**: `bun test`
- **Run specific test file**: `bun test <file-path>`

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

**Property-based testing (preferred):** For state machines, rollback/undo semantics, fork graphs, and any \"works for N\"
invariant, default to `fast-check` properties over generated traces rather than only hand-picked examples.

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
