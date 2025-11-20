# dep-updater Package

Automated dependency update tool with Expo SDK support, stacked PRs, and AI-powered changelog analysis.

## Package Overview

**Purpose:** Automate dependency updates across multiple ecosystems (npm, Expo, Nix) with intelligent PR stacking and
AI-powered changelog summaries.

**Key Features:**

- Expo SDK version management with syncpack integration
- Stacked PR workflow (base new PRs on previous update PRs)
- AI-powered changelog analysis using Claude
- Multi-ecosystem support (npm, Expo, Nix/devenv)
- Dry-run mode for safe testing

## Requirements

**This tool is designed for Nx monorepos.**

- **Nx workspace**: The generated GitHub Actions workflow uses Nx targets for task orchestration and caching
- **Bun package manager**: Used for build and execution (via `nx:run-commands`)
- **Git repository**: Required for all git operations and PR creation
- **GitHub CLI (gh)**: Optional but recommended for PR operations

**Why Nx-only?**

- Leverages Nx computation caching for faster CI runs
- Automatic task dependency management (build before run)
- Consistent with monorepo best practices
- Simpler architecture focusing on one deployment model

**Future considerations:** GitHub App version could support non-Nx projects by running on external infrastructure.

## Architecture

```
src/
├── git.ts                  # Git operations (all functions accept executor param for testing)
├── config.ts              # Configuration loading and defaults
├── logger.ts              # Logger interface and implementations (Console, Silent)
├── types.ts               # Shared TypeScript types
├── cli.ts                 # CLI entry point with commander.js
├── commands/              # CLI command implementations
│   ├── update-deps.ts     # Main dependency update command (refactored into 5 focused functions)
│   ├── update-expo.ts     # Expo SDK update command
│   ├── init.ts            # Interactive setup wizard (refactored, 195 lines)
│   ├── generate-workflow.ts # GitHub Actions workflow generator
│   └── generate-syncpack.ts
├── pr/
│   └── stacking.ts        # PR stacking algorithm (all functions accept executor param for testing)
├── changelog/
│   ├── fetcher.ts         # Fetch changelogs from npm/GitHub
│   └── analyzer.ts        # AI-powered changelog analysis
├── updaters/              # Ecosystem-specific updaters
│   ├── bun.ts
│   ├── devenv.ts
│   └── nixpkgs.ts
├── expo/
│   ├── sdk-checker.ts     # Check Expo SDK versions
│   └── versions-fetcher.ts
└── utils/
    ├── path-validation.ts      # Security: validate paths are within repo
    ├── workspace-detector.ts   # Workspace detection utilities
    ├── prompts.ts              # User interaction utilities (prompt, confirm)
    └── project-detection.ts    # Project setup detection (package manager, Expo, Nix, syncpack)

test/                      # Mirrors src/ structure
├── git/                   # Git operation tests (86 tests - COMPLETE)
│   ├── query-functions.test.ts        # Read-only operations (41 tests)
│   ├── modification-functions.test.ts # State-changing operations (31 tests)
│   └── workflow-functions.test.ts     # High-level orchestration (14 tests)
├── pr/                    # PR stacking tests (47 tests - COMPLETE)
│   ├── pure-functions.test.ts         # Pure functions (9 tests)
│   ├── query-functions.test.ts        # Read-only gh CLI operations (13 tests)
│   ├── modification-functions.test.ts # State-changing gh CLI operations (10 tests)
│   └── stacking-algorithm.test.ts     # Core stacking logic (15 tests)
├── integration/           # Integration tests (46 tests - COMPLETE)
│   ├── pr-stacking-workflow.test.ts   # End-to-end PR stacking workflows (14 tests)
│   └── config.test.ts                 # Config loading, merging, sanitization (32 tests)
├── changelog/             # Changelog tests (25 tests)
│   ├── analyzer.test.ts               # Commit message generation (8 tests)
│   └── fetcher.test.ts                # Changelog fetching and parsing (17 tests)
├── updaters/              # Updater tests (19 tests)
│   ├── devenv.test.ts                 # Devenv (Nix) updater (10 tests)
│   └── nixpkgs.test.ts                # Nixpkgs overlay updater (9 tests)
├── utils/                 # Utility tests (14 tests)
│   └── project-detection.test.ts      # Project setup detection (14 tests)
└── helpers/
    └── mock-execa.ts      # Test utilities for mocking command execution
```

## Testing Strategy

### Dependency Injection Pattern

All functions that execute commands accept an optional `executor` parameter for testing:

```typescript
export async function getCurrentBranch(
  repoRoot: string,
  executor: CommandExecutor = execa // Default to real execa
): Promise<string> {
  const { stdout } = await executor('git', ['rev-parse', '--abbrev-ref', 'HEAD'], {
    cwd: repoRoot,
  });
  return stdout.trim();
}
```

### Test Helpers (`test/helpers/mock-execa.ts`)

**Three types of mocks:**

1. **createMockExeca** - Returns predefined responses for commands

   ```typescript
   const mockExeca = createMockExeca({
     'git rev-parse --abbrev-ref HEAD': 'main\n',
   });
   ```

2. **createErrorExeca** - Always throws errors

   ```typescript
   const mockExeca = createErrorExeca('not a git repository');
   ```

3. **createExecaSpy** - Tracks calls and returns responses
   ```typescript
   const spy = createExecaSpy({ 'git add -A': '' });
   await stageAll('/repo', spy.mock);
   expect(spy.calls).toHaveLength(1);
   ```

### Test Organization

Tests are organized by function type:

- **Query functions** - Read-only git operations (status, branch checks, etc.)
- **Modification functions** - State-changing operations (commit, push, branch creation)
- **Workflow functions** - High-level orchestration (createUpdateCommit, createUpdateBranch)

### Integration Testing

Integration tests validate end-to-end workflows and module interactions:

**PR Stacking Workflows (`test/integration/pr-stacking-workflow.test.ts`)**

- Sequential PR creation (stacking on previous PRs)
- Conflict handling with `stopOnConflicts` flag
- Auto-closing oldest PRs when hitting `maxStackDepth`
- Custom base branch scenarios
- Stacking disabled mode

**Config Integration (`test/integration/config.test.ts`)**

- Deep merging of user config with defaults
- Realistic scenarios: minimal projects, Expo projects, monorepos
- API key sanitization for safe logging
- Type safety enforcement across config sections

**Mock Patterns for Integration Tests:**

- Create custom executors that simulate multi-step workflows
- Track state changes across multiple operations (e.g., PRs closed during workflow)
- Test realistic scenarios with multiple PRs, conflicts, and state transitions

## Key Design Decisions

### 1. Type Organization

**All shared types must be defined in `src/types.ts`**

- Never define types directly in implementation files (config.ts, commands/, etc.)
- Import types from `types.ts` where needed
- Keep types centralized for easy discovery and reuse
- Group related types together with clear JSDoc comments

**Example:**

```typescript
// ✅ Good: Define in types.ts
export interface ExpoProject {
  name?: string;
  packageJsonPath: string;
}

// ✅ Good: Import from types.ts
import type { ExpoProject } from './types.js';

// ❌ Bad: Define types in implementation files
export interface ExpoProject { ... } // in config.ts
```

**Why:**

- Single source of truth for all types
- Easier to find and update types
- Prevents duplicate type definitions
- Better IDE autocomplete and navigation
- Clearer separation of concerns

### 2. Partial Config in Workflow Functions

Workflow functions accept `Partial<DepUpdaterConfig>` instead of full config:

```typescript
export async function createUpdateCommit(
  config: Partial<DepUpdaterConfig>, // Not full DepUpdaterConfig
  commitMessage: string,
  commitBody?: string,
  executor: CommandExecutor = execa
): Promise<void>;
```

**Why:** Tests only need to provide relevant config fields (e.g., `{ repoRoot: '/repo' }`), not entire config structure.

### 3. Git Porcelain Format Parsing

`getChangedFiles` parses git porcelain format (`XY filename`):

- **Critical:** Don't `.trim()` before `.split('\n')` - preserves leading space
- Format: `XY filename` where X=index status, Y=worktree status (single chars), space, filename
- Extract filename with `.substring(3)` to skip status codes and space

### 4. CommandExecutor Type

```typescript
export type CommandExecutor = (
  file: string | URL,
  args?: readonly string[],
  options?: Record<string, any>
) => Promise<any>; // any needed for execa's complex ResultPromise type
```

**Why `Promise<any>`:** Execa's `ResultPromise` is a complex union type. Using `any` is acceptable here since:

- Only used for testing
- Actual usage is type-safe (we only access `.stdout`)
- Avoids complex type compatibility issues

### 5. PR Stacking Strategy

Located in `src/pr/stacking.ts` (274 lines, **FULLY TESTED** - 47 tests):

- Base branch selection algorithm (`determineBaseBranch`)
- Auto-close logic for old PRs (`autoCloseOldPRs`)
- Conflict detection (`checkPRConflicts`)
- Stack depth management
- PR creation with stacking workflow (`createStackedPR`)
- All functions accept `executor` parameter for testability

### 6. Command Refactoring for Maintainability

**update-deps.ts** - Refactored from 246-line monolithic function into 5 focused functions (301 lines total):

1. **`setupBranchForStacking()`** (42 lines)
   - Determines base branch for stacking (main or previous PR branch)
   - Checks out the base branch if stacking is enabled
   - Returns branch context for cleanup later

2. **`runAllUpdaters()`** (100 lines)
   - Orchestrates all updaters (Bun, devenv, nixpkgs)
   - Collects updates and errors from each ecosystem
   - Reports summary of updates found

3. **`generateCommitData()`** (39 lines)
   - Fetches changelogs from npm/GitHub
   - Uses AI analysis if enabled
   - Generates commit title and PR body

4. **`createPRWorkflow()`** (42 lines)
   - Creates branch from current position
   - Commits changes with generated message
   - Pushes to remote and creates PR

5. **`updateDeps()`** (62 lines) - Main orchestrator
   - Calls helper functions in sequence
   - Handles early returns (no updates, dry-run)
   - Switches back to original branch on completion

**Benefits:**

- Each function has single, clear responsibility
- Easier to test individual components
- Better error isolation
- More maintainable codebase

**init.ts** - Refactored from 271 lines into modular structure (195 lines core + 89 lines utilities):

- Extracted `src/utils/prompts.ts` - Reusable user interaction functions
- Extracted `src/utils/project-detection.ts` - Project setup detection logic
- Core init logic remains focused on workflow orchestration

## GitHub Actions Integration

### Workflow Generator (`src/commands/generate-workflow.ts`)

**Purpose:** Generate `.github/workflows/update-deps.yml` for automated dependency updates.

**Key Features:**

- Uses Nx target for CLI execution (automatic builds + caching)
- Configures git user for commits (github-actions[bot])
- Uses fine-grained PAT for PR creation
- Supports Nx computation caching for faster CI runs

**Authentication:** Fine-grained Personal Access Token (GH_PAT)

**Why PAT instead of GITHUB_TOKEN?**

- GITHUB_TOKEN doesn't trigger `pull_request` workflows (by design)
- Fine-grained PAT has repository-specific permissions (better security)
- Triggers CI/checks automatically when PR is created

**Workflow Steps:**

1. Checkout repo with full history (`fetch-depth: 0`)
2. Setup Bun
3. Install dependencies
4. Configure git identity
5. Run CLI via Nx target (`nx run @smoothbricks/dep-updater:update-deps`)
   - Nx automatically builds the CLI if needed
   - Nx caches build results for faster subsequent runs
   - Passes GH_PAT and optional ANTHROPIC_API_KEY via env vars

**TODO:** Add GitHub App authentication support for production (not user-dependent, better for orgs)

### Init Command (`src/commands/init.ts`)

**Purpose:** Interactive setup wizard for new projects.

**Architecture:** Refactored into modular structure (271 → 195 lines)

- Core logic in `init.ts`
- User interaction utilities extracted to `src/utils/prompts.ts` (37 lines)
- Project detection logic extracted to `src/utils/project-detection.ts` (52 lines)

**Features:**

- Auto-detects project setup:
  - Package manager (bun/npm/pnpm/yarn) - via lock file detection
  - Expo project - via package.json dependencies
  - Nix setup (devenv, flake.nix, .envrc)
  - Syncpack config (.syncpackrc.json, .syncpackrc.yml)
- Interactive prompts for configuration options
- Generates `.dep-updater.json` config file
- Calls `generate-workflow` to create GitHub Actions workflow
- Provides next steps with setup instructions

**Options:**

- `--yes`: Skip prompts and use defaults (for CI/automation)

**Reusable Utilities:**

- `prompts.ts`: `prompt()` and `confirm()` functions for user input
- `project-detection.ts`: `detectProjectSetup()` returns ProjectSetup interface

## Development Workflow

### Commands

```bash
bun run build      # Build CLI
bun run test       # Run all tests
bun run typecheck  # Type check src/ and test/
bun test --watch   # Watch mode for tests
```

### Nx Targets

The package includes an Nx target for running the CLI in CI environments:

```bash
nx run @smoothbricks/dep-updater:update-deps -- [CLI_FLAGS]
```

**Benefits:**

- **Automatic build**: Depends on the `build` target, so Nx ensures the CLI is built before running
- **Computation caching**: Nx can cache and reuse build results (locally and in CI with Nx Cloud)
- **Task orchestration**: Leverages Nx's dependency graph and task scheduling
- **Better CI performance**: Skips rebuilds when code hasn't changed, saving CI minutes

**Usage in CI:** The generated GitHub Actions workflow uses this target instead of manually building and running the
CLI. This allows Nx to cache the build and skip it on subsequent runs if nothing changed.

**Example:**

```bash
# Run with verbose logging and skip AI
nx run @smoothbricks/dep-updater:update-deps -- --verbose --skip-ai

# All CLI flags work with -- separator
nx run @smoothbricks/dep-updater:update-deps -- --dry-run
```

### Before Committing

**IMPORTANT:** After making code changes, always run these checks to catch issues early:

```bash
bun run format     # Format code with Biome (or let git hooks do it)
nx run @smoothbricks/dep-updater:lint  # Run linting
bun run typecheck  # Type check both src/ and test/
bun test           # Run all tests
```

These checks help ensure code quality and prevent CI failures. The git hooks will auto-format on commit, but it's good
practice to run lint and typecheck manually before committing.

### Type Checking

- Uses `tsconfig.typecheck.json` to check both `src/` and `test/` files
- Separate from build config (which only includes `src/`)
- Run `bun run typecheck` before committing

### Testing Guidelines

1. **Always use dependency injection** - Add `executor` param to any function that runs commands
2. **Test organization** - Group tests by function type (query/modification/workflow)
3. **Use appropriate mock** - Response-based for simple tests, spy for call verification
4. **Test error cases** - Use `createErrorExeca` for error scenarios
5. **Non-null assertions in tests** - Use `spy.calls[0]!` when you know array has items

## Configuration

Config structure in `src/config.ts`:

```typescript
interface DepUpdaterConfig {
  expo?: {
    enabled: boolean;
    autoDetect?: boolean; // Auto-detect Expo projects (default: true)
    projects?: ExpoProject[]; // Explicit list of projects
  };
  syncpack?: { configPath: string; preserveCustomRules: boolean };
  nix?: { enabled: boolean; devenvPath: string; nixpkgsOverlayPath: string };
  prStrategy: { stackingEnabled: boolean; maxStackDepth: number; ... };
  autoMerge: { enabled: boolean; mode: 'none' | 'patch' | 'minor'; ... };
  ai: { provider: 'anthropic'; apiKey?: string; model?: string };
  git?: { remote: string; baseBranch: string };
  repoRoot?: string;
}
```

### Expo Multi-Project Support

The tool supports managing multiple Expo projects in a monorepo:

**Auto-Detection (Recommended):**

```typescript
export default defineConfig({
  expo: {
    enabled: true,
    autoDetect: true, // Scans workspace for packages with "expo" dependency
  },
});
```

**Manual Project List:**

```typescript
export default defineConfig({
  expo: {
    enabled: true,
    autoDetect: false,
    projects: [
      { name: 'customer-app', packageJsonPath: './apps/customer/package.json' },
      { name: 'driver-app', packageJsonPath: './apps/driver/package.json' },
      { name: 'admin-app', packageJsonPath: './apps/admin/package.json' },
    ],
  },
});
```

**Implementation Details:**

- Auto-detection scans workspace patterns from root `package.json`
- Checks each package for `expo` in `dependencies` or `devDependencies`
- `resolveExpoProjects()` in `src/config.ts` handles priority: explicit list → auto-detection
- `detectExpoProjects()` in `src/utils/workspace-detector.ts` performs the scanning
- All projects update to the same Expo SDK version
- Commit message lists all updated projects with version changes

**Config File Loading:** The tool automatically searches for config files in `tooling/` directory (TypeScript first,
then JSON):

- `tooling/dep-updater.ts` (priority 1, supports custom logic and hooks)
- `tooling/dep-updater.json` (priority 2, simple configuration)

The search happens in the current directory and parent directories up to 10 levels. If not found, defaults from
`defaultConfig` are used.

TypeScript configs use the `defineConfig()` helper for type safety:

```typescript
// tooling/dep-updater.ts
import { defineConfig } from 'dep-updater';

export default defineConfig({
  expo: { enabled: true },
  prStrategy: { stackingEnabled: true, maxStackDepth: 3 },
});
```

See `src/config.ts:loadConfig()` for implementation details.

## Security Considerations

### Path Validation (`src/utils/path-validation.ts`)

Always validate user-provided paths to prevent path traversal attacks:

```typescript
import { validatePathWithinBase, safeResolve } from './utils/path-validation.js';

// Throws if targetPath tries to escape baseDir
validatePathWithinBase(baseDir, targetPath);

// Safe alternative to path.resolve with validation
const safePath = safeResolve(baseDir, userProvidedPath);
```

### Git Repository Root Validation

`getRepoRoot()` validates the git root path:

- Must be absolute path (starts with `/` or Windows drive letter)
- Prevents command injection via malicious repo paths
- Throws on empty or relative paths

## Code Coverage Status

### ✅ Tested (342 tests total)

- **Git operations** - All 19 functions (86 tests)
- **PR Stacking** - All 8 functions (47 tests)
  - Pure functions: `generateBranchName`, `generatePRTitle`
  - Query functions: `getOpenUpdatePRs`, `checkPRConflicts`
  - Modification functions: `createPR`, `autoCloseOldPRs`
  - Workflow functions: `determineBaseBranch`, `createStackedPR`
- **Integration tests** - End-to-end workflows (46 tests)
  - PR stacking workflows: Sequential stacking, conflict handling, auto-close, custom base branches
  - Config integration: Deep merging, realistic scenarios, sanitization, type safety
- **Config** - File loading, merging, validation, and sanitization (32 tests)
- **Changelog** - Analyzer and fetcher (25 tests)
  - Analyzer: Commit message generation, grouping by update type (8 tests)
  - Fetcher: npm/GitHub changelog fetching, batch processing, error handling (17 tests)
- **Updaters** - All updaters (19 tests)
  - Bun updater: Package.json diff parsing, version classification
  - Devenv (Nix): Dry-run mode, lock file parsing, error handling (10 tests)
  - Nixpkgs overlay: Version extraction from Nix expressions, malformed file handling (9 tests)
- **Utils** - Path validation, workspace detection, project detection (18 tests)
  - Project detection: Package manager, Expo, Nix, Syncpack detection (18 tests)
- **Expo SDK** - Version checking, package detection
- **Syncpack** - Config generation

### ❌ Not Yet Tested (Priority Order)

1. **Command workflows** - Full update-deps and update-expo command integration (e2e tests)
2. **Init command** - Interactive prompts and workflow generation
3. **Generate-workflow command** - GitHub Actions workflow generation

## Common Patterns

### Adding a New Git Function

1. Add `executor` parameter with default:

   ```typescript
   export async function myGitFunc(repoRoot: string, executor: CommandExecutor = execa): Promise<void> {
     await executor('git', ['...'], { cwd: repoRoot });
   }
   ```

2. Create test file in `test/git/`:

   ```typescript
   import { myGitFunc } from '../../src/git.js';
   import { createExecaSpy } from '../helpers/mock-execa.js';

   test('should do something', async () => {
     const spy = createExecaSpy({ 'git ...': '' });
     await myGitFunc('/repo', spy.mock);
     expect(spy.calls[0]![1]).toEqual(['...']);
   });
   ```

### Working with Partial Configs in Tests

```typescript
// Minimal config - only provide what you need
await createUpdateCommit({ repoRoot: '/repo' }, 'message');

// With git config
await createUpdateBranch({ git: { remote: 'upstream' } }, 'branch-name');

// Empty config - uses defaults (calls getRepoRoot)
await createUpdateCommit({}, 'message');
```

## Future Development

### Planned Features

- Auto-merge functionality (config exists, implementation TODO)
- Exclude patterns for dependencies

### Known Limitations

- GitHub API only (no GitLab/Bitbucket)
- Bun package manager only (uses npm registry)
- Anthropic AI only (no OpenAI/other providers)
- No retry logic for network failures

## Troubleshooting

### Tests Failing with "Unexpected command"

Check mock responses include all commands:

```typescript
const spy = createExecaSpy({
  'git rev-parse --show-toplevel': '/repo\n', // Don't forget this if config.repoRoot not set
  'git add -A': '',
  'git commit -m message': '',
});
```

### Type Errors with executor Parameter

Ensure signature matches `CommandExecutor`:

```typescript
const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
  // Must accept string | URL, not just string
  const command = typeof cmd === 'string' ? cmd : cmd.toString();
  // ...
};
```

### Git Porcelain Format Issues

Remember git status format: `XY filename` (2 status chars + space + filename)

- Use `.substring(3)` to extract filename
- Don't `.trim()` entire stdout before splitting
