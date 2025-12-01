# dep-updater Package

Automated dependency update tool with Expo SDK support, stacked PRs, and AI-powered changelog analysis.

## Package Overview

**Purpose:** Automate dependency updates across multiple ecosystems (npm, Expo, Nix) with intelligent PR stacking and
AI-powered changelog summaries.

**Key Features:**

- Expo SDK version management with syncpack integration
- Stacked PR workflow (base new PRs on previous update PRs)
- AI-powered changelog analysis (free by default via OpenCode, or premium with Anthropic/OpenAI/Google)
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
│   ├── generate-workflow.ts # GitHub Actions workflow generator (with enhanced comments)
│   ├── generate-syncpack.ts
│   └── validate-setup.ts  # Setup validation command (checks GitHub CLI, auth, app installation)
├── auth/                  # GitHub authentication
│   └── github-client.ts   # GitHub CLI client for PR operations
├── pr/
│   └── stacking.ts        # PR stacking algorithm (all functions accept executor param for testing)
├── ai/                    # AI integration
│   ├── opencode-client.ts # OpenCode SDK client for multi-provider AI
│   └── token-counter.ts   # Token counting with gpt-tokenizer
├── changelog/
│   ├── fetcher.ts         # Fetch changelogs from npm/GitHub
│   └── analyzer.ts        # AI-powered changelog analysis
├── updaters/              # Ecosystem-specific updaters
│   ├── bun.ts             # npm package updates via Bun
│   ├── devenv.ts          # Nix devenv updates (devenv.yaml, devenv.lock)
│   └── nixpkgs.ts         # nvfetcher overlay updates (parses generated.json)
├── expo/
│   ├── sdk-checker.ts     # Check Expo SDK versions
│   └── versions-fetcher.ts
└── utils/
    ├── path-validation.ts      # Security: validate paths are within repo
    ├── workspace-detector.ts   # Workspace detection utilities
    ├── prompts.ts              # User interaction utilities (prompt, confirm)
    └── project-detection.ts    # Project setup detection (package manager, Expo, Nix, syncpack)

test/                      # Mirrors src/ structure
├── auth/                  # GitHub client tests (20 tests - COMPLETE)
│   └── github-client.test.ts          # GitHubCLIClient unit tests (20 tests)
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
├── ai/                    # AI integration tests (30 tests)
│   ├── opencode-client.test.ts        # OpenCode SDK client tests (16 tests)
│   └── token-counter.test.ts          # Token counting tests (14 tests)
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

## Setup and Validation

### Comprehensive Setup Documentation

**File: `docs/GETTING-STARTED.md`** - Complete setup guide covering both authentication methods:

- **PAT Setup**: 5-minute simple setup for small teams
- **GitHub App Setup**: 15-minute organization-level setup with validation
- Creating GitHub App with correct permissions
- Installing app to organization
- Configuring organization-level secrets (reusable across all repos)
- Testing and troubleshooting
- Security best practices

**Why organization-level setup:**

- Single setup for entire organization (PAT or GitHub App)
- All repos inherit credentials automatically
- Easy to manage and rotate keys
- No per-repo configuration needed

### Setup Validation Command

**File: `src/commands/validate-setup.ts`**

New `validate-setup` command checks entire setup:

- ✓ GitHub CLI is installed
- ✓ GitHub CLI is authenticated
- ✓ GitHub App is installed on repository
- ✓ App has required permissions (contents:write, pull-requests:write)
- ✓ Can generate GitHub App tokens
- ✓ Config file is valid

Returns exit code 0 if all checks pass, 1 if any fail. Provides actionable error messages and links to documentation.

**Usage:**

```bash
dep-updater validate-setup
```

**Integration:**

- CLI command via `cli.ts`
- Exported from package for programmatic use
- Referenced in setup guide and workflow generator output

## GitHub Authentication Architecture

### Current Implementation: gh CLI Only

All GitHub operations (creating PRs, listing PRs, checking conflicts, closing PRs) use the **GitHub CLI (`gh`)**
exclusively. No Octokit SDK is used.

**How it works:**

1. GitHub Actions workflow generates a token using `actions/create-github-app-token@v2`
2. Token is passed as `GH_TOKEN` environment variable
3. `gh` CLI automatically uses `GH_TOKEN` for authentication
4. All operations go through `GitHubCLIClient` class

**File: `src/auth/github-client.ts`**

- `IGitHubClient` interface - Defines PR operations
- `GitHubCLIClient` class - Implements all operations via `gh` CLI commands
- Uses dependency injection pattern for testing
- **Enhanced error handling** - Wraps GitHub CLI errors with helpful troubleshooting information

**Error Enhancement:** All methods now catch errors and enhance them with:

- Detection of common error patterns (401, 404, 403)
- Actionable troubleshooting steps specific to the error type
- Links to setup documentation
- Reminder to run `validate-setup` command

Example enhanced error:

```
Failed to create PR: HTTP 401: Unauthorized

Troubleshooting:
  • Check that DEP_UPDATER_APP_ID is set correctly
  • Check that DEP_UPDATER_APP_PRIVATE_KEY contains valid PEM content
  • Verify GitHub App is installed on this repository
  • Ensure App has required permissions (contents:write, pull-requests:write)

📖 Setup guide: https://github.com/smoothbricks/smoothbricks/blob/main/packages/dep-updater/docs/GETTING-STARTED.md
🔍 Run: dep-updater validate-setup
```

**Why gh CLI instead of Octokit?**

✅ **Simpler implementation:**

- No token management or refresh logic needed
- No rate limit tracking required
- GitHub Actions handles token generation

✅ **Leverage existing tools:**

- `gh` CLI already installed in GitHub Actions
- Well-tested, production-ready tool
- Consistent with manual workflows

✅ **Better for CI:**

- Token generation handled by official GitHub action
- Auto-expiring tokens (1 hour)
- No credential storage needed

❌ **Trade-offs:**

- Requires `gh` CLI to be installed
- JSON parsing needed for output
- Less control over API requests

### Future Extensibility

The `IGitHubClient` interface allows for future Octokit implementation if needed:

```typescript
export interface IGitHubClient {
  listUpdatePRs(repoRoot: string): Promise<GitHubPR[]>;
  checkPRConflicts(repoRoot: string, prNumber: number): Promise<boolean>;
  createPR(repoRoot: string, options: {...}): Promise<{number, url}>;
  closePR(repoRoot: string, prNumber: number, comment: string): Promise<void>;
}
```

**When might Octokit be needed?**

- Running outside GitHub Actions (local development, other CI)
- Need for more granular API control
- Rate limit optimization with caching

**Current decision:** Keep it simple - gh CLI meets all current needs.

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

## Pending Architecture Decisions

These features require deeper architectural decisions and are documented for future implementation:

### 1. Smart Nixpkgs Detection

**Current Implementation:** Uses nvfetcher's `_sources/generated.json` output

- Parses JSON file generated by nvfetcher
- Avoids fragile Nix expression parsing
- Fast and reliable
- Only supports GitHub sources (nvfetcher limitation)

**Problem:** Nixpkgs updates daily with commit hash changes, but actual package versions may not change. This could
create unnecessary daily PRs.

**Potential Approaches:**

- **Config-based**: User lists packages to track in config (`trackedPackages: ['nodejs_22', 'bun']`)
  - Pros: Fast, reliable, user has full control
  - Cons: Manual maintenance required
- **Time-based heuristic**: Only report updates if >7 days since last nixpkgs update
  - Pros: Simple, no parsing/queries needed
  - Cons: May miss urgent security updates
- **Commit message parsing**: Parse nixpkgs commit messages for package name mentions
  - Pros: No nix eval queries, reasonably accurate
  - Cons: Requires git history access, may have false positives/negatives

**Current Behavior:** Simple hash comparison - reports all nixpkgs hash changes

**Why Not Implemented:** Initial approach (parsing devenv.nix + querying package versions via `nix eval`) had
fundamental issues:

- Fragile regex parsing of Nix syntax
- 10s timeouts per package query (performance impact)
- False positives from transient query failures
- Only supports GitHub sources

### 2. Expo SDK Detection via @expo/cli

**Current:** Uses `expo-constants` package dependency to detect Expo projects

**Proposed:** Use `@expo/cli` package instead, which may be more reliable

**Decision Needed:** Validate if `@expo/cli` is a better indicator of Expo projects than `expo-constants`

### 3. OpenCode SDK Integration

**Context:** OpenCode SDK provides a programmatic API for fetching package changelogs, release notes, and dependency
information. It could potentially replace or supplement the current changelog fetching logic.

**Current Implementation:**

- Uses `npm view` command to fetch package metadata
- Scrapes GitHub releases API for changelog information
- Manual parsing of changelog formats
- No structured API, relies on convention

**Potential Benefits of OpenCode SDK:**

- Structured API for changelog/release data
- Better reliability than scraping
- May have pre-processed changelog summaries
- Could reduce rate limiting issues with GitHub API

**Trade-offs:**

- Additional dependency
- Requires API key (?)
- May not cover all packages/ecosystems
- Need to validate coverage and data quality

**Decision Needed:**

1. Evaluate OpenCode SDK capabilities and coverage
2. Compare reliability vs current implementation
3. Assess if benefits justify additional dependency
4. Determine if it should replace or supplement current fetcher

## GitHub Actions Integration

### Workflow Generator (`src/commands/generate-workflow.ts`)

**Purpose:** Generate `.github/workflows/update-deps.yml` for automated dependency updates.

**Unified Template with Runtime Auth Detection:**

The workflow generator creates a **single unified template** that auto-detects authentication at runtime:

- **Template file:** `templates/workflows/unified.yml`
- **Auth detection:** If `vars.DEP_UPDATER_APP_ID` is set → GitHub App mode; otherwise → PAT mode
- **No regeneration needed:** Users can switch auth methods by just adding/removing secrets/variables

**Runtime Auth Detection Expression:**

```yaml
# GitHub App token generation (skipped if not configured)
- name: Generate GitHub App token
  if: ${{ vars.DEP_UPDATER_APP_ID != '' }}
  uses: actions/create-github-app-token@v2

# Token fallback in GH_TOKEN
GH_TOKEN: ${{ steps.app-token.outputs.token || secrets.DEP_UPDATER_TOKEN }}
```

**Template Processing System:**

The unified template uses minimal placeholder substitution (AI-related only):

- **Template file:** `templates/workflows/unified.yml`

- **Placeholders (AI-related only):**
  - `{{AI_HEADER_SUFFIX}}` - Header comment suffix
  - `{{AI_SETUP_NOTE}}` - Additional setup notes for paid providers
  - `{{AI_STEP_SUFFIX}}` - Step name suffix
  - `{{AI_ENV_VAR}}` - API key environment variable

- **Removed placeholders:** All `{{STEP_*}}` placeholders were removed (no longer needed with unified template)

**Key Features:**

- Uses Nx target for CLI execution (automatic builds + caching)
- Configures git user for commits (github-actions[bot])
- Supports Nx computation caching for faster CI runs
- **Runtime auth detection** - No need to regenerate workflow when switching auth methods
- **Conditional AI** - `vars.DEP_UPDATER_SKIP_AI` variable can disable AI at runtime
- **Enhanced workflow comments** - Generated workflows include:
  - Setup options for both PAT and GitHub App
  - Link to GETTING-STARTED.md documentation
  - Inline comments explaining each step

**CLI Usage:**

```bash
# Generate workflow (auth is auto-detected at runtime)
dep-updater generate-workflow

# Disable AI changelog analysis
dep-updater generate-workflow --skip-ai

# Enable AI explicitly (default with opencode free tier)
dep-updater generate-workflow --enable-ai

# Custom schedule
dep-updater generate-workflow --schedule "0 3 * * 1"
```

**AI Configuration:**

```typescript
// AI is enabled by default with free tier (opencode)
// Priority: explicit flag > skipAI flag > auto-detection (free tier enabled by default)
const isFreeProvider = !providerRequiresSecret(config.ai.provider);
const useAI = options.enableAI === true || (!options.skipAI && (isFreeProvider || config.ai?.apiKey !== undefined));
```

**Switching Auth Methods (No Regeneration Needed):**

```bash
# To use PAT: Add secret
gh secret set DEP_UPDATER_TOKEN --org YOUR_ORG

# To use GitHub App (takes priority if both configured):
# Add variable and secret
gh variable set DEP_UPDATER_APP_ID --org YOUR_ORG
gh secret set DEP_UPDATER_APP_PRIVATE_KEY --org YOUR_ORG

# To disable AI at runtime: Add variable
gh variable set DEP_UPDATER_SKIP_AI --body "true" --org YOUR_ORG
```

### Authentication Method Comparison

**Personal Access Token (PAT):**

- ✅ Simple 5-minute setup
- ✅ One token works for all repos in org
- ✅ No app creation needed
- ⚠️ 5,000 requests/hour rate limit
- ⚠️ PRs don't trigger CI workflows automatically
- ⚠️ 90-day token expiration (needs renewal)

**GitHub App:**

- ✅ Higher rate limits (15,000 requests/hour)
- ✅ PRs trigger CI workflows properly
- ✅ Auto-expiring tokens (1-hour, auto-renewed)
- ✅ Organization-scoped credentials
- ⚠️ 15-20 minute setup (one-time per org)
- ⚠️ Requires app creation and installation

**Workflow Steps (PAT):**

1. Checkout repo with full history (`fetch-depth: 0`)
2. Setup Bun
3. Install dependencies
4. Configure git identity
5. Run CLI with `GH_TOKEN: ${{ secrets.DEP_UPDATER_TOKEN }}`

**Workflow Steps (GitHub App):**

1. Checkout repo with full history (`fetch-depth: 0`)
2. Generate GitHub App token using `actions/create-github-app-token@v2`
   - Reads `DEP_UPDATER_APP_ID` from repository variables
   - Reads `DEP_UPDATER_APP_PRIVATE_KEY` from repository secrets
   - Token auto-expires after 1 hour
3. Setup Bun
4. Install dependencies
5. Configure git identity
6. Run CLI with `GH_TOKEN: ${{ steps.app-token.outputs.token }}`

**Setup Requirements:**

**For PAT:**

1. Generate PAT: https://github.com/settings/tokens/new (scope: `repo`)
2. Add organization secret: `DEP_UPDATER_TOKEN`
3. Optional: Add AI provider API key (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or `GOOGLE_API_KEY`)

**For GitHub App:**

1. Create GitHub App with repository permissions:
   - Contents: Read and write
   - Pull requests: Read and write
   - Workflows: Read and write
2. Install app to repository
3. Add organization variables and secrets:
   - Variable: `DEP_UPDATER_APP_ID` (App ID from GitHub App settings)
   - Secret: `DEP_UPDATER_APP_PRIVATE_KEY` (Private key PEM file content)
   - Secret (optional): AI provider API key for changelog analysis (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or
     `GOOGLE_API_KEY`)

### Init Command (`src/commands/init.ts`)

**Purpose:** Interactive setup wizard for new projects.

**Architecture:** Refactored into modular structure (271 → 195 lines)

- Core logic in `init.ts`
- User interaction utilities extracted to `src/utils/prompts.ts` (37 lines)
- Project detection logic extracted to `src/utils/project-detection.ts` (52 lines)

**Features:**

- **Authentication type selection** - First prompt asks user to choose:
  - PAT (Personal Access Token) - Simple 5-minute setup
  - GitHub App - Advanced 15-minute setup with higher rate limits
- Shows contextual setup notes based on selected auth type
- Conditional credential checking (only for GitHub App)
- Auto-detects project setup:
  - Package manager (bun/npm/pnpm/yarn) - via lock file detection
  - Expo project - via package.json dependencies
  - Nix setup (devenv, flake.nix, .envrc)
  - Syncpack config (.syncpackrc.json, .syncpackrc.yml)
- Interactive prompts for configuration options
- Generates `.dep-updater.json` or `.dep-updater.ts` config file
- Calls `generate-workflow` to create unified workflow (auth auto-detected at runtime)
- Provides unified next steps with both auth options explained

**Prompt Flow:**

1. **Auth info** - Shows unified auth setup note (both PAT and GitHub App options)
2. **Config format** - JSON or TypeScript
3. **Feature flags** - Expo, Nix, AI, PR stacking
4. **Workflow generation** - Confirm workflow creation

**Next Steps Output:**

The command shows unified next steps with both auth options:

1. Set up authentication (choose one):
   - Option A: PAT (5 min) - Add DEP_UPDATER_TOKEN secret
   - Option B: GitHub App (15 min) - Add DEP_UPDATER_APP_ID variable + private key secret
2. Review config file
3. (Optional) Add AI provider API key
4. Commit and push
5. Test workflow
6. Link to GETTING-STARTED.md

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
  ai: { provider: SupportedProvider; apiKey?: string; model?: string }; // SupportedProvider = 'opencode' | 'anthropic' | 'openai' | 'google'
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

### ✅ Tested (415 tests total)

- **GitHub Client** - GitHubCLIClient class (20 tests)
  - listUpdatePRs: JSON parsing, empty results, command verification, error handling (5 tests)
  - checkPRConflicts: All mergeable statuses, JSON validation, error handling (6 tests)
  - createPR: URL parsing, PR number extraction, invalid format handling (4 tests)
  - closePR: Comment inclusion, argument handling (3 tests)
  - Constructor: Custom and default executors (2 tests)
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
  - Nixpkgs overlay: Parses nvfetcher JSON (\_sources/generated.json), version extraction, malformed file handling (9
    tests)
- **Utils** - Path validation, workspace detection, project detection (18 tests)
  - Project detection: Package manager detection, Expo detection, Nix detection (flake.nix, .envrc, devenv.yaml),
    Syncpack detection (18 tests)
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
