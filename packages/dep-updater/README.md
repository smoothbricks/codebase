# dep-updater

Automated dependency update tool with Expo SDK support, stacked PRs, and AI-powered changelog analysis.

## Features

- 🎯 **Expo SDK Aware**: Automatically updates Expo SDK and regenerates syncpack config with compatible versions
- 📚 **Stacked PRs**: Creates incremental PRs for better tracking and easier review
- 🤖 **AI-Powered**: Uses Claude to analyze changelogs and highlight breaking changes
- 🔄 **Multiple Ecosystems**: Supports npm (via Bun), optional Nix/devenv, and nixpkgs
- 🚀 **GitHub Actions Ready**: Interactive setup wizard generates workflow for automated daily updates
- 🧪 **Dry Run Mode**: Test locally without making changes
- 📦 **Syncpack Integration**: Respects version constraints and regenerates config from Expo
- ⚙️ **Highly Configurable**: Works with any project structure through flexible configuration

## Prerequisites

Before using dep-updater, ensure you have the following installed:

**Required:**

- [Bun](https://bun.sh) - JavaScript runtime and package manager
- [Git](https://git-scm.com) - Version control system
- [GitHub CLI](https://cli.github.com) (`gh`) - For creating PRs
  - **Important**: Must be authenticated with `gh auth login`
  - Run `gh auth status` to verify authentication

**Optional (based on configuration):**

- [devenv](https://devenv.sh) - If using Nix updates (`nix.enabled: true`)
- [nvfetcher](https://github.com/berberman/nvfetcher) - For nixpkgs overlay updates
- [Nix](https://nixos.org) - Fallback for nixpkgs updates
- **Anthropic API Key** - For AI-powered changelog analysis
  - Set `ANTHROPIC_API_KEY` environment variable
  - Get key from [Anthropic Console](https://console.anthropic.com)

**Verification:**

```bash
bun --version
git --version
gh --version
gh auth status  # Verify GitHub authentication
```

## Installation

```bash
bun add dep-updater
```

## Quick Start

### Initialize dep-updater in your project

The easiest way to get started is with the interactive setup wizard:

```bash
dep-updater init
```

This will:

- Detect your project setup (package manager, Expo, Nix, Syncpack)
- Prompt for configuration options (including JSON vs TypeScript format)
- Generate `tooling/dep-updater.json` or `tooling/dep-updater.ts` config file
- Create GitHub Actions workflow for automated updates

**Options:**

- `--yes`: Skip prompts and use defaults

---

## CLI Usage

### Initialize project setup (recommended)

```bash
dep-updater init
```

Interactive wizard that sets up configuration and GitHub Actions workflow.

### Generate GitHub Actions workflow

```bash
dep-updater generate-workflow
```

Generates `.github/workflows/update-deps.yml` for automated daily dependency updates.

**Options:**

- `--schedule <cron>`: Custom cron schedule (default: `0 2 * * *` - 2 AM UTC daily)
- `--workflow-name <name>`: Custom workflow name (default: `Update Dependencies`)

### Check for Expo SDK updates

```bash
dep-updater check-expo
```

### Update Expo SDK

Updates Expo SDK, regenerates syncpack config, and updates all dependencies:

```bash
dep-updater update-expo
```

### Update all dependencies

Updates npm dependencies (via Bun) and optionally Nix ecosystems (devenv, nixpkgs) while respecting syncpack
constraints:

```bash
dep-updater update-deps
```

Note: Nix updates are disabled by default. Enable via configuration (see below).

### Generate syncpack config

Generate `.syncpackrc.json` from Expo recommended versions:

```bash
dep-updater generate-syncpack --expo-sdk 52
```

### Global Options

- `--dry-run`: Preview changes without making them
- `--skip-git`: Don't create branches or commits
- `--skip-ai`: Skip AI changelog analysis
- `--verbose`: Show detailed output

## Configuration

The tool uses sensible defaults and can be configured in three ways:

### Option 1: Declarative TypeScript Config (Recommended)

Create `tooling/dep-updater.ts` for type-safe declarative configuration:

```typescript
import { defineConfig } from 'dep-updater';

export default defineConfig({
  // Expo SDK management (optional)
  expo: {
    enabled: true,
    packageJsonPath: './package.json',
  },

  // Syncpack integration
  syncpack: {
    configPath: './.syncpackrc.json',
    preserveCustomRules: true,
    fixScriptName: 'syncpack:fix', // Customize your syncpack script name
  },

  // Nix ecosystem support (optional, disabled by default)
  nix: {
    enabled: false, // Set to true to enable Nix updates
    devenvPath: './tooling/direnv',
    nixpkgsOverlayPath: './tooling/direnv/nixpkgs-overlay',
  },

  // Stacked PR strategy
  prStrategy: {
    stackingEnabled: true,
    maxStackDepth: 5,
    autoCloseOldPRs: true,
    resetOnMerge: true,
    stopOnConflicts: true,
    branchPrefix: 'chore/update-deps',
    prTitlePrefix: 'chore: update dependencies',
  },

  // Auto-merge configuration
  autoMerge: {
    enabled: false,
    mode: 'none', // 'none' | 'patch' | 'minor'
    requireTests: true,
  },

  // AI-powered changelog analysis
  ai: {
    provider: 'anthropic',
    model: 'claude-sonnet-4-5-20250929',
  },

  // Git configuration
  git: {
    remote: 'origin',
    baseBranch: 'main',
  },
});
```

### Option 2: JSON Config (Simple)

Create `tooling/dep-updater.json` for simple, declarative configuration:

```json
{
  "expo": {
    "enabled": true,
    "packageJsonPath": "./package.json"
  },
  "nix": {
    "enabled": false,
    "devenvPath": "./tooling/direnv",
    "nixpkgsOverlayPath": "./tooling/direnv/nixpkgs-overlay"
  },
  "prStrategy": {
    "stackingEnabled": true,
    "maxStackDepth": 5,
    "autoCloseOldPRs": true,
    "resetOnMerge": true,
    "stopOnConflicts": true,
    "branchPrefix": "chore/update-deps",
    "prTitlePrefix": "chore: update dependencies"
  },
  "ai": {
    "provider": "anthropic",
    "model": "claude-sonnet-4-5-20250929"
  }
}
```

### Option 3: Script Mode (Advanced)

For complete control over the update process, export a function instead of a configuration object:

```typescript
// tooling/dep-updater.ts
import { updateBunDependencies, loadConfig } from 'dep-updater';

export default async function () {
  console.log('🚀 Running custom update script...\n');

  // Load config for settings
  const config = await loadConfig();

  // Run bun updater to detect available updates
  const result = await updateBunDependencies(config.repoRoot || process.cwd());

  if (result.updates.length === 0) {
    console.log('✓ No updates available');
    return;
  }

  console.log(`Found ${result.updates.length} updates:\n`);

  // Custom logic: Filter updates
  const filtered = result.updates.filter((update) => {
    // Skip React 19.x
    if (update.name === 'react' && update.toVersion.startsWith('19')) {
      console.log(`⏭️  Skipping ${update.name} ${update.toVersion} (React 19 not ready)`);
      return false;
    }

    // Skip major version bumps for specific packages
    if (update.updateType === 'major' && ['typescript', 'eslint'].includes(update.name)) {
      console.log(`⏭️  Skipping major update for ${update.name} (needs manual review)`);
      return false;
    }

    return true;
  });

  console.log(`\n${filtered.length} updates after filtering:\n`);
  for (const update of filtered) {
    console.log(`  • ${update.name}: ${update.fromVersion} → ${update.toVersion} (${update.updateType})`);
  }

  // Apply updates, create commits, generate PRs, etc.
  // You have full control over the workflow!
}
```

**When to use script mode:**

- Need custom filtering logic for specific packages
- Want to implement custom update strategies
- Need to integrate with other tools or APIs
- Require workflow customization beyond config options

See `examples/script-mode.ts` for a complete example.

**Config File Priority:**

1. `tooling/dep-updater.ts` - If exports a function, runs in script mode. If exports an object, uses as declarative
   config.
2. `tooling/dep-updater.json` - Always used as declarative config

The tool searches for config files in the current directory and parent directories up to 10 levels.

### Configuration Options

#### Expo (`expo`)

- `enabled`: Enable Expo SDK updates
- `packageJsonPath`: Path to package.json containing Expo dependency

#### Syncpack (`syncpack`)

- `configPath`: Path to .syncpackrc.json
- `preserveCustomRules`: Preserve custom rules when regenerating from Expo
- `fixScriptName`: Script name to run syncpack fix (default: `'syncpack:fix'`)

#### Nix (`nix`) - Optional

- `enabled`: Enable Nix ecosystem updates (default: `false`)
- `devenvPath`: Path to devenv directory
- `nixpkgsOverlayPath`: Path to nixpkgs overlay directory

#### PR Strategy (`prStrategy`)

- `stackingEnabled`: Enable PR stacking
- `maxStackDepth`: Maximum number of stacked PRs
- `autoCloseOldPRs`: Auto-close PRs older than maxStackDepth
- `resetOnMerge`: Reset stack after any PR is merged
- `stopOnConflicts`: Don't create new PR if base has conflicts
- `branchPrefix`: Branch name prefix
- `prTitlePrefix`: PR title prefix

#### Auto-merge (`autoMerge`)

> ⚠️ **Note**: Auto-merge feature is planned but not yet implemented. Configuration is reserved for future use.

- `enabled`: Enable auto-merge functionality
- `mode`: Auto-merge mode (`'none'` | `'patch'` | `'minor'`)
- `requireTests`: Require tests to pass before auto-merge

#### AI (`ai`)

- `provider`: AI provider (currently only `'anthropic'`)
- `model`: Model to use for changelog analysis

#### Git (`git`)

- `remote`: Remote name (default: `'origin'`)
- `baseBranch`: Base branch (default: `'main'`)

## Environment Variables

- `ANTHROPIC_API_KEY`: Required for AI-powered changelog analysis
- `GH_TOKEN` or `GH_PAT`: GitHub token for creating PRs (required in CI)

## GitHub Actions Setup

To run dep-updater automatically on a schedule, use the workflow generator:

```bash
dep-updater generate-workflow
```

This creates `.github/workflows/update-deps.yml` that runs daily at 2 AM UTC.

### Authentication Setup

GitHub Actions requires a fine-grained Personal Access Token (PAT) to create pull requests:

1. **Create fine-grained PAT:**
   - Go to https://github.com/settings/tokens?type=beta
   - Click "Generate new token"
   - Set token name (e.g., "dep-updater")
   - Set expiration (max 1 year)
   - Select repository access (this repo or specific repos)
   - Set permissions:
     - **Contents:** Read and write
     - **Pull requests:** Read and write
     - **Workflows:** Read and write
   - Generate and copy token

2. **Add as repository secret:**
   - Go to repository **Settings → Secrets and variables → Actions**
   - Click "New repository secret"
   - Name: `GH_PAT`
   - Value: paste your token
   - Click "Add secret"

3. **(Optional) Add ANTHROPIC_API_KEY for AI analysis:**
   - Same steps as above
   - Name: `ANTHROPIC_API_KEY`
   - Value: your Anthropic API key

### Why fine-grained PAT?

- ✅ Triggers CI workflows automatically (GITHUB_TOKEN doesn't)
- ✅ Repository-specific permissions (better security)
- ✅ Granular access control
- ℹ️ Must be regenerated annually

> **Note:** GitHub App authentication support is planned for production use (not user-dependent).

## Programmatic Usage

You can also use the package programmatically in your own scripts:

```typescript
import { updateDeps, loadConfig, mergeConfig } from 'dep-updater';

// Load default config
const baseConfig = await loadConfig();

// Customize for your project
const config = mergeConfig({
  nix: {
    enabled: true, // Enable Nix updates
    devenvPath: './nix',
    nixpkgsOverlayPath: './nix/overlay',
  },
  syncpack: {
    fixScriptName: 'fix:versions', // Custom script name
  },
});

// Run updates
await updateDeps(config, {
  dryRun: false,
  skipGit: false,
  skipAI: false,
});
```

## Development

```bash
# Install dependencies
bun install

# Build
bun run build

# Type check (includes src/ and test/)
bun run typecheck

# Test
bun test

# Run locally
bun run src/cli.ts check-expo --dry-run
```

## Testing

The project has comprehensive test coverage (318 tests):

- **Git operations** (86 tests) - All git commands with dependency injection
- **PR Stacking** (47 tests) - PR creation, stacking, conflict handling, auto-close
- **Integration tests** (46 tests) - End-to-end PR workflows and config loading
- **Config** (32 tests) - File loading, merging, validation, sanitization
- **Changelog** (25 tests) - Fetching and AI analysis
- **Updaters** (19 tests) - Bun, devenv (Nix), nixpkgs overlay
- **Utils** (14 tests) - Path validation, workspace detection, project detection
- **Expo SDK** - Version checking and package detection
- **Syncpack** - Config generation

Run tests with `bun test` or `bun test --watch` for development.

## License

MIT
