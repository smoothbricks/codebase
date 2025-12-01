# dep-updater

Automated dependency update tool with Expo SDK support, stacked PRs, and AI-powered changelog analysis.

## Features

- 🎯 **Expo SDK Aware**: Automatically updates Expo SDK and regenerates syncpack config with compatible versions
- 📚 **Stacked PRs**: Creates incremental PRs for better tracking and easier review
- 🤖 **AI-Powered**: Free AI changelog analysis out of the box (no API key required), with optional upgrade to premium
  providers
- 🔄 **Multiple Ecosystems**: Supports npm (via Bun), optional Nix/devenv, and nixpkgs
- 🚀 **GitHub Actions Ready**: Interactive setup wizard generates workflow for automated daily updates
- 🧪 **Dry Run Mode**: Test locally without making changes
- 📦 **Syncpack Integration**: Respects version constraints and regenerates config from Expo
- ⚙️ **Highly Configurable**: Works with any project structure through flexible configuration

## Getting Started

**👉 New to dep-updater?** Start with the getting started guide:

- **[Getting Started Guide](docs/GETTING-STARTED.md)** - Complete setup walkthrough
  - **PAT authentication** - Simple 5-minute setup for small teams
  - **GitHub App authentication** - Advanced 15-minute setup for organizations
  - Choose the method that fits your needs

**Interactive setup (recommended):**

```bash
# Interactive wizard guides you through setup
npx @smoothbricks/dep-updater init

# Choose PAT (simple) or GitHub App (advanced)
# Generates config and workflow automatically
```

**Manual workflow generation:**

```bash
# Generate workflow (auth is auto-detected at runtime)
npx @smoothbricks/dep-updater generate-workflow

# Validate setup
npx @smoothbricks/dep-updater validate-setup
```

## Documentation

📚 **Complete Guides:**

- **[Getting Started Guide](./docs/GETTING-STARTED.md)** - Setup walkthrough (PAT or GitHub App)
- **[Configuration Reference](./docs/CONFIGURATION.md)** - All configuration options and examples
- **[API Reference](./docs/API.md)** - Programmatic usage and TypeScript types
- **[Troubleshooting Guide](./docs/TROUBLESHOOTING.md)** - Common issues and solutions

## Prerequisites

> **⚠️ Important:** This tool is designed for **Nx monorepos**. The generated GitHub Actions workflow uses Nx targets
> for task orchestration and caching benefits.

Before using dep-updater, ensure you have the following installed:

**Required:**

- **[Nx](https://nx.dev)** - Monorepo build system (must be set up in your workspace)
- [Bun](https://bun.sh) - JavaScript runtime and package manager
- [Git](https://git-scm.com) - Version control system
- [GitHub CLI](https://cli.github.com) (`gh`) - For creating PRs
  - **Important**: Must be authenticated with `gh auth login`
  - Run `gh auth status` to verify authentication

**Optional (based on configuration):**

- [devenv](https://devenv.sh) - If using Nix updates (`nix.enabled: true`)
- [nvfetcher](https://github.com/berberman/nvfetcher) - For nixpkgs overlay updates
- [Nix](https://nixos.org) - Fallback for nixpkgs updates
- **AI Provider API Key** - Optional, for premium AI analysis (free tier works without any key)
  - Default: OpenCode (free, no API key required)
  - Premium: Set `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or `GOOGLE_API_KEY` for higher quality analysis

> **Note:** The generated GitHub Actions workflow automatically installs Nix, devenv, and nvfetcher when `devenv.yaml`
> is detected in your repository. No manual Nix setup required for CI.

**Verification:**

```bash
bun --version
git --version
gh --version
gh auth status  # Verify GitHub authentication
```

## Installation

```bash
bun add @smoothbricks/dep-updater
```

> **Note:** Once installed globally or in your project, you can use the `dep-updater` command directly.

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

The generated workflow uses **runtime auth detection** - it automatically uses GitHub App if `DEP_UPDATER_APP_ID` is
configured, otherwise falls back to PAT. No need to specify auth type.

**Options:**

- `--schedule <cron>`: Custom cron schedule (default: `0 2 * * *` - 2 AM UTC daily)
- `--workflow-name <name>`: Custom workflow name (default: `Update Dependencies`)
- `--enable-ai`: Explicitly enable AI changelog analysis
- `--skip-ai`: Disable AI changelog analysis

**Examples:**

```bash
# Generate workflow (auth auto-detected at runtime)
dep-updater generate-workflow

# Disable AI changelog analysis
dep-updater generate-workflow --skip-ai

# Custom schedule
dep-updater generate-workflow --schedule "0 3 * * 1" --workflow-name "Weekly Updates"
```

**Upgrading to Premium AI:**

The default free tier (OpenCode) works out of the box. To upgrade to premium AI:

```bash
# 1. Add your AI provider API key to organization secrets
# Supported: ANTHROPIC_API_KEY, OPENAI_API_KEY, or GOOGLE_API_KEY
gh secret set ANTHROPIC_API_KEY --org YOUR_ORG

# 2. That's it! The workflow automatically uses premium AI when API key is available
```

To disable AI entirely, set repository variable `DEP_UPDATER_SKIP_AI=true` or regenerate with `--skip-ai`.

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

The tool uses sensible defaults and can be configured using TypeScript or JSON config files in the `tooling/` directory.

### Quick Configuration

Create `tooling/dep-updater.ts` or `tooling/dep-updater.json`:

```typescript
import { defineConfig } from '@smoothbricks/dep-updater';

export default defineConfig({
  expo: { enabled: true, autoDetect: true },
  prStrategy: { stackingEnabled: true, maxStackDepth: 5 },
  // AI works out of the box with free tier, or upgrade to premium:
  // ai: { provider: 'anthropic' },
});
```

### Interactive Setup

The easiest way to configure is using the interactive wizard:

```bash
dep-updater init
```

This will auto-detect your project setup and generate the appropriate config file.

### Configuration Options

Key configuration sections:

- **Expo**: SDK management and multi-project support
- **Syncpack**: Version constraint management and custom rules
- **Nix**: devenv and nixpkgs overlay updates (optional)
- **PR Strategy**: Stacking, auto-close, conflict handling
- **AI**: Changelog analysis with Claude
- **Git**: Remote and base branch settings

📖 **See [Configuration Reference](./docs/CONFIGURATION.md) for complete documentation and examples.**

## Environment Variables

### Required (in GitHub Actions)

**For PAT authentication:**

- `GH_TOKEN`: Set to `${{ secrets.DEP_UPDATER_TOKEN }}` (your Personal Access Token)

**For GitHub App authentication:**

- `DEP_UPDATER_APP_ID`: GitHub App ID (from app settings)
- `DEP_UPDATER_APP_PRIVATE_KEY`: GitHub App private key (PEM format)
- `GH_TOKEN`: Auto-generated by `actions/create-github-app-token@v2` action

### Optional (Premium AI Analysis)

AI changelog analysis works out of the box with no API key (using OpenCode free tier). For higher quality analysis, add
one of:

- `ANTHROPIC_API_KEY`: For Anthropic/Claude (excellent quality)
- `OPENAI_API_KEY`: For OpenAI/GPT-4 (excellent quality)
- `GOOGLE_API_KEY`: For Google/Gemini (very good quality)

## GitHub Actions Setup

dep-updater supports two authentication methods for GitHub Actions:

### Option 1: Personal Access Token (PAT) - Simple & Fast

**Best for:** Small teams, quick setup, getting started

**Setup time:** ~5 minutes

**Quick steps:**

1. Generate PAT: https://github.com/settings/tokens/new (scope: `repo`)
2. Add to org secrets: `gh secret set DEP_UPDATER_TOKEN --org YOUR_ORG`
3. Generate workflow: `dep-updater generate-workflow`
4. Commit and push

**📖 Full guide:** [Getting Started Guide → PAT Setup](docs/GETTING-STARTED.md#option-a-pat-setup-5-minutes)

**Limitations:**

- 5,000 requests/hour rate limit (vs 15,000 for GitHub App)
- PRs don't trigger CI workflows automatically (requires manual trigger)
- Token needs renewal every 90 days

### Option 2: GitHub App - Production Ready

**Best for:** Organizations with many repos, higher volume, production use

**Setup time:** ~15-20 minutes (one-time per organization)

**Quick steps:**

1. Create GitHub App for your organization
2. Install app to your repositories
3. Configure organization variable (`DEP_UPDATER_APP_ID`) and secret (`DEP_UPDATER_APP_PRIVATE_KEY`)
4. Generate workflow: `dep-updater generate-workflow` (auth auto-detected)
5. Validate: `dep-updater validate-setup`
6. Commit and push

**📖 Full guide:**
[Getting Started Guide → GitHub App Setup](docs/GETTING-STARTED.md#option-b-github-app-setup-15-20-minutes)

**Benefits:**

- ✅ **Better rate limits:** 15,000 requests/hour vs 5,000 for PATs
- ✅ **Triggers CI workflows:** PRs properly trigger `pull_request` workflows
- ✅ **Auto-expiring tokens:** 1-hour expiration for better security
- ✅ **Organization-scoped:** Setup once, all repos inherit
- ✅ **No token renewal:** Tokens auto-refresh, no 90-day expiration

### Comparison Table

| Feature            | PAT                        | GitHub App             |
| ------------------ | -------------------------- | ---------------------- |
| **Setup time**     | 5 minutes                  | 15-20 minutes          |
| **Rate limit**     | 5,000 req/hour             | 15,000 req/hour        |
| **Triggers CI**    | No (manual trigger needed) | Yes                    |
| **Token lifetime** | 90 days (renewable)        | 1 hour (auto-renewed)  |
| **Best for**       | Small teams, quick start   | Large orgs, production |

### How it works

- Workflow uses `nx run @smoothbricks/dep-updater:update-deps` to execute the tool
- Nx automatically builds the package if needed (with `dependsOn: ["build"]`)
- Build results are cached by Nx for faster subsequent runs
- Leverages Nx's task orchestration and computation caching

## Programmatic Usage

You can use the package programmatically in your own scripts and tools:

```typescript
import { updateDeps, loadConfig, mergeConfig } from '@smoothbricks/dep-updater';

const config = await loadConfig();
await updateDeps(config, { dryRun: false });
```

📖 **See [API Reference](./docs/API.md) for complete API documentation, TypeScript types, and advanced examples.**

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

The project has comprehensive test coverage (415 tests):

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
