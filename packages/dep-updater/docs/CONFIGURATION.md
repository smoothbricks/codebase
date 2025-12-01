# Configuration Reference

This document provides a comprehensive reference for all `dep-updater` configuration options.

## Overview

The tool uses sensible defaults and can be configured in two ways: TypeScript config (recommended) or JSON config.
Configuration files are automatically discovered in the `tooling/` directory.

## Config File Discovery

The tool automatically searches for config files in your repository:

1. **`tooling/dep-updater.ts`** - TypeScript config with type safety (priority 1)
2. **`tooling/dep-updater.json`** - JSON config for simple setups (priority 2)

The search starts in the current directory and traverses up to 10 parent directories.

### TypeScript vs JSON

**TypeScript Config Benefits:**

- Type checking with `defineConfig()` helper
- Custom logic and computed values
- JSDoc comments in config file
- IDE autocomplete support

**JSON Config Benefits:**

- Simple, declarative
- No build step needed
- Easy to generate programmatically

## Configuration Formats

### TypeScript Config (Recommended)

Create `tooling/dep-updater.ts`:

```typescript
import { defineConfig } from 'dep-updater';

export default defineConfig({
  // Expo SDK management (optional)
  expo: {
    enabled: true,
    autoDetect: true, // Auto-detect all Expo projects in monorepo
    // OR specify projects explicitly:
    // projects: [
    //   { name: 'mobile', packageJsonPath: './apps/mobile/package.json' },
    //   { name: 'tablet', packageJsonPath: './apps/tablet/package.json' },
    // ],
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

  // AI-powered changelog analysis (free by default, no API key needed)
  ai: {
    provider: 'opencode', // Free tier, or: 'anthropic', 'openai', 'google'
  },

  // Git configuration
  git: {
    remote: 'origin',
    baseBranch: 'main',
  },
});
```

### JSON Config

Create `tooling/dep-updater.json`:

```json
{
  "expo": {
    "enabled": true,
    "autoDetect": true,
    "projects": []
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
    "provider": "opencode"
  }
}
```

## Configuration Options

### Expo (`expo`)

Controls Expo SDK version management and updates.

**Options:**

- `enabled` (boolean) - Enable Expo SDK updates
- `autoDetect` (boolean) - Auto-detect Expo projects by scanning workspace packages (default: `true`)
- `projects` (array) - Explicit list of Expo projects (array of `{ name?: string, packageJsonPath: string }`)

**Multi-Project Example:**

```typescript
expo: {
  enabled: true,
  projects: [
    { name: 'customer-app', packageJsonPath: './apps/customer/package.json' },
    { name: 'driver-app', packageJsonPath: './apps/driver/package.json' },
  ],
}
```

**Auto-Detection Example:**

```typescript
expo: {
  enabled: true,
  autoDetect: true, // Scans workspace for packages with "expo" dependency
}
```

### Syncpack (`syncpack`)

Controls syncpack integration for version constraint management.

**Options:**

- `configPath` (string) - Path to .syncpackrc.json
- `preserveCustomRules` (boolean) - Preserve custom rules when regenerating from Expo (default: `true`)
- `fixScriptName` (string) - Script name to run syncpack fix (default: `'syncpack:fix'`)

#### Adding Custom Syncpack Rules

The tool generates Expo-related syncpack rules automatically, but you can add your own custom rules by editing
`.syncpackrc.json` directly:

1. **Generate initial config**: Run `dep-updater generate-syncpack --expo-sdk 52` to create `.syncpackrc.json` with Expo
   rules

2. **Add your custom rules**: Edit `.syncpackrc.json` and add your own version groups:

   ```json
   {
     "versionGroups": [
       {
         "label": "Pin lodash to 4.17.21",
         "dependencies": ["lodash"],
         "pinVersion": "4.17.21"
       },
       {
         "label": "Keep TypeScript on 5.x",
         "dependencies": ["typescript"],
         "packages": ["**"],
         "policy": "sameRange",
         "dependencyTypes": ["prod", "dev"]
       }
     ]
   }
   ```

3. **Enable preservation**: Set `preserveCustomRules: true` in your dep-updater config

4. **Regenerate safely**: When the tool regenerates the config, it will filter out rules that match ANY of these
   conditions:
   - Label contains "Expo SDK"
   - Label contains "workspace protocol"
   - Dependencies include `react`, `react-native`, or `expo`

   All other custom rules (like the lodash rule above) are preserved and merged with new generated Expo rules.

This keeps a clean separation: dep-updater manages Expo SDK rules, you manage project-specific rules in the syncpack
config where they belong.

### Nix (`nix`)

Controls Nix ecosystem updates (devenv and nixpkgs overlay).

**Options:**

- `enabled` (boolean) - Enable Nix ecosystem updates (default: `false`)
- `devenvPath` (string) - Path to devenv directory
- `nixpkgsOverlayPath` (string) - Path to nixpkgs overlay directory

**Note:** Nix updates are disabled by default. Set `enabled: true` to enable.

### PR Strategy (`prStrategy`)

Controls pull request creation and stacking behavior.

**Options:**

- `stackingEnabled` (boolean) - Enable PR stacking
- `maxStackDepth` (number) - Maximum number of stacked PRs
- `autoCloseOldPRs` (boolean) - Auto-close PRs older than maxStackDepth
- `resetOnMerge` (boolean) - Reset stack after any PR is merged
- `stopOnConflicts` (boolean) - Don't create new PR if base has conflicts
- `branchPrefix` (string) - Branch name prefix
- `prTitlePrefix` (string) - PR title prefix

**Example:**

```typescript
prStrategy: {
  stackingEnabled: true,
  maxStackDepth: 3,
  autoCloseOldPRs: true,
  resetOnMerge: true,
  stopOnConflicts: true,
  branchPrefix: 'chore/update-deps',
  prTitlePrefix: 'chore: update dependencies',
}
```

### Auto-merge (`autoMerge`)

Controls automatic merging of dependency update PRs.

> ⚠️ **Note**: Auto-merge feature is planned but not yet implemented. Configuration is reserved for future use.

**Options:**

- `enabled` (boolean) - Enable auto-merge functionality
- `mode` (string) - Auto-merge mode (`'none'` | `'patch'` | `'minor'`)
- `requireTests` (boolean) - Require tests to pass before auto-merge

### AI (`ai`)

Controls AI-powered changelog analysis. Supports multiple AI providers via the OpenCode SDK.

**Key Feature:** AI analysis works out of the box with no API key required using the free OpenCode tier.

**Options:**

- `provider` (string) - AI provider (default: `'opencode'`)
- `model` (string) - Model to use (provider-specific defaults apply)
- `tokenBudget` (number) - Token budget for changelog prompts (optional, provider-specific defaults apply)

**Provider Comparison:**

| Provider    | API Key Required | Quality   | Cost | Default Model              |
| ----------- | ---------------- | --------- | ---- | -------------------------- |
| `opencode`  | ❌ No            | Good      | Free | big-pickle                 |
| `anthropic` | ✅ Yes           | Excellent | $$   | claude-sonnet-4-5-20250929 |
| `openai`    | ✅ Yes           | Excellent | $$   | gpt-4o                     |
| `google`    | ✅ Yes           | Very Good | $    | gemini-1.5-pro             |

**Environment Variables (for premium providers):**

| Provider  | Environment Variable |
| --------- | -------------------- |
| anthropic | `ANTHROPIC_API_KEY`  |
| openai    | `OPENAI_API_KEY`     |
| google    | `GOOGLE_API_KEY`     |

**Default Token Budgets:**

When changelogs exceed the token budget, they're automatically summarized to fit. Provider-specific defaults are:

| Provider    | Default Budget | Rationale                       |
| ----------- | -------------- | ------------------------------- |
| `opencode`  | 16,000         | Conservative for unknown limits |
| `anthropic` | 64,000         | Claude handles 200k context     |
| `openai`    | 64,000         | GPT-4o handles 128k context     |
| `google`    | 128,000        | Gemini handles 1M context       |

**Examples:**

```typescript
// Free tier (default) - works without any API key
ai: {
  provider: 'opencode',
}

// Premium provider - requires API key
ai: {
  provider: 'anthropic',
  // Set ANTHROPIC_API_KEY environment variable
}

// Custom token budget - override provider default
ai: {
  provider: 'anthropic',
  tokenBudget: 32000, // Use 32k instead of default 64k
}
```

### Git (`git`)

Controls git repository settings.

**Options:**

- `remote` (string) - Remote name (default: `'origin'`)
- `baseBranch` (string) - Base branch (default: `'main'`)

## Common Configuration Scenarios

### Minimal Setup (npm packages only)

```json
{
  "prStrategy": {
    "stackingEnabled": false
  }
}
```

### Expo Monorepo

```typescript
export default defineConfig({
  expo: {
    enabled: true,
    autoDetect: true,
  },
  syncpack: {
    preserveCustomRules: true,
  },
  prStrategy: {
    stackingEnabled: true,
    maxStackDepth: 5,
  },
});
```

### Nix + Expo + Premium AI

```typescript
export default defineConfig({
  expo: {
    enabled: true,
    autoDetect: true,
  },
  nix: {
    enabled: true,
    devenvPath: './tooling/direnv',
    nixpkgsOverlayPath: './tooling/direnv/nixpkgs-overlay',
  },
  // Upgrade to premium AI for better quality (requires API key)
  ai: {
    provider: 'anthropic',
  },
  prStrategy: {
    stackingEnabled: true,
    maxStackDepth: 3,
  },
});
```

## Interactive Configuration

The easiest way to generate a config file is using the interactive setup wizard:

```bash
dep-updater init
```

This will:

- Auto-detect your project setup
- Prompt for configuration options
- Generate the appropriate config file (TS or JSON)
- Create GitHub Actions workflow

## See Also

- [Getting Started Guide](./GETTING-STARTED.md) - Complete setup walkthrough
- [API Reference](./API.md) - Programmatic usage
- [Troubleshooting Guide](./TROUBLESHOOTING.md) - Common issues and solutions
- [README](../README.md) - Main documentation
