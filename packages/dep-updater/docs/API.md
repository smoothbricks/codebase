# API Reference

This document provides a comprehensive reference for using `dep-updater` programmatically in your own scripts and tools.

## Installation

```bash
bun add @smoothbricks/dep-updater
# or
npm install @smoothbricks/dep-updater
```

## Basic Usage

```typescript
import { updateDeps, loadConfig, mergeConfig } from '@smoothbricks/dep-updater';

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

## Core Functions

### `loadConfig()`

Loads configuration from the project's config file or returns defaults.

**Signature:**

```typescript
async function loadConfig(): Promise<DepUpdaterConfig>;
```

**Returns:** Fully populated configuration object with all defaults applied.

**Behavior:**

- Searches for config files in `tooling/` directory
- Priority: `tooling/dep-updater.ts` > `tooling/dep-updater.json`
- Searches current directory and up to 10 parent directories
- Returns default config if no file found

**Example:**

```typescript
const config = await loadConfig();
console.log(config.prStrategy.maxStackDepth); // 5 (default)
```

### `mergeConfig()`

Merges partial configuration with defaults.

**Signature:**

```typescript
function mergeConfig(partialConfig: Partial<DepUpdaterConfig>): DepUpdaterConfig;
```

**Parameters:**

- `partialConfig` - Partial configuration object to merge with defaults

**Returns:** Complete configuration object

**Behavior:**

- Deep merges provided config with defaults
- Allows overriding specific options without providing all fields
- Type-safe with TypeScript

**Example:**

```typescript
const config = mergeConfig({
  expo: {
    enabled: true,
    autoDetect: true,
  },
  prStrategy: {
    stackingEnabled: false, // Override single option
  },
});
```

### `defineConfig()`

Helper for creating type-safe configuration in TypeScript config files.

**Signature:**

```typescript
function defineConfig(config: Partial<DepUpdaterConfig>): Partial<DepUpdaterConfig>;
```

**Parameters:**

- `config` - Configuration object

**Returns:** Same configuration object (provides type checking)

**Usage:**

In `tooling/dep-updater.ts`:

```typescript
import { defineConfig } from '@smoothbricks/dep-updater';

export default defineConfig({
  expo: {
    enabled: true,
    autoDetect: true,
  },
  ai: {
    provider: 'anthropic',
  },
});
```

## Main Functions

### `updateDeps()`

Main function to update all dependencies across ecosystems.

**Signature:**

```typescript
async function updateDeps(config: DepUpdaterConfig, options: UpdateDepsOptions): Promise<void>;
```

**Parameters:**

- `config` - Full configuration object (from `loadConfig()` or `mergeConfig()`)
- `options` - Execution options:
  - `dryRun?: boolean` - Preview changes without applying them
  - `skipGit?: boolean` - Don't create branches or commits
  - `skipAI?: boolean` - Skip AI changelog analysis
  - `verbose?: boolean` - Enable detailed logging

**Behavior:**

- Updates npm packages via Bun
- Updates Expo SDK if enabled
- Updates Nix ecosystems (devenv, nixpkgs) if enabled
- Creates PR with stacking if configured
- Generates AI-powered changelogs if configured

**Example:**

```typescript
await updateDeps(config, {
  dryRun: false,
  skipGit: false,
  skipAI: false,
  verbose: true,
});
```

### `updateExpo()`

Update Expo SDK and regenerate syncpack configuration.

**Signature:**

```typescript
async function updateExpo(config: DepUpdaterConfig, options: UpdateExpoOptions): Promise<void>;
```

**Parameters:**

- `config` - Full configuration object
- `options` - Execution options:
  - `targetSdk?: number` - Specific SDK version to update to
  - `dryRun?: boolean` - Preview changes without applying them
  - `skipGit?: boolean` - Don't create branches or commits

**Behavior:**

- Checks for latest Expo SDK version
- Updates Expo dependencies in all detected/configured projects
- Regenerates `.syncpackrc.json` with Expo-compatible versions
- Creates commit and PR if not skipped

**Example:**

```typescript
await updateExpo(config, {
  targetSdk: 52,
  dryRun: false,
});
```

### `generateWorkflow()`

Generate GitHub Actions workflow file.

**Signature:**

```typescript
async function generateWorkflow(config: DepUpdaterConfig, options: GenerateWorkflowOptions): Promise<void>;
```

**Parameters:**

- `config` - Full configuration object
- `options` - Workflow generation options:
  - `schedule?: string` - Cron schedule (default: '0 2 \* \* \*')
  - `workflowName?: string` - Workflow display name
  - `enableAI?: boolean` - Force enable AI features
  - `skipAI?: boolean` - Force disable AI features
  - `dryRun?: boolean` - Print workflow content without writing file

**Behavior:**

- Generates `.github/workflows/update-deps.yml`
- Uses unified template with runtime auth detection
- Processes placeholders for AI features
- Validates YAML syntax before writing

**Example:**

```typescript
await generateWorkflow(config, {
  enableAI: true,
  schedule: '0 3 * * 1', // Weekly on Monday at 3 AM
});
```

### `validateSetup()`

Validate GitHub App setup and configuration.

**Signature:**

```typescript
async function validateSetup(config: DepUpdaterConfig): Promise<boolean>;
```

**Parameters:**

- `config` - Full configuration object

**Returns:** `true` if all checks pass, `false` otherwise

**Checks:**

- GitHub CLI installed and authenticated
- GitHub App installed on repository
- Required permissions granted
- Token generation works
- Config file is valid

**Example:**

```typescript
const isValid = await validateSetup(config);
if (!isValid) {
  console.error('Setup validation failed');
  process.exit(1);
}
```

## TypeScript Types

### `DepUpdaterConfig`

Full configuration interface.

```typescript
interface DepUpdaterConfig {
  expo?: {
    enabled: boolean;
    autoDetect?: boolean;
    projects?: ExpoProject[];
  };
  syncpack?: {
    configPath: string;
    preserveCustomRules: boolean;
    fixScriptName?: string;
  };
  nix?: {
    enabled: boolean;
    devenvPath: string;
    nixpkgsOverlayPath: string;
  };
  prStrategy: {
    stackingEnabled: boolean;
    maxStackDepth: number;
    autoCloseOldPRs: boolean;
    resetOnMerge: boolean;
    stopOnConflicts: boolean;
    branchPrefix: string;
    prTitlePrefix: string;
  };
  autoMerge: {
    enabled: boolean;
    mode: 'none' | 'patch' | 'minor';
    requireTests: boolean;
  };
  ai?: {
    provider: 'anthropic';
    apiKey?: string;
    model?: string;
  };
  git?: {
    remote: string;
    baseBranch: string;
  };
  repoRoot?: string;
  logger?: Logger;
}
```

### `ExpoProject`

Expo project configuration.

```typescript
interface ExpoProject {
  name?: string;
  packageJsonPath: string;
}
```

### `UpdateDepsOptions`

Options for `updateDeps()` function.

```typescript
interface UpdateDepsOptions {
  dryRun?: boolean;
  skipGit?: boolean;
  skipAI?: boolean;
  verbose?: boolean;
}
```

### `GenerateWorkflowOptions`

Options for `generateWorkflow()` function.

```typescript
interface GenerateWorkflowOptions {
  schedule?: string;
  workflowName?: string;
  enableAI?: boolean;
  skipAI?: boolean;
  dryRun?: boolean;
}
```

## Advanced Examples

### Custom Logger

```typescript
import { updateDeps, mergeConfig } from '@smoothbricks/dep-updater';

// Custom logger implementation
const customLogger = {
  info: (message: string) => console.log(`[INFO] ${message}`),
  error: (message: string) => console.error(`[ERROR] ${message}`),
  warn: (message: string) => console.warn(`[WARN] ${message}`),
  debug: (message: string) => console.debug(`[DEBUG] ${message}`),
};

const config = mergeConfig({
  logger: customLogger,
});

await updateDeps(config, { verbose: true });
```

### Conditional AI Usage

```typescript
import { updateDeps, mergeConfig } from '@smoothbricks/dep-updater';

// Only use AI in production environment
const shouldUseAI = process.env.NODE_ENV === 'production';

const config = mergeConfig({
  ai: shouldUseAI
    ? {
        provider: 'anthropic',
        apiKey: process.env.ANTHROPIC_API_KEY,
      }
    : undefined,
});

await updateDeps(config, {
  skipAI: !shouldUseAI,
});
```

### Multi-Project Expo Update

```typescript
import { updateExpo, mergeConfig } from '@smoothbricks/dep-updater';

const config = mergeConfig({
  expo: {
    enabled: true,
    projects: [
      { name: 'customer-app', packageJsonPath: './apps/customer/package.json' },
      { name: 'driver-app', packageJsonPath: './apps/driver/package.json' },
      { name: 'admin-app', packageJsonPath: './apps/admin/package.json' },
    ],
  },
});

// Update all Expo projects to SDK 52
await updateExpo(config, {
  targetSdk: 52,
  dryRun: false,
});
```

### Workflow Generation with Custom Schedule

```typescript
import { generateWorkflow, loadConfig } from '@smoothbricks/dep-updater';

const config = await loadConfig();

// Generate workflow for monthly updates (auth auto-detected at runtime)
await generateWorkflow(config, {
  schedule: '0 2 1 * *', // First day of month at 2 AM
  workflowName: 'Monthly Dependency Updates',
  enableAI: true,
});
```

### Dry-Run Testing

```typescript
import { updateDeps, loadConfig } from '@smoothbricks/dep-updater';

const config = await loadConfig();

// Test updates without making any changes
await updateDeps(config, {
  dryRun: true,
  verbose: true,
});
```

## Error Handling

All async functions may throw errors. Wrap in try-catch for proper error handling:

```typescript
import { updateDeps, loadConfig } from '@smoothbricks/dep-updater';

try {
  const config = await loadConfig();
  await updateDeps(config, { dryRun: false });
  console.log('✓ Updates completed successfully');
} catch (error) {
  console.error('✗ Update failed:', error.message);
  process.exit(1);
}
```

## Integration Examples

### Custom CI Script

```typescript
#!/usr/bin/env bun

import { updateDeps, loadConfig } from '@smoothbricks/dep-updater';

async function main() {
  console.log('Starting dependency updates...');

  const config = await loadConfig();

  await updateDeps(config, {
    dryRun: process.argv.includes('--dry-run'),
    skipAI: process.argv.includes('--skip-ai'),
    verbose: process.argv.includes('--verbose'),
  });

  console.log('✓ Updates completed');
}

main().catch((error) => {
  console.error('Error:', error);
  process.exit(1);
});
```

### Scheduled Task

```typescript
import { CronJob } from 'cron';
import { updateDeps, loadConfig } from '@smoothbricks/dep-updater';

// Run updates every Monday at 2 AM
const job = new CronJob('0 2 * * 1', async () => {
  console.log('Running scheduled dependency updates...');

  try {
    const config = await loadConfig();
    await updateDeps(config, { dryRun: false });
    console.log('✓ Scheduled updates completed');
  } catch (error) {
    console.error('✗ Scheduled updates failed:', error);
  }
});

job.start();
```

## See Also

- [Getting Started Guide](./GETTING-STARTED.md) - Complete setup walkthrough
- [Configuration Reference](./CONFIGURATION.md) - All configuration options
- [Troubleshooting](./TROUBLESHOOTING.md) - Common issues and solutions
- [README](../README.md) - Main documentation
