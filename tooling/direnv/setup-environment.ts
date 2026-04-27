#!/usr/bin/env bun
import { $ } from 'bun';
import path from 'path';

const devenvRoot = process.env.DEVENV_ROOT;
const projectRoot = path.resolve(`${devenvRoot}/../..`);

// Go to project root
process.chdir(projectRoot);

try {
  // Install dependencies first so node_modules/.bin tools are available
  if (process.env.CI) {
    try {
      await $`bun install --frozen-lockfile`;
    } catch {
      console.error('! Failed to install dependencies with frozen lockfile');
      await $`bun install`;
      console.error('git diff after install:');
      await $`git diff`;
      process.exit(1);
    }
  } else {
    await $`bun install --no-summary`.quiet();
  }

  // Make sure Biome is executable
  await $`chmod +x ${projectRoot}/node_modules/@biomejs/biome/bin/biome`;

  if (!process.env.CI) {
    const { syncRootRuntimeVersions } = await import('@smoothbricks/cli/monorepo/runtime');
    await syncRootRuntimeVersions(projectRoot);
  }

  const { applyWorkspaceGitConfig } = await import('@smoothbricks/cli/monorepo/git-config');
  await applyWorkspaceGitConfig(projectRoot);
} catch (error) {
  console.error(`--- ERROR: setup-environment.ts failed: ${error}`);
  if (error.stdout) {
    process.stdout.write(error.stdout);
  }
  if (error.stderr) {
    process.stderr.write(error.stderr);
  }
  console.error('\n---');
  process.exit(1);
}
