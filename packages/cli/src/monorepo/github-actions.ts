import { existsSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { printCommandOutput, runResult, runStatus } from '../lib/run.js';

// jactionlint v2's repository action search excludes dependencies and test data.
const skippedActionDirectories: Record<string, true> = {
  '.git': true,
  node_modules: true,
  vendor: true,
  testdata: true,
  fixtures: true,
  '.venv': true,
  venv: true,
  target: true,
  dist: true,
};

export async function validateGitHubActions(root: string, verbose = false): Promise<number> {
  let files: string[];
  try {
    files = githubActionFiles(root);
  } catch (error) {
    console.error(`GitHub Actions input discovery failed: ${error instanceof Error ? error.message : String(error)}`);
    return 1;
  }
  if (files.length === 0) {
    if (verbose) {
      console.log('skip           GitHub Actions validation (no workflows, actions, or Dependabot configuration)');
    }
    return 0;
  }

  const lookup = { PATH: process.env.PATH ?? '', cwd: root };
  const jactionlint = Bun.which('jactionlint', lookup);
  if (!jactionlint) {
    console.error('jactionlint requires jactionlint on the managed shell/CI PATH');
    return 1;
  }
  const shellcheck = Bun.which('shellcheck', lookup);
  if (!shellcheck) {
    console.error('jactionlint requires shellcheck on the managed shell/CI PATH');
    return 1;
  }
  const pyflakes = Bun.which('pyflakes', lookup);
  if (!pyflakes) {
    console.error('jactionlint requires pyflakes on the managed shell/CI PATH');
    return 1;
  }
  const config = ['jactionlint.yaml', 'jactionlint.yml', 'actionlint.yaml', 'actionlint.yml'].find((name) =>
    existsSync(join(root, '.github', name)),
  );
  const args = [
    '--no-online',
    '--config-file',
    config ? join('.github', config) : '/dev/null',
    '--shellcheck',
    shellcheck,
    '--pyflakes',
    pyflakes,
  ];
  // Repository mode owns discovery and caller context. v2 also accepts explicit
  // inputs for standalone Dependabot/subdirectory-action repos it cannot detect.
  const repositoryDiscoveryRoot =
    ['.github/workflows', '.github/actions'].some((path) => {
      const directory = join(root, path);
      return existsSync(directory) && statSync(directory).isDirectory();
    }) || ['action.yml', 'action.yaml'].some((path) => existsSync(join(root, path)));
  if (!repositoryDiscoveryRoot) {
    args.push('--', ...files);
  }
  const result = verbose
    ? { exitCode: await runStatus(jactionlint, args, root, false), stdout: '', stderr: '' }
    : await runResult(jactionlint, args, root);
  if (result.exitCode !== 0) {
    if (!verbose) {
      printCommandOutput(result.stdout, result.stderr);
    }
    console.error(`jactionlint GitHub Actions validation failed with exit code ${result.exitCode}`);
    return 1;
  }
  return 0;
}

function githubActionFiles(root: string): string[] {
  const files: string[] = [];
  const workflows = join(root, '.github', 'workflows');
  if (existsSync(workflows)) {
    for (const entry of readdirSync(workflows, { withFileTypes: true })) {
      if (!entry.isDirectory() && /\.ya?ml$/.test(entry.name)) {
        files.push(join('.github', 'workflows', entry.name));
      }
    }
    // Repository mode will discover the remaining inputs; no second tree walk
    // is needed just to establish that this check applies.
    if (files.length > 0) {
      return files;
    }
  }
  for (const name of ['dependabot.yml', 'dependabot.yaml']) {
    if (existsSync(join(root, '.github', name))) {
      files.push(join('.github', name));
    }
  }
  function visit(directory: string): void {
    for (const entry of readdirSync(join(root, directory), { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) {
        if (!Object.hasOwn(skippedActionDirectories, entry.name)) {
          visit(path);
        }
      } else if (directory !== '.github/workflows' && (entry.name === 'action.yml' || entry.name === 'action.yaml')) {
        files.push(path);
      }
    }
  }
  visit('');
  return files.sort();
}
