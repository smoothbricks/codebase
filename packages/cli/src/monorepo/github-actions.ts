import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { printCommandOutput, runResult, runStatus } from '../lib/run.js';

// The GitHub Actions inputs as git pathspecs: the YAML files directly in .github/workflows (GitHub loads no
// other workflow), action metadata at any depth, and the Dependabot configuration. jactionlint itself reads
// an action.yml directly in .github/workflows as a workflow.
const githubActionPathspecs = [
  ':(glob).github/workflows/*.yml',
  ':(glob).github/workflows/*.yaml',
  ':(glob)**/action.yml',
  ':(glob)**/action.yaml',
  ':(glob).github/dependabot.yml',
  ':(glob).github/dependabot.yaml',
];

export async function validateGitHubActions(root: string, verbose = false): Promise<number> {
  const inputs = await githubActionFiles(root);
  if ('failure' in inputs) {
    console.error(`GitHub Actions input discovery failed: ${inputs.failure}`);
    return 1;
  }
  const { files } = inputs;
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
  // Explicit inputs only: repository mode walks the filesystem into ignored directories and lints what it
  // finds there. Given files, jactionlint still detects the project and the local callers of each action.
  const args = [
    '--no-online',
    '--config-file',
    config ? join('.github', config) : '/dev/null',
    '--shellcheck',
    shellcheck,
    '--pyflakes',
    pyflakes,
    '--',
    ...files,
  ];
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

/**
 * The GitHub Actions files of the working tree as git sees it: tracked files and new files not yet added, never
 * one the repository ignores (a dependency install, a tool cache inside the checkout). A tracked file the working
 * tree does not hold (an unstaged deletion, a sparse checkout) is no input either: jactionlint reads the working
 * tree.
 */
async function githubActionFiles(root: string): Promise<{ files: string[] } | { failure: string }> {
  const listed = await runResult(
    'git',
    ['ls-files', '-z', '--deduplicate', '--cached', '--others', '--exclude-standard', '--', ...githubActionPathspecs],
    root,
  );
  if (listed.exitCode !== 0) {
    return { failure: `git ls-files in ${root} exited with code ${listed.exitCode}: ${listed.stderr.trim()}` };
  }
  return {
    files: listed.stdout
      .split('\0')
      .filter((path) => path !== '' && existsSync(join(root, path)))
      .sort(),
  };
}
