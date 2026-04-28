import { existsSync, lstatSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { listReleasePackages, readPackageJson } from '../lib/workspace.js';

type ManagedKind = 'raw' | 'template';

interface ManagedFile {
  kind: ManagedKind;
  source: string;
  target: string;
  executable?: boolean;
  releasePackagesOnly?: boolean;
}

export interface FileResult {
  target: string;
  action: 'created' | 'updated' | 'unchanged' | 'skipped' | 'skipped-symlink' | 'drifted' | 'ok-symlink';
}

interface ManagedFileContext {
  hasReleasePackages: boolean;
  nodeModulesCacheKey: string;
  repoName: string;
}

const managedFiles: ManagedFile[] = [
  {
    kind: 'raw',
    source: 'tooling/direnv/github-actions-bootstrap.sh',
    target: 'tooling/direnv/github-actions-bootstrap.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/git-hooks/pre-commit.sh',
    target: 'tooling/git-hooks/pre-commit.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/git-hooks/commit-msg.sh',
    target: 'tooling/git-hooks/commit-msg.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: '.git-format-staged.yml',
    target: '.git-format-staged.yml',
  },
  {
    kind: 'template',
    source: 'github/workflows/ci.yml',
    target: '.github/workflows/ci.yml',
  },
  {
    kind: 'template',
    source: 'github/workflows/publish.yml',
    target: '.github/workflows/publish.yml',
    releasePackagesOnly: true,
  },
  {
    kind: 'template',
    source: 'github/actions/cache-nix-devenv/action.yml',
    target: '.github/actions/cache-nix-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/setup-devenv/action.yml',
    target: '.github/actions/setup-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/save-nix-devenv/action.yml',
    target: '.github/actions/save-nix-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-node-modules/action.yml',
    target: '.github/actions/cache-node-modules/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-nx/action.yml',
    target: '.github/actions/cache-nx/action.yml',
  },
];

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

export function applyManagedFiles(root: string, mode: 'update' | 'check' | 'diff'): FileResult[] {
  const context = getManagedFileContext(root);
  return managedFiles.map((file) => applyManagedFile(root, file, mode, context));
}

function applyManagedFile(
  root: string,
  file: ManagedFile,
  mode: 'update' | 'check' | 'diff',
  context: ManagedFileContext,
): FileResult {
  if (file.releasePackagesOnly === true && !context.hasReleasePackages) {
    return { target: file.target, action: 'skipped' };
  }
  const target = resolve(root, file.target);
  const content = getManagedContent(file, context);
  if (existsSync(target)) {
    const info = lstatSync(target);
    if (info.isSymbolicLink()) {
      return { target: file.target, action: mode === 'check' ? 'ok-symlink' : 'skipped-symlink' };
    }
    if (!info.isFile()) {
      throw new Error(`${file.target} exists but is not a regular file or symlink`);
    }
    const current = readFileSync(target, 'utf8');
    if (current === content) {
      return { target: file.target, action: 'unchanged' };
    }
    if (mode === 'check' || mode === 'diff') {
      return { target: file.target, action: 'drifted' };
    }
    writeManagedFile(target, content, file.executable === true);
    return { target: file.target, action: 'updated' };
  }
  if (mode === 'check' || mode === 'diff') {
    return { target: file.target, action: 'drifted' };
  }
  writeManagedFile(target, content, file.executable === true);
  return { target: file.target, action: 'created' };
}

function getManagedContent(file: ManagedFile, context: ManagedFileContext): string {
  const sourceRoot = file.kind === 'raw' ? 'managed/raw' : 'managed/templates';
  const sourcePath = join(packageRoot, sourceRoot, file.source);
  const content = readFileSync(sourcePath, 'utf8');
  if (file.kind === 'raw') {
    return content;
  }
  return renderTemplate(context, content);
}

function getManagedFileContext(root: string): ManagedFileContext {
  const packageJson = readPackageJson(join(root, 'package.json'));
  const repoName = packageJson?.name ?? 'monorepo';
  const nodeModulesCacheKey = existsSync(join(root, 'bun.lock'))
    ? `$${"{{ hashFiles('bun.lock', 'package.json', 'packages/*/package.json') }}"}`
    : `$${"{{ hashFiles('bun.lockb', 'package.json', 'packages/*/package.json') }}"}`;
  return { hasReleasePackages: listReleasePackages(root, packageJson).length > 0, nodeModulesCacheKey, repoName };
}

function renderTemplate(context: ManagedFileContext, template: string): string {
  return template
    .replaceAll('{{REPO_NAME}}', context.repoName)
    .replaceAll('{{NODE_MODULES_CACHE_KEY}}', context.nodeModulesCacheKey);
}

function writeManagedFile(path: string, content: string, executable: boolean): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content, { mode: executable ? 0o755 : 0o644 });
}

export function printResults(results: FileResult[]): void {
  for (const result of results) {
    console.log(`${result.action.padEnd(15)} ${result.target}`);
  }
}

export function validateManagedFiles(root: string): number {
  const results = applyManagedFiles(root, 'check');
  printResults(results);
  const failures = results.filter((result) => result.action === 'drifted').length;
  if (failures > 0) {
    console.error('Managed monorepo files are out of date. Run: smoo monorepo update');
  }
  return failures;
}
