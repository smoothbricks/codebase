import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { hasOwn, hasOwnString, isRecord } from '@smoothbricks/validation';
import { $ } from 'bun';
import { publint } from 'publint';
import { formatMessage } from 'publint/utils';

type ManagedKind = 'raw' | 'template';

interface ManagedFile {
  kind: ManagedKind;
  source: string;
  target: string;
  executable?: boolean;
}

interface FileResult {
  target: string;
  action: 'created' | 'updated' | 'unchanged' | 'skipped-symlink' | 'drifted' | 'ok-symlink';
}

interface PackageInfo {
  name: string;
  version: string;
  private: boolean;
  tags: string[];
  path: string;
  packageJsonPath: string;
  json: Record<string, unknown>;
}

interface RepositoryInfo {
  type: string;
  url: string;
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
    source: 'tooling/direnv/apply-workspace-git-config.sh',
    target: 'tooling/direnv/apply-workspace-git-config.sh',
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
  },
  {
    kind: 'template',
    source: 'github/actions/checkout/action.yml',
    target: '.github/actions/checkout/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-nix-devenv/action.yml',
    target: '.github/actions/cache-nix-devenv/action.yml',
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

const validCommitTypes = new Set([
  'build',
  'chore',
  'ci',
  'docs',
  'feat',
  'fix',
  'perf',
  'refactor',
  'revert',
  'style',
  'test',
]);

const workspaceDependencyFields = ['dependencies', 'devDependencies', 'optionalDependencies'] as const;

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

export async function runCli(argv = process.argv.slice(2)): Promise<void> {
  try {
    await dispatch(argv);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
  }
}

async function dispatch(argv: string[]): Promise<void> {
  const [first, second, ...rest] = argv;
  const root = await findRepoRoot();
  if (!first || first === '--help' || first === '-h') {
    printHelp();
    return;
  }
  if (first === 'release') {
    await dispatchRelease(root, second, rest);
    return;
  }
  if (first === 'github-ci') {
    await dispatchGithubCi(root, second, rest);
    return;
  }
  if (first !== 'monorepo') {
    throw new Error(`Unknown command "${first}". Run: smoo --help`);
  }
  switch (second) {
    case 'init':
      await initMonorepo(root, parseArgs(rest));
      return;
    case 'validate':
      await validateMonorepo(root);
      return;
    case 'update':
      printResults(applyManagedFiles(root, 'update'));
      return;
    case 'check': {
      const results = applyManagedFiles(root, 'check');
      printResults(results);
      if (results.some((result) => result.action === 'drifted')) {
        throw new Error('Managed monorepo files are out of date. Run: smoo monorepo update');
      }
      return;
    }
    case 'diff':
      printResults(applyManagedFiles(root, 'diff'));
      return;
    case 'validate-commit-msg':
      validateCommitMessageCommand(rest[0]);
      return;
    case 'sync-bun-lockfile-versions':
      syncBunLockfileVersions(root);
      return;
    case 'list-public-projects':
      console.log(
        listPublicPackages(root)
          .map((pkg) => pkg.name)
          .join(','),
      );
      return;
    case 'validate-public-tags':
      if (validatePublicTags(root) > 0) {
        throw new Error('npm:public tag validation failed.');
      }
      return;
    case 'release-state':
      await printReleaseState(root);
      return;
    default:
      throw new Error(`Unknown monorepo command "${second ?? ''}". Run: smoo --help`);
  }
}

function printHelp(): void {
  console.log(`smoo - SmoothBricks monorepo tooling

Usage:
  smoo release version --bump <auto|patch|minor|major|prerelease> [--dry-run]
  smoo release publish --bump <auto|patch|minor|major|prerelease> [--dry-run]
  smoo release github-release --tags <tags> --bump <auto|patch|minor|major|prerelease>
  smoo github-ci cleanup-cache
  smoo github-ci nx-smart --target <target> --name <check-name> --step <number>
  smoo github-ci nx-run-many --targets <targets> [--projects <projects>]
  smoo monorepo init [--runtime-only] [--sync-runtime]
  smoo monorepo validate
  smoo monorepo update
  smoo monorepo check
  smoo monorepo diff
  smoo monorepo validate-commit-msg <commit-msg-file>
  smoo monorepo sync-bun-lockfile-versions
  smoo monorepo list-public-projects
  smoo monorepo validate-public-tags
  smoo monorepo release-state`);
}

async function dispatchRelease(root: string, command: string | undefined, argv: string[]): Promise<void> {
  switch (command) {
    case 'version':
      await releaseVersion(root, parseArgs(argv));
      return;
    case 'publish':
      await releasePublish(root, parseArgs(argv));
      return;
    case 'github-release':
      await releaseGithubRelease(root, parseArgs(argv));
      return;
    default:
      throw new Error(`Unknown release command "${command ?? ''}". Run: smoo --help`);
  }
}

async function dispatchGithubCi(root: string, command: string | undefined, argv: string[]): Promise<void> {
  switch (command) {
    case 'cleanup-cache':
      await cleanupGithubCiCache(root);
      return;
    case 'nx-smart':
      await githubCiNxSmart(root, parseArgs(argv));
      return;
    case 'nx-run-many':
      await githubCiNxRunMany(root, parseArgs(argv));
      return;
    default:
      throw new Error(`Unknown github-ci command "${command ?? ''}". Run: smoo --help`);
  }
}

function parseArgs(argv: string[]): Map<string, string | boolean> {
  const args = new Map<string, string | boolean>();
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith('--')) {
      continue;
    }
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args.set(key, true);
      continue;
    }
    args.set(key, next);
    i++;
  }
  return args;
}

function applyManagedFiles(root: string, mode: 'update' | 'check' | 'diff'): FileResult[] {
  return managedFiles.map((file) => applyManagedFile(root, file, mode));
}

function applyManagedFile(root: string, file: ManagedFile, mode: 'update' | 'check' | 'diff'): FileResult {
  const target = resolve(root, file.target);
  const content = getManagedContent(root, file);
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

function getManagedContent(root: string, file: ManagedFile): string {
  const sourceRoot = file.kind === 'raw' ? 'managed/raw' : 'managed/templates';
  const sourcePath = join(packageRoot, sourceRoot, file.source);
  const content = readFileSync(sourcePath, 'utf8');
  if (file.kind === 'raw') {
    return content;
  }
  return renderTemplate(root, content);
}

function renderTemplate(root: string, template: string): string {
  const packageJson = readPackageJson(join(root, 'package.json'));
  const repoName = packageJson?.name ?? 'monorepo';
  const nodeModulesCacheKey = existsSync(join(root, 'bun.lock'))
    ? `$${"{{ hashFiles('bun.lock') }}"}`
    : `$${"{{ hashFiles('bun.lockb') }}"}`;
  return template.replaceAll('{{REPO_NAME}}', repoName).replaceAll('{{NODE_MODULES_CACHE_KEY}}', nodeModulesCacheKey);
}

function writeManagedFile(path: string, content: string, executable: boolean): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content, { mode: executable ? 0o755 : 0o644 });
}

function printResults(results: FileResult[]): void {
  for (const result of results) {
    console.log(`${result.action.padEnd(15)} ${result.target}`);
  }
}

async function initMonorepo(root: string, args: Map<string, string | boolean>): Promise<void> {
  if (booleanArg(args, 'runtime-only')) {
    await syncRootRuntimeVersions(root);
    return;
  }

  printResults(applyManagedFiles(root, 'update'));
  ensureLocalSmooShim(root);
  if (process.env.DEVENV_ROOT || booleanArg(args, 'sync-runtime')) {
    await syncRootRuntimeVersions(root);
  } else {
    console.log('skip           root runtime versions (outside devenv; pass --sync-runtime to force)');
  }
  applyPublicPackageDefaults(root);
  applyWorkspaceDependencyDefaults(root);
  await fixPackageHygiene(root);
}

function ensureLocalSmooShim(root: string): void {
  const shim = join(root, 'tooling', 'smoo');
  if (!existsSync(shim)) {
    return;
  }
  if ((statSync(shim).mode & 0o755) === 0o755) {
    console.log('unchanged      tooling/smoo executable bit');
    return;
  }
  chmodSync(shim, 0o755);
  console.log('updated        tooling/smoo executable bit');
}

async function validateMonorepo(root: string): Promise<void> {
  let failures = 0;
  failures += validateManagedFiles(root);
  failures += validateRootPackagePolicy(root);
  failures += validateNxReleaseConfig(root);
  failures += validatePublicTags(root);
  failures += validatePublicPackageMetadata(root);
  failures += validateWorkspaceDependencies(root);
  failures += await validatePackageHygiene(root);
  failures += await validatePackedPublicPackages(root);

  if (failures > 0) {
    throw new Error(`Monorepo validation failed with ${failures} problem(s). Run: smoo monorepo init`);
  }
  console.log('Monorepo configuration is valid.');
}

async function syncRootRuntimeVersions(root: string): Promise<void> {
  const packageJsonPath = join(root, 'package.json');
  const packageJson = readJsonObject(packageJsonPath);
  if (!packageJson) {
    throw new Error('package.json not found or invalid');
  }
  const nodeVersion = (await $`node --version`.cwd(root).text()).trim().replace(/^v/, '');
  const bunVersion = (await $`bun --version`.cwd(root).text()).trim();
  const nodeMajor = nodeVersion.split('.', 1)[0];
  if (!nodeMajor) {
    throw new Error(`Unable to derive Node major version from ${nodeVersion}`);
  }

  let changed = false;
  const engines = getOrCreateRecord(packageJson, 'engines');
  changed = setStringProperty(engines, 'node', `>=${nodeMajor}.0.0`) || changed;
  changed = setStringProperty(packageJson, 'packageManager', `bun@${bunVersion}`) || changed;
  const devDependencies = getOrCreateRecord(packageJson, 'devDependencies');
  changed = setStringProperty(devDependencies, '@types/node', `~${nodeMajor}.0.0`) || changed;
  changed = setStringProperty(devDependencies, '@types/bun', bunVersion) || changed;

  if (changed) {
    writeJsonObject(packageJsonPath, packageJson);
    console.log('updated        package.json runtime versions');
  } else {
    console.log('unchanged      package.json runtime versions');
  }
}

function applyPublicPackageDefaults(root: string): void {
  const rootPackage = requiredJsonObject(join(root, 'package.json'));
  const rootLicense = stringProperty(rootPackage, 'license');
  const rootRepository = repositoryInfo(rootPackage);
  if (!rootLicense) {
    throw new Error('Root package.json must define license before public package defaults can be applied.');
  }
  if (!rootRepository) {
    throw new Error('Root package.json must define repository.url before public package defaults can be applied.');
  }

  for (const pkg of listPublicPackages(root)) {
    let changed = false;
    changed = setMissingStringProperty(pkg.json, 'license', rootLicense) || changed;
    const publishConfig = getOrCreateRecord(pkg.json, 'publishConfig');
    changed = setStringProperty(publishConfig, 'access', 'public') || changed;

    const repository = getOrCreateRecord(pkg.json, 'repository');
    changed = setStringProperty(repository, 'type', rootRepository.type) || changed;
    changed = setStringProperty(repository, 'url', rootRepository.url) || changed;
    changed = setStringProperty(repository, 'directory', pkg.path.replaceAll('\\', '/')) || changed;
    changed = normalizeExportConditionOrder(pkg.json.exports) || changed;
    if (hasDevelopmentSourceExport(pkg.json.exports)) {
      changed = addFileEntry(pkg.json, 'src') || changed;
    }

    if (changed) {
      writeJsonObject(pkg.packageJsonPath, pkg.json);
      console.log(`updated        ${pkg.path}/package.json public metadata`);
    } else {
      console.log(`unchanged      ${pkg.path}/package.json public metadata`);
    }
  }
}

function applyWorkspaceDependencyDefaults(root: string): void {
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  for (const pkg of listPackageJsonRecords(root)) {
    const changed = fixWorkspaceDependencyRanges(pkg.json, workspaceNames);
    if (changed) {
      writeJsonObject(pkg.packageJsonPath, pkg.json);
      console.log(`updated        ${pkg.path}/package.json workspace dependency ranges`);
    } else {
      console.log(`unchanged      ${pkg.path}/package.json workspace dependency ranges`);
    }
  }
}

async function fixPackageHygiene(root: string): Promise<void> {
  await run('sherif', ['--fix', '--select', 'highest'], root);
}

function validateManagedFiles(root: string): number {
  const results = applyManagedFiles(root, 'check');
  printResults(results);
  const failures = results.filter((result) => result.action === 'drifted').length;
  if (failures > 0) {
    console.error('Managed monorepo files are out of date. Run: smoo monorepo update');
  }
  return failures;
}

function validateRootPackagePolicy(root: string): number {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  if (!rootPackage) {
    console.error('package.json not found or invalid');
    return 1;
  }
  let failures = 0;
  if (!stringProperty(rootPackage, 'name')) {
    console.error('package.json must define name');
    failures++;
  }
  if (!stringProperty(rootPackage, 'license')) {
    console.error('package.json must define repo-wide license');
    failures++;
  }
  if (!repositoryInfo(rootPackage)) {
    console.error('package.json must define repository.url');
    failures++;
  }
  const packageManager = stringProperty(rootPackage, 'packageManager');
  if (!packageManager?.startsWith('bun@')) {
    console.error('package.json packageManager must use bun@<version>');
    failures++;
  }
  const bunVersion = packageManager?.startsWith('bun@') ? packageManager.slice('bun@'.length) : null;
  const devDependencies = recordProperty(rootPackage, 'devDependencies');
  if (!bunVersion || !devDependencies || devDependencies['@types/bun'] !== bunVersion) {
    console.error('package.json devDependencies.@types/bun must match packageManager bun version');
    failures++;
  }
  const engines = recordProperty(rootPackage, 'engines');
  if (!engines || !stringProperty(engines, 'node')) {
    console.error('package.json engines.node must be defined');
    failures++;
  }
  return failures;
}

function validateNxReleaseConfig(root: string): number {
  const nxJson = readJsonObject(join(root, 'nx.json'));
  if (!nxJson) {
    console.error('nx.json not found or invalid');
    return 1;
  }
  const release = recordProperty(nxJson, 'release');
  const version = release ? recordProperty(release, 'version') : null;
  let failures = 0;
  if (!release) {
    console.error('nx.json release config is missing');
    failures++;
  }
  if (release && stringProperty(release, 'projectsRelationship') !== 'independent') {
    console.error('nx.json release.projectsRelationship must be independent');
    failures++;
  }
  if (!version) {
    console.error('nx.json release.version config is missing');
    failures++;
  }
  if (version && stringProperty(version, 'specifierSource') !== 'conventional-commits') {
    console.error('nx.json release.version.specifierSource must be conventional-commits');
    failures++;
  }
  if (version && !stringProperty(version, 'preVersionCommand')) {
    console.error('nx.json release.version.preVersionCommand must be defined');
    failures++;
  }
  return failures;
}

function validateCommitMessageCommand(path: string | undefined): void {
  if (!path) {
    throw new Error('Usage: smoo monorepo validate-commit-msg <commit-msg-file>');
  }
  const message = readFileSync(path, 'utf8');
  const error = validateCommitMessage(message);
  if (error) {
    throw new Error(error);
  }
}

function validateCommitMessage(message: string): string | null {
  const subject = message.split('\n', 1)[0]?.trim() ?? '';
  if (!subject) {
    return 'Commit message subject is empty.';
  }
  if (/^(Merge|Revert ")/.test(subject) || /^(fixup|squash)! /.test(subject)) {
    return null;
  }
  const match = /^(?<type>[a-z]+)(\([a-z0-9._/-]+\))?(?<breaking>!)?: .+$/.exec(subject);
  const type = match?.groups?.type;
  if (type && validCommitTypes.has(type)) {
    return null;
  }
  return `Invalid conventional commit subject: ${subject}

Expected examples:
  feat(statebus-core): add optimistic transactions
  fix(money): round negative amounts consistently
  chore(release): publish 1.2.3
  feat!: remove deprecated API`;
}

function syncBunLockfileVersions(root: string): void {
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    throw new Error('bun.lock not found');
  }
  const packages = getWorkspacePackages(root);
  let lockfile = readFileSync(lockfilePath, 'utf8');
  let updated = 0;
  for (const pkg of packages) {
    const relativePath = pkg.path.replaceAll('\\', '/');
    const escaped = escapeRegex(relativePath);
    const pattern = new RegExp(`("${escaped}":\\s*\\{[^}]*"version":\\s*")([^"]+)(")`);
    const match = lockfile.match(pattern);
    if (!match) {
      console.log(`skip: ${relativePath} (not found in lockfile)`);
      continue;
    }
    const lockVersion = match[2];
    if (lockVersion === pkg.version) {
      console.log(`ok:   ${relativePath} = ${pkg.version}`);
      continue;
    }
    lockfile = lockfile.replace(pattern, `$1${pkg.version}$3`);
    console.log(`fix:  ${relativePath}: ${lockVersion} -> ${pkg.version}`);
    updated++;
  }
  if (updated > 0) {
    writeFileSync(lockfilePath, lockfile);
  }
  console.log(
    updated > 0 ? `Updated ${updated} workspace version(s) in bun.lock` : 'All workspace versions already in sync.',
  );
}

function listPublicPackages(root: string): PackageInfo[] {
  return getWorkspacePackages(root).filter((pkg) => !pkg.private && pkg.tags.includes('npm:public'));
}

function validatePublicTags(root: string): number {
  let failures = 0;
  for (const pkg of getWorkspacePackages(root)) {
    const hasPublicTag = pkg.tags.includes('npm:public');
    if (pkg.private && hasPublicTag) {
      console.error(`${pkg.path}: private package must not have nx tag npm:public`);
      failures++;
    }
    if (!pkg.private && !hasPublicTag) {
      console.error(`${pkg.path}: public package must have nx tag npm:public`);
      failures++;
    }
  }
  if (failures > 0) {
    return failures;
  }
  console.log('npm:public tags are valid.');
  return 0;
}

function validatePublicPackageMetadata(root: string): number {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  const rootRepository = rootPackage ? repositoryInfo(rootPackage) : null;
  let failures = 0;
  for (const pkg of listPublicPackages(root)) {
    if (!stringProperty(pkg.json, 'license')) {
      console.error(`${pkg.path}: public package must define license`);
      failures++;
    }
    const publishConfig = recordProperty(pkg.json, 'publishConfig');
    if (!publishConfig || stringProperty(publishConfig, 'access') !== 'public') {
      console.error(`${pkg.path}: public package must define publishConfig.access = public`);
      failures++;
    }
    const repository = recordProperty(pkg.json, 'repository');
    const packageRepository = repository ? repositoryInfo(pkg.json) : null;
    if (!rootRepository || !packageRepository || packageRepository.url !== rootRepository.url) {
      console.error(`${pkg.path}: public package repository.url must match root package.json repository.url`);
      failures++;
    }
    if (!repository || stringProperty(repository, 'directory') !== pkg.path.replaceAll('\\', '/')) {
      console.error(`${pkg.path}: public package repository.directory must be ${pkg.path.replaceAll('\\', '/')}`);
      failures++;
    }
    if (!Array.isArray(pkg.json.files)) {
      console.error(`${pkg.path}: public package must define files`);
      failures++;
    }
    if (!isRecord(pkg.json.exports) && !isRecord(pkg.json.bin)) {
      console.error(`${pkg.path}: public package must define exports or bin`);
      failures++;
    }
    if (!hasOwnString(pkg.json, 'types') && !isRecord(pkg.json.bin)) {
      console.error(`${pkg.path}: public library package must define types`);
      failures++;
    }
  }
  return failures;
}

function validateWorkspaceDependencies(root: string): number {
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  let failures = 0;
  for (const pkg of listPackageJsonRecords(root)) {
    for (const field of workspaceDependencyFields) {
      const dependencies = recordProperty(pkg.json, field);
      if (!dependencies) {
        continue;
      }
      for (const [name, range] of Object.entries(dependencies)) {
        if (workspaceNames.has(name) && range !== 'workspace:*') {
          console.error(`${pkg.path}: ${field}.${name} must use workspace:*`);
          failures++;
        }
      }
    }
  }
  if (failures === 0) {
    console.log('Workspace dependency ranges are valid.');
  }
  return failures;
}

async function validatePackageHygiene(root: string): Promise<number> {
  const status = await runStatus('sherif', [], root);
  if (status !== 0) {
    console.error('sherif package hygiene validation failed');
    return 1;
  }
  return 0;
}

async function validatePackedPublicPackages(root: string): Promise<number> {
  let failures = 0;
  for (const pkg of listPublicPackages(root)) {
    failures += await validatePackedPublicPackage(root, pkg);
  }
  return failures;
}

async function validatePackedPublicPackage(root: string, pkg: PackageInfo): Promise<number> {
  let failures = 0;
  const packed = await packPackage(root, pkg);
  try {
    const lint = await publint({ pack: { tarball: packed.arrayBuffer }, level: 'warning', strict: true });
    for (const message of lint.messages) {
      console.error(`${pkg.path}: publint ${message.type} ${message.code}: ${formatMessage(message, lint.pkg)}`);
      failures++;
    }

    const attwArgs = [
      packed.path,
      '--format',
      'table',
      '--no-color',
      '--profile',
      'node16',
      '--ignore-rules',
      'cjs-resolves-to-esm',
      ...attwExcludedEntrypointArgs(pkg),
    ];
    const attwStatus = await runStatus('attw', attwArgs, root);
    if (attwStatus !== 0) {
      console.error(`${pkg.path}: are-the-types-wrong validation failed`);
      failures++;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`${pkg.path}: packed package validation failed: ${message}`);
    failures++;
  } finally {
    unlinkSync(packed.path);
  }
  if (failures === 0) {
    console.log(`${pkg.path}: packed package is valid.`);
  }
  return failures;
}

function attwExcludedEntrypointArgs(pkg: PackageInfo): string[] {
  const excluded = wasmExportEntrypoints(pkg.json.exports);
  return excluded.length === 0 ? [] : ['--exclude-entrypoints', ...excluded];
}

function wasmExportEntrypoints(exports: unknown): string[] {
  if (!isRecord(exports)) {
    return [];
  }
  return Object.entries(exports)
    .filter(([key, value]) => key.startsWith('.') && exportPointsToWasm(value))
    .map(([key]) => key);
}

function exportPointsToWasm(value: unknown): boolean {
  if (typeof value === 'string') {
    return value.endsWith('.wasm');
  }
  return isRecord(value) && Object.values(value).some(exportPointsToWasm);
}

async function packPackage(root: string, pkg: PackageInfo): Promise<{ path: string; arrayBuffer: ArrayBuffer }> {
  const packageDir = join(root, pkg.path);
  const tarballName = `.smoo-${process.pid}-${Date.now()}.tgz`;
  const tarballPath = join(root, tarballName);
  try {
    await run('bun', ['pm', 'pack', '--filename', tarballName, '--ignore-scripts', '--quiet'], packageDir);
    const bytes = new Uint8Array(readFileSync(tarballPath));
    return { path: tarballPath, arrayBuffer: bytes.slice().buffer };
  } catch (error) {
    rmSync(tarballPath, { force: true });
    throw error;
  }
}

async function printReleaseState(root: string): Promise<void> {
  console.log(JSON.stringify(await getReleaseState(root), null, 2));
}

async function npmVersionExists(name: string, version: string): Promise<boolean> {
  const result = await $`bun pm view ${`${name}@${version}`} version`.cwd(process.cwd()).quiet().nothrow();
  return result.exitCode === 0 && decode(result.stdout).trim() === version;
}

async function releaseVersion(root: string, args: Map<string, string | boolean>): Promise<void> {
  const bump = releaseBumpArg(args);
  const dryRun = booleanArg(args, 'dry-run');
  const projects = listPublicPackages(root)
    .map((pkg) => pkg.name)
    .join(',');
  const state = await getReleaseState(root);
  if (state.allPublished) {
    console.log('Current package versions are already published; skipping version bump.');
    return;
  }
  if ((await gitTagsAtHead(root)).length > 0) {
    console.log('HEAD already has release tags; skipping version bump and resuming publish.');
    return;
  }

  const nxArgs = ['release', 'version'];
  if (bump !== 'auto') {
    nxArgs.push(bump);
  }
  nxArgs.push(`--projects=${projects}`, '--yes');
  if (dryRun) {
    nxArgs.push('--dryRun');
  }
  await run('nx', nxArgs, root);

  if (existsSync(join(root, 'bun.lock'))) {
    syncBunLockfileVersions(root);
  }
  if (dryRun) {
    return;
  }
  if (existsSync(join(root, 'bun.lock'))) {
    await run('git', ['add', 'bun.lock'], root);
  }
  if ((await runStatus('git', ['diff', '--cached', '--quiet'], root)) !== 0) {
    await run('git', ['commit', '-m', 'chore(release): sync bun lockfile versions'], root);
  }
  await run('git', ['push'], root);
  await run('git', ['push', '--tags'], root);
}

async function releasePublish(root: string, args: Map<string, string | boolean>): Promise<void> {
  const tag = releaseNpmTagArg(args);
  const dryRun = booleanArg(args, 'dry-run');
  const projects = listPublicPackages(root)
    .map((pkg) => pkg.name)
    .join(',');
  const nxArgs = ['release', 'publish', `--projects=${projects}`, '--tag', tag];
  if (dryRun) {
    nxArgs.push('--dryRun');
  }
  await run('nx', nxArgs, root);
}

async function releaseGithubRelease(root: string, args: Map<string, string | boolean>): Promise<void> {
  if (booleanArg(args, 'dry-run')) {
    console.log('Dry run; skipping GitHub Release creation.');
    return;
  }
  const npmTag = releaseNpmTagArg(args);
  const tagsArg = stringArg(args, 'tags');
  const tags = tagsArg ? tagsArg.split(/\s+/).filter(Boolean) : await gitTagsAtHead(root);
  if (tags.length === 0) {
    throw new Error('No release tags found. Pass --tags or run from a tagged release commit.');
  }
  const latestFlag = npmTag === 'latest' ? 'true' : 'false';
  for (const tag of tags) {
    const exists = (await runStatus('gh', ['release', 'view', tag], root, true)) === 0;
    if (exists) {
      await run('gh', ['release', 'edit', tag, '--title', tag, `--latest=${latestFlag}`], root);
    } else {
      await run('gh', ['release', 'create', tag, '--title', tag, '--generate-notes', `--latest=${latestFlag}`], root);
    }
  }
}

async function cleanupGithubCiCache(root: string): Promise<void> {
  await run('nix-collect-garbage', ['--quiet'], root);
  const nar = process.env.NIX_STORE_NAR;
  if (!nar) {
    return;
  }
  await runStatus('/nix/var/nix/profiles/default/bin/nix-store', ['--verify', '--check-contents', '--repair'], root);
  const rootsResult = await $`sudo find /nix/var/nix/gcroots -type l -exec readlink {} ;`.cwd(root).quiet().nothrow();
  const roots = decode(rootsResult.stdout)
    .split('\n')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .sort()
    .join(' ');
  if (!roots) {
    return;
  }
  await $`bash -lc ${`sudo /nix/var/nix/profiles/default/bin/nix-store --export --quiet $(sudo /nix/var/nix/profiles/default/bin/nix-store -qR ${roots} 2>/dev/null) > "${nar}" || true`}`.cwd(
    root,
  );
}

async function githubCiNxSmart(root: string, args: Map<string, string | boolean>): Promise<void> {
  const target = requiredStringArg(args, 'target');
  const name = stringArg(args, 'name') ?? target;
  const step = stringArg(args, 'step') ?? '';
  await createGithubStatus(name, step);
  const mode =
    process.env.GITHUB_EVENT_NAME === 'push' && process.env.GITHUB_REF_NAME === 'main' ? 'run-many' : 'affected';
  const nxArgs =
    mode === 'run-many' ? ['run-many', '-t', target, '--parallel=5'] : ['affected', '-t', target, '--parallel=5'];
  const status = await runStatus('nx', nxArgs, root);
  await updateGithubStatus(name, status === 0 ? 'success' : 'failure', step);
  if (status !== 0) {
    throw new Error(`nx ${nxArgs.join(' ')} failed with exit code ${status}`);
  }
}

async function githubCiNxRunMany(root: string, args: Map<string, string | boolean>): Promise<void> {
  const targets = requiredStringArg(args, 'targets');
  const projects = stringArg(args, 'projects');
  const nxArgs = ['run-many', '-t', targets, '--parallel=5'];
  if (projects) {
    nxArgs.push(`--projects=${projects}`);
  }
  await run('nx', nxArgs, root);
}

async function createGithubStatus(name: string, step: string): Promise<void> {
  await postGithubStatus(name, 'pending', `Running ${name}...`, step);
}

async function updateGithubStatus(name: string, state: 'success' | 'failure' | 'error', step: string): Promise<void> {
  const suffix = state === 'success' ? 'passed' : state === 'failure' ? 'failed' : 'errored';
  await postGithubStatus(name, state, `${name} ${suffix}`, step);
}

async function postGithubStatus(name: string, state: string, description: string, step: string): Promise<void> {
  const repository = process.env.GITHUB_REPOSITORY;
  const sha = process.env.GITHUB_SHA;
  if (!repository || !sha) {
    return;
  }
  const targetUrl = await getGithubStepUrl(step);
  const args = [
    'api',
    '--method',
    'POST',
    '-H',
    'Accept: application/vnd.github+json',
    `/repos/${repository}/statuses/${sha}`,
    '-f',
    `state=${state}`,
    '-f',
    `context=${name}`,
    '-f',
    `description=${description}`,
  ];
  if (targetUrl) {
    args.push('-f', `target_url=${targetUrl}`);
  }
  await run('gh', args, process.cwd());
}

async function getGithubStepUrl(step: string): Promise<string | null> {
  const repository = process.env.GITHUB_REPOSITORY;
  const runId = process.env.GITHUB_RUN_ID;
  const job = process.env.GITHUB_JOB;
  if (!repository || !runId || !job) {
    return null;
  }
  const result =
    await $`gh api -H ${'Accept: application/vnd.github+json'} ${`/repos/${repository}/actions/runs/${runId}/jobs`} --jq ${`.jobs[] | select(.name == "${job}") | .id`}`
      .quiet()
      .nothrow();
  const jobId = decode(result.stdout).trim();
  if (!jobId) {
    return `https://github.com/${repository}/actions/runs/${runId}`;
  }
  return step
    ? `https://github.com/${repository}/actions/runs/${runId}/job/${jobId}#step:${step}:1`
    : `https://github.com/${repository}/actions/runs/${runId}/job/${jobId}`;
}

interface ReleaseState {
  packages: { name: string; version: string; published: boolean }[];
  allPublished: boolean;
}

async function getReleaseState(root: string): Promise<ReleaseState> {
  const packages = listPublicPackages(root);
  const states = await Promise.all(
    packages.map(async (pkg) => {
      const published = await npmVersionExists(pkg.name, pkg.version);
      return { name: pkg.name, version: pkg.version, published };
    }),
  );
  return { packages: states, allPublished: states.every((state) => state.published) };
}

async function gitTagsAtHead(root: string): Promise<string[]> {
  const result = await $`git tag --points-at HEAD`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    return [];
  }
  return decode(result.stdout)
    .split('\n')
    .map((tag) => tag.trim())
    .filter(Boolean);
}

function stringArg(args: Map<string, string | boolean>, key: string): string | null {
  const value = args.get(key);
  return typeof value === 'string' ? value : null;
}

function requiredStringArg(args: Map<string, string | boolean>, key: string): string {
  const value = stringArg(args, key);
  if (!value) {
    throw new Error(`Missing required --${key}`);
  }
  return value;
}

function releaseBumpArg(args: Map<string, string | boolean>): string {
  const bump = stringArg(args, 'bump') ?? 'auto';
  if (!['auto', 'patch', 'minor', 'major', 'prerelease'].includes(bump)) {
    throw new Error(`Invalid --bump "${bump}". Expected auto, patch, minor, major, or prerelease.`);
  }
  return bump;
}

function releaseNpmTagArg(args: Map<string, string | boolean>): string {
  const bump = releaseBumpArg(args);
  const derivedTag = bump === 'prerelease' ? 'next' : 'latest';
  const explicitTag = stringArg(args, 'tag') ?? stringArg(args, 'npm-tag');
  if (!explicitTag) {
    return derivedTag;
  }
  if (explicitTag !== derivedTag) {
    throw new Error(`--bump ${bump} publishes with npm dist-tag ${derivedTag}, not ${explicitTag}.`);
  }
  return explicitTag;
}

function booleanArg(args: Map<string, string | boolean>, key: string): boolean {
  const value = args.get(key);
  return value === true || value === 'true';
}

async function run(command: string, args: string[], cwd: string): Promise<void> {
  const status = await runStatus(command, args, cwd);
  if (status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${status}`);
  }
}

async function runStatus(command: string, args: string[], cwd: string, quiet = false): Promise<number> {
  const invocation = resolveCommandInvocation(cwd, command, args);
  const shell = $`${invocation.command} ${invocation.args}`.cwd(cwd).nothrow();
  const result = quiet ? await shell.quiet() : await shell;
  return result.exitCode;
}

function resolveCommandInvocation(root: string, command: string, args: string[]): { command: string; args: string[] } {
  const localCommand = join(root, 'node_modules', '.bin', command);
  if (existsSync(localCommand)) {
    return { command: localCommand, args };
  }
  const bundledCommand = resolveBundledCommand(command);
  if (bundledCommand) {
    return { command: 'bun', args: [bundledCommand, ...args] };
  }
  return { command, args };
}

function resolveBundledCommand(command: string): string | null {
  try {
    if (command === 'sherif') {
      return fileURLToPath(import.meta.resolve('sherif'));
    }
    if (command === 'attw') {
      const packageJson = fileURLToPath(import.meta.resolve('@arethetypeswrong/cli/package.json'));
      return join(dirname(packageJson), 'dist', 'index.js');
    }
  } catch {
    return null;
  }
  return null;
}

async function findRepoRoot(): Promise<string> {
  const result = await $`git rev-parse --show-toplevel`.cwd(process.cwd()).quiet().nothrow();
  if (result.exitCode === 0) {
    return decode(result.stdout).trim();
  }
  return process.cwd();
}

function decode(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

function repositoryInfo(pkg: Record<string, unknown>): RepositoryInfo | null {
  const repository = pkg.repository;
  if (typeof repository === 'string') {
    return { type: 'git', url: repository };
  }
  if (!isRecord(repository)) {
    return null;
  }
  const url = stringProperty(repository, 'url');
  if (!url) {
    return null;
  }
  return { type: stringProperty(repository, 'type') ?? 'git', url };
}

function stringProperty(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function recordProperty(record: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = record[key];
  return isRecord(value) ? value : null;
}

function getOrCreateRecord(record: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = record[key];
  if (isRecord(value)) {
    return value;
  }
  const next: Record<string, unknown> = {};
  record[key] = next;
  return next;
}

function setStringProperty(record: Record<string, unknown>, key: string, value: string): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

function setMissingStringProperty(record: Record<string, unknown>, key: string, value: string): boolean {
  if (typeof record[key] === 'string') {
    return false;
  }
  record[key] = value;
  return true;
}

function fixWorkspaceDependencyRanges(pkg: Record<string, unknown>, workspaceNames: Set<string>): boolean {
  let changed = false;
  for (const field of workspaceDependencyFields) {
    const dependencies = recordProperty(pkg, field);
    if (!dependencies) {
      continue;
    }
    for (const name of Object.keys(dependencies)) {
      if (workspaceNames.has(name) && dependencies[name] !== 'workspace:*') {
        dependencies[name] = 'workspace:*';
        changed = true;
      }
    }
  }
  return changed;
}

function normalizeExportConditionOrder(value: unknown): boolean {
  if (!isRecord(value)) {
    return false;
  }
  let changed = false;
  for (const child of Object.values(value)) {
    changed = normalizeExportConditionOrder(child) || changed;
  }
  const keys = Object.keys(value);
  if (!keys.includes('types') && !keys.includes('default')) {
    return changed;
  }
  const ordered = [
    ...(keys.includes('types') ? ['types'] : []),
    ...keys.filter((key) => key !== 'types' && key !== 'default'),
    ...(keys.includes('default') ? ['default'] : []),
  ];
  if (keys.join('\n') === ordered.join('\n')) {
    return changed;
  }
  const entries = new Map(keys.map((key) => [key, value[key]]));
  for (const key of keys) {
    delete value[key];
  }
  for (const key of ordered) {
    value[key] = entries.get(key);
  }
  return true;
}

function hasDevelopmentSourceExport(value: unknown): boolean {
  if (!isRecord(value)) {
    return false;
  }
  for (const [key, child] of Object.entries(value)) {
    if ((key === 'development' || key === 'bun') && typeof child === 'string' && child.startsWith('./src/')) {
      return true;
    }
    if (hasDevelopmentSourceExport(child)) {
      return true;
    }
  }
  return false;
}

function addFileEntry(pkg: Record<string, unknown>, entry: string): boolean {
  const files = pkg.files;
  if (!Array.isArray(files) || files.includes(entry)) {
    return false;
  }
  const firstNegated = files.findIndex((file) => typeof file === 'string' && file.startsWith('!'));
  if (firstNegated === -1) {
    files.push(entry);
  } else {
    files.splice(firstNegated, 0, entry);
  }
  return true;
}

function requiredJsonObject(path: string): Record<string, unknown> {
  const json = readJsonObject(path);
  if (!json) {
    throw new Error(`${path} not found or invalid`);
  }
  return json;
}

function readJsonObject(path: string): Record<string, unknown> | null {
  const json = readJson(path);
  return isRecord(json) ? json : null;
}

function writeJsonObject(path: string, value: Record<string, unknown>): void {
  writeFileSync(path, jsonObjectText(value));
}

function jsonObjectText(value: Record<string, unknown>): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

function getWorkspacePackages(root: string): PackageInfo[] {
  if (!readPackageJson(join(root, 'package.json'))) {
    throw new Error('package.json not found or invalid');
  }
  const workspacePatterns = getWorkspacePatterns(root);
  const packages: PackageInfo[] = [];
  for (const pattern of workspacePatterns) {
    if (!pattern.endsWith('/*')) {
      continue;
    }
    const parent = join(root, pattern.slice(0, -2));
    if (!existsSync(parent) || !statSync(parent).isDirectory()) {
      continue;
    }
    for (const entry of readdirSync(parent)) {
      const pkgPath = join(parent, entry, 'package.json');
      const pkg = readPackageJson(pkgPath);
      if (!pkg?.name || !pkg.version) {
        continue;
      }
      packages.push({
        name: pkg.name,
        version: pkg.version,
        private: pkg.private,
        tags: pkg.tags,
        path: relative(root, dirname(pkgPath)),
        packageJsonPath: pkg.packageJsonPath,
        json: pkg.json,
      });
    }
  }
  return packages.sort((a, b) => a.name.localeCompare(b.name));
}

function listPackageJsonRecords(root: string): PackageInfo[] {
  const rootPackage = readPackageJson(join(root, 'package.json'));
  if (!rootPackage) {
    throw new Error('package.json not found or invalid');
  }
  return [{ ...rootPackage, path: '.' }, ...getWorkspacePackages(root)];
}

function getWorkspacePatterns(root: string): string[] {
  const raw = readJson(join(root, 'package.json'));
  if (!isRecord(raw) || !hasOwn(raw, 'workspaces')) {
    return ['packages/*'];
  }
  const workspaces = raw.workspaces;
  if (Array.isArray(workspaces)) {
    return workspaces.filter((entry): entry is string => typeof entry === 'string');
  }
  if (isRecord(workspaces) && hasOwn(workspaces, 'packages') && Array.isArray(workspaces.packages)) {
    return workspaces.packages.filter((entry): entry is string => typeof entry === 'string');
  }
  return ['packages/*'];
}

function readPackageJson(path: string): PackageInfo | null {
  const parsed = readJsonObject(path);
  if (!isRecord(parsed) || !hasOwnString(parsed, 'name') || !hasOwnString(parsed, 'version')) {
    return null;
  }
  const privateValue = hasOwn(parsed, 'private') && typeof parsed.private === 'boolean' ? parsed.private : false;
  const tags = getNxTags(parsed);
  return {
    name: parsed.name,
    version: parsed.version,
    private: privateValue,
    tags,
    path: dirname(path),
    packageJsonPath: path,
    json: parsed,
  };
}

function getNxTags(pkg: Record<string, unknown>): string[] {
  if (!hasOwn(pkg, 'nx') || !isRecord(pkg.nx) || !hasOwn(pkg.nx, 'tags') || !Array.isArray(pkg.nx.tags)) {
    return [];
  }
  return pkg.nx.tags.filter((tag): tag is string => typeof tag === 'string');
}

function readJson(path: string): unknown {
  if (!existsSync(path)) {
    return null;
  }
  return JSON.parse(readFileSync(path, 'utf8'));
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
