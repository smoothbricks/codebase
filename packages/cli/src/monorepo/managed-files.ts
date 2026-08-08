import { appendFileSync, existsSync, lstatSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { PLATFORM_TARGET_GLOBS } from '@smoothbricks/nx-plugin/workspace-config-policy';
// Nx package exports are CLI-oriented; this is the module the CLI uses for graph + daemon IPC.
import { createProjectGraphAsync } from 'nx/src/project-graph/project-graph.js';
import type { PackageJson } from '../lib/json.js';
import { listReleasePackages, readPackageJson } from '../lib/workspace.js';
import { renderCiWorkflowYaml } from './ci-workflow.js';
import { renderPublishWorkflowYaml } from './publish-workflow.js';

type ManagedKind = 'raw' | 'template' | 'generated';

/**
 * Repos may append their own content to a managed file below this marker —
 * e.g. extra merge drivers in .gitattributes. Everything from the marker
 * line onward is preserved verbatim across updates and ignored by the
 * drift check; the managed section above it stays byte-exact.
 */
export const LOCAL_SECTION_MARKER = '# smoo-local: everything below this line is repo-owned and preserved';

interface ManagedFile {
  kind: ManagedKind;
  source: string;
  target: string;
  executable?: boolean;
  releasePackagesOnly?: boolean;
}

/** Split a managed target's content into the managed part and the repo-owned tail. */
function splitLocalSection(current: string): { managed: string; localTail: string } {
  const index = current.indexOf(LOCAL_SECTION_MARKER);
  if (index === -1) return { managed: current, localTail: '' };
  return { managed: current.slice(0, index), localTail: current.slice(index) };
}

/** Test seam for the pure splitter. */
export const splitLocalSectionForTest = splitLocalSection;

/**
 * A repo-owned block INSIDE the managed section — e.g. one extra pattern
 * spliced into a formatter's list, where a trailing marker (LOCAL_SECTION_MARKER)
 * can't express it because it isn't at the end of the file. Wrap it in
 * `# smoo-local-begin` / `# smoo-local-end`; the block is anchored to the line
 * immediately before `# smoo-local-begin`. On update, the block is re-spliced
 * right after that same anchor line in the freshly rendered template — if the
 * anchor no longer appears there (the template reworked that section), the
 * update refuses rather than silently dropping the repo's customization.
 */
export const INLINE_LOCAL_BEGIN = '# smoo-local-begin';
export const INLINE_LOCAL_END = '# smoo-local-end';

interface InlineLocalBlock {
  anchor: string;
  lines: string;
  markerIndent?: string;
}

/** Pull inline local blocks out of a managed section, returning the section
 * with each block (and its markers) removed, plus the extracted blocks in
 * the order they appeared. */
function extractInlineLocalBlocks(managed: string): { withoutInline: string; blocks: InlineLocalBlock[] } {
  const lines = managed.split('\n');
  const kept: string[] = [];
  const blocks: InlineLocalBlock[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (line !== undefined && line.trim() === INLINE_LOCAL_BEGIN) {
      const anchor = kept.at(-1);
      if (anchor === undefined) {
        throw new Error(`${INLINE_LOCAL_BEGIN} on line ${i + 1} has no preceding anchor line`);
      }
      const blockLines: string[] = [];
      i += 1;
      while (i < lines.length && lines[i]?.trim() !== INLINE_LOCAL_END) {
        blockLines.push(lines[i] as string);
        i += 1;
      }
      if (i >= lines.length) {
        throw new Error(`${INLINE_LOCAL_BEGIN} anchored on "${anchor}" has no matching ${INLINE_LOCAL_END}`);
      }
      const markerIndent = line.slice(0, line.length - line.trimStart().length);
      blocks.push({
        anchor,
        lines: blockLines.join('\n'),
        ...(markerIndent === '' ? {} : { markerIndent }),
      });
      i += 1; // skip the END marker line itself
      continue;
    }
    kept.push(line);
    i += 1;
  }
  return { withoutInline: kept.join('\n'), blocks };
}

/** Test seam for the pure extractor. */
export const extractInlineLocalBlocksForTest = extractInlineLocalBlocks;

/** Re-splice extracted inline blocks into freshly rendered managed content,
 * each immediately after its anchor line. A no-op when there are no blocks. */
function reinsertInlineLocalBlocks(content: string, blocks: InlineLocalBlock[]): string {
  if (blocks.length === 0) return content;
  const lines = content.split('\n');
  for (const block of blocks) {
    const index = lines.indexOf(block.anchor);
    if (index === -1) {
      throw new Error(
        `${INLINE_LOCAL_BEGIN} block anchored on "${block.anchor}" no longer matches any line in the updated ` +
          'template — reconcile the repo-owned block manually',
      );
    }
    const markerIndent = block.markerIndent ?? '';
    lines.splice(
      index + 1,
      0,
      `${markerIndent}${INLINE_LOCAL_BEGIN}`,
      ...block.lines.split('\n'),
      `${markerIndent}${INLINE_LOCAL_END}`,
    );
  }
  return lines.join('\n');
}

/** Test seam for the pure re-splicer. */
export const reinsertInlineLocalBlocksForTest = reinsertInlineLocalBlocks;

export interface FileResult {
  target: string;
  action: 'created' | 'updated' | 'unchanged' | 'skipped' | 'skipped-symlink' | 'drifted' | 'ok-symlink';
}

interface ManagedFileContext {
  hasReleasePackages: boolean;
  hasStagingDeployTargets: boolean;
  hasProductionDeployTargets: boolean;
  stagingDeployProvider?: 'cloudflare';
  productionDeployProvider?: 'cloudflare';
  ciPushBranches: string[];
  ciRunsOn: string | string[];
  nodeModulesCacheKey: string;
  repoName: string;
  platformTargetGlobs: string[];
}

interface DeployTargetInfo {
  exists: boolean;
  provider?: 'cloudflare';
}

const managedFiles: ManagedFile[] = [
  {
    kind: 'raw',
    source: 'envrc',
    target: '.envrc',
  },
  {
    kind: 'raw',
    source: 'tooling/devenv',
    target: 'tooling/devenv',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/repo-path',
    target: 'tooling/direnv/repo-path',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/github-actions-bootstrap.sh',
    target: 'tooling/direnv/github-actions-bootstrap.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/setup-environment.ts',
    target: 'tooling/direnv/setup-environment.ts',
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
    source: 'git-format-staged.yml',
    target: '.git-format-staged.yml',
  },
  {
    kind: 'raw',
    source: 'gitattributes',
    target: '.gitattributes',
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/merge-newer-pins.sh',
    target: 'tooling/direnv/merge-newer-pins.sh',
    executable: true,
  },
  {
    kind: 'generated',
    source: 'ci-workflow',
    target: '.github/workflows/ci.yml',
  },
  {
    kind: 'generated',
    source: 'publish-workflow',
    target: '.github/workflows/publish.yml',
    releasePackagesOnly: true,
  },
  {
    kind: 'template',
    source: 'github/workflows/managed-files.yml',
    target: '.github/workflows/managed-files.yml',
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
    source: 'github/actions/cache-ttsc-plugins/action.yml',
    target: '.github/actions/cache-ttsc-plugins/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-nx/action.yml',
    target: '.github/actions/cache-nx/action.yml',
  },
];

export const managedFileTargetsForTest = managedFiles.map(({ target, executable }) => ({ target, executable }));

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

export async function applyManagedFiles(root: string, mode: 'update' | 'check' | 'diff'): Promise<FileResult[]> {
  const context = await getManagedFileContext(root);
  return managedFiles.map((file) => applyManagedFile(root, file, mode, context));
}

function applyManagedFile(
  root: string,
  file: ManagedFile,
  mode: 'update' | 'check' | 'diff',
  context: ManagedFileContext,
): FileResult {
  if (file.releasePackagesOnly === true && !context.hasReleasePackages && !context.hasProductionDeployTargets) {
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
    const { managed, localTail } = splitLocalSection(current);
    const { withoutInline, blocks } = extractInlineLocalBlocks(managed);
    if (withoutInline === content || (localTail !== '' && withoutInline === `${content}\n`)) {
      return { target: file.target, action: 'unchanged' };
    }
    if (mode === 'check' || mode === 'diff') {
      return { target: file.target, action: 'drifted' };
    }
    const rendered = reinsertInlineLocalBlocks(content, blocks);
    const next = localTail === '' ? rendered : `${rendered}\n${localTail}`;
    writeManagedFile(target, next, file.executable === true);
    return { target: file.target, action: 'updated' };
  }
  if (mode === 'check' || mode === 'diff') {
    return { target: file.target, action: 'drifted' };
  }
  writeManagedFile(target, content, file.executable === true);
  return { target: file.target, action: 'created' };
}

function getManagedContent(file: ManagedFile, context: ManagedFileContext): string {
  if (file.kind === 'generated') {
    if (file.source === 'ci-workflow') {
      return renderCiWorkflowYaml({
        deploy: context.hasStagingDeployTargets,
        deployProvider: context.stagingDeployProvider,
        pushBranches: context.ciPushBranches,
        runsOn: context.ciRunsOn,
      });
    }
    if (file.source === 'publish-workflow') {
      return renderPublishWorkflowYaml({
        deploy: context.hasProductionDeployTargets,
        deployProvider: context.productionDeployProvider,
        repoName: context.repoName,
        platformTargetGlobs: context.platformTargetGlobs,
        runsOn: context.ciRunsOn,
      });
    }
    throw new Error(`Unknown generated managed file source ${file.source}`);
  }
  const sourceRoot = file.kind === 'raw' ? 'managed/raw' : 'managed/templates';
  const sourcePath = join(packageRoot, sourceRoot, file.source);
  const content = readFileSync(sourcePath, 'utf8');
  if (file.kind === 'raw') {
    return content;
  }
  return renderTemplate(context, content);
}

async function getManagedFileContext(root: string): Promise<ManagedFileContext> {
  const packageJson = readPackageJson(join(root, 'package.json'));
  const repoName = packageJson?.name ?? 'monorepo';
  const ciPushBranches = getCiPushBranches(packageJson?.json);
  const ciRunsOn = getCiRunsOn(packageJson?.json);
  // In-process Nx API → daemon socket (no second Node/`nx` CLI process).
  const nxProjects = await loadNxGraphProjects(root);
  const stagingDeploy = deployTargetInfoFromProjects(nxProjects, 'staging');
  const productionDeploy = deployTargetInfoFromProjects(nxProjects, 'production');
  const platformTargetGlobs = platformTargetGlobsForTest(targetNamesFromProjects(nxProjects));
  const nodeModulesCacheKey = existsSync(join(root, 'bun.lock'))
    ? `$${"{{ hashFiles('bun.lock', 'package.json', 'packages/*/package.json') }}"}`
    : `$${"{{ hashFiles('bun.lockb', 'package.json', 'packages/*/package.json') }}"}`;
  return {
    hasReleasePackages: listReleasePackages(root, packageJson).length > 0,
    hasStagingDeployTargets: stagingDeploy.exists,
    hasProductionDeployTargets: productionDeploy.exists,
    stagingDeployProvider: stagingDeploy.provider,
    productionDeployProvider: productionDeploy.provider,
    ciPushBranches,
    ciRunsOn,
    nodeModulesCacheKey,
    repoName,
    platformTargetGlobs,
  };
}

export function platformTargetGlobsForTest(targetNames: Iterable<string>): string[] {
  const names = [...targetNames];
  return PLATFORM_TARGET_GLOBS.filter((glob) => {
    const suffix = glob.startsWith('*') ? glob.slice(1) : glob;
    return names.some((name) => name.endsWith(suffix));
  });
}

/** Resolved Nx project node as returned under `graph.nodes` / project-graph.json. */
export type NxGraphProjectNode = {
  data?: { targets?: Record<string, NxGraphTarget> };
  targets?: Record<string, NxGraphTarget>;
};

type NxGraphTarget = {
  command?: string;
  options?: { command?: string };
  configurations?: Record<string, { command?: string; options?: { command?: string } }>;
};

/**
 * Load resolved project nodes via Nx's programmatic API.
 *
 * `createProjectGraphAsync` is what the CLI uses: with the daemon up it speaks the
 * daemon unix socket instead of rebuilding the graph in a fresh Node process.
 * Spawning `nx graph` / `nx show` only added CLI boot + plugin-worker overhead on
 * top of that. smoo runs under Bun; Bun can import this CJS module directly.
 */
async function loadNxGraphProjects(root: string): Promise<Record<string, NxGraphProjectNode>> {
  const prevCwd = process.cwd();
  process.chdir(root);
  try {
    const graph = (await createProjectGraphAsync()) as {
      nodes: Record<string, { data?: { targets?: Record<string, NxGraphTarget> } }>;
    };
    const out: Record<string, NxGraphProjectNode> = {};
    for (const [name, node] of Object.entries(graph.nodes)) {
      out[name] = { data: { targets: node.data?.targets ?? {} } };
    }
    return out;
  } finally {
    process.chdir(prevCwd);
  }
}

function targetsOfProject(node: NxGraphProjectNode): Record<string, NxGraphTarget> {
  return node.data?.targets ?? node.targets ?? {};
}

/** Test seam: collect target names from a graph.nodes map. */
export function targetNamesFromProjects(nodes: Record<string, NxGraphProjectNode>): string[] {
  const targetNames = new Set<string>();
  for (const node of Object.values(nodes)) {
    for (const name of Object.keys(targetsOfProject(node))) {
      targetNames.add(name);
    }
  }
  return [...targetNames];
}

/** Test seam: deploy configuration presence/provider from graph nodes. */
export function deployTargetInfoFromProjects(
  nodes: Record<string, NxGraphProjectNode>,
  configuration: string,
): DeployTargetInfo {
  let exists = false;
  let provider: DeployTargetInfo['provider'];
  for (const node of Object.values(nodes)) {
    const info = deployTargetInfoFromTargets(targetsOfProject(node), configuration);
    if (!info.exists) {
      continue;
    }
    exists = true;
    provider ??= info.provider;
  }
  return { exists, provider };
}

function deployTargetInfoFromTargets(targets: Record<string, NxGraphTarget>, configuration: string): DeployTargetInfo {
  const deploy = targets.deploy;
  if (!deploy) {
    return { exists: false };
  }
  const config = deploy.configurations?.[configuration];
  if (!config) {
    return { exists: false };
  }
  const commandValue = config.command ?? config.options?.command ?? deploy.options?.command ?? deploy.command;
  const command = typeof commandValue === 'string' ? commandValue : '';
  return { exists: true, provider: command.includes('wrangler ') ? 'cloudflare' : undefined };
}

function renderTemplate(context: ManagedFileContext, template: string): string {
  return template
    .replaceAll('{{REPO_NAME}}', context.repoName)
    .replaceAll('__SMOO_CI_PUSH_BRANCHES__', renderYamlFlowList(context.ciPushBranches))
    .replaceAll('{{NODE_MODULES_CACHE_KEY}}', context.nodeModulesCacheKey);
}

function getCiRunsOn(packageJson: PackageJson | null | undefined): string | string[] {
  const configured = packageJson?.smoo?.github?.runsOn;
  if (configured === undefined) {
    return 'ubuntu-latest';
  }
  if (typeof configured === 'string') {
    return configured.length > 0 ? configured : 'ubuntu-latest';
  }
  const labels = configured.filter((label) => label.length > 0);
  return labels.length > 0 ? labels : 'ubuntu-latest';
}

function getCiPushBranches(packageJson: PackageJson | null | undefined): string[] {
  const configured = readCiPushBranches(packageJson);
  return configured.length > 0 ? configured : ['main'];
}

function readCiPushBranches(packageJson: PackageJson | null | undefined): string[] {
  const branches = packageJson?.smoo?.github?.pushBranches ?? [];
  return branches.filter((branch) => branch.length > 0);
}

function renderYamlFlowList(values: string[]): string {
  return JSON.stringify(values);
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

export async function validateManagedFiles(root: string): Promise<number> {
  const results = await applyManagedFiles(root, 'check');
  printResults(results);
  const failures = results.filter((result) => result.action === 'drifted').length;
  if (failures > 0) {
    console.error('Managed monorepo files are out of date. Run: smoo monorepo update');
  }
  return failures;
}

/**
 * Non-blocking drift report: prints the per-file table and surfaces drift as
 * GitHub Actions warning annotations (plain stderr elsewhere) without failing
 * the run. Managed-file drift is derived state with its own remediation flow
 * (the persistent managed-files PR); only `monorepo check` without --warn and
 * PR-scoped gates treat it as an error.
 *
 * Under GitHub Actions the drifted-file count is published as the step output
 * `drifted`, so downstream steps gate declaratively instead of parsing logs.
 */
export async function warnOnManagedFileDrift(root: string): Promise<void> {
  const results = await applyManagedFiles(root, 'check');
  printResults(results);
  const drifted = results.filter((result) => result.action === 'drifted');
  if (process.env.GITHUB_OUTPUT) {
    appendFileSync(process.env.GITHUB_OUTPUT, `drifted=${drifted.length}\n`);
  }
  if (drifted.length === 0) {
    return;
  }
  if (process.env.GITHUB_ACTIONS === 'true') {
    for (const result of drifted) {
      console.log(
        `::warning title=Managed file drift::${result.target} drifted from the @smoothbricks/cli template; run 'smoo monorepo update'`,
      );
    }
  }
  console.error(`${drifted.length} managed monorepo file(s) drifted (non-blocking). Run: smoo monorepo update`);
}
