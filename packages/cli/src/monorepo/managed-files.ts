import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { generateManagedFiles } from '@smoothbricks/nx-plugin/managed-files/generator';
import type { FileResult } from '@smoothbricks/nx-plugin/managed-files/tree';
import { FsTree } from 'nx/src/generators/tree.js';
import { loadNxProjects } from '../nx/index.js';
import { finishManagedFiles } from './managed-fs.js';

export * from '@smoothbricks/nx-plugin/managed-files/context';
export {
  type ManagedFileContext,
  managedFileTargetsForContext,
  managedFileTargetsForTest,
  renderManagedWorkflowForTest,
} from '@smoothbricks/nx-plugin/managed-files/files';
export {
  INLINE_LOCAL_BEGIN,
  INLINE_LOCAL_END,
  LOCAL_SECTION_MARKER,
} from '@smoothbricks/nx-plugin/managed-files/managed-content';
export type { FileResult } from '@smoothbricks/nx-plugin/managed-files/tree';

export async function applyManagedFiles(root: string, mode: 'update' | 'check' | 'diff'): Promise<FileResult[]> {
  const projects = await loadNxProjects(root);
  const tree = new FsTree(root, false);
  return finishManagedFiles(root, tree, generateManagedFiles(tree, projects), mode);
}

export function printResults(results: FileResult[]): void {
  for (const result of results) {
    console.log(`${result.action.padEnd(15)} ${result.target}${result.reason ? ` — ${result.reason}` : ''}`);
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

/** The devenv module is inert until the repo-owned devenv.nix imports it. */
export const DEVENV_MODULE_IMPORT = './devenv.smoo.nix';

/**
 * Report, never rewrite: the import belongs to a repo-owned Nix file whose
 * shape this tool has no business editing. A missing import would otherwise be
 * silent — the managed module simply would not apply.
 */
export function validateDevenvModuleImport(root: string): number {
  const target = join(root, 'tooling/direnv/devenv.nix');
  if (!existsSync(target)) {
    return 0;
  }
  if (readFileSync(target, 'utf8').includes(DEVENV_MODULE_IMPORT)) {
    return 0;
  }
  console.error(
    `tooling/direnv/devenv.nix does not import ${DEVENV_MODULE_IMPORT}: add "imports = [${DEVENV_MODULE_IMPORT}];" so the managed shell contract applies`,
  );
  return 1;
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
