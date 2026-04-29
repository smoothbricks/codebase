import { chmodSync, existsSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { runStatus } from '../../lib/run.js';
import { syncBunLockfileVersions, validateBunLockfileVersions } from '../lockfile.js';
import { validateManagedFiles } from '../managed-files.js';
import { fixNxSync, validateNxSync } from '../nx-sync.js';
import { fixPackageHygiene, validatePackageHygiene } from '../package-hygiene.js';
import {
  applyFixableMonorepoDefaults,
  applyNxReleaseDefaults,
  applyPublicPackageDefaults,
  applyWorkspaceDependencyDefaults,
  validateNxProjectNames,
  validateNxReleaseConfig,
  validatePublicPackageMetadata,
  validatePublicTags,
  validateRootPackagePolicy,
  validateWorkspaceDependencies,
} from '../package-policy.js';
import { validatePackedPublicPackages } from '../packed-package.js';
import { syncRootRuntimeVersions } from '../runtime.js';
import { applyToolConfigDefaults, validateToolConfig } from '../tool-validation.js';

export interface MonorepoContext {
  root: string;
  syncRuntime: boolean;
}

export interface ValidatePackOptions {
  failFast?: boolean;
  fix?: boolean;
}

export interface MonorepoPack {
  name: string;
  init?(ctx: MonorepoContext): Promise<void> | void;
  fixPreBuild?(ctx: MonorepoContext): Promise<void> | void;
  fixPostBuild?(ctx: MonorepoContext): Promise<void> | void;
  validatePreBuild?(ctx: MonorepoContext): Promise<number> | number;
  validatePostBuild?(ctx: MonorepoContext): Promise<number> | number;
}

export interface ValidatePackRunHooks {
  packs?: readonly MonorepoPack[];
  runBuild?: (ctx: MonorepoContext) => Promise<number> | number;
}

const packs: MonorepoPack[] = [
  {
    name: 'core',
    async init(ctx) {
      ensureLocalSmooShim(ctx.root);
      applyNxReleaseDefaults(ctx.root);
      if (ctx.syncRuntime) {
        await syncRootRuntimeVersions(ctx.root);
      } else {
        console.log('skip           root runtime versions (outside devenv; pass --sync-runtime to force)');
      }
    },
    async fixPreBuild(ctx) {
      applyFixableMonorepoDefaults(ctx.root);
      await applyToolConfigDefaults(ctx.root);
      syncBunLockfileVersions(ctx.root);
      await fixNxSync(ctx.root);
    },
    async validatePreBuild(ctx) {
      return (
        validateManagedFiles(ctx.root) +
        validateRootPackagePolicy(ctx.root) +
        validateToolConfig(ctx.root) +
        validateNxProjectNames(ctx.root) +
        validateNxReleaseConfig(ctx.root) +
        validateBunLockfileVersions(ctx.root) +
        (await validateNxSync(ctx.root))
      );
    },
  },
  {
    name: 'publishing',
    init(ctx) {
      applyPublicPackageDefaults(ctx.root);
    },
    fixPreBuild(ctx) {
      applyPublicPackageDefaults(ctx.root);
    },
    validatePreBuild(ctx) {
      return validatePublicTags(ctx.root) + validatePublicPackageMetadata(ctx.root);
    },
  },
  {
    name: 'workspace-dependencies',
    async init(ctx) {
      applyWorkspaceDependencyDefaults(ctx.root);
      await fixPackageHygiene(ctx.root);
    },
    async fixPostBuild(ctx) {
      applyWorkspaceDependencyDefaults(ctx.root);
      await fixPackageHygiene(ctx.root);
    },
    async validatePostBuild(ctx) {
      return validateWorkspaceDependencies(ctx.root) + (await validatePackageHygiene(ctx.root));
    },
  },
  {
    name: 'packed-packages',
    validatePostBuild(ctx) {
      return validatePackedPublicPackages(ctx.root);
    },
  },
];

export async function runInitPacks(ctx: MonorepoContext): Promise<void> {
  for (const pack of packs) {
    await pack.init?.(ctx);
  }
}

export async function runValidatePacks(
  ctx: MonorepoContext,
  options: ValidatePackOptions = {},
  hooks: ValidatePackRunHooks = {},
): Promise<number> {
  const selectedPacks = hooks.packs ?? packs;
  const build = hooks.runBuild ?? runBuild;

  if (options.fix) {
    await runFixPhase(ctx, selectedPacks, 'pre-build', 'fixPreBuild');
    const buildFailures = await build(ctx);
    if (buildFailures > 0) {
      return buildFailures;
    }
    await runFixPhase(ctx, selectedPacks, 'post-build', 'fixPostBuild');
    return runValidationPhases(ctx, selectedPacks, options, false);
  }

  const preBuildFailures = await runValidationPhase(
    ctx,
    selectedPacks,
    options,
    'pre-build',
    'validatePreBuild',
    false,
  );
  if (preBuildFailures > 0 && options.failFast) {
    return preBuildFailures;
  }
  const buildFailures = await build(ctx);
  if (buildFailures > 0) {
    return preBuildFailures + buildFailures;
  }
  return (
    preBuildFailures + (await runValidationPhase(ctx, selectedPacks, options, 'post-build', 'validatePostBuild', true))
  );
}

async function runValidationPhases(
  ctx: MonorepoContext,
  selectedPacks: readonly MonorepoPack[],
  options: ValidatePackOptions,
  printedBefore: boolean,
): Promise<number> {
  const preBuildFailures = await runValidationPhase(
    ctx,
    selectedPacks,
    options,
    'pre-build',
    'validatePreBuild',
    printedBefore,
  );
  if (preBuildFailures > 0 && options.failFast) {
    return preBuildFailures;
  }
  return (
    preBuildFailures + (await runValidationPhase(ctx, selectedPacks, options, 'post-build', 'validatePostBuild', true))
  );
}

async function runValidationPhase(
  ctx: MonorepoContext,
  selectedPacks: readonly MonorepoPack[],
  options: ValidatePackOptions,
  phase: 'pre-build' | 'post-build',
  method: 'validatePreBuild' | 'validatePostBuild',
  printedBefore: boolean,
): Promise<number> {
  let failures = 0;
  let checkedPacks = 0;
  let hasPrinted = printedBefore;
  for (const pack of selectedPacks) {
    const validate = pack[method];
    if (!validate) {
      continue;
    }
    console.log(`${hasPrinted || checkedPacks > 0 ? '\n' : ''}== ${pack.name} (${phase}) ==`);
    hasPrinted = true;
    checkedPacks++;
    const packFailures = await validate(ctx);
    failures += packFailures;
    if (packFailures > 0 && options.failFast) {
      break;
    }
  }
  return failures;
}

async function runFixPhase(
  ctx: MonorepoContext,
  selectedPacks: readonly MonorepoPack[],
  phase: 'pre-build' | 'post-build',
  method: 'fixPreBuild' | 'fixPostBuild',
): Promise<void> {
  let checkedPacks = 0;
  for (const pack of selectedPacks) {
    const fix = pack[method];
    if (!fix) {
      continue;
    }
    console.log(`${checkedPacks === 0 ? '' : '\n'}== ${pack.name} (${phase} fix) ==`);
    checkedPacks++;
    await fix(ctx);
  }
}

async function runBuild(ctx: MonorepoContext): Promise<number> {
  console.log('\n== build ==');
  const status = await runStatus('nx', ['run-many', '-t', 'build'], ctx.root);
  if (status !== 0) {
    console.error('nx run-many -t build failed');
    return 1;
  }
  return 0;
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
