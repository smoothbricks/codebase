import { chmodSync, existsSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { validateManagedFiles } from '../managed-files.js';
import { validateNxSync } from '../nx-sync.js';
import { fixPackageHygiene, validatePackageHygiene } from '../package-hygiene.js';
import {
  applyPublicPackageDefaults,
  applyWorkspaceDependencyDefaults,
  validateNxReleaseConfig,
  validatePublicPackageMetadata,
  validatePublicTags,
  validateRootPackagePolicy,
  validateWorkspaceDependencies,
} from '../package-policy.js';
import { validatePackedPublicPackages } from '../packed-package.js';
import { syncRootRuntimeVersions } from '../runtime.js';

export interface MonorepoContext {
  root: string;
  syncRuntime: boolean;
}

export interface ValidatePackOptions {
  failFast?: boolean;
}

interface MonorepoPack {
  name: string;
  init?(ctx: MonorepoContext): Promise<void> | void;
  validate?(ctx: MonorepoContext): Promise<number> | number;
}

const packs: MonorepoPack[] = [
  {
    name: 'core',
    async init(ctx) {
      ensureLocalSmooShim(ctx.root);
      if (ctx.syncRuntime) {
        await syncRootRuntimeVersions(ctx.root);
      } else {
        console.log('skip           root runtime versions (outside devenv; pass --sync-runtime to force)');
      }
    },
    async validate(ctx) {
      return (
        validateManagedFiles(ctx.root) +
        validateRootPackagePolicy(ctx.root) +
        validateNxReleaseConfig(ctx.root) +
        (await validateNxSync(ctx.root))
      );
    },
  },
  {
    name: 'publishing',
    init(ctx) {
      applyPublicPackageDefaults(ctx.root);
    },
    validate(ctx) {
      return validatePublicTags(ctx.root) + validatePublicPackageMetadata(ctx.root);
    },
  },
  {
    name: 'workspace-dependencies',
    async init(ctx) {
      applyWorkspaceDependencyDefaults(ctx.root);
      await fixPackageHygiene(ctx.root);
    },
    async validate(ctx) {
      return validateWorkspaceDependencies(ctx.root) + (await validatePackageHygiene(ctx.root));
    },
  },
  {
    name: 'packed-packages',
    validate(ctx) {
      return validatePackedPublicPackages(ctx.root);
    },
  },
];

export async function runInitPacks(ctx: MonorepoContext): Promise<void> {
  for (const pack of packs) {
    await pack.init?.(ctx);
  }
}

export async function runValidatePacks(ctx: MonorepoContext, options: ValidatePackOptions = {}): Promise<number> {
  let failures = 0;
  let checkedPacks = 0;
  for (const pack of packs) {
    if (!pack.validate) {
      continue;
    }
    console.log(`${checkedPacks === 0 ? '' : '\n'}== ${pack.name} ==`);
    checkedPacks++;
    const packFailures = await pack.validate(ctx);
    failures += packFailures;
    if (packFailures > 0 && options.failFast) {
      break;
    }
  }
  return failures;
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
