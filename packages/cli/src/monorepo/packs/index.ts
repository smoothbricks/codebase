import { chmodSync, existsSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { runStatus } from '../../lib/run.js';
import { readProjectTargets } from '../../nx/index.js';
import { syncBunLockfileVersions, validateBunLockfileVersions } from '../lockfile.js';
import { validateManagedFiles } from '../managed-files.js';
import { fixNxSync, validateNxSync } from '../nx-sync.js';
import { fixPackageHygiene, validatePackageHygiene } from '../package-hygiene.js';
import {
  applyFixableMonorepoDefaults,
  applyNxReleaseDefaults,
  applyPublicPackageDefaults,
  applyWorkspaceDependencyDefaults,
  type ResolvedProjectTargets,
  validateNxProjectNames,
  validateNxReleaseConfig,
  validatePublicPackageMetadata,
  validatePublicTags,
  validateRootPackagePolicy,
  validateWorkspaceDependencies,
} from '../package-policy.js';
import {
  validatePackedPublicPackageManifest,
  validatePackedPublicPackagePublint,
  validatePackedPublicPackageTypes,
} from '../packed-package.js';
import { syncRootRuntimeVersions } from '../runtime.js';
import { applyToolConfigDefaults, validateToolConfig } from '../tool-validation.js';

export interface MonorepoContext {
  root: string;
  syncRuntime: boolean;
  verbose?: boolean;
}

export interface ValidatePackOptions {
  failFast?: boolean;
  fix?: boolean;
  verbose?: boolean;
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
  runBuild?: (ctx: MonorepoContext, options?: ValidatePackOptions) => Promise<number> | number;
}

export interface ValidatePackResult {
  failures: number;
  failedChecks: number;
}

interface CapturedOutput {
  logs: string[];
  errors: string[];
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
      await fixNxSync(ctx.root, ctx.verbose === true);
      applyWorkspaceDependencyDefaults(ctx.root, { resolvedTargetsByProject: await readResolvedTargetsByProject(ctx) });
    },
    async validatePreBuild(ctx) {
      return (
        validateManagedFiles(ctx.root) +
        validateRootPackagePolicy(ctx.root) +
        validateToolConfig(ctx.root) +
        validateNxProjectNames(ctx.root) +
        validateNxReleaseConfig(ctx.root) +
        validateBunLockfileVersions(ctx.root) +
        (await validateNxSync(ctx.root, ctx.verbose === true))
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
    init(ctx) {
      applyWorkspaceDependencyDefaults(ctx.root);
    },
    async fixPostBuild(ctx) {
      applyWorkspaceDependencyDefaults(ctx.root, { resolvedTargetsByProject: await readResolvedTargetsByProject(ctx) });
    },
    async validatePostBuild(ctx) {
      return validateWorkspaceDependencies(ctx.root, {
        resolvedTargetsByProject: await readResolvedTargetsByProject(ctx),
      });
    },
  },
  {
    name: 'package-hygiene',
    async init(ctx) {
      await fixPackageHygiene(ctx.root, ctx.verbose === true);
    },
    async fixPostBuild(ctx) {
      await fixPackageHygiene(ctx.root);
    },
    async validatePostBuild(ctx) {
      return validatePackageHygiene(ctx.root, ctx.verbose === true);
    },
  },
  {
    name: 'packed-package-publint',
    validatePostBuild(ctx) {
      return validatePackedPublicPackagePublint(ctx.root);
    },
  },
  {
    name: 'packed-package-manifest',
    validatePostBuild(ctx) {
      return validatePackedPublicPackageManifest(ctx.root);
    },
  },
  {
    name: 'packed-package-types',
    validatePostBuild(ctx) {
      return validatePackedPublicPackageTypes(ctx.root);
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
): Promise<ValidatePackResult> {
  const selectedPacks = hooks.packs ?? packs;
  const build = hooks.runBuild ?? runBuild;

  if (options.fix) {
    await runFixPhase(ctx, selectedPacks, 'pre-build', 'fixPreBuild');
    const buildFailures = await build(ctx, options);
    if (buildFailures > 0) {
      return { failures: buildFailures, failedChecks: 1 };
    }
    await runFixPhase(ctx, selectedPacks, 'post-build', 'fixPostBuild');
    return runValidationPhases(ctx, selectedPacks, options, false);
  }

  const preBuild = await runValidationPhase(ctx, selectedPacks, options, 'pre-build', 'validatePreBuild', false);
  if (preBuild.failures > 0 && options.failFast) {
    return preBuild;
  }
  const buildFailures = await build(ctx, options);
  if (buildFailures > 0) {
    return { failures: preBuild.failures + buildFailures, failedChecks: preBuild.failedChecks + 1 };
  }
  return sumResults(
    preBuild,
    await runValidationPhase(ctx, selectedPacks, options, 'post-build', 'validatePostBuild', true),
  );
}

async function runValidationPhases(
  ctx: MonorepoContext,
  selectedPacks: readonly MonorepoPack[],
  options: ValidatePackOptions,
  printedBefore: boolean,
): Promise<ValidatePackResult> {
  const preBuild = await runValidationPhase(
    ctx,
    selectedPacks,
    options,
    'pre-build',
    'validatePreBuild',
    printedBefore,
  );
  if (preBuild.failures > 0 && options.failFast) {
    return preBuild;
  }
  return sumResults(
    preBuild,
    await runValidationPhase(ctx, selectedPacks, options, 'post-build', 'validatePostBuild', true),
  );
}

async function runValidationPhase(
  ctx: MonorepoContext,
  selectedPacks: readonly MonorepoPack[],
  options: ValidatePackOptions,
  phase: 'pre-build' | 'post-build',
  method: 'validatePreBuild' | 'validatePostBuild',
  printedBefore: boolean,
): Promise<ValidatePackResult> {
  let failures = 0;
  let failedChecks = 0;
  let checkedPacks = 0;
  let hasPrinted = printedBefore;
  for (const pack of selectedPacks) {
    const validate = pack[method];
    if (!validate) {
      continue;
    }
    const label = `${pack.name} (${phase})`;
    if (options.verbose) {
      printCheckHeading(label, hasPrinted || checkedPacks > 0);
    }
    hasPrinted = true;
    checkedPacks++;
    let packFailures: number;
    let output = emptyCapturedOutput();
    try {
      if (options.verbose) {
        packFailures = await validate(ctx);
      } else {
        const captured = await captureConsoleOutput(() => validate(ctx));
        packFailures = captured.result;
        output = captured.output;
      }
    } catch (error) {
      if (!options.verbose) {
        printCheckHeading(label, true);
        if (error instanceof CapturedConsoleError) {
          output = error.output;
        }
        printCapturedOutput(output);
      }
      throw error instanceof CapturedConsoleError ? error.cause : error;
    }
    failures += packFailures;
    if (packFailures > 0) {
      failedChecks++;
    }
    if (!options.verbose && packFailures > 0) {
      printCheckHeading(label, true);
      printCapturedOutput(output);
    }
    printCheckStatus(label, packFailures);
    if (packFailures > 0 && options.failFast) {
      break;
    }
  }
  return { failures, failedChecks };
}

function sumResults(left: ValidatePackResult, right: ValidatePackResult): ValidatePackResult {
  return { failures: left.failures + right.failures, failedChecks: left.failedChecks + right.failedChecks };
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
    const label = `${pack.name} (${phase} fix)`;
    if (ctx.verbose) {
      printCheckHeading(label, checkedPacks > 0);
    }
    checkedPacks++;
    if (ctx.verbose) {
      await fix(ctx);
      continue;
    }
    let output = emptyCapturedOutput();
    try {
      output = (await captureConsoleOutput(() => fix(ctx))).output;
    } catch (error) {
      if (error instanceof CapturedConsoleError) {
        output = error.output;
      }
      printCheckHeading(label, true);
      printCapturedOutput(output);
      throw error instanceof CapturedConsoleError ? error.cause : error;
    }
  }
}

async function runBuild(ctx: MonorepoContext, options: ValidatePackOptions = {}): Promise<number> {
  if (options.verbose) {
    printCheckHeading('build', true);
  }
  const status = await runStatus('nx', ['run-many', '-t', 'build'], ctx.root, options.verbose !== true);
  if (status !== 0) {
    if (!options.verbose) {
      printCheckHeading('build', true);
    }
    console.error('nx run-many -t build failed');
    printCheckStatus('build', 1);
    return 1;
  }
  printCheckStatus('build', 0);
  return 0;
}

function printCheckStatus(label: string, failures: number): void {
  if (failures === 0) {
    console.log(`🆗 ${label}`);
    return;
  }
  const noun = failures === 1 ? 'problem' : 'problems';
  console.log(`👎 ${label} (${failures} ${noun})\n`);
}

function printCheckHeading(label: string, separate: boolean): void {
  console.log(`${separate ? '\n' : ''}== ${label} ==`);
}

function emptyCapturedOutput(): CapturedOutput {
  return { logs: [], errors: [] };
}

async function captureConsoleOutput<T>(fn: () => Promise<T> | T): Promise<{ result: T; output: CapturedOutput }> {
  const output = emptyCapturedOutput();
  const originalLog = console.log;
  const originalError = console.error;
  console.log = (...args: unknown[]) => {
    output.logs.push(args.join(' '));
  };
  console.error = (...args: unknown[]) => {
    output.errors.push(args.join(' '));
  };
  try {
    return { result: await fn(), output };
  } catch (error) {
    throw new CapturedConsoleError(error, output);
  } finally {
    console.log = originalLog;
    console.error = originalError;
  }
}

class CapturedConsoleError extends Error {
  constructor(
    public override readonly cause: unknown,
    public readonly output: CapturedOutput,
  ) {
    super(cause instanceof Error ? cause.message : String(cause));
  }
}

function printCapturedOutput(output: CapturedOutput): void {
  for (const line of output.logs) {
    console.log(line);
  }
  for (const line of output.errors) {
    console.log(line);
  }
}

async function readResolvedTargetsByProject(ctx: MonorepoContext): Promise<Map<string, ResolvedProjectTargets>> {
  const projects = await readProjectTargets(ctx.root);
  return new Map(
    projects.map((project) => [
      project.project,
      {
        targets: new Set(project.targets),
        ...(project.buildDependsOn ? { buildDependsOn: project.buildDependsOn } : {}),
      },
    ]),
  );
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
