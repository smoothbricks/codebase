import { run } from '../lib/run.js';

const plugin = '@smoothbricks/nx-plugin';

// Variant registry — drives both CLI subcommands and dispatch.
//
// Each entry maps a `smoo g <variant>` subcommand to an Nx generator in
// @smoothbricks/nx-plugin. cli.ts iterates this record to register
// commander subcommands, so adding a variant here wires it end-to-end.
//
// Generator implementations:
//   create-package  packages/nx-plugin/src/generators/create-package/generator.ts
//   make-public     packages/nx-plugin/src/generators/make-public/generator.ts
//
// To add a new variant:
//   1. Add/extend a generator in packages/nx-plugin/src/generators/
//   2. Register it in packages/nx-plugin/generators.json
//   3. Add a variants entry below

export interface GenerateVariant {
  /** Nx generator name inside @smoothbricks/nx-plugin (e.g. 'create-package'). */
  generator: string;
  /** One-line description shown in `smoo g --help`. */
  description: string;
  /** Build the generator-specific args from the positional <name>. */
  args: (name: string) => string[];
  /** Extra CLI flags beyond the universal --dry-run. */
  options?: readonly VariantOption[];
}

interface VariantOption {
  /** Commander flag syntax (e.g. '--public'). */
  flag: string;
  /** Help text for this flag. */
  description: string;
}

export const variants: Record<string, GenerateVariant> = {
  'ts-lib': {
    generator: 'create-package',
    description: 'Create a TypeScript library package',
    args: (name) => ['--name', name, '--variant', 'ts-lib'],
    options: [{ flag: '--public', description: 'configure for npm publication' }],
  },
  'ts-zig': {
    generator: 'create-package',
    description: 'Create a TypeScript + Zig/WASM hybrid package',
    args: (name) => ['--name', name, '--variant', 'ts-zig'],
    options: [{ flag: '--public', description: 'configure for npm publication' }],
  },
  'make-public': {
    generator: 'make-public',
    description: 'Promote a private package to npm publication',
    args: (name) => ['--project', name],
  },
};

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

export async function generate(
  root: string,
  variantName: string,
  name: string,
  options: Record<string, unknown>,
): Promise<void> {
  const variant = variants[variantName];
  if (!variant) {
    const known = Object.keys(variants).join(', ');
    throw new Error(`Unknown generator variant "${variantName}". Available: ${known}`);
  }

  const args = ['g', `${plugin}:${variant.generator}`, ...variant.args(name)];
  for (const opt of variant.options ?? []) {
    const key = opt.flag.replace(/^--/, '');
    if (options[key]) {
      args.push(opt.flag);
    }
  }
  if (options.dryRun) {
    args.push('--dry-run');
  }

  await run('nx', args, root);

  // Sync TypeScript project references so the new package is immediately
  // visible in the build graph without a manual `nx sync` step.
  if (!options.dryRun) {
    await run('nx', ['sync'], root);
  }
}
