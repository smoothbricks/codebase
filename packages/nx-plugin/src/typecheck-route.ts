import { existsSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import typia from 'typia';

/**
 * What a package whose ttsc plugin lowers a dialect declares in its manifest, as `smoo.typecheck`: every project that
 * depends on it directly compiles sources the stock checker refuses (a construct the plugin's ProgramPlugin rewrites
 * before emit, its refusal withdrawn through the driver's DiagnosticPlugin seam), so that project's typecheck runs
 * through ttsc, never the pinned native compiler, which cannot judge the dialect.
 */
export interface DialectTypecheck {
  /** The compiler a dependent project's typecheck runs. */
  readonly compiler: 'ttsc';
  /**
   * The Nx targets (`project:target`) that prepare the plugin binaries ttsc loads, so a typecheck loads built bytes
   * and never compiles the plugin inside the check. The declaring package names them; the inference guesses none.
   */
  readonly dependsOn: readonly string[];
}

/** The one manifest field this module reads. Any other field is the manifest owner's. */
interface DeclaringManifest {
  readonly smoo?: { readonly typecheck?: DialectTypecheck };
}

const validateManifest = typia.createValidate<DeclaringManifest>();

/** A project's typecheck route: the direct dependencies that declare a dialect, and the targets they name, once each. */
export interface TypecheckRoute {
  readonly declaredBy: readonly string[];
  readonly dependsOn: readonly string[];
}

/**
 * The typecheck route of the project whose manifest is `packageJsonPath`, over its direct dependencies (the ones ttsc
 * discovers plugins on: `dependencies` and `devDependencies`). Each dependency's manifest is the one Node resolution
 * reaches from the project, as ttsc reads it; a dependency that is not installed declares nothing. A manifest that
 * declares `smoo.typecheck` in another shape refuses the inference by its path and the field it got wrong.
 */
export function typecheckRoute(packageJsonPath: string, dependencies: readonly string[]): TypecheckRoute {
  const declaredBy: string[] = [];
  const dependsOn: string[] = [];
  const lookup = createRequire(packageJsonPath);
  for (const name of new Set(dependencies)) {
    const manifest = (lookup.resolve.paths(name) ?? [])
      .map((root) => join(root, ...name.split('/'), 'package.json'))
      .find((candidate) => existsSync(candidate));
    if (manifest === undefined) continue;
    const checked = validateManifest(JSON.parse(readFileSync(manifest, 'utf-8')));
    if (!checked.success) {
      throw new Error(
        `${manifest}: smoo.typecheck must be { compiler: 'ttsc', dependsOn: ['<project>:<target>', ...] }: ${checked.errors
          .map((error) => `${error.path} expected ${error.expected}`)
          .join('; ')}`,
      );
    }
    const declared = checked.data.smoo?.typecheck;
    if (declared === undefined) continue;
    declaredBy.push(name);
    for (const target of declared.dependsOn) if (!dependsOn.includes(target)) dependsOn.push(target);
  }
  return { declaredBy, dependsOn };
}
