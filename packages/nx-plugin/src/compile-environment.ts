import typia from 'typia';
import type { DependencyManifest } from './dependency-manifests.js';

/**
 * What a package whose ttsc plugin lowers by environment variables declares in its manifest, as `smoo.compileEnv`:
 * the variables the plugin's output depends on. `@smoothbricks/lmao-ttsc` declares `NODE_ENV`, which decides whether
 * trace statements compile away (specs/lmao/01o_typescript_transformer.md §6.3). Every project that depends on such a
 * package directly compiles through its plugin, so each target whose output is that compile hashes the variables.
 */
interface DeclaringManifest {
  readonly smoo?: { readonly compileEnv?: readonly string[] };
}

const validateManifest = typia.createValidate<DeclaringManifest>();

/**
 * The environment variables a project's transformed output depends on, over its direct dependencies' manifests, once
 * each in declaration order. A manifest that declares `smoo.compileEnv` in another shape refuses the inference by its
 * path and the field it got wrong.
 */
export function compileEnvironment(manifests: readonly DependencyManifest[]): string[] {
  const names: string[] = [];
  for (const manifest of manifests) {
    const checked = validateManifest(manifest.json);
    if (!checked.success) {
      throw new Error(
        `${manifest.path}: smoo.compileEnv must be ['<VARIABLE>', ...]: ${checked.errors
          .map((error) => `${error.path} expected ${error.expected}`)
          .join('; ')}`,
      );
    }
    for (const name of checked.data.smoo?.compileEnv ?? []) if (!names.includes(name)) names.push(name);
  }
  return names;
}
