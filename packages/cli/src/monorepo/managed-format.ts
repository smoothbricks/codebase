import { join } from 'node:path';
import type * as PrettierModule from 'prettier';
import type { Options as PrettierOptions } from 'prettier';

/**
 * Extensions the managed `.git-format-staged.yml` routes to Biome rather than
 * Prettier. Prettier has a parser for every one of them, so they have to be
 * named: running Prettier over them here would fight the commit hook instead
 * of agreeing with it, which is the failure this module exists to remove.
 *
 * Everything Prettier cannot parse — `.nix` (Alejandra), `.rs` (rustfmt),
 * `.sh`, `.envrc`, `.gitattributes` — needs no entry: Prettier infers no
 * parser and the content passes through untouched, exactly as
 * `prettier --ignore-unknown` leaves it in the hook.
 *
 * `managed-format.test.ts` pins this list to that config.
 */
export const BIOME_OWNED_EXTENSIONS: readonly string[] = [
  '.js',
  '.cjs',
  '.mjs',
  '.ts',
  '.cts',
  '.mts',
  '.jsx',
  '.tsx',
  '.json',
  '.jsonc',
  '.html',
  '.css',
  '.graphql',
];

/**
 * The ignore files Prettier's CLI consults by default, and the CLI is what
 * `.git-format-staged.yml` invokes. A repository that tells Prettier to leave
 * a path alone means the hook leaves it alone, so this writer must too.
 */
const IGNORE_FILES = ['.gitignore', '.prettierignore'] as const;

/**
 * Loaded on first use, not at import: most `smoo` commands never write a
 * managed file, and Prettier is not a cheap module to pull into every one.
 */
let prettierModule: Promise<typeof PrettierModule> | undefined;

function loadPrettier(): Promise<typeof PrettierModule> {
  prettierModule ??= import('prettier');
  return prettierModule;
}

/**
 * The bytes a managed target has *after* the consuming repository's commit
 * hook has had its say.
 *
 * `smoo monorepo update` writes into a repository whose hook
 * (`.git-format-staged.yml`) reformats whatever it stages. Writing the
 * generator's own idea of formatting there means the hook rewrites the file on
 * the next commit, the next `update` writes the generator's version back, and
 * the drift check reports a difference forever — on a tree where nothing
 * meaningful changed. A warning that is always on is a warning nobody reads,
 * and it hides the real drift underneath it.
 *
 * So the writer renders into the consumer's own formatting space, using the
 * config Prettier resolves for *this* path in *this* repository: overrides,
 * `.editorconfig` and ignore files included. The written bytes are then a
 * fixed point of the hook, `update` is idempotent, and `check` reports only
 * differences that are real.
 *
 * A consumer's formatter config is the consumer's business. This reads it and
 * obeys; it never asks the repository to change it.
 */
export async function formatManagedContent(root: string, target: string, content: string): Promise<string> {
  if (BIOME_OWNED_EXTENSIONS.some((extension) => target.endsWith(extension))) {
    return content;
  }
  const prettier = await loadPrettier();
  const path = join(root, target);
  const ignorePath = IGNORE_FILES.map((file) => join(root, file));
  // `resolveConfig` so a repository that assigns a parser through an override
  // gets the parser its own Prettier run would infer.
  const info = await prettier.getFileInfo(path, { ignorePath, resolveConfig: true });
  if (info.ignored || info.inferredParser === null) {
    return content;
  }
  let options: PrettierOptions | null;
  try {
    options = await prettier.resolveConfig(path, { editorconfig: true });
  } catch (error) {
    throw new Error(
      `${target}: the repository's Prettier configuration could not be read, so managed content cannot be written in the bytes its commit hook would leave behind`,
      { cause: error },
    );
  }
  try {
    return await prettier.format(content, { ...options, filepath: path });
  } catch (error) {
    throw new Error(`${target}: generated managed content is not valid ${info.inferredParser}`, { cause: error });
  }
}
