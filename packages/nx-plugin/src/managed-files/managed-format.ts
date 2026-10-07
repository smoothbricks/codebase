import { spawnSync } from 'node:child_process';
import { existsSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
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
 * `managed-format.test.ts` pins this list to that config. Content in these
 * extensions that a template supplies verbatim passes through here; JSON that
 * smoo synthesizes goes through {@link formatJsonWithBiome} instead.
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
  // Prettier resolves configuration from disk, not pending Nx Tree edits. If
  // another generator stages formatter config, the next run after flush converges.
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

/**
 * The arguments `.git-format-staged.yml` gives Biome for every file it owns,
 * with `stdinFilePath` standing in for the hook's `{}`. A test pins the joined
 * command to that config.
 */
export function biomeHookArguments(stdinFilePath: string): string[] {
  return [
    'check',
    '--files-ignore-unknown=true',
    '--use-editorconfig=true',
    `--stdin-file-path=${stdinFilePath}`,
    '--fix',
  ];
}

/** The missing-Biome diagnostic is one line per process, not one per file written. */
let missingBiomeReported = false;

/**
 * The bytes the commit hook leaves in a JSON file whose content `text` is.
 *
 * `formatManagedContent` passes Biome-owned content through because a template
 * is copied from this package's own, already formatted, sources. JSON that smoo
 * *synthesizes* is different: `JSON.stringify` expands every array that Biome
 * keeps on one line, so a generated `tsconfig.test.json` or a rewritten
 * `nx.json` fails `biome check` until the hook has run. Locally the hook runs on
 * commit; the Managed files workflow commits with no hook, and its pull request
 * arrives red. Running the hook's own Biome command here makes the written bytes
 * a fixed point of it, under the consumer's own `biome.json`.
 *
 * Biome is the repository's, found as the hook finds it: the nearest
 * `node_modules/.bin/biome` above the file, else the one on `PATH`. A tree with
 * no Biome at all (a repository mid-bootstrap, before its first install) has no
 * hook output to agree with, so the text passes through and the first `update`
 * after the install converges. Every other failure is loud: a Biome that
 * rejects the repository's configuration would fail the hook the same way.
 */
export function formatJsonWithBiome(path: string, text: string): string {
  const absolute = resolve(path);
  const cwd = nearestExistingDirectory(dirname(absolute));
  const child = spawnSync(biomeBinary(cwd), biomeHookArguments(absolute), { cwd, input: text, encoding: 'utf8' });
  if (child.error) {
    if ('code' in child.error && child.error.code === 'ENOENT') {
      if (!missingBiomeReported) {
        missingBiomeReported = true;
        console.warn(
          `${path}: Biome is not installed here, so generated JSON keeps JSON.stringify's formatting; run smoo monorepo update again after the install.`,
        );
      }
      return text;
    }
    throw new Error(`${path}: Biome could not be run to format generated JSON`, { cause: child.error });
  }
  if (child.status !== 0) {
    throw new Error(
      `${path}: Biome exited ${child.status ?? `on signal ${child.signal}`}, so generated JSON cannot be written in the bytes the commit hook would leave:\n${child.stderr}`,
    );
  }
  // Biome echoes what it cannot format, so an empty answer to real input is a fault, never a result.
  if (child.stdout === '' && text !== '') {
    throw new Error(`${path}: Biome returned no content for generated JSON:\n${child.stderr}`);
  }
  return child.stdout;
}

/** `JSON.stringify` of `value`, in the bytes the commit hook leaves. */
export function jsonFileText(path: string, value: unknown): string {
  return formatJsonWithBiome(path, `${JSON.stringify(value, null, 2)}\n`);
}

/** The one writer for JSON files `smoo monorepo update` rewrites in place. */
export function writeJsonFile(path: string, value: unknown): void {
  writeFileSync(path, jsonFileText(path, value));
}

/** The directory a file about to be created would sit in, as far up as exists: spawning needs a real `cwd`. */
function nearestExistingDirectory(directory: string): string {
  let current = directory;
  while (!existsSync(current)) current = dirname(current);
  return current;
}

function biomeBinary(directory: string): string {
  for (let current = directory; ; current = dirname(current)) {
    const local = join(current, 'node_modules', '.bin', 'biome');
    if (existsSync(local)) return local;
    if (dirname(current) === current) return 'biome';
  }
}
