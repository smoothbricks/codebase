import { execFile } from 'node:child_process';
import { accessSync, constants, readFileSync, statSync } from 'node:fs';
import { delimiter, join, resolve } from 'node:path';
import { promisify } from 'node:util';
import type { Result } from './ram-temp.js';

/**
 * A leg that runs `bun` runs the Bun its workspace declares.
 *
 * The root `package.json` names that Bun in `packageManager` (`bun@1.4.3`); the Bun the command gets is the first `bun`
 * on the PATH it runs with. When the two disagree the leg is refused before it asks for anything, naming both versions
 * and the binary. A Bun other than the declared one does not fail at startup: on Bun 1.4.2 `console.log` drops what a
 * full pipe refuses, so a child's JSON document reaches the parent that reads it cut in the middle of a string, the
 * child exits 0, and the leg fails three layers from the cause as `JSON Parse error: Unterminated string`. The refusal
 * is that mismatch, said where it happens.
 */

/**
 * `bun --version` answers in milliseconds. The bound is for a `bun` that never answers (a wrapper waiting on a lock):
 * it refuses the leg, which would otherwise hang before its own bounds start.
 */
const VERSION_DEADLINE_MS = 120_000;

/**
 * A word `bun` that the shell resolves through PATH, in command position or in a string a wrapper runs: `bun test`,
 * `cd x && bun run y`, `FOO=1 bun a`, `sh -c 'bun test'`. A path-qualified `../runtime/bun` names its binary itself,
 * so PATH decides nothing for it; `bunx`, `bun-runtime` and `bun.js` are other words.
 */
const BUN_COMMAND = /(?<=^|[\s;&|()'"`])bun(?=$|[\s;&|()'"`])/;

/** `bun@<version>` as corepack spells `packageManager`, with the content hash it may add after `+`. */
const BUN_DECLARATION = /^bun@(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)(?:\+.+)?$/;

const execFileAsync = promisify(execFile);

/** What the workspace declares: `bun@<version>` in its root `package.json`'s `packageManager`. */
export interface DeclaredBun {
  /** The manifest that declares it. */
  readonly manifest: string;
  /** The field as written. */
  readonly declaration: string;
  /** The version it names, without the content hash. */
  readonly version: string;
}

/** Where the command's PATH comes from: the target's own `env`, or the process the executor runs in. */
export type PathSource = "the target's env" | 'the inherited environment';

/** Why a leg that runs `bun` is not run, as the values the message is made of. */
export type RuntimePinRefusal =
  | {
      readonly code: 'mismatch';
      readonly declared: DeclaredBun;
      /** The first `bun` on the command's PATH. */
      readonly bun: string;
      /** What that `bun --version` printed. */
      readonly version: string;
      readonly pathSource: PathSource;
    }
  | {
      readonly code: 'unreadable-version';
      readonly declared: DeclaredBun;
      readonly bun: string;
      readonly reason: string;
      readonly pathSource: PathSource;
    }
  | { readonly code: 'malformed-declaration'; readonly manifest: string; readonly declaration: string }
  | { readonly code: 'unreadable-manifest'; readonly manifest: string; readonly reason: string };

/** What `bun --version` printed for `bun` run in `env` from `cwd`, or why it printed none. */
export type BunProbe = (bun: string, env: NodeJS.ProcessEnv, cwd: string) => Promise<Result<string, string>>;

export interface RuntimePinCheck {
  /** The workspace root: its `package.json` declares the Bun. */
  readonly root: string;
  /** The command line, as the shell will run it. */
  readonly command: string;
  /** The environment the command runs with: its PATH decides which `bun` it gets. */
  readonly env: NodeJS.ProcessEnv;
  /** Where the command starts; a relative PATH entry is relative to it. */
  readonly cwd: string;
  readonly pathSource: PathSource;
}

/** Whether `command` names `bun` for the shell to find on PATH: the legs this guard is for. */
export function runsBun(command: string): boolean {
  return BUN_COMMAND.test(command);
}

/**
 * The first executable regular file `name` on `pathValue`, as the shell the command runs under finds it. An empty entry
 * is the working directory, like `.`: the command starts in `cwd`, which is not the executor's.
 */
export function resolveOnPath(name: string, pathValue: string | undefined, cwd: string): string | null {
  for (const entry of (pathValue ?? '').split(delimiter)) {
    const candidate = resolve(cwd, entry === '' ? '.' : entry, name);
    try {
      if (!statSync(candidate).isFile()) {
        continue;
      }
      accessSync(candidate, constants.X_OK);
    } catch {
      continue;
    }
    return candidate;
  }
  return null;
}

/**
 * The Bun the workspace declares, `null` when it declares none: no manifest, no `packageManager`, or another package
 * manager's. A `packageManager` that names `bun` and no version is refused, not read as no declaration: it says a Bun
 * is declared, and a guard that skipped it would be silent exactly where the declaration is wrong.
 */
export function declaredBun(root: string): Result<DeclaredBun | null, RuntimePinRefusal> {
  const manifest = join(root, 'package.json');
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(manifest, 'utf8'));
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return { ok: true, value: null };
    }
    const reason = error instanceof Error ? error.message : String(error);
    return { ok: false, error: { code: 'unreadable-manifest', manifest, reason } };
  }
  const declaration: unknown =
    typeof parsed === 'object' && parsed !== null && 'packageManager' in parsed ? parsed.packageManager : undefined;
  if (typeof declaration !== 'string' || !declaration.startsWith('bun@')) {
    return { ok: true, value: null };
  }
  const version = BUN_DECLARATION.exec(declaration)?.[1];
  if (version === undefined) {
    return { ok: false, error: { code: 'malformed-declaration', manifest, declaration } };
  }
  return { ok: true, value: { manifest, declaration, version } };
}

/** The version `bun --version` prints in the command's own environment, which is what a wrapper reads to route. */
export const probeBunVersion: BunProbe = async (bun, env, cwd) => {
  try {
    const { stdout } = await execFileAsync(bun, ['--version'], {
      env,
      cwd,
      encoding: 'utf8',
      timeout: VERSION_DEADLINE_MS,
      killSignal: 'SIGKILL',
      maxBuffer: 64 * 1024,
    });
    return { ok: true, value: stdout.trim() };
  } catch (error) {
    if (error instanceof Error && 'killed' in error && error.killed === true) {
      return { ok: false, error: `\`bun --version\` gave no answer within ${VERSION_DEADLINE_MS} ms` };
    }
    const said =
      error instanceof Error && 'stderr' in error && typeof error.stderr === 'string' ? error.stderr.trim() : '';
    const reason = error instanceof Error ? error.message : String(error);
    return { ok: false, error: `\`bun --version\` failed: ${reason}${said === '' ? '' : `: ${said}`}` };
  }
};

/**
 * The refusal that keeps a leg off a Bun its workspace does not declare, or `null` when the leg may run: it names no
 * `bun`, the workspace declares none, PATH finds none (the shell will say so), or the one it finds is the declared
 * version. Not checked on Windows, where the executor starts no lifecycle supervisor either.
 */
export async function checkRuntimePin(
  check: RuntimePinCheck,
  probe: BunProbe = probeBunVersion,
): Promise<RuntimePinRefusal | null> {
  if (process.platform === 'win32' || !runsBun(check.command)) {
    return null;
  }
  const declared = declaredBun(check.root);
  if (!declared.ok) {
    return declared.error;
  }
  if (declared.value === null) {
    return null;
  }
  const bun = resolveOnPath('bun', check.env.PATH, check.cwd);
  if (bun === null) {
    return null;
  }
  const printed = await probe(bun, check.env, check.cwd);
  if (!printed.ok) {
    return {
      code: 'unreadable-version',
      declared: declared.value,
      bun,
      reason: printed.error,
      pathSource: check.pathSource,
    };
  }
  return printed.value === declared.value.version
    ? null
    : { code: 'mismatch', declared: declared.value, bun, version: printed.value, pathSource: check.pathSource };
}

/** What the symptom of running another Bun looks like, so a reader sees what the refusal replaces. */
const SYMPTOM =
  'A leg on another Bun can pass its startup and fail far from the cause: on Bun 1.4.2 `console.log` drops what a ' +
  "full pipe refuses, so a child's JSON reaches its parent cut mid-string while the child exits 0.";

/** The refusal as the run's stderr says it, one line per fact, naming the task when the run has one. */
export function describeRuntimePinRefusal(refusal: RuntimePinRefusal, task?: string): string {
  const refused = `${task ?? 'bounded-exec'}: refusing to run the command: `;
  switch (refusal.code) {
    case 'mismatch':
      return `${[
        `${refused}the workspace declares Bun ${refusal.declared.version}, and the \`bun\` it would run is Bun ${refusal.version}.`,
        `  declared by: packageManager "${refusal.declared.declaration}" in ${refusal.declared.manifest}`,
        `  resolved by: PATH from ${refusal.pathSource}; its first \`bun\` is ${refusal.bun}, which prints ${refusal.version}`,
        SYMPTOM,
        "Run the leg from the workspace's own shell, or put the declared Bun first on PATH.",
      ].join('\n')}\n`;
    case 'unreadable-version':
      return `${[
        `${refused}the workspace declares Bun ${refusal.declared.version}, and the \`bun\` it would run did not say which Bun it is.`,
        `  declared by: packageManager "${refusal.declared.declaration}" in ${refusal.declared.manifest}`,
        `  resolved by: PATH from ${refusal.pathSource}; its first \`bun\` is ${refusal.bun}: ${refusal.reason}`,
        SYMPTOM,
      ].join('\n')}\n`;
    case 'malformed-declaration':
      return `${[
        `${refused}${refusal.manifest} declares packageManager "${refusal.declaration}", which names no Bun version to compare.`,
        'Declare an exact version, `bun@<major>.<minor>.<patch>`.',
      ].join('\n')}\n`;
    case 'unreadable-manifest':
      return `${refused}cannot read the workspace's declaration in ${refusal.manifest}: ${refusal.reason}\n`;
  }
}
