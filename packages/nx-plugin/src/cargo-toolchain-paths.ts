import { accessSync, constants, realpathSync } from 'node:fs';
import { delimiter, join } from 'node:path';

/**
 * The programs the shell declares for Cargo to compile and link with, each named by the Nix store object it is: the
 * half of a cargo task's toolchain identity that no file states.
 *
 * `devenv.lock` locks the inputs, not what the shell builds from them. A patch to the linker moves no byte of the lock
 * and no line of `rustc -vV`, yet it changes what links every unit. Hashing the shell's Nix sources instead was
 * measured to rekey every cargo verdict on every unrelated shell edit (the managed module alone changed 43 times in 30
 * days). The store object is the fact itself: devenv evaluates a package to the path of a derivation whose hash covers
 * everything it was built from, so the path moves when the toolchain moves, once, and not when anything else in the
 * shell does.
 *
 * What counts is what Cargo keys on. The linker of each target is `CARGO_TARGET_<TRIPLE>_LINKER`, which Cargo hashes
 * into every unit. The programs cc-rs runs for build scripts are `CC`, `CXX`, `AR` and `RANLIB` (generic, per target,
 * and their `HOST_` and `TARGET_` forms), each of which cc-rs declares `rerun-if-env-changed`. Flag variables name no
 * program. The wrapper devenv builds for a Linux linker names its clang and its lld, so a change to either moves that
 * wrapper's object, and one entry covers all three.
 *
 * rustc and cargo are not here on purpose. Cargo keys units on the output of `rustc -vV`, never on rustc's path, and the
 * version recipe beside this one hashes it. On a non-Linux host the default shell and its linux-cross profile carry
 * different rust-std targets, so a rustc path would split the pre-push probe from the cached cross-check verdict.
 *
 * Only a path in the Nix store counts. A store object is named by the hash of everything it was built from, so one
 * shell gives one object on every machine of a platform. A path elsewhere belongs to the machine and takes no part.
 */
export const NIX_STORE_ROOT = '/nix/store';

/** The variables naming a program Cargo or one of its build scripts runs. */
const PROGRAM_VARIABLE =
  /^(?:CARGO_TARGET_[A-Z0-9_]+_LINKER|(?:CC|CXX|AR|RANLIB)(?:_[a-z].*)?|(?:HOST_|TARGET_)(?:CC|CXX|AR|RANLIB))$/;

export interface CargoToolchainProbe {
  readonly environment: Readonly<Record<string, string | undefined>>;
  readonly storeRoot: string;
  /** What `program` is once every link is followed, or undefined where it names no executable file. */
  readonly resolve: (program: string) => string | undefined;
}

/** The store object holding `path`, whichever of its files the path names, or undefined outside the store. */
function storeObject(path: string, storeRoot: string): string | undefined {
  const prefix = `${storeRoot}/`;
  if (!path.startsWith(prefix)) return undefined;
  const [object] = path.slice(prefix.length).split('/', 1);
  return object === undefined || object === '' ? undefined : `${prefix}${object}`;
}

/**
 * One `NAME store-object` line, in name order, for each program variable whose program lives in the store; empty where
 * the shell declares none. A variable holds a program name or path, then arguments (`ccache cc`): the first word is
 * the program, a bare name is looked up on PATH.
 */
export function cargoToolchainPaths(probe: CargoToolchainProbe): string {
  const lines: string[] = [];
  for (const name of Object.keys(probe.environment).sort()) {
    if (!PROGRAM_VARIABLE.test(name)) continue;
    const program = probe.environment[name]?.trim().split(/\s+/, 1)[0];
    if (program === undefined || program === '') continue;
    const resolved = probe.resolve(program);
    const object = resolved === undefined ? undefined : storeObject(resolved, probe.storeRoot);
    if (object !== undefined) lines.push(`${name} ${object}\n`);
  }
  return lines.join('');
}

/** Errors that say a candidate is not a program, as opposed to a machine that cannot be read. */
const NOT_A_PROGRAM = new Set(['ENOENT', 'ENOTDIR', 'EACCES', 'ELOOP', 'ENAMETOOLONG']);

/** The real path of `program`: itself where it holds a path, else the first executable file `searchPath` names. */
export function resolveProgram(program: string, searchPath: string | undefined): string | undefined {
  const candidates = program.includes('/')
    ? [program]
    : (searchPath ?? '')
        .split(delimiter)
        .filter((directory) => directory !== '')
        .map((directory) => join(directory, program));
  for (const candidate of candidates) {
    try {
      accessSync(candidate, constants.X_OK);
      return realpathSync(candidate);
    } catch (error) {
      if (
        error instanceof Error &&
        'code' in error &&
        typeof error.code === 'string' &&
        NOT_A_PROGRAM.has(error.code)
      ) {
        continue;
      }
      throw error;
    }
  }
  return undefined;
}
