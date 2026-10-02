#!/usr/bin/env bun
/**
 * Managed by `smoo monorepo`. The managed .envrc runs it inside a cowshed
 * sandbox only, before `use devenv`:
 *
 *   bun tooling/direnv/shared-devenv.ts enter <project root>
 *     prints the shared copy of this project's devenv inputs, or exits 1 for an
 *     in-place evaluation, saying why;
 *   bun tooling/direnv/shared-devenv.ts export <project root> <copy> <devenv args…>
 *     stands in for devenv as `use devenv`'s DEVENV_BIN.
 *
 * devenv evaluates a project at its path, and it cannot do otherwise, because
 * the result really differs per path. The absolute root, the .devenv dotfile,
 * the runtime directory derived from XDG_RUNTIME_DIR, and TMPDIR are fields of
 * the arguments devenv hands Nix (devenv-core nix_args.rs `NixArgs`
 * devenv_root, devenv_dotfile, devenv_tmpdir, devenv_runtime), and those
 * arguments are the evaluation-cache key (evaluator.rs
 * `eval_cache_key_args`). The modules compile them into the shell:
 * - top-level.nix exports DEVENV_ROOT, DEVENV_STATE, DEVENV_RUNTIME and
 *   DEVENV_DOTFILE (:388-391), and writes TMPDIR (:412) and the runtime and
 *   dotfile (:442-443) into the shell hook;
 * - files.nix bakes the state (:24) and the root (:174) into the
 *   files-cleanup script;
 * - rust.nix bakes the state into CARGO_INSTALL_ROOT (:398);
 * - processes.nix bakes the runtime into the process manager (:673).
 * nixpkgs also reads impure overlays from HOME.
 *
 * A cowshed workspace is a copy of another checkout at another path, with its
 * own private HOME, TMPDIR and runtime directory, so its first shell entry
 * evaluated nixpkgs from scratch. Measured on a large monorepo: about 10 s of
 * CPU and 37,000 synchronous nix-daemon round trips, 27 s on an idle host and
 * over 8 minutes at load 60-80.
 *
 * Inside a sandbox, the supervisor names COWSHED_DEVENV_CACHE, a directory
 * every workspace on the host shares, and the evaluation is keyed by content.
 * The files devenv reads are copied in the repository's layout to
 * $COWSHED_DEVENV_CACHE/<digest of their content>:
 * - tooling/direnv, read through symlinks;
 * - the local `path:` inputs devenv.yaml names, as Nix sees them.
 * devenv evaluates that copy with HOME, TMPDIR and XDG_RUNTIME_DIR fixed
 * under the cache. Workspaces with byte-identical inputs reach the same path
 * and the same cache entry; a changed input is a new digest and a fresh
 * evaluation.
 *
 * The export devenv prints is then moved onto the workspace before direnv
 * imports it. Every replaced string is an absolute path this program chose
 * (the copy's root, and the fixed HOME, TMPDIR and runtime), unique by
 * construction, so the replacement is exact and never interprets devenv's
 * script. A path under the copy becomes the same path under the project
 * root, and the fixed HOME, TMPDIR and runtime become the sandbox's own. The
 * shell hook therefore runs against the workspace exactly as an in-place
 * evaluation's would. The compiled task file (DEVENV_TASK_FILE, DEVENV_TASKS)
 * is a store path whose contents name the copy and cannot be moved, so the
 * export unsets it. `devenv tasks`, `devenv up` and `devenv test` evaluate the
 * workspace in place, so none of them reads it.
 *
 * devenv runs the enterShell tasks itself, in the directory it evaluated. Its
 * own devenv:enterShell writes only into the copy's .devenv, and so do
 * devenv:files and devenv:files:cleanup when no `files` are declared. A project
 * whose enterShell tasks are anything else writes into its project from those
 * tasks, so it evaluates in place. The task graph that evaluation leaves in the
 * checkout's .devenv, which a clone inherits, sends later entries straight
 * there.
 *
 * The host never takes this path. The cache is writable by every sandbox, so a
 * host shell that evaluated or imported from it would run whatever any
 * sandbox wrote there.
 */
import {
  chmodSync,
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';

const DEVENV_DIR = 'tooling/direnv';
/** Marks a copy's root. The managed enterShell prologue skips workspace installs under it. */
const SNAPSHOT_MARKER = '.smoo-devenv-snapshot';
/** Touched whenever a shell enters a copy; a copy untouched for 30 days is dropped. */
const USED_MARKER = '.used';
const RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const STAGING_PREFIX = '.new.';
const STAGING_RETENTION_MS = 24 * 60 * 60 * 1000;
/**
 * The tasks devenv's own modules put before devenv:enterShell that write
 * nowhere but the evaluated root's .devenv, provided they have no command
 * (devenv:files with no `files` declared) or are listed here with one.
 */
const COPY_SAFE_TASKS = new Set(['devenv:enterShell', 'devenv:files', 'devenv:files:cleanup']);

/** The checkout evaluates in place. `reason` is empty outside a sandbox, where nothing needs saying. */
class InPlace extends Error {
  constructor(readonly reason: string) {
    super(reason);
  }
}

function fixedDirectories(cache: string): { home: string; tmp: string; run: string } {
  return { home: path.join(cache, 'home'), tmp: path.join(cache, 'tmp'), run: path.join(cache, 'run') };
}

/**
 * devenv's runtime directory for a .devenv path. devenv derives it itself from
 * XDG_RUNTIME_DIR, or /tmp, and ignores a DEVENV_RUNTIME it is handed
 * (devenv-core paths.rs `resolve_runtime_dir` and its test
 * `resolve_runtime_dir_ignores_devenv_runtime`). The shell must carry exactly
 * the directory devenv computes for the workspace, or `devenv up` and the
 * shell disagree about where sockets live. The managed test pins this against
 * devenv itself.
 */
export function devenvRuntimeDir(xdgRuntimeDir: string | undefined, dotfile: string): string {
  const digest = new Bun.CryptoHasher('sha256').update(dotfile).digest('hex');
  const base = xdgRuntimeDir === undefined || xdgRuntimeDir === '' ? '/tmp' : xdgRuntimeDir;
  return path.join(base, `devenv-${digest.slice(0, 7)}`);
}

export interface Relocation {
  readonly from: string;
  readonly to: string;
}

/** Copy-side paths and their workspace counterparts. The runtime, inside `run/`, goes first. */
export function relocations(copy: string, workspace: string, env: NodeJS.ProcessEnv): Relocation[] {
  const fixed = fixedDirectories(path.dirname(copy));
  const run = env.XDG_RUNTIME_DIR === undefined || env.XDG_RUNTIME_DIR === '' ? '/tmp' : env.XDG_RUNTIME_DIR;
  return [
    {
      from: devenvRuntimeDir(fixed.run, path.join(copy, DEVENV_DIR, '.devenv')),
      to: devenvRuntimeDir(run, path.join(workspace, DEVENV_DIR, '.devenv')),
    },
    { from: fixed.run, to: run },
    { from: fixed.tmp, to: env.TMPDIR === undefined || env.TMPDIR === '' ? '/tmp' : env.TMPDIR },
    { from: fixed.home, to: env.HOME ?? '' },
    { from: copy, to: workspace },
  ];
}

export function relocate(text: string, moves: readonly Relocation[]): string {
  let moved = text;
  for (const { from, to } of moves) {
    moved = moved.replaceAll(from, to);
  }
  return moved;
}

/** The local `path:` inputs devenv.yaml names outside tooling/direnv, relative to `root`. */
function pathInputs(root: string): string[] {
  const devenvDir = path.join(root, DEVENV_DIR);
  const yamlFile = path.join(devenvDir, 'devenv.yaml');
  if (!existsSync(yamlFile)) {
    throw new InPlace(`${yamlFile} is missing`);
  }
  const document: unknown = Bun.YAML.parse(readFileSync(yamlFile, 'utf8'));
  if (typeof document !== 'object' || document === null || Array.isArray(document)) {
    throw new InPlace(`${yamlFile} is not a YAML mapping`);
  }
  // `imports` can name modules anywhere; only `path:` inputs are copied.
  const imports = 'imports' in document ? document.imports : undefined;
  if (Array.isArray(imports) && imports.length > 0) {
    throw new InPlace(`${yamlFile} has imports, which are not copied`);
  }
  const inputs = 'inputs' in document ? document.inputs : undefined;
  const outside = new Set<string>();
  for (const [name, input] of Object.entries(typeof inputs === 'object' && inputs !== null ? inputs : {})) {
    const url: unknown = typeof input === 'object' && input !== null && 'url' in input ? input.url : undefined;
    if (typeof url !== 'string' || !url.startsWith('path:')) {
      continue;
    }
    const target = path.resolve(devenvDir, url.slice('path:'.length));
    const resolved = existsSync(target) && statSync(target).isDirectory() ? realpathSync(target) : undefined;
    if (resolved === undefined || !resolved.startsWith(`${root}/`)) {
      throw new InPlace(`path input ${name} (${url}) is not a directory inside ${root}`);
    }
    const relative = path.relative(root, resolved);
    if (relative !== DEVENV_DIR && !relative.startsWith(`${DEVENV_DIR}/`)) {
      outside.add(relative);
    }
  }
  return [...outside].sort();
}

type Entry =
  | { readonly kind: 'file'; readonly relative: string; readonly executable: boolean }
  | { readonly kind: 'link'; readonly relative: string; readonly target: string };

/** tooling/direnv as devenv reads it: every file through symlinks, without the checkout's .devenv and .direnv. */
function devenvFiles(root: string): Entry[] {
  const entries: Entry[] = [];
  const visit = (relative: string, ancestors: ReadonlySet<string>): void => {
    const absolute = path.join(root, relative);
    const real = realpathSync(absolute);
    if (ancestors.has(real)) {
      throw new InPlace(`${absolute} links back into itself`);
    }
    const stat = statSync(absolute);
    if (stat.isDirectory()) {
      const inside = new Set(ancestors).add(real);
      for (const name of readdirSync(absolute).sort()) {
        if (relative === DEVENV_DIR && (name === '.devenv' || name === '.direnv')) {
          continue;
        }
        visit(path.join(relative, name), inside);
      }
    } else if (stat.isFile()) {
      entries.push({ kind: 'file', relative, executable: (stat.mode & 0o100) !== 0 });
    }
  };
  visit(DEVENV_DIR, new Set());
  return entries;
}

/** A `path:` input as Nix copies it into the store: files, and symlinks as links. */
function inputFiles(root: string, input: string): Entry[] {
  const entries: Entry[] = [];
  const visit = (relative: string): void => {
    const absolute = path.join(root, relative);
    const stat = lstatSync(absolute);
    if (stat.isSymbolicLink()) {
      entries.push({ kind: 'link', relative, target: readlinkSync(absolute) });
    } else if (stat.isDirectory()) {
      for (const name of readdirSync(absolute).sort()) {
        visit(path.join(relative, name));
      }
    } else if (stat.isFile()) {
      entries.push({ kind: 'file', relative, executable: (stat.mode & 0o100) !== 0 });
    }
  };
  visit(input);
  return entries;
}

/** Every entry's kind, path, executable bit (Nix hashes it into a narHash), and bytes or link target. */
function digestOf(root: string, entries: readonly Entry[]): string {
  const hasher = new Bun.CryptoHasher('sha256');
  for (const entry of entries) {
    if (entry.kind === 'link') {
      hasher.update(`link\0${entry.relative}\0${entry.target}\0`);
    } else {
      const bytes = readFileSync(path.join(root, entry.relative));
      hasher.update(`file\0${entry.relative}\0${entry.executable ? 'x' : '-'}\0${bytes.byteLength}\0`);
      hasher.update(bytes);
    }
  }
  return hasher.digest('hex').slice(0, 32);
}

function copyEntries(root: string, entries: readonly Entry[], into: string): void {
  for (const entry of entries) {
    const destination = path.join(into, entry.relative);
    mkdirSync(path.dirname(destination), { recursive: true });
    if (entry.kind === 'link') {
      symlinkSync(entry.target, destination);
    } else {
      copyFileSync(path.join(root, entry.relative), destination);
      chmodSync(destination, entry.executable ? 0o755 : 0o644);
    }
  }
}

function touch(file: string): void {
  const now = new Date();
  if (existsSync(file)) {
    utimesSync(file, now, now);
  } else {
    writeFileSync(file, '');
  }
}

/** Drop copies no shell entered for RETENTION_MS, and staging directories a killed entry left. */
function prune(cache: string, keep: string): void {
  const now = Date.now();
  for (const name of readdirSync(cache)) {
    const directory = path.join(cache, name);
    if (directory === keep) {
      continue;
    }
    if (name.startsWith(STAGING_PREFIX)) {
      if (now - lstatSync(directory).mtimeMs > STAGING_RETENTION_MS) {
        rmSync(directory, { recursive: true, force: true });
      }
      continue;
    }
    const used = path.join(directory, USED_MARKER);
    if (existsSync(used) && now - statSync(used).mtimeMs > RETENTION_MS) {
      rmSync(directory, { recursive: true, force: true });
    }
  }
}

/** The shared copy for the checkout at `projectRoot`, published once per content. */
export function enter(projectRoot: string, env: NodeJS.ProcessEnv): string {
  const configured = env.COWSHED_DEVENV_CACHE;
  if (configured === undefined || configured === '') {
    throw new InPlace('');
  }
  if (!existsSync(configured) || !statSync(configured).isDirectory()) {
    throw new InPlace(`COWSHED_DEVENV_CACHE (${configured}) is not a directory`);
  }
  // devenv canonicalizes the root it evaluates; every fixed path is spelled
  // the way its export will spell it.
  const cache = realpathSync(configured);
  const root = realpathSync(projectRoot);
  const devenvDir = path.join(root, DEVENV_DIR);
  // The copy evaluates with HOME fixed, so nixpkgs configuration under HOME
  // would silently stop applying. Checkout-local files never leave the
  // checkout: they are the place for secrets and machine-specific overrides.
  for (const local of [
    path.join(env.HOME ?? '', '.config', 'nixpkgs'),
    path.join(devenvDir, '.env'),
    path.join(devenvDir, 'devenv.local.nix'),
    path.join(devenvDir, 'devenv.local.yaml'),
  ]) {
    if (existsSync(local)) {
      throw new InPlace(`${local} exists and is not shared`);
    }
  }
  // The checkout's own last in-place evaluation, a clone's inherited one
  // included, already says whether its enterShell tasks would write into it.
  const previous = enterShellTasks(path.join(devenvDir, '.devenv'));
  if (previous.kind === 'writes') {
    throw new InPlace(previous.reason);
  }
  const entries = [...devenvFiles(root), ...pathInputs(root).flatMap((input) => inputFiles(root, input))];
  const copy = path.join(cache, digestOf(root, entries));
  if (!existsSync(copy)) {
    const staging = mkdtempSync(path.join(cache, STAGING_PREFIX));
    copyEntries(root, entries, staging);
    writeFileSync(path.join(staging, SNAPSHOT_MARKER), '');
    writeFileSync(path.join(staging, USED_MARKER), '');
    try {
      renameSync(staging, copy);
    } catch (error) {
      rmSync(staging, { recursive: true, force: true });
      // A shell that lost the race finds the winner's identical copy published.
      if (!existsSync(copy)) {
        throw error;
      }
    }
    prune(cache, copy);
  }
  // Marked on entry, before devenv runs in it, so a prune elsewhere never takes
  // a copy a shell is evaluating.
  touch(path.join(copy, USED_MARKER));
  for (const directory of Object.values(fixedDirectories(cache))) {
    mkdirSync(directory, { recursive: true });
  }
  return copy;
}

interface Task {
  readonly name: string;
  readonly command: string | null;
  readonly before: readonly string[];
  readonly after: readonly string[];
}

/** What the enterShell tasks of one evaluation do, read from the task graph it left. */
type EnterShellTasks =
  | { readonly kind: 'absent'; readonly graph: string }
  | { readonly kind: 'confined' }
  | { readonly kind: 'writes'; readonly reason: string };

function strings(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : [];
}

/**
 * Whether the enterShell tasks of the evaluation whose .devenv is `dotfile`
 * write only into that .devenv. The graph is the file devenv loaded them from,
 * behind its GC root `gc/task-config-devenv-config-task-config` (devenv
 * mod.rs `load_tasks`, backend.rs `build_devenv`).
 */
function enterShellTasks(dotfile: string): EnterShellTasks {
  const graph = path.join(dotfile, 'gc', 'task-config-devenv-config-task-config');
  if (!existsSync(graph)) {
    return { kind: 'absent', graph };
  }
  const parsed: unknown = JSON.parse(readFileSync(graph, 'utf8'));
  if (!Array.isArray(parsed)) {
    return {
      kind: 'writes',
      reason: `${graph} is not devenv's list of tasks, so its enterShell tasks cannot be checked`,
    };
  }
  const tasks = new Map<string, Task>();
  for (const raw of parsed) {
    if (typeof raw !== 'object' || raw === null || !('name' in raw) || typeof raw.name !== 'string') {
      return {
        kind: 'writes',
        reason: `${graph} holds a task without a name, so its enterShell tasks cannot be checked`,
      };
    }
    tasks.set(raw.name, {
      name: raw.name,
      command: 'command' in raw && typeof raw.command === 'string' ? raw.command : null,
      before: strings('before' in raw ? raw.before : undefined),
      after: strings('after' in raw ? raw.after : undefined),
    });
  }
  const closure = new Set<string>();
  const pending = ['devenv:enterShell'];
  for (let name = pending.pop(); name !== undefined; name = pending.pop()) {
    if (closure.has(name)) {
      continue;
    }
    closure.add(name);
    pending.push(...(tasks.get(name)?.after ?? []));
    for (const task of tasks.values()) {
      if (task.before.includes(name)) {
        pending.push(task.name);
      }
    }
  }
  // devenv:files has a command only when `files` are declared, and then it writes them into the root.
  const writing = [...closure].filter((name) => {
    const task = tasks.get(name);
    return task !== undefined && task.command !== null && (!COPY_SAFE_TASKS.has(name) || name === 'devenv:files');
  });
  return writing.length === 0
    ? { kind: 'confined' }
    : {
        kind: 'writes',
        reason: `enterShell runs ${writing.sort().join(', ')}, which write into the project they run in`,
      };
}

/**
 * Run devenv in `copy` with the fixed HOME, TMPDIR and runtime, print its
 * export moved onto `projectRoot`, and leave the workspace what an in-place
 * evaluation would have: the inputs `use devenv` watches, a GC root for the
 * shell, and a devenv.lock that devenv rewrote. A project whose enterShell
 * tasks write into it is evaluated in place instead. Returns the exit status
 * `use devenv` reads.
 */
export function exportShell(
  projectRoot: string,
  copy: string,
  args: readonly string[],
  env: NodeJS.ProcessEnv,
): number {
  const workspace = realpathSync(projectRoot);
  const fixed = fixedDirectories(path.dirname(copy));
  const devenv = Bun.spawnSync(['devenv', ...args], {
    cwd: path.join(copy, DEVENV_DIR),
    env: { ...env, HOME: fixed.home, TMPDIR: fixed.tmp, XDG_RUNTIME_DIR: fixed.run },
    stdin: 'inherit',
    stdout: 'pipe',
    stderr: 'inherit',
  });
  if (devenv.exitCode !== 0) {
    return devenv.exitCode;
  }
  // devenv ran the enterShell tasks in the copy. When they write into the
  // project they ran in, the workspace still needs them run against it.
  const ran = enterShellTasks(path.join(copy, DEVENV_DIR, '.devenv'));
  if (ran.kind !== 'confined') {
    const reason =
      ran.kind === 'writes'
        ? ran.reason
        : `devenv left no task graph at ${ran.graph}, so its enterShell tasks cannot be checked`;
    console.error(`shared devenv: ${reason}; evaluating in place`);
    const inPlace = Bun.spawnSync(['devenv', ...args], {
      cwd: path.join(workspace, DEVENV_DIR),
      env,
      stdin: 'inherit',
      stdout: 'inherit',
      stderr: 'inherit',
    });
    return inPlace.exitCode;
  }
  const moves = relocations(copy, workspace, env);
  process.stdout.write(relocate(devenv.stdout.toString(), moves));
  process.stdout.write('\nunset DEVENV_TASK_FILE DEVENV_TASKS\n');

  const copyDevenv = path.join(copy, DEVENV_DIR);
  const workspaceDevenv = path.join(workspace, DEVENV_DIR);
  const copyDot = path.join(copyDevenv, '.devenv');
  const workspaceDot = path.join(workspaceDevenv, '.devenv');
  mkdirSync(path.join(workspaceDot, 'gc'), { recursive: true });
  const inputPaths = path.join(copyDot, 'input-paths.txt');
  if (existsSync(inputPaths)) {
    const staged = path.join(workspaceDot, `input-paths.txt.${process.pid}`);
    writeFileSync(staged, relocate(readFileSync(inputPaths, 'utf8'), moves));
    renameSync(staged, path.join(workspaceDot, 'input-paths.txt'));
  }
  const copyShellRoot = path.join(copyDot, 'gc', 'shell');
  const workspaceShellRoot = path.join(workspaceDot, 'gc', 'shell');
  if (existsSync(copyShellRoot)) {
    const shell = readlinkSync(copyShellRoot);
    const current = lstatSync(workspaceShellRoot, { throwIfNoEntry: false })?.isSymbolicLink()
      ? readlinkSync(workspaceShellRoot)
      : '';
    if (current !== shell) {
      const registered = Bun.spawnSync(['nix-store', '--realise', shell, '--add-root', workspaceShellRoot], {
        env,
        stdout: 'ignore',
        stderr: 'inherit',
      });
      if (registered.exitCode !== 0) {
        console.error(
          `shared devenv: nix-store exited ${registered.exitCode} registering ${workspaceShellRoot} for ${shell}; the shell this workspace imported has no GC root of its own`,
        );
      }
    }
  }
  const copyLock = path.join(copyDevenv, 'devenv.lock');
  const workspaceLock = path.join(workspaceDevenv, 'devenv.lock');
  if (existsSync(copyLock)) {
    const written = readFileSync(copyLock);
    if (!existsSync(workspaceLock) || !written.equals(readFileSync(workspaceLock))) {
      writeFileSync(workspaceLock, written);
      console.error(`shared devenv: devenv rewrote devenv.lock; copied it into ${workspaceDevenv}`);
    }
  }
  return 0;
}

if (import.meta.main) {
  const [command, projectRoot, ...rest] = process.argv.slice(2);
  if (command === 'enter' && projectRoot !== undefined && rest.length === 0) {
    try {
      process.stdout.write(`${enter(projectRoot, process.env)}\n`);
    } catch (error) {
      if (!(error instanceof InPlace)) {
        console.error(
          `shared devenv: entering the shared copy for ${projectRoot} failed; evaluating in place: ${String(error)}`,
        );
      } else if (error.reason !== '') {
        console.error(`shared devenv: ${error.reason}; evaluating in place`);
      }
      process.exit(1);
    }
  } else if (command === 'export' && projectRoot !== undefined && rest.length > 0) {
    const [copy = '', ...args] = rest;
    process.exit(exportShell(projectRoot, copy, args, process.env));
  } else {
    console.error(
      `shared-devenv.ts: expected \`enter <project root>\` or \`export <project root> <copy> <devenv args…>\`, got: ${process.argv.slice(2).join(' ')}`,
    );
    process.exit(2);
  }
}
