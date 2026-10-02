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
 * The merged enterShell option is read through devenv's evaluator without
 * running it. An `enterShell:string ""` override must evaluate to the empty
 * hook before devenv may execute a task in the copy. `devenv tasks list --json`
 * then evaluates the copy's current task graph without running those tasks:
 * only devenv's empty-hook enterShell and files/cleanup with no declared files
 * may run there. Any other task evaluates the workspace in place instead.
 *
 * The resulting export and the original merged hook are relocated onto the
 * workspace before direnv imports them. Every replaced string is an absolute
 * path this program chose (copy root, fixed HOME, TMPDIR, runtime), so no shell
 * syntax is interpreted. A path inside the copy becomes its workspace path;
 * direnv executes the original hook once there, including project extensions
 * and managed installs. The compiled task file (DEVENV_TASK_FILE, DEVENV_TASKS)
 * is a store path naming the copy and is unset; `devenv tasks`, `devenv up` and
 * `devenv test` evaluate the workspace in place.
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
/** Touched whenever a shell enters a copy; a copy untouched for 30 days is dropped. */
const USED_MARKER = '.used';
const RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const STAGING_PREFIX = '.new.';
const STAGING_RETENTION_MS = 24 * 60 * 60 * 1000;

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
  const entries = [...devenvFiles(root), ...pathInputs(root).flatMap((input) => inputFiles(root, input))];
  const copy = path.join(cache, digestOf(root, entries));
  if (!existsSync(copy)) {
    const staging = mkdtempSync(path.join(cache, STAGING_PREFIX));
    copyEntries(root, entries, staging);
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
  | { readonly kind: 'confined'; readonly names: ReadonlySet<string>; readonly graph: string }
  | { readonly kind: 'writes'; readonly reason: string };

function strings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item): item is string => typeof item === 'string');
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
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(graph, 'utf8'));
  } catch (error) {
    return { kind: 'writes', reason: `${graph} cannot be read as devenv tasks (${String(error)})` };
  }
  if (!Array.isArray(parsed)) {
    return {
      kind: 'writes',
      reason: `${graph} is not devenv's list of tasks, so its enterShell tasks cannot be checked`,
    };
  }
  const tasks = new Map<string, Task>();
  for (const raw of parsed) {
    if (
      typeof raw !== 'object' ||
      raw === null ||
      !('name' in raw) ||
      typeof raw.name !== 'string' ||
      !('command' in raw) ||
      (raw.command !== null && typeof raw.command !== 'string') ||
      !('before' in raw) ||
      !strings(raw.before) ||
      !('after' in raw) ||
      !strings(raw.after) ||
      tasks.has(raw.name)
    ) {
      return {
        kind: 'writes',
        reason: `${graph} holds a malformed or duplicate task, so its enterShell tasks cannot be checked`,
      };
    }
    tasks.set(raw.name, { name: raw.name, command: raw.command, before: raw.before, after: raw.after });
  }
  if (!tasks.has('devenv:enterShell')) {
    return { kind: 'writes', reason: `${graph} has no devenv:enterShell task` };
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
  // devenv:files has a command only when files are declared; that command writes into the root.
  const writing = [...closure].filter((name) => {
    const task = tasks.get(name);
    return (
      task !== undefined && task.command !== null && name !== 'devenv:enterShell' && name !== 'devenv:files:cleanup'
    );
  });
  return writing.length === 0
    ? { kind: 'confined', names: new Set(tasks.keys()), graph: realpathSync(graph) }
    : {
        kind: 'writes',
        reason: `enterShell runs ${writing.sort().join(', ')}, which write into the project they run in`,
      };
}

/**
 * Evaluate the current task graph with enterShell disabled before executing
 * anything in the shared copy. The evaluator's original merged enterShell hook
 * is relocated and run once by direnv in the workspace, not by devenv in the
 * copy. Other tasks that write outside .devenv require in-place evaluation.
 */
export function exportShell(
  projectRoot: string,
  copy: string,
  args: readonly string[],
  env: NodeJS.ProcessEnv,
): number {
  const workspace = realpathSync(projectRoot);
  const fixed = fixedDirectories(path.dirname(copy));
  const copyDevenv = path.join(copy, DEVENV_DIR);
  const workspaceDevenv = path.join(workspace, DEVENV_DIR);
  const inPlace = (reason: string): number => {
    console.error(`shared devenv: ${reason}; evaluating in place`);
    const evaluated = Bun.spawnSync(['devenv', ...args], {
      cwd: workspaceDevenv,
      env,
      stdin: 'inherit',
      stdout: 'inherit',
      stderr: 'inherit',
    });
    return evaluated.exitCode;
  };
  if (args.at(-1) !== 'direnv-export') {
    return inPlace('only direnv-export can use the shared copy');
  }
  const copyEnv = { ...env, HOME: fixed.home, TMPDIR: fixed.tmp, XDG_RUNTIME_DIR: fixed.run };
  const originalHook = Bun.spawnSync(['devenv', ...args.slice(0, -1), 'eval', 'enterShell'], {
    cwd: copyDevenv,
    env: copyEnv,
    stdin: 'inherit',
    stdout: 'pipe',
    stderr: 'inherit',
  });
  if (originalHook.exitCode !== 0) {
    return inPlace(`devenv eval enterShell exited ${originalHook.exitCode}`);
  }
  // An option override replaces the whole merged hook, including contributions
  // from project modules, rust and devenv itself. Check the evaluator's value
  // before the first command that can execute a task in the shared copy.
  const withoutHook = [...args.slice(0, -1), '--option', 'enterShell:string', ''];
  const disabledHook = Bun.spawnSync(['devenv', ...withoutHook, 'eval', 'enterShell'], {
    cwd: copyDevenv,
    env: copyEnv,
    stdin: 'inherit',
    stdout: 'pipe',
    stderr: 'inherit',
  });
  if (disabledHook.exitCode !== 0) {
    return inPlace(`devenv eval with enterShell disabled exited ${disabledHook.exitCode}`);
  }
  let original: unknown;
  let disabled: unknown;
  try {
    original = JSON.parse(originalHook.stdout.toString());
    disabled = JSON.parse(disabledHook.stdout.toString());
  } catch (error) {
    return inPlace(`devenv eval returned invalid JSON (${String(error)})`);
  }
  if (
    typeof original !== 'object' ||
    original === null ||
    !('enterShell' in original) ||
    typeof original.enterShell !== 'string' ||
    typeof disabled !== 'object' ||
    disabled === null ||
    !('enterShell' in disabled) ||
    disabled.enterShell !== ''
  ) {
    return inPlace('devenv did not prove that its copy has an empty enterShell hook');
  }
  const hook = original.enterShell;
  const listed = Bun.spawnSync(['devenv', ...withoutHook, 'tasks', 'list', '--json'], {
    cwd: copyDevenv,
    env: copyEnv,
    stdin: 'inherit',
    stdout: 'pipe',
    stderr: 'inherit',
  });
  if (listed.exitCode !== 0) {
    return inPlace(`devenv tasks list exited ${listed.exitCode}, so its enterShell tasks cannot be checked`);
  }
  let tasks: unknown;
  try {
    tasks = JSON.parse(listed.stdout.toString());
  } catch (error) {
    return inPlace(`devenv tasks list returned invalid JSON (${String(error)})`);
  }
  if (!Array.isArray(tasks)) {
    return inPlace('devenv tasks list did not return an array');
  }
  const listedNames = new Set<string>();
  for (const row of tasks) {
    const task: unknown = row;
    if (
      typeof task !== 'object' ||
      task === null ||
      !('name' in task) ||
      typeof task.name !== 'string' ||
      listedNames.has(task.name)
    ) {
      return inPlace('devenv tasks list returned malformed or duplicate task names');
    }
    listedNames.add(task.name);
  }
  if (!listedNames.has('devenv:enterShell')) {
    return inPlace('devenv tasks list did not include devenv:enterShell');
  }
  const planned = enterShellTasks(path.join(copyDevenv, '.devenv'));
  if (planned.kind !== 'confined') {
    return inPlace(
      planned.kind === 'writes'
        ? planned.reason
        : `devenv left no task graph at ${planned.graph}, so its enterShell tasks cannot be checked`,
    );
  }
  if (planned.names.size !== listedNames.size || [...listedNames].some((name) => !planned.names.has(name))) {
    return inPlace('devenv task list differs from the compiled task graph');
  }
  const devenv = Bun.spawnSync(['devenv', ...withoutHook, 'direnv-export'], {
    cwd: copyDevenv,
    env: copyEnv,
    stdin: 'inherit',
    stdout: 'pipe',
    stderr: 'inherit',
  });
  if (devenv.exitCode !== 0) {
    return devenv.exitCode;
  }
  const ran = enterShellTasks(path.join(copyDevenv, '.devenv'));
  if (ran.kind !== 'confined' || ran.graph !== planned.graph) {
    console.error('shared devenv: task graph changed after preflight; the export cannot be relocated');
    return 1;
  }
  const moves = relocations(copy, workspace, env);
  process.stdout.write(relocate(devenv.stdout.toString(), moves));
  process.stdout.write('\nunset DEVENV_TASK_FILE DEVENV_TASKS\n');
  const copyDot = path.join(copyDevenv, '.devenv');
  const workspaceDot = path.join(workspaceDevenv, '.devenv');
  mkdirSync(path.join(workspaceDot, 'state'), { recursive: true });
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
  process.stdout.write(`\n${relocate(hook, moves)}\n`);
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
