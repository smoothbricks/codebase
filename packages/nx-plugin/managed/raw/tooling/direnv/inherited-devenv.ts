#!/usr/bin/env bun
/**
 * Managed by `smoo monorepo`. A cowshed clone inherits its origin's .devenv
 * along with the source tree. Reuse a real devenv export from that private
 * image only while the evaluated inputs and toolchain still agree. No shell
 * reads a writable cache belonging to another workspace.
 */
import {
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  renameSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';

const DEVENV_DIR = 'tooling/direnv';
const ARTIFACT = 'inherited-shell.json';
const ENV_PATHS = [
  'HOME',
  'TMPDIR',
  'XDG_RUNTIME_DIR',
  'XDG_CONFIG_HOME',
  'XDG_CACHE_HOME',
  'XDG_DATA_HOME',
  'XDG_STATE_HOME',
  'DEVENV_HOME',
  'DIRENV_CONFIG',
  'NX_SOCKET_DIR',
] as const;
// Nix receives no caller credential, gateway token, or ambient impure override
// while creating an artifact. A live shell still runs with its exact caller env.
const EVALUATOR_ENV = [
  'PATH',
  'HOME',
  'TMPDIR',
  'XDG_RUNTIME_DIR',
  'XDG_CONFIG_HOME',
  'XDG_CACHE_HOME',
  'XDG_DATA_HOME',
  'XDG_STATE_HOME',
  'DEVENV_HOME',
  'DIRENV_CONFIG',
  'NX_SOCKET_DIR',
  'USER',
  'LOGNAME',
  'SHELL',
  'LANG',
  'LC_ALL',
  'LC_CTYPE',
  'TERM',
  'NIX_SSL_CERT_FILE',
  'SSL_CERT_FILE',
  'NODE_EXTRA_CA_CERTS',
  'GIT_SSL_CAINFO',
  'DEVELOPER_DIR',
  'SDKROOT',
] as const;

type Watch = { readonly scope: 'root' | 'home' | 'runtime' | 'tmp' | 'store'; readonly relative: string };
type Entry = {
  readonly relative: string;
  readonly kind: 'file' | 'link';
  readonly executable?: boolean;
  readonly target?: string;
};
interface Artifact {
  readonly version: 1;
  readonly root: string;
  readonly paths: Readonly<Record<string, string>>;
  readonly basis: string;
  readonly watches: readonly Watch[];
  readonly watchBasis: string;
  readonly toolchain: string;
  readonly export: string;
}

function privateEnvironment(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const selected: NodeJS.ProcessEnv = {};
  for (const name of EVALUATOR_ENV) {
    const value = env[name];
    if (value !== undefined) selected[name] = value;
  }
  return selected;
}

function toolchain(env: NodeJS.ProcessEnv): string | undefined {
  const versions: string[] = [];
  for (const command of [
    ['devenv', 'version'],
    ['nix', '--version'],
  ]) {
    const result = Bun.spawnSync(command, { env, stdout: 'pipe', stderr: 'pipe' });
    if (result.exitCode !== 0) return undefined;
    versions.push(result.stdout.toString().trim());
  }
  return `${process.platform}/${process.arch}\n${versions.join('\n')}`;
}

/** devenv's runtime directory is keyed by the dotfile, not by DEVENV_RUNTIME. */
export function devenvRuntimeDir(xdg: string | undefined, dotfile: string): string {
  const hash = new Bun.CryptoHasher('sha256').update(dotfile).digest('hex');
  return path.join(xdg || '/tmp', `devenv-${hash.slice(0, 7)}`);
}

function pathsOf(root: string, env: NodeJS.ProcessEnv): Record<string, string> {
  const paths: Record<string, string> = {};
  for (const name of ENV_PATHS) {
    const value = env[name];
    if (value) paths[name] = value;
  }
  paths.DEVENV_RUNTIME = devenvRuntimeDir(env.XDG_RUNTIME_DIR, path.join(root, DEVENV_DIR, '.devenv'));
  return paths;
}

export interface Relocation {
  readonly from: string;
  readonly to: string;
}
export function relocations(
  origin: string,
  originPaths: Readonly<Record<string, string>>,
  root: string,
  env: NodeJS.ProcessEnv,
): Relocation[] {
  const current = pathsOf(root, env);
  const moves: Relocation[] = [];
  for (const [name, from] of Object.entries(originPaths)) {
    const to = current[name];
    if (to && from !== to) moves.push({ from, to });
  }
  moves.push({ from: origin, to: root });
  moves.sort((left, right) => right.from.length - left.from.length);
  return moves;
}
export function relocate(text: string, moves: readonly Relocation[]): string {
  let result = text;
  for (const { from, to } of moves) result = result.replaceAll(from, to);
  return result;
}

/** Local path inputs have Nix's layout; unknown YAML imports are not guessed. */
function pathInputs(root: string): string[] {
  const dir = path.join(root, DEVENV_DIR);
  const yaml = path.join(dir, 'devenv.yaml');
  const document: unknown = Bun.YAML.parse(readFileSync(yaml, 'utf8'));
  if (typeof document !== 'object' || document === null || Array.isArray(document))
    throw new Error(`${yaml} is not a mapping`);
  if ('imports' in document && Array.isArray(document.imports) && document.imports.length > 0) {
    throw new Error(`${yaml} imports modules outside the captured inputs`);
  }
  const inputs = 'inputs' in document ? document.inputs : undefined;
  const directories = new Set<string>();
  for (const [name, input] of Object.entries(typeof inputs === 'object' && inputs !== null ? inputs : {})) {
    const url: unknown = typeof input === 'object' && input !== null && 'url' in input ? input.url : undefined;
    if (typeof url !== 'string' || !url.startsWith('path:')) continue;
    const resolved = realpathSync(path.resolve(dir, url.slice(5)));
    if (!resolved.startsWith(`${root}/`) || !statSync(resolved).isDirectory()) {
      throw new Error(`path input ${name} is not a directory inside ${root}`);
    }
    const relative = path.relative(root, resolved);
    if (relative !== DEVENV_DIR && !relative.startsWith(`${DEVENV_DIR}/`)) directories.add(relative);
  }
  return [...directories].sort();
}

function entriesUnder(
  root: string,
  relative: string,
  followLinks: boolean,
  entries: Entry[],
  ancestors: ReadonlySet<string>,
): void {
  const absolute = path.join(root, relative);
  const stat = lstatSync(absolute);
  if (stat.isSymbolicLink() && !followLinks) {
    entries.push({ relative, kind: 'link', target: readlinkSync(absolute) });
    return;
  }
  const real = realpathSync(absolute);
  if (!real.startsWith(`${root}/`)) throw new Error(`${absolute} resolves outside ${root}`);
  if (ancestors.has(real)) throw new Error(`${absolute} links back into itself`);
  const target = statSync(absolute);
  if (target.isDirectory()) {
    const inside = new Set(ancestors).add(real);
    for (const name of readdirSync(absolute).sort()) {
      if (relative === DEVENV_DIR && (name === '.devenv' || name === '.direnv')) continue;
      entriesUnder(root, path.join(relative, name), followLinks, entries, inside);
    }
  } else if (target.isFile()) {
    entries.push({ relative, kind: 'file', executable: (target.mode & 0o100) !== 0 });
  }
}

function sourceBasis(root: string): string {
  const entries: Entry[] = [];
  entriesUnder(root, DEVENV_DIR, true, entries, new Set());
  for (const relative of pathInputs(root)) entriesUnder(root, relative, false, entries, new Set());
  entries.sort((left, right) => left.relative.localeCompare(right.relative));
  const hash = new Bun.CryptoHasher('sha256');
  for (const entry of entries) {
    if (entry.kind === 'link') {
      hash.update(`link\0${entry.relative}\0${entry.target}\0`);
    } else {
      const bytes = readFileSync(path.join(root, entry.relative));
      hash.update(`file\0${entry.relative}\0${entry.executable ? 'x' : '-'}\0${bytes.byteLength}\0`);
      hash.update(bytes);
    }
  }
  return hash.digest('hex');
}

function watchPaths(root: string, env: NodeJS.ProcessEnv): Watch[] {
  const inputPaths = readFileSync(path.join(root, DEVENV_DIR, '.devenv', 'input-paths.txt'), 'utf8');
  const home = env.HOME;
  const runtime = devenvRuntimeDir(env.XDG_RUNTIME_DIR, path.join(root, DEVENV_DIR, '.devenv'));
  const tmp = env.TMPDIR || '/tmp';
  const bases: readonly [Watch['scope'], string | undefined][] = [
    ['runtime', runtime],
    ['home', home],
    ['root', root],
    ['tmp', tmp],
    ['store', '/nix/store'],
  ];
  const watches = new Map<string, Watch>();
  for (const input of inputPaths.split('\n').filter(Boolean)) {
    if (!path.isAbsolute(input)) throw new Error(`devenv input is not absolute: ${input}`);
    const match = [...bases]
      .sort((a, b) => (b[1]?.length ?? 0) - (a[1]?.length ?? 0))
      .find(([, base]) => base && (input === base || input.startsWith(`${base}/`)));
    if (!match || !match[1]) throw new Error(`devenv input is outside isolated roots: ${input}`);
    const relative = path.relative(match[1], input);
    const watch: Watch = { scope: match[0], relative };
    watches.set(`${watch.scope}:${watch.relative}`, watch);
  }
  return [...watches.values()].sort((a, b) => `${a.scope}:${a.relative}`.localeCompare(`${b.scope}:${b.relative}`));
}

function watchedBasis(root: string, env: NodeJS.ProcessEnv, watches: readonly Watch[]): string {
  const bases: Record<Watch['scope'], string> = {
    root,
    home: env.HOME ?? '',
    runtime: devenvRuntimeDir(env.XDG_RUNTIME_DIR, path.join(root, DEVENV_DIR, '.devenv')),
    tmp: env.TMPDIR || '/tmp',
    store: '/nix/store',
  };
  const hash = new Bun.CryptoHasher('sha256');
  for (const watch of watches) {
    const base = bases[watch.scope];
    if (!base) throw new Error(`missing ${watch.scope} for a watched devenv input`);
    const absolute = path.resolve(base, watch.relative);
    if (absolute !== base && !absolute.startsWith(`${base}/`)) throw new Error(`watch escapes ${watch.scope}`);
    hash.update(`${watch.scope}:${watch.relative}\0`);
    const stat = lstatSync(absolute, { throwIfNoEntry: false });
    if (!stat) {
      hash.update('absent\0');
    } else if (watch.scope === 'store') {
      hash.update('store-present\0'); // Nix store paths are immutable; their names identify their contents.
    } else if (stat.isSymbolicLink()) {
      const target = readlinkSync(absolute);
      hash.update(`link\0${target}\0`);
      const resolved = realpathSync(absolute);
      if (!resolved.startsWith(`${root}/`) && !resolved.startsWith('/nix/store/')) {
        throw new Error(`watched link resolves outside isolated roots: ${absolute}`);
      }
      hash.update(readFileSync(resolved));
    } else if (stat.isFile()) {
      hash.update(`file\0${stat.mode & 0o111}\0${stat.size}\0`);
      hash.update(readFileSync(absolute));
    } else {
      throw new Error(`watched input is neither a file nor a missing path: ${absolute}`);
    }
  }
  return hash.digest('hex');
}

function readArtifact(file: string): Artifact | undefined {
  if (!lstatSync(file, { throwIfNoEntry: false })?.isFile()) return undefined;
  const value: unknown = JSON.parse(readFileSync(file, 'utf8'));
  if (
    typeof value !== 'object' ||
    value === null ||
    !('version' in value) ||
    value.version !== 1 ||
    !('root' in value) ||
    typeof value.root !== 'string' ||
    !('paths' in value) ||
    typeof value.paths !== 'object' ||
    value.paths === null ||
    Array.isArray(value.paths) ||
    !('basis' in value) ||
    typeof value.basis !== 'string' ||
    !('watches' in value) ||
    !Array.isArray(value.watches) ||
    !('watchBasis' in value) ||
    typeof value.watchBasis !== 'string' ||
    !('toolchain' in value) ||
    typeof value.toolchain !== 'string' ||
    !('export' in value) ||
    typeof value.export !== 'string'
  )
    return undefined;
  const paths: Record<string, string> = {};
  for (const [name, entry] of Object.entries(value.paths)) {
    if (typeof entry !== 'string') return undefined;
    paths[name] = entry;
  }
  const watches: Watch[] = [];
  for (const entry of value.watches) {
    const watch: unknown = entry;
    if (
      typeof watch !== 'object' ||
      watch === null ||
      !('scope' in watch) ||
      (watch.scope !== 'root' &&
        watch.scope !== 'home' &&
        watch.scope !== 'runtime' &&
        watch.scope !== 'tmp' &&
        watch.scope !== 'store') ||
      !('relative' in watch) ||
      typeof watch.relative !== 'string'
    )
      return undefined;
    watches.push({ scope: watch.scope, relative: watch.relative });
  }
  return {
    version: 1,
    root: value.root,
    paths,
    basis: value.basis,
    watches,
    watchBasis: value.watchBasis,
    toolchain: value.toolchain,
    export: value.export,
  };
}

/** Check inherited data from this checkout only. A sibling cannot write this file or its GC roots. */
function inherited(root: string, env: NodeJS.ProcessEnv, file: string): string | undefined {
  const artifact = readArtifact(file);
  if (!artifact) return undefined;
  if (
    artifact.basis !== sourceBasis(root) ||
    artifact.toolchain !== toolchain(privateEnvironment(env)) ||
    artifact.watchBasis !== watchedBasis(root, env, artifact.watches)
  )
    return undefined;
  const graph = enterShellTasks(path.join(root, DEVENV_DIR, '.devenv'));
  if (graph.kind !== 'confined') return undefined;
  const moves = relocations(artifact.root, artifact.paths, root, env);
  const dotfile = path.join(root, DEVENV_DIR, '.devenv');
  const inputPaths = path.join(dotfile, 'input-paths.txt');
  if (existsSync(inputPaths)) {
    const staged = `${inputPaths}.${process.pid}`;
    writeFileSync(staged, relocate(readFileSync(inputPaths, 'utf8'), moves));
    renameSync(staged, inputPaths);
  }
  const shell = path.join(dotfile, 'gc', 'shell');
  if (lstatSync(shell, { throwIfNoEntry: false })?.isSymbolicLink()) {
    const target = readlinkSync(shell);
    if (!target.startsWith('/nix/store/')) return undefined;
    const registered = Bun.spawnSync(['nix-store', '--realise', target, '--add-root', shell], {
      env,
      stdout: 'ignore',
      stderr: 'inherit',
    });
    if (registered.exitCode !== 0) return undefined;
  } else return undefined;
  return `${relocate(artifact.export, moves)}\nunset DEVENV_TASK_FILE DEVENV_TASKS\n`;
}

interface Task {
  readonly name: string;
  readonly command: string | null;
  readonly before: readonly string[];
  readonly after: readonly string[];
}
function strings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item): item is string => typeof item === 'string');
}
/** Classify the real graph emitted by devenv, not script text or source names. */
function enterShellTasks(dotfile: string): { kind: 'confined' } | { kind: 'writes'; reason: string } {
  const graph = path.join(dotfile, 'gc', 'task-config-devenv-config-task-config');
  let rows: unknown;
  try {
    rows = JSON.parse(readFileSync(graph, 'utf8'));
  } catch (error) {
    return { kind: 'writes', reason: `task graph ${graph}: ${String(error)}` };
  }
  if (!Array.isArray(rows)) return { kind: 'writes', reason: `${graph} is not a list of tasks` };
  const tasks = new Map<string, Task>();
  for (const row of rows) {
    const raw: unknown = row;
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
    )
      return { kind: 'writes', reason: `${graph} has a malformed or duplicate task` };
    tasks.set(raw.name, { name: raw.name, command: raw.command, before: raw.before, after: raw.after });
  }
  if (!tasks.has('devenv:enterShell')) return { kind: 'writes', reason: `${graph} has no devenv:enterShell` };
  const pending = ['devenv:enterShell'];
  const closure = new Set<string>();
  for (let name = pending.pop(); name !== undefined; name = pending.pop()) {
    if (closure.has(name)) continue;
    const task = tasks.get(name);
    if (!task) return { kind: 'writes', reason: `${graph} depends on missing task ${name}` };
    closure.add(name);
    pending.push(...task.after);
    for (const candidate of tasks.values()) if (candidate.before.includes(name)) pending.push(candidate.name);
  }
  const writing = [...closure].filter((name) => {
    const task = tasks.get(name);
    return (
      task?.command !== null &&
      task?.command !== undefined &&
      name !== 'devenv:enterShell' &&
      name !== 'devenv:files:cleanup'
    );
  });
  return writing.length === 0
    ? { kind: 'confined' }
    : { kind: 'writes', reason: `enterShell runs ${writing.sort().join(', ')}, which write into its evaluation root` };
}

/** Forward the live export; only an independently reproduced public export becomes inherited data. */
export function exportShell(rootPath: string, args: readonly string[], env: NodeJS.ProcessEnv): number {
  const root = realpathSync(rootPath);
  const cwd = path.join(root, DEVENV_DIR);
  if (args.at(-1) !== 'direnv-export' || !env.COWSHED_PORT_BASE) {
    const live = Bun.spawnSync(['devenv', ...args], {
      cwd,
      env,
      stdin: 'inherit',
      stdout: 'inherit',
      stderr: 'inherit',
    });
    return live.exitCode;
  }
  const file = path.join(cwd, '.devenv', ARTIFACT);
  try {
    const cached = inherited(root, env, file);
    if (cached !== undefined) {
      process.stdout.write(cached);
      console.error(
        `inherited devenv: reused this checkout's evaluated shell from ${JSON.parse(readFileSync(file, 'utf8')).root}`,
      );
      return 0;
    }
  } catch (error) {
    console.error(`inherited devenv: private artifact cannot be used (${String(error)}); evaluating in place`);
  }
  const live = Bun.spawnSync(['devenv', ...args], { cwd, env, stdin: 'inherit', stdout: 'pipe', stderr: 'inherit' });
  if (live.exitCode !== 0) return live.exitCode;
  process.stdout.write(live.stdout);
  const graph = enterShellTasks(path.join(cwd, '.devenv'));
  if (graph.kind !== 'confined') {
    console.error(`inherited devenv: ${graph.reason}; this checkout evaluates in place`);
    return 0;
  }
  try {
    const safe = privateEnvironment(env);
    const before = sourceBasis(root);
    const candidate = Bun.spawnSync(['devenv', ...args], {
      cwd,
      env: safe,
      stdin: 'inherit',
      stdout: 'pipe',
      stderr: 'inherit',
    });
    if (candidate.exitCode !== 0 || !Buffer.from(candidate.stdout).equals(Buffer.from(live.stdout))) {
      console.error('inherited devenv: a credential-free evaluation differs; this checkout evaluates in place');
      return 0;
    }
    const watches = watchPaths(root, env);
    const after = sourceBasis(root);
    const version = toolchain(safe);
    if (before !== after || !version) return 0;
    const artifact: Artifact = {
      version: 1,
      root,
      paths: pathsOf(root, env),
      basis: after,
      watches,
      watchBasis: watchedBasis(root, env, watches),
      toolchain: version,
      export: candidate.stdout.toString(),
    };
    mkdirSync(path.dirname(file), { recursive: true });
    const staged = `${file}.${process.pid}`;
    writeFileSync(staged, `${JSON.stringify(artifact)}\n`, { mode: 0o600 });
    renameSync(staged, file);
  } catch (error) {
    console.error(
      `inherited devenv: cannot publish a private evaluated shell (${String(error)}); this checkout evaluates in place`,
    );
  }
  return 0;
}

if (import.meta.main) {
  const [root, ...args] = process.argv.slice(2);
  if (!root || args.length === 0) {
    console.error('inherited-devenv.ts: expected <project root> <devenv args…>');
    process.exit(2);
  }
  process.exit(exportShell(root, args, process.env));
}
