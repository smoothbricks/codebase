import { execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import type { BigIntStats } from 'node:fs';
import {
  accessSync,
  constants,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { delimiter, dirname, isAbsolute, join, relative, resolve } from 'node:path';
import type { NxJsonConfiguration } from 'nx/src/config/nx-json';
import type * as NxDaemonEnvironment from 'nx/src/daemon/client/daemon-environment';
import type * as NxNative from 'nx/src/native';
import type { FileData } from 'nx/src/native';
import type * as NxCacheDirectory from 'nx/src/utils/cache-directory';
import type * as TypeScript from 'typescript';
import typia from 'typia';
import type { NodeWitness, WorkspaceSnapshot } from './ensure-built-witness.js';
import {
  censusStart,
  movedNodes,
  nodeStamp,
  nodesUnchanged,
  recordedStamp,
  unproven,
  workspaceSnapshot,
} from './ensure-built-witness.js';

/** An owner-approved contract, pinned to the reviewed module bytes, never an inferred approval. */
export interface GraphInputContract {
  readonly version: 1;
  readonly kind: 'native-only' | 'tsconfig-extends' | 'reference-scan';
  readonly modules: readonly { readonly path: string; readonly sha256: string }[];
  readonly packages: readonly string[];
}
interface ResolutionWitness {
  readonly kind: 'node' | 'bun';
  readonly from: string;
  readonly specifier: string;
  readonly target: string | null;
}
interface ToolWitness {
  readonly name: 'node' | 'bun' | 'git';
  readonly path: string;
}
interface GitQueryWitness {
  readonly cwd: string;
  readonly args:
    | readonly ['config', '--show-origin', '--null', '--list']
    | readonly ['ls-files', '-z', '--cached', '--others', '--exclude-standard']
    | readonly ['check-attr', '--all', '-z', '--stdin']
    | readonly ['var', 'GIT_ATTR_SYSTEM']
    | readonly ['var', 'GIT_ATTR_GLOBAL']
    | readonly ['rev-parse', '--shared-index-path'];
  readonly stdin: string;
  readonly sha256: string;
}
interface ReadWitnessBasis {
  readonly version: 1;
  readonly workspaceRoot: string;
  readonly plugin: string;
  readonly contractSha256: string;
}
export type GraphInputReadWitness =
  | (ReadWitnessBasis & {
      readonly status: 'complete';
      readonly nativeIdentity: string;
      readonly nativeNodes: readonly NodeWitness[];
      readonly nodes: readonly GraphInputNodeWitness[];
      readonly missing: readonly string[];
      readonly resolutions: readonly ResolutionWitness[];
      readonly tools: readonly ToolWitness[];
      readonly gitQueries: readonly GitQueryWitness[];
      readonly environmentIdentity: string;
    })
  | (ReadWitnessBasis & { readonly status: 'unproved'; readonly detail: string });
export interface GraphInputContractProof {
  readonly plugins: readonly string[];
  readonly identity: string;
  readonly packages: readonly { readonly name: string; readonly from: string }[];
}

function digest(value: string | Uint8Array): string {
  return createHash('sha256').update(value).digest('hex');
}
function systemError(error: unknown): error is Error & { readonly code: string } {
  return error instanceof Error && 'code' in error && typeof error.code === 'string';
}
function nativeIdentity(files: readonly FileData[]): string {
  return digest(
    JSON.stringify(
      files.map(({ file, hash }) => [file, hash]).sort(([left], [right]) => (left ?? '').localeCompare(right ?? '')),
    ),
  );
}
/** Ancestors prove path binding; only a read directory's membership belongs to the input closure. */
interface GraphInputNodeWitness extends NodeWitness {
  readonly scope: 'path' | 'node';
}

function pathStamp(stats: BigIntStats): string {
  return `${stats.dev}:${stats.ino}:${stats.mode}`;
}

function readNodesUnchanged(nodes: readonly GraphInputNodeWitness[]): boolean {
  for (const node of nodes) {
    try {
      const stats = lstatSync(node.path, { bigint: true });
      if ((node.scope === 'path' ? pathStamp(stats) : nodeStamp(stats)) !== node.stamp) return false;
    } catch (error) {
      if (systemError(error)) return false;
      throw error;
    }
  }
  return true;
}

function workspaceRequire(root: string): NodeRequire {
  return createRequire(join(root, 'package.json'));
}
function dataDirectory(root: string): string {
  const module: typeof NxCacheDirectory = workspaceRequire(root)('nx/src/utils/cache-directory');
  return module.workspaceDataDirectoryForWorkspace(root);
}
function captureNative(root: string, previous?: WorkspaceSnapshot): WorkspaceSnapshot {
  const module: typeof NxNative = workspaceRequire(root)('nx/src/native');
  return workspaceSnapshot(root, new module.WorkspaceContext(root, dataDirectory(root)), previous);
}
function environmentIdentity(root: string): string {
  const module: typeof NxDaemonEnvironment = workspaceRequire(root)('nx/src/daemon/client/daemon-environment');
  return digest(
    JSON.stringify(
      Object.entries(module.normalizeDaemonEnvironmentForGraph(module.getDaemonEnv())).sort(([left], [right]) =>
        left.localeCompare(right),
      ),
    ),
  );
}
function contractOf(options: unknown): GraphInputContract | null {
  if (typeof options !== 'object' || options === null || !('graphInputContract' in options)) return null;
  const contract = options.graphInputContract;
  return typia.is<GraphInputContract>(contract) &&
    contract.version === 1 &&
    contract.modules.length > 0 &&
    contract.modules.every(({ sha256 }) => /^[a-f0-9]{64}$/.test(sha256))
    ? contract
    : null;
}
/**
 * The SHA-256 of each preload audited to leave graph evaluation alone. A preload's own bytes are not its whole input: it
 * can load more code and read more files. So only an audited one is bounded, and every other loader stays opaque.
 * The one entry is cowshed's port-0 preload (`packages/cowshed/crates/cowshed-core/src/port_zero.cjs`), which loads only
 * Node and Bun builtins, reads no file and no variable but `COWSHED_PORT_BASE` and `COWSHED_PORT_BLOCK_SIZE`, and changes
 * only which port a listener or a child's inspector takes. A changed preload is unaudited until its digest is added here.
 */
export const AUDITED_PRELOADS: ReadonlySet<string> = new Set([
  '8e4a5d9aeba854edf74b64b732392bf680e1831a80607e5d1b4aeace002205d2',
]);

/** Each runtime's options variable and the one flag in it whose argument is a file the runtime preloads. */
const PRELOAD_FLAGS = [
  ['NODE_OPTIONS', '--require'],
  ['BUN_OPTIONS', '--preload'],
] as const;

/**
 * The files a loader variable preloads when it holds nothing but `flag <file>` or `flag=<file>` clauses, or null for
 * any other text: another flag, a stray word, a flag without its file. Quoting is not parsed, so a quote or a backslash
 * makes the variable opaque too.
 */
export function loaderPreloads(value: string, flag: '--require' | '--preload'): string[] | null {
  if (/["'\\]/.test(value)) return null;
  const files: string[] = [];
  let awaitingFile = false;
  for (const token of value.split(/\s+/)) {
    if (token === '') continue;
    if (awaitingFile) {
      if (token.startsWith('-')) return null;
      files.push(token);
      awaitingFile = false;
    } else if (token === flag) {
      awaitingFile = true;
    } else if (token.startsWith(`${flag}=`) && token.length > flag.length + 1) {
      files.push(token.slice(flag.length + 1));
    } else {
      return null;
    }
  }
  return awaitingFile ? null : files;
}

function witnessPath(root: string, plugin: string): string {
  return join(dataDirectory(root), `graph-input-${digest(plugin)}.json`);
}
function gitEnvironment(): NodeJS.ProcessEnv {
  return { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' };
}
function gitQuery(path: string, query: Pick<GitQueryWitness, 'cwd' | 'args' | 'stdin'>): string {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git query: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
  return execFileSync(path, ['-c', 'core.excludesFile=/dev/null', ...query.args], {
    cwd: query.cwd,
    env: gitEnvironment(),
    input: query.stdin,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
}

/** Retain read/resolution byproducts at the reader; never reconstruct its extends/import algorithm. */
export class GraphInputReadCollector {
  private readonly contract: GraphInputContract | null;
  private readonly before: WorkspaceSnapshot | undefined;
  private readonly environment: string | undefined;
  private readonly nodes = new Map<string, GraphInputNodeWitness>();
  private readonly missing = new Set<string>();
  private readonly resolutions: ResolutionWitness[] = [];
  private readonly tools = new Map<ToolWitness['name'], ToolWitness>();
  private readonly gitQueries: GitQueryWitness[] = [];
  private readonly scanned = new Set<string>();
  private readonly gitScopes = new Set<string>();
  private detail: string | undefined;
  /** Read before the first observation: what each node stamp this reader records is judged by (unproven). */
  private readonly start = censusStart();
  /** The witness's stamp of each node this reader read in the tick it changed in (recordedStamp), by path. */
  private readonly unprovenStamps = new Map<string, string>();
  constructor(
    readonly workspaceRoot: string,
    readonly plugin: string,
    options: unknown,
  ) {
    this.contract = contractOf(options);
    if (this.contract === null) return;
    try {
      this.before = captureNative(workspaceRoot);
      this.environment = environmentIdentity(workspaceRoot);
      if (process.versions.bun === undefined) this.executable('node', process.execPath);
      else {
        const executable = this.executable('bun');
        this.file(process.execPath);
        if (executable !== realpathSync(process.execPath))
          this.refuse('Bun runtime is not the PATH-selected executable');
      }
      if (!this.before.stable) this.refuse('native file list contains an unproved node');
      // A preload clause names one file the runtime loads: an audited preload is witnessed like any
      // read, and anything else is opaque. A cowshed job preloads its port-0 module this way.
      for (const [name, flag] of PRELOAD_FLAGS) {
        const value = process.env[name];
        if (!value) continue;
        const files = loaderPreloads(value, flag);
        if (files === null) this.refuse(`unbounded loader environment: ${name}`);
        else
          for (const file of files) {
            this.file(file);
            if (!AUDITED_PRELOADS.has(digest(readFileSync(file)))) this.refuse(`unaudited preload in ${name}: ${file}`);
          }
      }
      for (const name of ['LD_PRELOAD', 'DYLD_INSERT_LIBRARIES']) {
        if (process.env[name]) this.refuse(`unbounded loader environment: ${name}`);
      }
    } catch (error) {
      this.refuse(error instanceof Error ? error.message : String(error));
    }
  }
  refuse(detail: string): void {
    this.detail ??= detail;
  }
  private protect(work: () => void): void {
    if (this.contract === null || this.detail !== undefined) return;
    try {
      work();
    } catch (error) {
      this.refuse(error instanceof Error ? error.message : String(error));
    }
  }
  /** Observe each alias and its physical target, including a missing candidate's exact path. */
  file(path: string): void {
    this.protect(() => this.observe(resolve(path)));
  }
  private observe(path: string, membership = false): void {
    // Every ancestor binds the path, without treating unrelated siblings as reads.
    const parent = dirname(path);
    if (parent !== path && !this.nodes.has(parent) && !this.missing.has(parent)) this.observe(parent);
    let stats: BigIntStats;
    try {
      stats = lstatSync(path, { bigint: true });
    } catch (error) {
      if (systemError(error) && ['ENOENT', 'ENOTDIR'].includes(error.code)) {
        this.missing.add(path);
        return;
      }
      throw error;
    }
    const previous = this.nodes.get(path);
    const scope = previous?.scope === 'node' || !stats.isDirectory() || membership ? 'node' : 'path';
    const stamp = scope === 'path' ? pathStamp(stats) : nodeStamp(stats);
    if (previous !== undefined && previous.stamp !== (previous.scope === scope ? stamp : pathStamp(stats)))
      this.refuse(`input changed during read: ${path}`);
    if (previous === undefined || previous.scope !== scope) {
      this.nodes.set(path, { path, scope, stamp });
      // A path stamp holds no time; a node stamp read in its node's change tick is recorded unproven.
      if (scope === 'node' && unproven(stats, this.start))
        this.unprovenStamps.set(path, recordedStamp(stats, this.start));
    }
    if (stats.isSymbolicLink()) this.observe(realpathSync(path), membership);
    else if (stats.isFile()) accessSync(path, constants.R_OK);
    else if (!stats.isDirectory()) this.refuse(`unproved filesystem node: ${path}`);
  }
  read<T>(path: string, reader: () => T): T {
    this.file(path);
    return reader();
  }
  exists(path: string): boolean {
    this.file(path);
    return existsSync(path);
  }
  /** The whole immediate candidate directory, including absent/alternative extensions and alias nodes. */
  directory(path: string): void {
    this.protect(() => {
      this.observe(resolve(path), true);
      if (this.missing.has(resolve(path))) return;
      for (const name of readdirSync(path).sort()) this.observe(join(path, name));
    });
  }
  /** A finite candidate namespace: every node and alias, not only the previously selected file. */
  namespace(path: string): void {
    this.protect(() => this.walk(resolve(path)));
  }
  private walk(path: string): void {
    this.observe(path, true);
    if (this.missing.has(path)) return;
    const physical = realpathSync(path);
    if (this.scanned.has(physical)) return;
    this.scanned.add(physical);
    const stats = lstatSync(physical);
    if (stats.isDirectory()) {
      for (const name of readdirSync(physical).sort()) this.walk(join(physical, name));
    }
  }
  resolve(from: string, specifier: string, resolver: () => string, kind: ResolutionWitness['kind'] = 'node'): string {
    this.protect(() => {
      const require = createRequire(join(from, 'package.json'));
      if (specifier.startsWith('.') || isAbsolute(specifier)) {
        // Census the literal candidate's directory, independent of which extension/index the resolver selects.
        const base = resolve(from, specifier);
        this.directory(dirname(base));
        if (existsSync(base) && lstatSync(base).isDirectory()) this.walk(base);
      } else {
        const name = specifier.startsWith('@')
          ? specifier.split('/').slice(0, 2).join('/')
          : (specifier.split('/')[0] ?? specifier);
        for (const search of require.resolve.paths(specifier) ?? []) this.walk(join(search, name));
      }
      for (let directory = resolve(from); ; directory = dirname(directory)) {
        this.observe(join(directory, 'package.json'));
        this.observe(join(directory, 'tsconfig.json'));
        this.observe(join(directory, 'jsconfig.json'));
        if (directory === dirname(directory)) break;
      }
    });
    try {
      const target = resolver();
      this.file(target);
      this.protect(() => this.resolutions.push({ kind, from, specifier, target: realpathSync(target) }));
      return target;
    } catch (error) {
      this.protect(() => this.resolutions.push({ kind, from, specifier, target: null }));
      throw error;
    }
  }
  executable(name: ToolWitness['name'], actual?: string): string | undefined {
    let found: string | undefined;
    this.protect(() => {
      if (actual !== undefined) {
        this.observe(actual);
        found = realpathSync(actual);
      } else {
        for (const directory of (process.env.PATH ?? '').split(delimiter)) {
          const candidate = resolve(directory || '.', name);
          this.observe(candidate);
          if (this.missing.has(candidate)) continue;
          try {
            accessSync(candidate, constants.X_OK);
          } catch {
            continue;
          }
          found = realpathSync(candidate);
          break;
        }
      }
      if (found === undefined) this.refuse(`unresolved executable: ${name}`);
      else this.tools.set(name, { name, path: found });
    });
    return found;
  }
  /** Git itself supplies effective config/include origins, membership and attributes; no config reparser. */
  git(root: string, paths: readonly string[]): void {
    this.protect(() => {
      const scope = realpathSync(root);
      if (this.gitScopes.has(scope)) return;
      this.gitScopes.add(scope);
      const tool = this.executable('git');
      if (tool === undefined) return;
      const config: Pick<GitQueryWitness, 'cwd' | 'args' | 'stdin'> = {
        cwd: scope,
        args: ['config', '--show-origin', '--null', '--list'],
        stdin: '',
      };
      const output = gitQuery(tool, config);
      this.gitQueries.push({ ...config, sha256: digest(output) });
      const fields = output.split('\0');
      for (let index = 0; index + 1 < fields.length; index += 2) {
        const origin = fields[index] ?? '';
        const entry = fields[index + 1] ?? '';
        if (origin.startsWith('file:')) this.observe(resolve(scope, origin.slice(5)));
        if (/^(?:core\.fsmonitor|core\.attributesfile)\n/iu.test(entry)) {
          const value = entry.slice(entry.indexOf('\n') + 1);
          if (entry.startsWith('core.fsmonitor\n') && value !== '' && value !== 'false')
            this.refuse('Git fsmonitor has an unbounded external input closure');
          if (entry.startsWith('core.attributesfile\n'))
            this.observe(isAbsolute(value) ? value : resolve(scope, value));
        }
      }
      for (const variable of ['GIT_ATTR_SYSTEM', 'GIT_ATTR_GLOBAL'] as const) {
        const query: Pick<GitQueryWitness, 'cwd' | 'args' | 'stdin'> = {
          cwd: scope,
          args: ['var', variable],
          stdin: '',
        };
        const output = gitQuery(tool, query);
        this.gitQueries.push({ ...query, sha256: digest(output) });
        const path = output.trim();
        if (path !== '') this.observe(resolve(scope, path));
      }
      const splitIndex: Pick<GitQueryWitness, 'cwd' | 'args' | 'stdin'> = {
        cwd: scope,
        args: ['rev-parse', '--shared-index-path'],
        stdin: '',
      };
      const sharedIndex = gitQuery(tool, splitIndex);
      this.gitQueries.push({ ...splitIndex, sha256: digest(sharedIndex) });
      if (sharedIndex.trim() !== '') this.observe(resolve(scope, sharedIndex.trim()));
      for (const name of ['index', 'info/exclude', 'info/attributes']) {
        // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git path lookup: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
        const path = execFileSync(tool, ['-c', 'core.excludesFile=/dev/null', 'rev-parse', '--git-path', name], {
          cwd: scope,
          env: gitEnvironment(),
          encoding: 'utf8',
        }).trim();
        this.observe(resolve(scope, path));
      }
      const membership: Pick<GitQueryWitness, 'cwd' | 'args' | 'stdin'> = {
        cwd: scope,
        args: ['ls-files', '-z', '--cached', '--others', '--exclude-standard'],
        stdin: '',
      };
      const members = gitQuery(tool, membership);
      this.gitQueries.push({ ...membership, sha256: digest(members) });
      const names = [...new Set([...members.split('\0').filter(Boolean), ...paths])].sort();
      for (const path of names) this.observe(resolve(scope, path));
      const attributes: Pick<GitQueryWitness, 'cwd' | 'args' | 'stdin'> = {
        cwd: scope,
        args: ['check-attr', '--all', '-z', '--stdin'],
        stdin: names.map((path) => `${path}\0`).join(''),
      };
      this.gitQueries.push({ ...attributes, sha256: digest(gitQuery(tool, attributes)) });
    });
  }
  finish(): GraphInputReadWitness {
    const basis: ReadWitnessBasis = {
      version: 1,
      workspaceRoot: this.workspaceRoot,
      plugin: this.plugin,
      contractSha256: digest(JSON.stringify(this.contract)),
    };
    this.protect(() => {
      if (this.before === undefined) return;
      const after = captureNative(this.workspaceRoot, this.before);
      // `after` re-hashed every node whose stamp does not prove it (its ctime alone moved, or `before` read it in the
      // tick it changed in), and its native identity must be `before`'s.
      if (
        !after.stable ||
        nativeIdentity(after.files) !== nativeIdentity(this.before.files) ||
        movedNodes(this.before.nodes, () => true) !== null ||
        !readNodesUnchanged([...this.nodes.values()])
      )
        this.refuse('graph inputs changed during the owning reader');
      if (environmentIdentity(this.workspaceRoot) !== this.environment)
        this.refuse('graph environment changed during the owning reader');
      if (!missingStillMissing([...this.missing]))
        this.refuse('a missing resolution candidate appeared during the owning reader');
    });
    const witness: GraphInputReadWitness =
      this.detail === undefined && this.before !== undefined && this.environment !== undefined
        ? {
            ...basis,
            status: 'complete',
            nativeIdentity: nativeIdentity(this.before.files),
            nativeNodes: this.before.nodes,
            nodes: [...this.nodes.values()].map((node) => {
              const stamp = this.unprovenStamps.get(node.path);
              return stamp === undefined ? node : { ...node, stamp };
            }),
            missing: [...this.missing].sort(),
            resolutions: this.resolutions,
            tools: [...this.tools.values()],
            gitQueries: this.gitQueries,
            environmentIdentity: this.environment,
          }
        : { ...basis, status: 'unproved', detail: this.detail ?? 'no owner-approved graphInputContract' };
    if (this.contract !== null) {
      try {
        const path = witnessPath(this.workspaceRoot, this.plugin);
        mkdirSync(dirname(path), { recursive: true });
        const temporary = `${path}.${randomUUID()}`;
        try {
          writeFileSync(temporary, JSON.stringify(witness), { flag: 'wx', mode: 0o600 });
          renameSync(temporary, path);
        } finally {
          rmSync(temporary, { force: true });
        }
      } catch (error) {
        if (!systemError(error)) throw error;
        return {
          ...basis,
          status: 'unproved',
          detail: `Cannot publish optional graph-input evidence: ${error.message}`,
        };
      }
    }
    return witness;
  }
}

function readInputWitness(path: string): GraphInputReadWitness | null {
  try {
    return typia.json.isParse<GraphInputReadWitness>(readFileSync(path, 'utf8'));
  } catch (error) {
    if (error instanceof SyntaxError || systemError(error)) return null;
    throw error;
  }
}
function missingStillMissing(paths: readonly string[]): boolean {
  return paths.every((path) => {
    try {
      lstatSync(path);
      return false;
    } catch (error) {
      if (systemError(error) && ['ENOENT', 'ENOTDIR'].includes(error.code)) return true;
      throw error;
    }
  });
}
/** Every local plugin must prove its own approved code and actual outside-input byproducts. */
export function graphInputContractIdentity(
  root: string,
  nxJson: NxJsonConfiguration,
  snapshot: WorkspaceSnapshot,
): GraphInputContractProof | null {
  const plugins: string[] = [];
  const packages: { name: string; from: string }[] = [];
  const identities: unknown[] = [];
  if (!snapshot.stable) return null;
  try {
    for (const configured of nxJson.plugins ?? []) {
      const plugin = typeof configured === 'string' ? configured : configured.plugin;
      if (plugin === '@smoothbricks/nx-plugin') continue;
      if (typeof configured === 'string' || !plugin.startsWith('./')) return null;
      const contract = contractOf(configured.options);
      if (contract === null || contract.modules[0]?.path !== plugin.slice(2)) return null;
      const nodes: NodeWitness[] = [];
      const paths = new Set(contract.modules.map(({ path }) => resolve(root, path)));
      const require = workspaceRequire(root);
      const ts: typeof TypeScript = require('typescript');
      for (const module of contract.modules) {
        const path = resolve(root, module.path);
        if (isAbsolute(module.path) || relative(root, path).startsWith('..')) return null;
        const before = lstatSync(path, { bigint: true });
        const text = readFileSync(path, 'utf8');
        if (digest(text) !== module.sha256) return null;
        nodes.push({ path, stamp: nodeStamp(before) });
        const physical = realpathSync(path);
        nodes.push({ path: physical, stamp: nodeStamp(lstatSync(physical, { bigint: true })) });
        for (const imported of ts.preProcessFile(text, true, true).importedFiles) {
          const specifier = imported.fileName;
          if (specifier.startsWith('node:')) continue;
          if (specifier.startsWith('.')) {
            const base = resolve(dirname(path), specifier);
            const candidates = [
              base,
              base.replace(/\.js$/u, '.ts'),
              `${base}.ts`,
              `${base}.js`,
              join(base, 'index.ts'),
              join(base, 'index.js'),
            ];
            if (!candidates.some((candidate) => paths.has(candidate))) return null;
          } else {
            const name = specifier.startsWith('@')
              ? specifier.split('/').slice(0, 2).join('/')
              : (specifier.split('/')[0] ?? specifier);
            if (!contract.packages.includes(name)) return null;
            packages.push({ name, from: dirname(physical) });
          }
        }
      }
      if (!nodesUnchanged(nodes)) return null;
      let witness: GraphInputReadWitness | null = null;
      if (contract.kind !== 'native-only') {
        witness = readInputWitness(witnessPath(root, plugin));
        if (
          witness === null ||
          witness.status !== 'complete' ||
          witness.workspaceRoot !== root ||
          witness.plugin !== plugin ||
          witness.contractSha256 !== digest(JSON.stringify(contract)) ||
          witness.nativeIdentity !== nativeIdentity(snapshot.files) ||
          witness.environmentIdentity !== environmentIdentity(root) ||
          !nodesUnchanged(witness.nativeNodes) ||
          !readNodesUnchanged(witness.nodes) ||
          !missingStillMissing(witness.missing)
        )
          return null;
        for (const tool of witness.tools) {
          if (realpathSync(tool.path) !== tool.path) return null;
        }
        const bunResolutions = witness.resolutions.filter(({ kind }) => kind === 'bun');
        if (bunResolutions.length > 0) {
          const bun = witness.tools.find(({ name }) => name === 'bun');
          if (bun === undefined) return null;
          const recipe =
            "console.log(JSON.stringify(JSON.parse(require('node:fs').readFileSync(0,'utf8')).map(({from,specifier})=>{try{return require('node:fs').realpathSync(Bun.resolveSync(specifier,from))}catch{return null}})))";
          // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: bun resolves the specifiers on its stdin from the filesystem and exits; it waits on no other process
          const actual = execFileSync(bun.path, ['--eval', recipe], {
            input: JSON.stringify(bunResolutions),
            cwd: root,
            encoding: 'utf8',
            maxBuffer: 64 * 1024 * 1024,
          });
          const targets = typia.json.isParse<(string | null)[]>(actual);
          if (
            targets === null ||
            targets.length !== bunResolutions.length ||
            targets.some((target, index) => target !== bunResolutions[index]?.target)
          )
            return null;
        }
        for (const resolution of witness.resolutions) {
          if (resolution.kind !== 'node') continue;
          let target: string | null;
          try {
            target = realpathSync(createRequire(join(resolution.from, 'package.json')).resolve(resolution.specifier));
          } catch {
            target = null;
          }
          if (target !== resolution.target) return null;
        }
        const git = witness.tools.find(({ name }) => name === 'git');
        for (const query of witness.gitQueries) {
          if (git === undefined || digest(gitQuery(git.path, query)) !== query.sha256) return null;
        }
        if (
          !nodesUnchanged(witness.nativeNodes) ||
          !readNodesUnchanged(witness.nodes) ||
          !missingStillMissing(witness.missing)
        )
          return null;
      }
      plugins.push(plugin);
      identities.push({ plugin, contract, nodes, witness });
    }
    return { plugins, packages, identity: JSON.stringify(identities) };
  } catch (error) {
    if (systemError(error) && ['EACCES', 'EPERM', 'EIO'].includes(error.code)) throw error;
    // Optional evidence never hides uncertainty behind a guessed local-plugin contract.
    if (error instanceof Error) return null;
    throw error;
  }
}
