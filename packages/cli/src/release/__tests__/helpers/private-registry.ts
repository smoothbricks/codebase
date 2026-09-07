import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';

/**
 * Publication-free proof harness for private-scope npm routing.
 *
 * Every guarantee this fixture exercises is an operational one that a mocked
 * HTTP client cannot show: whether the pinned Bun expands `$VAR` inside
 * `[install.scopes]`, whether the owner path survives into the request line,
 * whether a credential is attached to an origin that must never see it, and
 * which registry failures are genuinely "absent" versus "blocked". So the
 * fixture serves a real loopback registry over real HTTP, packs real tarballs
 * with the pinned Bun, and runs the real `bun install` / `npm view` clients
 * against it. Nothing here contacts a remote registry, and the only
 * credentials in play are the fixture strings below.
 */

/** The private scope under test; is a generic private scope. */
export const FIXTURE_SCOPE = '@priv.test';
/** Forgejo serves each owner's npm registry under its own path prefix. */
export const FIXTURE_OWNER = 'priv-owner';
/** Environment variable names are the declared configuration; values stay fixture-local. */
export const FIXTURE_REGISTRY_ENV = 'PRIV_NPM_REGISTRY';
export const FIXTURE_READ_TOKEN_ENV = 'PRIV_NPM_READ_TOKEN';
export const FIXTURE_PUBLISH_TOKEN_ENV = 'PRIV_NPM_PUBLISH_TOKEN';
export const FIXTURE_READ_TOKEN = 'fixture-read-token-3f9c1a';
export const FIXTURE_PUBLISH_TOKEN = 'fixture-publish-token-8d02be';

/**
 * npm retries 5xx by default; an unavailable-registry test would otherwise
 * outlive the per-test timeout. Only the retry count is set: lowering
 * fetch-retry-maxtimeout below npm's default mintimeout makes npm refuse the
 * command outright ("minTimeout is greater than maxTimeout").
 */
const NPM_NO_RETRY_ARGS = ['--fetch-retries=0'];

export interface FixtureRegistryRequest {
  method: string;
  /**
   * Decoded pathname. Bun asks for `<base>@scope/name`, npm for
   * `<base>@scope%2fname`; decoding makes one assertion cover both clients.
   */
  path: string;
  /** Scheme+host of the server that received it, so cross-origin leakage is observable. */
  origin: string;
  authorization: string | null;
}

export interface FixturePackageSpec {
  name: string;
  version: string;
  /** Extra files beside the generated package.json, relative to the package root. */
  files?: Record<string, string>;
  dependencies?: Record<string, string>;
}

export interface FixturePackage {
  name: string;
  version: string;
  tarballPath: string;
  integrity: string;
  shasum: string;
}

export interface FixtureNpmRegistry {
  /** `http://127.0.0.1:<port>` */
  readonly origin: string;
  /** Credential-free registry URL including the owner path and its trailing slash. */
  readonly registry: string;
  /** npmrc auth key for this endpoint: `//host/owner/path:_authToken`. */
  readonly authKey: string;
  readonly requests: readonly FixtureRegistryRequest[];
  publishPackage(spec: FixturePackageSpec): Promise<FixturePackage>;
  /** Serve `status` for every package request until reset to `null`. */
  failWith(status: number | null): void;
  requestsFor(name: string): FixtureRegistryRequest[];
  stop(): void;
}

export interface FixtureConsumerBunfigScope {
  /** Written verbatim, so `"$PRIV_NPM_REGISTRY"` reaches Bun unexpanded. */
  url: string;
  token?: string;
}

export interface FixtureConsumerOptions {
  dependencies: Record<string, string>;
  scopes?: Record<string, FixtureConsumerBunfigScope>;
  /** `[install] registry`, written verbatim. */
  registry?: string;
  /** Complete bunfig.toml text; overrides `scopes`/`registry` for replaying a real repo config. */
  bunfig?: string;
}

export interface FixtureInstallResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

export interface FixtureConsumer {
  readonly root: string;
  /** Isolated HOME: no user `.npmrc`, no global Bun config, no ambient credential. */
  readonly home: string;
  readonly cache: string;
  install(env: Record<string, string>): Promise<FixtureInstallResult>;
  installedVersion(name: string): Promise<string | null>;
  /** Absolute paths of every consumer/HOME/cache file whose bytes contain `secret`. */
  findSecretLeaks(secret: string): Promise<string[]>;
}

export interface FixtureNpmClient {
  readonly userconfig: string;
  view(spec: string, extraArgs?: string[]): Promise<FixtureInstallResult>;
}

export interface FixtureNpmClientOptions {
  registry: FixtureNpmRegistry;
  /**
   * Use this userconfig verbatim — the point of the option is to run npm
   * against the file the production generator produced, not a copy of it.
   */
  userconfig?: string;
  /** Reference the token through `${ENV}` in npmrc (production shape) instead of inlining it. */
  tokenEnv?: string;
  token?: string;
  /** Omit the auth line entirely, to prove an unauthenticated read is refused rather than retried. */
  anonymous?: boolean;
}

export interface PrivateNpmFixture {
  readonly root: string;
  /** Forgejo-shaped endpoint under `/api/packages/<owner>/npm/`. */
  readonly privateRegistry: FixtureNpmRegistry;
  /** Root-path stand-in for the public default registry; must never receive a credential. */
  readonly publicRegistry: FixtureNpmRegistry;
  createConsumer(options: FixtureConsumerOptions): Promise<FixtureConsumer>;
  createNpmClient(options: FixtureNpmClientOptions): Promise<FixtureNpmClient>;
}

export async function withPrivateNpmFixture(fn: (fixture: PrivateNpmFixture) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-private-npm-'));
  const privateRegistry = await startFixtureRegistry({ root, owner: FIXTURE_OWNER });
  const publicRegistry = await startFixtureRegistry({ root, owner: '' });
  let consumers = 0;
  let npmClients = 0;
  try {
    await fn({
      root,
      privateRegistry,
      publicRegistry,
      createConsumer: (options) => createFixtureConsumer(join(root, `consumer-${++consumers}`), options),
      createNpmClient: (options) => createFixtureNpmClient(join(root, `npm-${++npmClients}`), options),
    });
  } finally {
    privateRegistry.stop();
    publicRegistry.stop();
    await rm(root, { recursive: true, force: true });
  }
}

async function startFixtureRegistry(options: { root: string; owner: string }): Promise<FixtureNpmRegistry> {
  const base = options.owner ? `/api/packages/${options.owner}/npm/` : '/';
  const packages = new Map<string, FixturePackage>();
  const tarballs = new Map<string, FixturePackage>();
  const requests: FixtureRegistryRequest[] = [];
  const packDir = join(options.root, `pack-${options.owner || 'public'}`);
  await mkdir(packDir, { recursive: true });
  let failure: number | null = null;

  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    fetch(request) {
      const url = new URL(request.url);
      const path = decodeURIComponent(url.pathname);
      requests.push({
        method: request.method,
        path,
        origin: `${url.protocol}//${url.host}`,
        authorization: request.headers.get('authorization'),
      });
      if (!path.startsWith(base)) {
        return registryError(404, `no such registry path: ${path}`);
      }
      // A write reaching a fixture registry is a failed "artifact-only"
      // guarantee, not a case to emulate: refuse it loudly and let the
      // recorded request fail the assertion.
      if (request.method !== 'GET') {
        return registryError(405, `${request.method} is not served by the fixture registry`);
      }
      if (failure !== null) {
        return registryError(failure, 'fixture registry failure injection');
      }
      const rest = path.slice(base.length);
      const tarball = tarballs.get(rest);
      if (tarball) {
        return new Response(Bun.file(tarball.tarballPath), {
          headers: { 'content-type': 'application/octet-stream' },
        });
      }
      const pkg = packages.get(rest);
      if (!pkg) {
        return registryError(404, `${rest} is not published to the fixture registry`);
      }
      return Response.json(packument(pkg, `${origin}${base}`));
    },
  });

  const origin = `http://127.0.0.1:${server.port}`;
  const registryUrl = new URL(`${origin}${base}`);

  return {
    origin,
    registry: registryUrl.href,
    authKey: `//${registryUrl.host}${registryUrl.pathname}:_authToken`,
    requests,
    async publishPackage(spec) {
      const pkg = await packFixturePackage(packDir, spec);
      packages.set(spec.name, pkg);
      tarballs.set(tarballPath(spec.name, spec.version), pkg);
      return pkg;
    },
    failWith(status) {
      failure = status;
    },
    requestsFor(name) {
      return requests.filter((request) => request.path.includes(name));
    },
    stop() {
      server.stop(true);
    },
  };
}

function registryError(status: number, message: string): Response {
  return Response.json({ error: message }, { status });
}

function tarballPath(name: string, version: string): string {
  return `${name}/-/${unscopedName(name)}-${version}.tgz`;
}

function unscopedName(name: string): string {
  return name.includes('/') ? (name.split('/').at(-1) ?? name) : name;
}

function packument(pkg: FixturePackage, base: string): unknown {
  return {
    name: pkg.name,
    'dist-tags': { latest: pkg.version },
    versions: {
      [pkg.version]: {
        name: pkg.name,
        version: pkg.version,
        main: 'index.js',
        dist: {
          tarball: `${base}${tarballPath(pkg.name, pkg.version)}`,
          integrity: pkg.integrity,
          shasum: pkg.shasum,
        },
      },
    },
  };
}

/** Real tarball, produced by the pinned Bun that runs the test. */
async function packFixturePackage(packDir: string, spec: FixturePackageSpec): Promise<FixturePackage> {
  const sourceDir = join(packDir, `${unscopedName(spec.name)}-${spec.version}`);
  const files = spec.files ?? { 'index.js': `export const name = ${JSON.stringify(spec.name)};\n` };
  await mkdir(sourceDir, { recursive: true });
  await writeFile(
    join(sourceDir, 'package.json'),
    `${JSON.stringify(
      {
        name: spec.name,
        version: spec.version,
        main: 'index.js',
        files: Object.keys(files),
        ...(spec.dependencies ? { dependencies: spec.dependencies } : {}),
      },
      null,
      2,
    )}\n`,
  );
  for (const [path, content] of Object.entries(files)) {
    await writeFile(join(sourceDir, path), content);
  }
  const tarballFile = join(packDir, `${unscopedName(spec.name)}-${spec.version}.tgz`);
  const packed = await Bun.$`${bunBinary()} pm pack --filename ${tarballFile} --ignore-scripts --quiet`
    .cwd(sourceDir)
    .nothrow()
    .quiet();
  if (packed.exitCode !== 0) {
    throw new Error(`fixture pack of ${spec.name}@${spec.version} failed: ${packed.stderr.toString()}`);
  }
  const bytes = new Uint8Array(await Bun.file(tarballFile).arrayBuffer());
  return {
    name: spec.name,
    version: spec.version,
    tarballPath: tarballFile,
    integrity: `sha512-${createHash('sha512').update(bytes).digest('base64')}`,
    shasum: createHash('sha1').update(bytes).digest('hex'),
  };
}

/**
 * The Bun running the tests is the workspace-pinned one, so the expansion
 * behaviour these tests prove is the behaviour the repo actually gets.
 */
export function bunBinary(): string {
  return process.execPath;
}

async function createFixtureConsumer(root: string, options: FixtureConsumerOptions): Promise<FixtureConsumer> {
  const home = join(root, 'home');
  const cache = join(root, 'cache');
  await mkdir(root, { recursive: true });
  await mkdir(home, { recursive: true });
  await mkdir(cache, { recursive: true });
  await writeFile(
    join(root, 'package.json'),
    `${JSON.stringify(
      { name: 'fixture-consumer', version: '0.0.0', private: true, dependencies: options.dependencies },
      null,
      2,
    )}\n`,
  );
  await writeFile(join(root, 'bunfig.toml'), options.bunfig ?? bunfigText(options));

  return {
    root,
    home,
    cache,
    async install(env) {
      // A stale lockfile or warm cache would answer the next install without a
      // request, and the request line is the evidence.
      await rm(join(root, 'node_modules'), { recursive: true, force: true });
      await rm(join(root, 'bun.lock'), { force: true });
      await rm(cache, { recursive: true, force: true });
      await mkdir(cache, { recursive: true });
      const result = await Bun.$`${bunBinary()} install`
        .cwd(root)
        .env({ PATH: process.env.PATH ?? '', HOME: home, BUN_INSTALL_CACHE_DIR: cache, ...env })
        .nothrow()
        .quiet();
      return {
        exitCode: result.exitCode,
        stdout: result.stdout.toString(),
        stderr: result.stderr.toString(),
      };
    },
    async installedVersion(name) {
      const manifest = join(root, 'node_modules', ...name.split('/'), 'package.json');
      try {
        const parsed: unknown = JSON.parse(await readFile(manifest, 'utf8'));
        if (parsed && typeof parsed === 'object' && 'version' in parsed && typeof parsed.version === 'string') {
          return parsed.version;
        }
        return null;
      } catch {
        return null;
      }
    },
    // HOME and the install cache live under the consumer root, so one walk
    // covers the lockfile, node_modules, the isolated npmrc location and every
    // cached manifest.
    findSecretLeaks: (secret) => findSecretLeaks(root, secret),
  };
}

function bunfigText(options: FixtureConsumerOptions): string {
  const lines = ['[install]', 'linker = "isolated"'];
  if (options.registry !== undefined) {
    lines.push(`registry = ${JSON.stringify(options.registry)}`);
  }
  const scopes = Object.entries(options.scopes ?? {});
  if (scopes.length > 0) {
    lines.push('', '[install.scopes]');
    for (const [scope, entry] of scopes) {
      const fields = [`url = ${JSON.stringify(entry.url)}`];
      if (entry.token !== undefined) {
        fields.push(`token = ${JSON.stringify(entry.token)}`);
      }
      lines.push(`${JSON.stringify(scope)} = { ${fields.join(', ')} }`);
    }
  }
  return `${lines.join('\n')}\n`;
}

/**
 * Bytewise scan: a token can reach disk through a lockfile field, a cached
 * manifest, or an extracted file, and a string compare over raw bytes catches
 * all three without guessing which.
 */
async function findSecretLeaks(root: string, secret: string): Promise<string[]> {
  const needle = Buffer.from(secret, 'utf8');
  const hits: string[] = [];
  for (const path of await walkFiles(root)) {
    const bytes = await readFile(path).catch(() => null);
    if (bytes && bytes.indexOf(needle) >= 0) {
      hits.push(relative(root, path));
    }
  }
  return hits.sort();
}

async function walkFiles(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true, recursive: true }).catch(() => []);
  return entries
    .filter((entry) => entry.isFile() || entry.isSymbolicLink())
    .map((entry) => join(entry.parentPath, entry.name));
}

/**
 * npm client configured exactly the way the private CLI path configures it: a
 * mode-0600 userconfig naming the scope registry and a path-scoped auth key
 * whose value is an environment reference, never a literal secret on disk.
 */
async function createFixtureNpmClient(root: string, options: FixtureNpmClientOptions): Promise<FixtureNpmClient> {
  const home = join(root, 'home');
  const cache = join(root, 'cache');
  await mkdir(home, { recursive: true });
  await mkdir(cache, { recursive: true });
  let userconfig = options.userconfig;
  if (userconfig === undefined) {
    userconfig = join(root, 'userconfig');
    const lines = [`${FIXTURE_SCOPE}:registry=${options.registry.registry}`];
    if (options.anonymous !== true) {
      lines.push(`${options.registry.authKey}=${options.tokenEnv ? `\${${options.tokenEnv}}` : (options.token ?? '')}`);
    }
    await writeFile(userconfig, `${lines.join('\n')}\n`, { mode: 0o600 });
  }

  return {
    userconfig,
    async view(spec, extraArgs = []) {
      const args = [
        'view',
        spec,
        'version',
        '--json',
        '--registry',
        options.registry.registry,
        ...NPM_NO_RETRY_ARGS,
        ...extraArgs,
      ];
      const env: Record<string, string> = {
        PATH: process.env.PATH ?? '',
        HOME: home,
        NPM_CONFIG_USERCONFIG: userconfig,
        npm_config_cache: cache,
        // The notifier otherwise issues its own request to whatever
        // `--registry` names, which would pollute the recorded request list.
        npm_config_update_notifier: 'false',
      };
      if (options.tokenEnv) {
        env[options.tokenEnv] = options.token ?? FIXTURE_READ_TOKEN;
      }
      const result = await Bun.$`npm ${args}`.cwd(root).env(env).nothrow().quiet();
      return {
        exitCode: result.exitCode,
        stdout: result.stdout.toString(),
        stderr: result.stderr.toString(),
      };
    },
  };
}
