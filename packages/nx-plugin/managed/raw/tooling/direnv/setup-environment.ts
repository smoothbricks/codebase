#!/usr/bin/env bun
import { dlopen, FFIType } from 'bun:ffi';
import {
  chmodSync,
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { $ } from 'bun';
import {
  type DeferredSecret,
  dependentGroups,
  maskSecretValues,
  resolveSecretEnvironment,
  SHELL_GROUP,
} from './secret-references.ts';

// DEVENV_ROOT is set by the devenv shell, which is how this script normally
// runs. CI jobs that install dependencies without building that shell (the
// publishing job installs Bun and Node directly) have no such variable, so fall
// back to the git top level rather than resolving "undefined/../.." two levels
// above the repository and failing with "could not find a package.json".
const devenvRoot = process.env.DEVENV_ROOT;

class CapturedCommandError extends Error {
  constructor(
    public readonly command: string,
    public readonly exitCode: number,
    public readonly stdout: Uint8Array,
    public readonly stderr: Uint8Array,
  ) {
    super(`${command} failed with exit code ${exitCode}`);
  }
}

// Every value resolved out of smoo.secrets below. The install inherits them,
// so whatever a failing child echoed is redacted before its captured output
// is replayed. Declared above the first replay site (resolveProjectRoot's own
// failure) because module consts are not hoisted: a reference from there to a
// const declared further down would report a temporal-dead-zone error instead
// of the failure it was called to report.
const resolvedSecretValues: string[] = [];

// The declared secrets shell entry deliberately did NOT resolve — see THE
// RULE in secret-references.ts: shell entry resolves the `shell` group and
// nothing else. Nonempty only when this repository declares a secret in
// another group that is absent from this environment. An installed checkout
// contacts no registry and Nx needs no cache to run, so this stays silent;
// it is printed by reportDegradedSetup, where an install has already failed
// and a 401 is one of the things it might have been.
const deferredSecrets: DeferredSecret[] = [];

async function resolveProjectRoot(): Promise<string> {
  if (devenvRoot) {
    return path.resolve(`${devenvRoot}/../..`);
  }

  const result = await $`git rev-parse --show-toplevel`.quiet().nothrow();
  if (result.exitCode !== 0) {
    const error = new CapturedCommandError(
      'git rev-parse --show-toplevel',
      result.exitCode,
      result.stdout,
      result.stderr,
    );
    replayCapturedOutput(error);
    throw error;
  }
  return new TextDecoder().decode(result.stdout).trim();
}

const projectRoot = await resolveProjectRoot();

// Unscoped require("typescript") must expose the TS6 compiler API for Nx.
// @typescript/native installs another package also named "typescript" (TS7). Bun's
// isolated linker RACES the node_modules/.bun/node_modules/typescript fallback link
// between the two versions on every install (verified nondeterministic on bun 1.3.14
// AND 1.4.0 — the #33834 alias-resolution fix did not cover it), so packages resolved
// from the .bun store (nx) sometimes get version.cjs without readConfigFile.
// After every install, force both unscoped typescript slots onto typescript@6.0.3.
// Keep @typescript/native for ttsc via TTSC_TSGO_BINARY. Do not retire this on a bun
// bump until https://github.com/oven-sh/bun/issues/40355 is fixed and re-verified.
const TYPESCRIPT_API_VERSION = '6.0.3';

// post-commit is the one hook slot smoo shares. Tools that nudge a backup or a
// mirror after every commit (git-backup, git-auto-remote) install themselves by
// appending a fenced block to whatever hook file is already there, so a symlink
// is doubly wrong here: it would delete their block, and their next install
// would write through the link into the managed template. Own a fenced block
// that calls the managed script instead, which is the convention they document.
// Declared above the bootstrap block below, which installs the hook: module
// consts are not hoisted, so declaring them beside installPostCommitHook would
// report a temporal-dead-zone error instead of installing anything.
const POST_COMMIT_BEGIN = '# >>> smoo post-commit >>>';
const POST_COMMIT_END = '# <<< smoo post-commit <<<';
const POST_COMMIT_BLOCK = [
  POST_COMMIT_BEGIN,
  '# Restore the index for the paths the commit just wrote: a partial commit',
  '# (git commit --only -- <paths>) builds its tree from the worktree, so the',
  '# pre-commit formatter never reaches the real index. Mechanism in the script.',
  '"$(git rev-parse --show-toplevel)/tooling/git-hooks/post-commit.sh"',
  POST_COMMIT_END,
].join('\n');

// The managed devenv module passes `--python <interpreter>` for a uv project:
// the interpreter devenv's languages.python would use, as a store path, so the
// environment uv builds does not name this checkout's profile as its home.
// Declared above the bootstrap block for the same hoisting reason as above.
const { values: flags } = parseArgs({ options: { python: { type: 'string' } } });

// Go to project root
process.chdir(projectRoot);

try {
  // Bootstrap only: install deps + wire local git hooks/config.
  // Do not import workspace packages here — this script is what installs them,
  // and package resolution/Typia transforms are not available yet.
  const bunInputs = bunInstallInputs();
  const uvInputs = uvSyncInputs();
  recordInstallInputs([...bunInputs, ...uvInputs]);
  // A CI runner is GitHub Actions or Forgejo Actions, which mirrors every FORGEJO_*
  // variable as GITHUB_*. Not `CI`: agent harnesses set CI=true on every command
  // they run, and this branch installs unconditionally.
  if (process.env.GITHUB_ACTIONS === 'true') {
    const uv = uvInstaller(uvInputs, { locked: true });
    await resolveSecrets();
    // Failures are captured and reported below. Exiting in the catch would
    // skip the git-diff diagnostic that follows a frozen-lockfile miss. The
    // exits inside the install lock are safe: the kernel releases it.
    await withInstallLock(async () => {
      let frozenError: unknown;
      let fallbackError: unknown;
      try {
        await runSetupCommand('bun install --frozen-lockfile', $`bun install --frozen-lockfile`, { quiet: false });
      } catch (error) {
        frozenError = error;
        console.error('! Failed to install dependencies with frozen lockfile');
        replayCapturedOutput(error);
        try {
          await runSetupCommand('bun install', $`bun install`, { quiet: false });
        } catch (fallback) {
          fallbackError = fallback;
        }
      }
      if (fallbackError !== undefined) {
        reportSetupFailure(fallbackError);
      }
      if (frozenError !== undefined) {
        console.error('git diff after install:');
        try {
          await runSetupCommand('git diff', $`git diff`, { quiet: false });
        } catch (diffError) {
          // This diff is diagnostics for the install failure we are already
          // reporting, so it must never become the failure: say it broke and keep
          // the original exit path. Plain `git diff` exits 0 even when the
          // lockfile drifted, so a non-zero here means git itself failed.
          console.error(`! git diff failed: ${diffError instanceof Error ? diffError.message : String(diffError)}`);
        }
        process.exit(1);
      }
      if (uv !== null) {
        await uv.install({ quiet: false });
      }
    });
  } else {
    // A local secret-resolution or install failure (a provider that is not
    // signed in, an unpublished private package, a missing registry
    // credential, a lockfile that needs `devenv update`) must not take the
    // shell down with it: direnv drops the whole environment on a non-zero
    // exit, and then bun, nx, op and every other tool needed to repair the
    // install are gone too. Report it, finish what does not depend on the
    // install, and load the shell. CI above stays strict.
    const uv = uvInstaller(uvInputs, { locked: false });
    const bun = bunInstaller(bunInputs);
    const installError = await withInstallLock(async () => {
      const error = await installLocalDependencies(uv === null ? [bun] : [bun, uv]);
      if (error === undefined) {
        // Pin unscoped typescript → API 6 for root and Bun's shared .bun hoist (Nx).
        ensureTypeScriptApiPackage(projectRoot);
      }
      // Under the same lock: the repository config and hooks are one more thing two shell
      // entries would otherwise write at once.
      await applyWorkspaceGitConfig(projectRoot);
      return error;
    });
    if (installError !== undefined) {
      reportDegradedSetup(installError);
    }
  }
} catch (error) {
  reportSetupFailure(error);
}

/**
 * Provider-declared secrets (smoo.secrets) resolve before an install. The
 * values land in THIS process environment only — bun install, the prepare
 * scripts it runs, and every later child of this script inherit them (for
 * example .npmrc `${VAR}` auth); the direnv shell itself does not, which is
 * the point: this script must never act as a global shell export. Nothing
 * else here reads them, so a shell entry that installs nothing resolves none.
 *
 * `shell` is the group, and it is what keeps a credential only a deliberate
 * command needs — a registry token, the Nx cache token — from running its
 * provider command on every direnv reload. Such a variable comes back
 * deferred instead of resolved, and an install proceeds without it — which
 * is the normal case, because an installed checkout contacts no registry.
 * The install that does need one fails, and reportDegradedSetup then names
 * the variable and the exact command that supplies it.
 */
async function resolveSecrets(): Promise<void> {
  const resolution = await resolveSecretEnvironment({ root: projectRoot, group: SHELL_GROUP });
  for (const [name, value] of Object.entries(resolution.values)) {
    process.env[name] = value;
    resolvedSecretValues.push(value);
  }
  deferredSecrets.push(...resolution.deferred);
}

/**
 * Runs every installer whose inputs changed since its last successful run.
 * Returned, not thrown: the caller still keeps the git config, then reports a
 * degraded shell. Called under the install lock, so an installer another
 * shell entry just ran is already current here.
 */
async function installLocalDependencies(installers: readonly Installer[]): Promise<unknown> {
  const pending = installers.filter((installer) => !installer.isCurrent());
  if (pending.length === 0) {
    return undefined;
  }
  try {
    await resolveSecrets();
  } catch (error) {
    return error;
  }
  for (const installer of pending) {
    try {
      await installer.install({ quiet: true });
    } catch (error) {
      return error;
    }
  }
  return undefined;
}

/**
 * One install at a time per checkout, across processes. Shell entries race
 * whenever two shells load at once — a shell pool warming several, two
 * terminals, an editor beside a terminal — and two installs into one
 * node_modules collide on its links (`EEXIST … symlink`). The second entry
 * waits, then finds the first entry's stamps current and installs nothing.
 * The repository's git config and hooks are kept under the same lock, for the
 * same reason.
 *
 * flock(2), not a lock directory or file the script creates and removes: the
 * kernel releases it when the holder exits however it exits, so a killed
 * shell entry cannot leave a lock behind for the next one to wait out. Bun
 * has no flock binding, so it comes from libc; LOCK_EX and LOCK_NB have the
 * same values on Darwin and Linux. The waiting entry says why it is waiting.
 */
async function withInstallLock<T>(run: () => Promise<T>): Promise<T> {
  const LOCK_EX = 2;
  const LOCK_NB = 4;
  const libc = dlopen(process.platform === 'darwin' ? '/usr/lib/libSystem.B.dylib' : 'libc.so.6', {
    flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
  });
  const lockPath = path.join(projectRoot, 'node_modules', '.smoo-install.lock');
  mkdirSync(path.dirname(lockPath), { recursive: true });
  const fd = openSync(lockPath, 'a');
  try {
    if (libc.symbols.flock(fd, LOCK_EX | LOCK_NB) !== 0) {
      console.error(`setup-environment: waiting for another shell entry's install in ${projectRoot}`);
      while (libc.symbols.flock(fd, LOCK_EX | LOCK_NB) !== 0) {
        await Bun.sleep(100);
      }
    }
    return await run();
  } finally {
    closeSync(fd);
    libc.close();
  }
}

/**
 * One dependency installer shell entry drives. Shell entry runs on every
 * direnv reload, and an installer whose inputs did not change would only
 * re-derive the tree it already wrote — for `bun install` in a large
 * workspace that is most of an unchanged shell entry. So each installer
 * records the digest of its inputs after a successful run, in a stamp that
 * lives inside what it installed (deleting node_modules or the uv environment
 * deletes the stamp with it), and runs again only when the digest moved. A
 * failed run records nothing, so the next entry retries it.
 */
interface Installer {
  isCurrent(): boolean;
  install(options: { quiet: boolean }): Promise<void>;
}

interface InstallStamp {
  /** sha256 over the installer's identity and the bytes of every input file. */
  readonly inputs: string;
}

/**
 * `bun install`. Its result is location-independent: the isolated linker
 * links packages from the install cache by absolute path and workspace
 * members by relative path, so a copy of this checkout at another path — a
 * copy-on-write clone — is installed exactly when this one is. Besides the
 * stamp, the TypeScript API package must still resolve through its link, the
 * one package every managed repository installs, which catches a cache that
 * was pruned out from under node_modules.
 */
function bunInstaller(inputs: readonly string[]): Installer {
  const stampPath = path.join(projectRoot, 'node_modules', '.smoo-install');
  const identity = ['bun install', Bun.version, Bun.revision];
  return {
    isCurrent: () =>
      readInstallStamp(stampPath)?.inputs === inputsDigest(identity, inputs) &&
      findInstalledTypeScriptApiPackage(projectRoot) !== null,
    install: async ({ quiet }) => {
      await runSetupCommand('bun install --no-summary', $`bun install --no-summary`, { quiet });
      writeInstallStamp(stampPath, { inputs: inputsDigest(identity, inputs) });
    },
  };
}

/**
 * `uv sync` for a repository whose devenv shell enables uv: the managed
 * module passes `--python` exactly then, devenv exports the environment's
 * path as UV_PROJECT_ENVIRONMENT, and a root pyproject.toml is the uv
 * project. Null for any other shell — the flag, not an inherited variable,
 * decides, because an outer shell's UV_PROJECT_ENVIRONMENT leaks into a
 * checkout that syncs nothing.
 *
 * Every package of the workspace and every dependency group is installed,
 * which is what `bun install` does for the JavaScript half: a development
 * shell carries the whole workspace. CI adds `--locked`, the counterpart of
 * `--frozen-lockfile`.
 *
 * Like node_modules, the environment is location-independent, so a copy of
 * this checkout at another path — a copy-on-write clone — is installed exactly
 * when this one is, and its shell entry syncs nothing. uv would otherwise
 * write a path into three places:
 *
 * - Entry-point and activation scripts name the environment. An environment
 *   created with `uv venv --relocatable` finds itself relative to each script
 *   instead, and `uv sync` keeps an existing environment relocatable, so one
 *   that is not — built before this, or by hand — is replaced.
 * - The editable install of each workspace member is a `.pth` file holding the
 *   member's absolute source directory. Python resolves a relative `.pth` line
 *   against site-packages, so after every sync each line naming a directory in
 *   this checkout is rewritten relative to it.
 * - The interpreter is a store path, which names this checkout's devenv profile
 *   unless devenv.smoo.nix clears languages.python.libraries (it does).
 *
 * The installed members' direct_url.json keeps the absolute URL each was
 * installed from. Only uv reads it, when a changed input makes it sync, and it
 * then reinstalls those members from this checkout. Where the environment sits
 * in the checkout is part of the identity: the relative `.pth` lines hold only
 * there.
 */
function uvInstaller(inputs: readonly string[], options: { locked: boolean }): Installer | null {
  const python = flags.python;
  if (python === undefined || !existsSync(path.join(projectRoot, 'pyproject.toml'))) {
    return null;
  }
  const environment = process.env.UV_PROJECT_ENVIRONMENT;
  if (environment === undefined) {
    throw new Error(
      'setup-environment.ts was given --python but UV_PROJECT_ENVIRONMENT is unset: ' +
        "devenv's languages.python.uv exports it, and uv would otherwise build .venv at the project root.",
    );
  }
  const stampPath = path.join(environment, '.smoo-sync');
  const argv = ['sync', '--python', python, '--all-packages', '--all-groups', ...(options.locked ? ['--locked'] : [])];
  const create = ['venv', '--relocatable', '--python', python, environment];
  const uv = Bun.which('uv');
  const identity = [
    'uv',
    ...argv,
    path.relative(projectRoot, environment),
    uv === null ? 'uv not on PATH' : realpathSync(uv),
  ];
  return {
    isCurrent: () => readInstallStamp(stampPath)?.inputs === inputsDigest(identity, inputs),
    install: async ({ quiet }) => {
      // An inherited VIRTUAL_ENV names some other environment; uv would warn
      // that it does not match the project environment and ignore it.
      const env = Object.fromEntries(Object.entries(process.env).filter(([name]) => name !== 'VIRTUAL_ENV'));
      // pyvenv.cfg records `relocatable = true` for an environment uv created
      // with --relocatable. uv venv refuses a directory that already holds one.
      const config = readFileIfPresent(path.join(environment, 'pyvenv.cfg'));
      if (config === null || !/^relocatable\s*=\s*true\s*$/m.test(config.toString('utf8'))) {
        rmSync(environment, { recursive: true, force: true });
        await runSetupCommand(`uv ${create.join(' ')}`, $`uv ${create}`.env(env), { quiet });
      }
      await runSetupCommand(`uv ${argv.join(' ')}`, $`uv ${argv}`.env(env), { quiet });
      relativizeCheckoutPaths(environment);
      writeInstallStamp(stampPath, { inputs: inputsDigest(identity, inputs) });
    },
  };
}

/**
 * Rewrite every `.pth` line in the environment's site-packages that names a
 * directory inside this checkout as a path relative to that site-packages,
 * which is how Python's `site` resolves a relative line. Every other line — an
 * `import` line, a path outside the checkout, a directory that does not exist
 * — stays as uv wrote it.
 */
function relativizeCheckoutPaths(environment: string): void {
  const root = `${realpathSync(projectRoot)}${path.sep}`;
  const lib = path.join(environment, 'lib');
  for (const python of existsSync(lib) ? readdirSync(lib) : []) {
    const sitePackages = path.join(lib, python, 'site-packages');
    if (!python.startsWith('python') || !existsSync(sitePackages)) {
      continue;
    }
    const site = realpathSync(sitePackages);
    for (const name of readdirSync(site).filter((entry) => entry.endsWith('.pth'))) {
      const file = path.join(site, name);
      const content = readFileSync(file, 'utf8');
      const rewritten = content
        .split('\n')
        .map((line) => {
          const target = path.isAbsolute(line) && existsSync(line) ? realpathSync(line) : null;
          return target?.startsWith(root) ? path.relative(site, target) : line;
        })
        .join('\n');
      if (rewritten !== content) {
        writeFileSync(file, rewritten);
      }
    }
  }
}

/**
 * What `bun install` reads, relative to the project root and sorted: the root
 * manifest (dependencies, catalogs, overrides, patchedDependencies), every
 * workspace member's manifest, the lockfile, bunfig.toml, and the patch files
 * it applies.
 */
function bunInstallInputs(): string[] {
  const manifest = parseOrUndefined(() => JSON.parse(readFileSync(path.join(projectRoot, 'package.json'), 'utf8')));
  const workspaces = field(manifest, 'workspaces');
  const patterns = Array.isArray(workspaces) ? stringList(workspaces) : stringList(field(workspaces, 'packages'));
  const patchedDependencies = field(manifest, 'patchedDependencies');
  const patches =
    typeof patchedDependencies === 'object' && patchedDependencies !== null
      ? stringList(Object.values(patchedDependencies))
      : [];
  const inputs = ['package.json', 'bun.lock', 'bun.lockb', 'bunfig.toml', ...workspaceFiles(patterns, 'package.json')];
  return [...new Set([...inputs, ...patches])].sort();
}

/**
 * What `uv sync` reads, relative to the project root and sorted: the root
 * pyproject.toml, uv.lock, and every workspace member's pyproject.toml.
 */
function uvSyncInputs(): string[] {
  const pyproject = path.join(projectRoot, 'pyproject.toml');
  const project = existsSync(pyproject)
    ? parseOrUndefined(() => Bun.TOML.parse(readFileSync(pyproject, 'utf8')))
    : undefined;
  const workspace = field(field(field(project, 'tool'), 'uv'), 'workspace');
  const patterns = [
    ...stringList(field(workspace, 'members')),
    ...stringList(field(workspace, 'exclude')).map((pattern) => `!${pattern}`),
  ];
  return [...new Set(['pyproject.toml', 'uv.lock', ...workspaceFiles(patterns, 'pyproject.toml')])].sort();
}

/**
 * A manifest that does not parse contributes no members, never a failure
 * here: its bytes are still an input, so the installer runs and reports the
 * parse error itself, loudly and in its own words.
 */
function parseOrUndefined(parse: () => unknown): unknown {
  try {
    return parse();
  } catch {
    return undefined;
  }
}

/** The member manifests a workspace glob list selects; `!pattern` excludes. */
function workspaceFiles(patterns: readonly string[], manifest: string): string[] {
  const excluded = patterns
    .filter((pattern) => pattern.startsWith('!'))
    .map((pattern) => new Bun.Glob(pattern.slice(1)));
  const files: string[] = [];
  for (const pattern of patterns.filter((candidate) => !candidate.startsWith('!'))) {
    for (const match of new Bun.Glob(path.posix.join(pattern, manifest)).scanSync({
      cwd: projectRoot,
      onlyFiles: true,
    })) {
      const member = path.posix.dirname(match);
      if (!match.split('/').includes('node_modules') && !excluded.some((glob) => glob.match(member))) {
        files.push(match);
      }
    }
  }
  return files;
}

function field(value: unknown, key: string): unknown {
  return typeof value === 'object' && value !== null && Object.hasOwn(value, key) ? Reflect.get(value, key) : undefined;
}

function stringList(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : [];
}

/**
 * Inputs enter the digest by their path relative to the project root, so the
 * same bytes at another checkout path digest the same; so does every part of
 * an installer's identity.
 */
function inputsDigest(identity: readonly string[], inputs: readonly string[]): string {
  const hasher = new Bun.CryptoHasher('sha256');
  for (const part of identity) {
    hasher.update(`${part}\0`);
  }
  for (const input of inputs) {
    const bytes = readFileIfPresent(path.join(projectRoot, input));
    hasher.update(`${input}\0${bytes === null ? 'absent' : `present ${bytes.byteLength}`}\0`);
    if (bytes !== null) {
      hasher.update(bytes);
    }
  }
  return hasher.digest('hex');
}

function readInstallStamp(file: string): InstallStamp | null {
  const bytes = readFileIfPresent(file);
  if (bytes === null) {
    return null;
  }
  // A torn or foreign stamp proves nothing was installed, which is what an
  // absent one says too: the installer runs and writes a fresh one.
  const inputs = field(
    parseOrUndefined(() => JSON.parse(bytes.toString('utf8'))),
    'inputs',
  );
  return typeof inputs === 'string' ? { inputs } : null;
}

function writeInstallStamp(file: string, stamp: InstallStamp): void {
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, `${JSON.stringify(stamp)}\n`);
}

function readFileIfPresent(file: string): Buffer | null {
  try {
    return readFileSync(file);
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

/**
 * The files whose change must re-run shell entry's installs, one absolute
 * path per line in $DEVENV_STATE/install-inputs. The managed .envrc watches
 * each of them, so a shell direnv keeps loaded re-enters exactly when an
 * installer would do something. The uv half is recorded whether or not this
 * shell syncs one, so that adding a pyproject.toml is itself a change.
 */
function recordInstallInputs(inputs: readonly string[]): void {
  const state = process.env.DEVENV_STATE;
  if (state === undefined) {
    return;
  }
  const file = path.join(state, 'install-inputs');
  const content = `${[...new Set(inputs)]
    .sort()
    .map((input) => path.join(projectRoot, input))
    .join('\n')}\n`;
  if (readFileIfPresent(file)?.toString('utf8') !== content) {
    mkdirSync(state, { recursive: true });
    writeFileSync(file, content);
  }
}

function ensureTypeScriptApiPackage(root: string): void {
  const apiPackageRoot = findInstalledTypeScriptApiPackage(root);
  if (!apiPackageRoot) {
    throw new Error(
      `typescript@${TYPESCRIPT_API_VERSION} was not installed under node_modules/.bun. ` +
        `Keep root devDependency typescript@^${TYPESCRIPT_API_VERSION} (compiler API for Nx).`,
    );
  }

  // Root bare require("typescript") and Bun store-local requires (nx lives under
  // node_modules/.bun/...) must both see the API package — not @typescript/native's TS7.
  // A link this call did not have to write still points at the package it was
  // verified against, so only a rewritten link pays for loading the compiler
  // API again (a tenth of a second, on every shell entry that installs nothing).
  const relinked = [
    forceSymlink(path.join(root, 'node_modules', 'typescript'), apiPackageRoot),
    forceSymlink(path.join(root, 'node_modules', '.bun', 'node_modules', 'typescript'), apiPackageRoot),
  ];
  if (relinked.includes(true)) {
    assertTypescriptApiAt(path.join(root, 'node_modules', 'typescript'), apiPackageRoot);
    assertTypescriptApiAt(path.join(root, 'node_modules', '.bun', 'node_modules', 'typescript'), apiPackageRoot);
  }

  const nativeBin = path.join(root, 'node_modules', '@typescript', 'native', 'bin', 'tsc');
  const rootPackageJson = path.join(root, 'package.json');
  const declaresNative =
    existsSync(rootPackageJson) &&
    (() => {
      try {
        const pkg = JSON.parse(readFileSync(rootPackageJson, 'utf8')) as {
          devDependencies?: Record<string, string>;
          dependencies?: Record<string, string>;
        };
        return (
          typeof pkg.devDependencies?.['@typescript/native'] === 'string' ||
          typeof pkg.dependencies?.['@typescript/native'] === 'string'
        );
      } catch {
        return false;
      }
    })();
  if (declaresNative && !existsSync(nativeBin)) {
    throw new Error(
      `Missing ${nativeBin}. package.json declares @typescript/native but it is not installed. ` +
        'Run bun install (or direnv reload). @typescript/native is the TypeScript 7 native compiler for ttsc.',
    );
  }
}

function findInstalledTypeScriptApiPackage(root: string): string | null {
  const bunStore = path.join(root, 'node_modules', '.bun');
  if (!existsSync(bunStore)) {
    return null;
  }
  const exact = `typescript@${TYPESCRIPT_API_VERSION}`;
  const names = readdirSync(bunStore)
    .filter((name) => name === exact || name.startsWith(`${exact}+`) || name.startsWith('typescript@6.'))
    .sort((a, b) => {
      const aExact = a === exact || a.startsWith(`${exact}+`) ? 0 : 1;
      const bExact = b === exact || b.startsWith(`${exact}+`) ? 0 : 1;
      return aExact - bExact || a.localeCompare(b);
    });
  for (const name of names) {
    const candidate = path.join(bunStore, name, 'node_modules', 'typescript');
    if (existsSync(path.join(candidate, 'package.json'))) {
      return candidate;
    }
  }
  return null;
}

/** Points linkPath at targetPath; true when the link had to be (re)written. */
function forceSymlink(linkPath: string, targetPath: string): boolean {
  mkdirSync(path.dirname(linkPath), { recursive: true });
  const relativeTarget = path.relative(path.dirname(linkPath), targetPath);
  let current: string | null = null;
  try {
    current = readlinkSync(linkPath);
  } catch {
    current = null;
  }
  if (
    current === relativeTarget ||
    (current !== null && path.resolve(path.dirname(linkPath), current) === targetPath)
  ) {
    return false;
  }
  rmSync(linkPath, { recursive: true, force: true });
  symlinkSync(relativeTarget, linkPath);
  return true;
}

function assertTypescriptApiAt(typescriptRoot: string, expectedTarget: string): void {
  if (!existsSync(typescriptRoot)) {
    throw new Error(`Missing ${typescriptRoot} after linking TypeScript ${TYPESCRIPT_API_VERSION} API package`);
  }
  const requireFromInstall = createRequire(path.join(typescriptRoot, 'package.json'));
  const typed = requireFromInstall(typescriptRoot) as { version?: string; readConfigFile?: unknown };
  if (typeof typed.readConfigFile !== 'function' || typed.version !== TYPESCRIPT_API_VERSION) {
    throw new Error(
      `${typescriptRoot} must export TypeScript ${TYPESCRIPT_API_VERSION} compiler API (readConfigFile). ` +
        `Resolved version ${typed.version ?? 'unknown'} (expected link target ${expectedTarget}). ` +
        'Bun isolated linking races .bun/node_modules/typescript between typescript@6 and ' +
        "@typescript/native's TS7. See https://github.com/oven-sh/bun/issues/40355.",
    );
  }
}

/**
 * Keep git hook wiring local to bootstrap. Runtime pin sync
 * (`syncRootRuntimeVersions`) belongs to explicit monorepo tooling after the
 * package graph exists — not the installer. Called under the install lock.
 */
async function applyWorkspaceGitConfig(root: string): Promise<void> {
  await keepRepositoryConfig(root);

  const gitDirResult = await $`git rev-parse --git-dir`.cwd(root).quiet().nothrow();
  if (gitDirResult.exitCode !== 0) {
    throw new CapturedCommandError(
      'git rev-parse --git-dir',
      gitDirResult.exitCode,
      gitDirResult.stdout,
      gitDirResult.stderr,
    );
  }

  const gitDir = path.resolve(root, new TextDecoder().decode(gitDirResult.stdout).trim());
  const tooling = path.join(root, 'tooling');
  linkHook(gitDir, tooling, 'pre-commit');
  installPostCommitHook(gitDir, tooling);
  linkHook(gitDir, tooling, 'commit-msg');
  linkHook(gitDir, tooling, 'pre-push');
}

function linkHook(gitDir: string, tooling: string, name: string): void {
  const source = path.join(tooling, 'git-hooks', `${name}.sh`);
  if (!existsSync(source)) {
    throw new Error(`Missing ${name} hook source: ${source}`);
  }

  const target = path.join(gitDir, 'hooks', name);
  if (readLinkOrNull(target) === source) {
    return;
  }

  mkdirSync(path.dirname(target), { recursive: true });
  rmSync(target, { force: true });
  symlinkSync(source, target);
}

function installPostCommitHook(gitDir: string, tooling: string): void {
  const source = path.join(tooling, 'git-hooks', 'post-commit.sh');
  if (!existsSync(source)) {
    throw new Error(`Missing post-commit hook source: ${source}`);
  }

  const target = path.join(gitDir, 'hooks', 'post-commit');
  const link = readLinkOrNull(target);
  if (link !== null) {
    // Never append through a symlink: that writes into whatever it points at.
    // Say which link went, so a hook belonging to something else can be put
    // back as a block beside ours instead of vanishing silently.
    console.warn(`Replaced symlinked post-commit hook (was ${link}) with a chainable block`);
  }

  // Every foreign block is preserved, ours is replaced rather than repeated,
  // and the shebang is only ours to choose when the file did not exist.
  const existing = link === null && existsSync(target) ? readFileSync(target, 'utf8') : '';
  const kept = stripPostCommitBlock(existing).replace(/\s+$/, '');
  const next = `${kept === '' ? '#!/usr/bin/env bash' : kept}\n\n${POST_COMMIT_BLOCK}\n`;
  if (existing === next) {
    // A hook that is not executable is a hook git silently skips.
    chmodSync(target, 0o755);
    return;
  }

  mkdirSync(path.dirname(target), { recursive: true });
  rmSync(target, { force: true });
  writeFileSync(target, next, { mode: 0o755 });
}

function stripPostCommitBlock(content: string): string {
  const lines = content.split('\n');
  const start = lines.findIndex((line) => line.trim() === POST_COMMIT_BEGIN);
  if (start === -1) {
    return content;
  }

  // A block whose end marker was lost runs to the end of the file: it is ours
  // to replace either way, and leaving half of it behind would double it.
  const offset = lines.slice(start + 1).findIndex((line) => line.trim() === POST_COMMIT_END);
  const resume = offset === -1 ? lines.length : start + offset + 2;
  return [...lines.slice(0, start), ...lines.slice(resume)].join('\n');
}

function readLinkOrNull(hookPath: string): string | null {
  try {
    return readlinkSync(hookPath);
  } catch {
    return null;
  }
}

async function runSetupCommand(
  command: string,
  shell: ReturnType<typeof $>,
  options: { quiet?: boolean; cwd?: string } = {},
): Promise<void> {
  const result = await shell
    .quiet(options.quiet ?? true)
    .nothrow()
    .cwd(options.cwd ?? projectRoot);
  if (result.exitCode !== 0) {
    throw new CapturedCommandError(command, result.exitCode, result.stdout, result.stderr);
  }
}

/**
 * One value the repository's own config holds. `replaces` is a pattern over the
 * key's existing values: the values it matches are this one's to replace, and
 * every other value of the key stays. Without it the value replaces them all.
 */
interface RepositorySetting {
  readonly key: string;
  readonly value: string;
  readonly replaces?: string;
}

/**
 * The repository config shell entry keeps, read first and written only where
 * it differs.
 *
 * Shell entry runs on every direnv reload, and git writes a config by taking
 * `config.lock` exclusively: of two shell entries writing at once — a shell
 * pool warming a spare beside a command's own activation — one fails with
 * `could not lock config file`. The caller holds the install lock, so no two
 * entries write together, and an entry that finds the config already correct
 * starts no writer at all.
 *
 * tooling/workspace.gitconfig is included by a path relative to the config
 * file, so the include names this checkout wherever the checkout is. An
 * absolute path names the checkout that first wrote it: a copy-on-write clone
 * copies .git/config verbatim, and git refuses to run at all — `fatal: unable
 * to access` — when an include exists but cannot be read, which is exactly
 * what the original checkout is from inside a sandboxed clone. Every include
 * of a workspace.gitconfig is replaced; any other include the user added
 * stays.
 *
 * Reads and writes therefore go through nothing of git's repository
 * discovery: the config file is located on disk and read and edited with
 * `--file` from outside the repository, which loads no local config and so
 * follows no stale include.
 */
async function keepRepositoryConfig(root: string): Promise<void> {
  const configFile = repositoryConfigFile(root);
  const outside = path.parse(configFile).root;
  const settings: RepositorySetting[] = [
    {
      key: 'include.path',
      value: path.relative(path.dirname(configFile), path.join(root, 'tooling', 'workspace.gitconfig')),
      replaces: '(^|/)tooling/workspace\\.gitconfig$',
    },
    // Keep the newer runtime version pins on any merge (nvfetcher overlay +
    // devenv.lock) so a mirror sync's `git am --3way` never stalls on a
    // version conflict. Mapped by the managed .gitattributes
    // (merge=smoo-newer-pins); implemented in tooling/direnv/merge-newer-pins.sh.
    { key: 'merge.smoo-newer-pins.name', value: 'keep the newer devenv/nvfetcher runtime pins' },
    { key: 'merge.smoo-newer-pins.driver', value: 'bash tooling/direnv/merge-newer-pins.sh %O %A %B %P' },
  ];
  const keys = `^(${settings.map((setting) => setting.key.replaceAll('.', '\\.')).join('|')})$`;
  const read = await $`git config --file ${configFile} --get-regexp ${keys}`.cwd(outside).quiet().nothrow();
  // Exit 1 is git's "no such key": nothing is set yet.
  if (read.exitCode !== 0 && read.exitCode !== 1) {
    throw new CapturedCommandError(
      `git config --file ${configFile} --get-regexp '${keys}'`,
      read.exitCode,
      read.stdout,
      read.stderr,
    );
  }
  const current = new Map<string, string[]>();
  for (const line of read.stdout.toString().split('\n')) {
    if (line === '') {
      continue;
    }
    const space = line.indexOf(' ');
    const key = space === -1 ? line : line.slice(0, space);
    current.set(key, [...(current.get(key) ?? []), space === -1 ? '' : line.slice(space + 1)]);
  }
  for (const setting of settings) {
    const replaced = setting.replaces === undefined ? null : new RegExp(setting.replaces);
    const owned = (current.get(setting.key) ?? []).filter((value) => replaced?.test(value) ?? true);
    if (owned.length === 1 && owned[0] === setting.value) {
      continue;
    }
    const pattern = setting.replaces === undefined ? [] : [setting.replaces];
    await runSetupCommand(
      `git config --file ${configFile} --replace-all ${setting.key} ${setting.value}${pattern.map((value) => ` '${value}'`).join('')}`,
      $`git config --file ${configFile} --replace-all ${setting.key} ${setting.value} ${pattern}`,
      { quiet: false, cwd: outside },
    );
  }
}

/**
 * The config file git reads as this checkout's local config: `.git/config`,
 * or, when `.git` is a worktree's `gitdir:` pointer, the config in that
 * worktree's common directory.
 */
function repositoryConfigFile(root: string): string {
  const dotGit = path.join(root, '.git');
  if (statSync(dotGit).isDirectory()) {
    return path.join(dotGit, 'config');
  }
  const pointer = /^gitdir: (.+)$/m.exec(readFileSync(dotGit, 'utf8'));
  if (pointer?.[1] === undefined) {
    throw new Error(`${dotGit} is neither a git directory nor a gitdir pointer`);
  }
  const gitDir = path.resolve(root, pointer[1].trim());
  const commonDir = readFileIfPresent(path.join(gitDir, 'commondir'));
  return path.join(commonDir === null ? gitDir : path.resolve(gitDir, commonDir.toString('utf8').trim()), 'config');
}

function reportSetupFailure(error: unknown): never {
  describeFailure('ERROR', error);
  process.exit(1);
}

function reportDegradedSetup(error: unknown): void {
  describeFailure('WARNING', error);
  reportDeferredSecrets();
  console.error(
    'The shell is loaded WITHOUT installed dependencies so the tools to repair this stay available.\n' +
      'Fix the cause above (missing registry credential, unpublished package, stale lockfile → `devenv update`),\n' +
      'then run `bun install` or `direnv reload`.',
  );
  console.error('---');
}

/**
 * The declared secrets this shell entry deliberately did not resolve, named
 * now that an install has actually failed. Printing them on a healthy shell
 * entry would be noise on every reload — an installed checkout contacts no
 * registry and needs none of them — while an install that failed may be
 * exactly the 401 they explain, and then the useful output is the variable
 * and the command that supplies it.
 */
function reportDeferredSecrets(): void {
  if (deferredSecrets.length === 0) {
    return;
  }
  console.error('Secrets outside the `shell` group are not resolved at shell entry, by design:');
  for (const secret of deferredSecrets) {
    console.error(`- ${secret.name} (${secret.group}): ${secret.guidance}`);
  }
  // Only a group the install itself reads can explain this failure, and only
  // for those is re-running the install the command worth printing. A group
  // nothing here consumes — the Nx cache token — is listed above and left
  // alone: it did not cause this and re-running the install with it would
  // not fix it.
  for (const group of dependentGroups(SHELL_GROUP)) {
    if (!deferredSecrets.some((secret) => secret.group === group)) {
      continue;
    }
    console.error(`The install reads group \`${group}\`; supply it to exactly that one command with:`);
    console.error(`  smoo secrets run ${group} bun install`);
  }
}

function describeFailure(level: 'ERROR' | 'WARNING', error: unknown): void {
  if (error instanceof CapturedCommandError) {
    console.error(`--- ${level}: setup-environment.ts failed while running: ${error.command}`);
    console.error(`exit code: ${error.exitCode}`);
  } else {
    console.error(`--- ${level}: setup-environment.ts failed: ${error}`);
  }
  replayCapturedOutput(error);
  console.error('\n---');
}

function replayCapturedOutput(error: unknown): void {
  if (!(error instanceof CapturedCommandError)) {
    return;
  }
  // The install ran with every resolved secret in its environment, so its
  // output is redacted before replay. Masking is same-length, which keeps the
  // failure this replay exists to show intact.
  const stdout = maskSecretValues(error.stdout, resolvedSecretValues);
  const stderr = maskSecretValues(error.stderr, resolvedSecretValues);
  if (stdout.length > 0) {
    process.stdout.write(stdout);
  }
  if (stderr.length > 0) {
    process.stderr.write(stderr);
  }
}
