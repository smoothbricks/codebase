#!/usr/bin/env bun
import { mkdir, readdir, readFile, readlink, realpath, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { $ } from 'bun';
import { EMIT_OVERRIDES } from '../../packages/nx-plugin/src/executors/typescript-emit/emit-overrides.js';

const devenvRoot = process.env.DEVENV_ROOT;
if (!devenvRoot) {
  throw new Error('DEVENV_ROOT must be set before running enter-shell.ts');
}

const projectRoot = path.resolve(devenvRoot, '../..');
process.chdir(projectRoot);

/**
 * What a plugin rebuild that changed its output invalidates in `.nx/workspace-data`, by pinned Nx 23.2.1's own names:
 * the daemon's directory (`daemon/tmp-dir.js`), whose record names the daemon that loaded the old plugin, which exits
 * once the record is gone; and the project graph that plugin computed (`project-graph/nx-deps-cache.js`).
 */
const pluginDerivedNxState = ['d', 'project-graph.json', 'file-map.json', 'source-maps.json'] as const;

/**
 * Where a package's shell entry records that its `dist` was last verified current: in the package's own tool state
 * (`node_modules/.cache`, a build-volume path in a cowshed checkout), never under `dist`. The tsc-js target owns `dist`
 * whole: its emit is not incremental, so it writes no TypeScript build info, and it removes from `dist` whatever it did
 * not write, the build info this entry's own emit leaves there included. Nx restoring the target's cache artifact
 * replaces `dist` the same way. A marker under `dist` would be gone after every build, and the next shell entry would
 * rebuild a package that is current. Its mtime is when the package was verified current, and its content the SHA-256
 * of the output tree it verified ({@link outputDigest}); a marker an earlier entry wrote is empty and records none.
 */
const buildMarkerFile = 'node_modules/.cache/smoo/dist-verified';

/** The configuration each bootstrap program compiles. */
const programConfiguration = { library: 'tsconfig.lib.json', 'process-lineage': 'tsconfig.lineage.json' } as const;

// devenv runs this hook twice per shell entry. First it captures the shell's environment: a pass that runs the shell
// hook to read what the prologue exports, marked DEVENV_SKIP_TASKS (`capture_shell_environment` in cachix/devenv sets
// it on that subprocess alone), and that runs BEFORE the tasks ordered ahead of devenv:enterShell (smoo:install,
// smoo:cargo-fetch). Then it runs those tasks and starts the shell, whose hook runs again with everything installed. A
// fresh checkout, or a runner that restored node_modules cached for another lockfile, has no working `ttsc` in the
// first pass, and a failing hook aborts that capture ("Shell environment capture failed") before smoo:install could
// make one. So only the entry proper rebuilds the plugin and CLI and syncs the runtime pins.
if ((process.env.DEVENV_SKIP_TASKS ?? '') === '') {
  await rebuildWorkspaceToolsIfStale();

  await syncRuntimePinsIfDrifted();
}

/** Shell entry tracks runtime pins: cheap local `--version` probes, sync on drift, best-effort (offline failure warns, validate is the gate, a CI runner never rewrites). */
async function syncRuntimePinsIfDrifted(): Promise<void> {
  // A CI runner never rewrites pins. GITHUB_ACTIONS, not CI: agent harnesses set
  // CI=true on every command, and keying on it left the pins drifting locally.
  if (process.env.GITHUB_ACTIONS === 'true') {
    return;
  }
  const manifest = await Bun.file(path.join(projectRoot, 'package.json'))
    .json()
    .catch(() => null);
  const pinnedBun = typeof manifest?.packageManager === 'string' ? manifest.packageManager : null;
  const pinnedNode = typeof manifest?.engines?.node === 'string' ? manifest.engines.node : null;
  const [bun, node] = await Promise.all([
    $`bun --version`
      .cwd(projectRoot)
      .quiet(true)
      .nothrow()
      .then((r) => (r.exitCode === 0 ? r.text().trim() : null)),
    $`node --version`
      .cwd(projectRoot)
      .quiet(true)
      .nothrow()
      .then((r) => (r.exitCode === 0 ? r.text().trim().replace(/^v/, '') : null)),
  ]);
  const nodeMajor = node?.split('.', 1)[0];
  if (
    (pinnedBun === null || pinnedBun === `bun@${bun}`) &&
    (pinnedNode === null || nodeMajor === undefined || pinnedNode === `>=${nodeMajor}.0.0`)
  ) {
    return;
  }
  try {
    await runQuietly('smoo', ['monorepo', 'init', '--runtime-only'], projectRoot);
  } catch (error) {
    console.error(
      `! Runtime pin sync failed (${error instanceof Error ? error.message : String(error)}); shell continues with pinned versions. Run \`smoo monorepo init --runtime-only\` once online.`,
    );
  }
}

type BootstrapPackage = {
  directory: string;
  invalidatesPlugin: boolean;
  program: keyof typeof programConfiguration;
  /** The directory the program emits into, which bootstrap judges and digests. */
  output: string;
  /** The packages it compiles against: its direct `workspace:*` runtime dependencies, by directory. */
  dependencies: string[];
};

/**
 * The plugin, CLI and their transitive workspace runtime dependencies, once each and dependency first. The plugin's
 * closure comes first so a rebuild can invalidate only the Nx state it owns, never for CLI-only packages. The lineage
 * program compiles its entry alone, so it compiles against no dependency.
 */
async function bootstrapPackages(): Promise<BootstrapPackage[]> {
  const ordered: BootstrapPackage[] = [];
  const visit = async (
    directory: string,
    invalidatesPlugin: boolean,
    program: BootstrapPackage['program'] = 'library',
  ): Promise<void> => {
    if (ordered.some((entry) => entry.directory === directory)) return;
    const manifest: unknown = JSON.parse(await readFile(path.join(directory, 'package.json'), 'utf8'));
    const declared =
      typeof manifest === 'object' && manifest !== null && 'dependencies' in manifest
        ? manifest.dependencies
        : undefined;
    const dependencies: string[] = [];
    if (program === 'library' && typeof declared === 'object' && declared !== null) {
      for (const [name, range] of Object.entries(declared)) {
        if (range !== 'workspace:*') continue;
        const dependency = await realpath(path.join(directory, 'node_modules', name));
        dependencies.push(dependency);
        await visit(dependency, invalidatesPlugin, name === '@smoothbricks/cowshed' ? 'process-lineage' : 'library');
      }
    }
    ordered.push({
      directory,
      invalidatesPlugin,
      program,
      // A full cowshed API dist does not supply its independent public lineage entry.
      output: path.join(directory, program === 'process-lineage' ? 'dist/lineage' : 'dist'),
      dependencies,
    });
  };
  await visit(path.join(projectRoot, 'packages/nx-plugin'), true);
  await visit(path.join(projectRoot, 'packages/cli'), false);
  return ordered;
}

async function rebuildWorkspaceToolsIfStale(): Promise<void> {
  const packages = await bootstrapPackages();
  // Each stale package, by directory, with the digest of the output its marker verified.
  const stale = new Map<string, string | null>();
  for (const entry of packages) {
    const verification = await readVerification(path.join(entry.directory, buildMarkerFile));
    if (await isStale(entry, verification)) stale.set(entry.directory, verification?.digest ?? null);
  }
  // The first look comes before anything is emitted: the emit rewrites the plugin's dist, which a running Nx run loads
  // its executors and inference from.
  const refusal = packages.some((entry) => entry.invalidatesPlugin && stale.has(entry.directory))
    ? await nxStateRefusal()
    : null;
  // A refusal holds back every stale member of the plugin's closure, and every package whose workspace dependency
  // closure reaches one: it compiles against their dist, so a change to both (a rebase onto a main that changed the
  // plugin and the CLI together) would fail its emit. Packages outside that closure rebuild.
  const held = new Set<string>();
  if (refusal !== null) {
    for (const { directory, invalidatesPlugin, dependencies } of packages) {
      if ((invalidatesPlugin && stale.has(directory)) || dependencies.some((dependency) => held.has(dependency))) {
        held.add(directory);
      }
    }
    const waiting = [...held]
      .filter((directory) => stale.has(directory))
      .map((directory) => path.relative(projectRoot, directory));
    console.error(
      `! Not rebuilding ${waiting.join(', ')}: ${refusal}\n  Their dist, the Nx daemon record and the project graph stay as they are; the next shell entry that finds no other holder rebuilds them.`,
    );
  }
  // Members of the plugin's closure whose emit left bytes other than those their marker verified. Their markers are
  // recorded only once the Nx state computed from the earlier bytes is cleared, so until then they stay stale and keep
  // the digest a later entry compares against.
  const changed: { directory: string; digest: string }[] = [];
  for (const { directory, invalidatesPlugin, program, output } of packages) {
    const verified = stale.get(directory);
    if (verified === undefined || held.has(directory)) continue;
    // Bootstrap invokes neither nx (whose graph loads the plugin) nor smoo (whose launcher needs CLI dist).
    // The lineage program is the same plain-tsc producer the Nx compile lane owns: no addon,
    // generated API or native transformer. Other packages retain the ordinary library emit.
    if (program === 'process-lineage') {
      await runQuietly('tsc', ['--build', programConfiguration[program], '--force'], directory);
    } else {
      await runQuietly('ttsc', ['-p', programConfiguration[program], '--emit', ...EMIT_OVERRIDES], directory);
    }
    // A compiler run is not a change. ttsc is content-incremental, so an mtime-only change (a checkout, a revert, a
    // formatter rewriting a file identically) recompiles to the bytes the marker verified; clearing the daemon record
    // and graph anyway cleared them under a running gate's test tasks. A marker that records no digest verified none,
    // so its package's first emit counts as a change.
    const digest = await outputDigest(output);
    if (invalidatesPlugin && digest !== verified) {
      changed.push({ directory, digest });
    } else {
      await recordBuild(path.join(directory, buildMarkerFile), digest);
    }
  }
  if (changed.length === 0) return;
  // The emit takes seconds, and an Nx run that started during it reads the state the clear deletes, so the census is
  // asked again right before the clear, as a retirement asks again before it retires (02_workspaces.md, "Holders
  // cowshed does not run").
  const late = await nxStateRefusal();
  if (late !== null) {
    const names = changed.map(({ directory }) => path.relative(projectRoot, directory)).join(', ');
    console.error(
      `! The output of ${names} changed, but the Nx daemon record and project graph computed from the earlier bytes stay: ${late}\n  ${names} stay stale, so the next shell entry that finds no other holder clears them.`,
    );
    return;
  }
  await clearNxDaemonState();
  for (const { directory, digest } of changed) await recordBuild(path.join(directory, buildMarkerFile), digest);
}

/** What a package's marker says: when its output was last verified current, and the digest of that output, if any. */
type Verification = { readonly mtimeMs: number; readonly digest: string | null };

/** The verification `marker` records, or null when there is none. */
async function readVerification(marker: string): Promise<Verification | null> {
  try {
    const text = await readFile(marker, 'utf8');
    const { mtimeMs } = await stat(marker);
    const digest = text.trim();
    // A marker an earlier entry wrote is empty, and one torn mid-write names no digest either: both verified no bytes.
    return { mtimeMs, digest: /^[0-9a-f]{64}$/.test(digest) ? digest : null };
  } catch (cause) {
    // No marker: this package was never verified, the expected reason to bootstrap.
    if (isMissing(cause)) return null;
    throw new Error(`Cannot read bootstrap marker ${marker}`, { cause });
  }
}

/**
 * Whether `entry` needs an emit: it has no marker, no output (for the lineage entry, not both of its public files), or a
 * source or configuration file newer than its marker.
 */
async function isStale(
  { directory, program, output }: BootstrapPackage,
  verification: Verification | null,
): Promise<boolean> {
  const outputs = await readdir(output, { withFileTypes: true }).catch((cause: unknown) => {
    // A cold or removed output directory is the expected reason to bootstrap.
    if (isMissing(cause)) return [];
    throw new Error(`Cannot read bootstrap output directory ${output}`, { cause });
  });
  const built =
    program === 'process-lineage'
      ? ['process-lineage.d.ts', 'process-lineage.js'].every((file) =>
          outputs.some((listed) => listed.name === file && listed.isFile()),
        )
      : outputs.length > 0;
  // Directory mtimes miss edits to existing nested files, so compare source file mtimes against the build marker.
  return (
    verification === null ||
    !built ||
    (await hasFileNewerThanMarker(directory, verification.mtimeMs, programConfiguration[program]))
  );
}

/**
 * SHA-256 over an output tree: each entry's path below `directory`, its kind, and its bytes or link text, in path
 * order; a missing tree digests as `absent`. The plugin's closure is a few megabytes, read after its emit and compared
 * with the digest its marker recorded, so a rebuild is judged by the bytes it left rather than by the compiler running.
 */
async function outputDigest(directory: string): Promise<string> {
  const root = await stat(directory).catch((cause: unknown) => {
    if (isMissing(cause)) return null;
    throw cause;
  });
  if (root === null) return 'absent';
  const hash = new Bun.CryptoHasher('sha256');
  const visit = async (relative: string): Promise<void> => {
    const entries = await readdir(path.join(directory, relative), { withFileTypes: true });
    for (const entry of entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))) {
      const name = path.join(relative, entry.name);
      const file = path.join(directory, name);
      if (entry.isDirectory()) {
        hash.update(`directory\0${name}\0`);
        await visit(name);
      } else if (entry.isSymbolicLink()) {
        hash.update(`link\0${name}\0${await readlink(file)}\0`);
      } else if (entry.isFile()) {
        const bytes = await readFile(file);
        hash.update(`file\0${name}\0${bytes.length}\0`);
        hash.update(bytes);
      } else {
        throw new Error(`Bootstrap output ${file} is neither a file, a directory nor a link`);
      }
    }
  };
  await visit('');
  return hash.digest('hex');
}

/**
 * Why the plugin's Nx state must not be cleared now, or null when no other process holds it. Clearing it destroys what
 * every Nx run of this checkout in flight reads (a gate's test tasks failed reading the project graph a concurrent shell
 * entry had cleared), so a cowshed checkout asks the census cowshed puts in front of a destructive operation
 * (specs/cowshed/02_workspaces.md, "Holders cowshed does not run": `cowshed tree-holders`), exempting the daemon the
 * checkout's own record names, as clearing a checkout's Nx state does (16_build_volumes.md). A census that cannot be
 * taken refuses too, saying why. A checkout cowshed does not manage runs no gate and takes no census.
 */
async function nxStateRefusal(): Promise<string | null> {
  if (!(await Bun.file(path.join(projectRoot, '.cowshed/workspace.json')).exists())) return null;
  try {
    const tree = await realpath(path.join(projectRoot, '.nx/workspace-data')).catch((cause: unknown) => {
      // No Nx state yet, so nothing can hold it.
      if (isMissing(cause)) return null;
      throw cause;
    });
    if (tree === null) return null;
    const census = await $`cowshed tree-holders ${tree}`.quiet(true).nothrow();
    if (census.exitCode !== 0) {
      throw new Error(`\`cowshed tree-holders ${tree}\` exited ${census.exitCode}: ${census.stderr.toString().trim()}`);
    }
    const lines = census.stdout
      .toString()
      .split('\n')
      .filter((line) => line !== '');
    if (lines.length === 0) return null;
    const daemon = await recordedDaemon(path.join(tree, 'd/server-process.json'));
    const holders = lines.filter((line) => {
      // The prefix a shell reader may match (02_workspaces.md, "Holders cowshed does not run").
      const pid = /^\{"pid":(\d+),"hold":"[a-z-]+",/.exec(line)?.[1];
      if (pid === undefined) {
        throw new Error(`\`cowshed tree-holders ${tree}\` printed a line that is no TreeHolder: ${line}`);
      }
      return Number(pid) !== daemon;
    });
    if (holders.length === 0) return null;
    // Ten named and the rest counted, as cowshed's own refusals name holders.
    const named = holders
      .slice(0, 10)
      .map((line) => `\n  ${line}`)
      .join('');
    const rest = holders.length > 10 ? `\n  and ${holders.length - 10} more` : '';
    return `another process holds this checkout's Nx state in ${tree}:${named}${rest}`;
  } catch (error) {
    return `the census of who holds this checkout's Nx state could not be taken: ${error instanceof Error ? error.message : String(error)}`;
  }
}

/** The pid the checkout's Nx daemon record names, or null when there is no record. */
async function recordedDaemon(record: string): Promise<number | null> {
  const text = await readFile(record, 'utf8').catch((cause: unknown) => {
    if (isMissing(cause)) return null;
    throw cause;
  });
  if (text === null) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (cause) {
    throw new Error(
      `Nx daemon record ${record} is not JSON: ${cause instanceof Error ? cause.message : String(cause)}`,
      {
        cause,
      },
    );
  }
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('processId' in parsed) ||
    typeof parsed.processId !== 'number' ||
    !Number.isSafeInteger(parsed.processId)
  ) {
    throw new Error(`Nx daemon record ${record} names no process id`);
  }
  return parsed.processId;
}

/** Whether `cause` says the path names nothing: it is absent, or a component above it is not a directory. */
function isMissing(cause: unknown): boolean {
  return cause instanceof Error && 'code' in cause && (cause.code === 'ENOENT' || cause.code === 'ENOTDIR');
}

/**
 * Records that `marker`'s package was verified current now, holding `digest`, the bytes it verified. Failing only costs
 * the next shell entry a rebuild, so it warns and lets the shell enter.
 */
async function recordBuild(marker: string, digest: string): Promise<void> {
  try {
    await mkdir(path.dirname(marker), { recursive: true });
    await writeFile(marker, `${digest}\n`);
  } catch (error) {
    console.error(
      `! Could not record the build marker ${marker} (${error instanceof Error ? error.message : String(error)}); the next shell entry rebuilds that package.`,
    );
  }
}

async function clearNxDaemonState(): Promise<void> {
  // Only what a rebuilt plugin invalidates (`pluginDerivedNxState`). NOT
  // `.nx/cache`, and NOT anything else in `.nx/workspace-data`.
  //
  // `.nx/cache` holds task results and the `terminalOutputs/<hash>` files the task
  // orchestrator writes when a task exits. Deleting it here raced every concurrent
  // Nx invocation: a shell entry or direnv reload (both run this file, and direnv
  // watches the devenv config) removed the directory while another terminal's Nx
  // was mid-run, and that run then died writing its own terminal output —
  // `ENOENT: open '.nx/cache/terminalOutputs/<hash>'` strictly after its tasks had
  // succeeded. `--skip-nx-cache` does not avoid it, because the orchestrator writes
  // that file regardless of whether results are cached.
  //
  // Keeping the task cache is safe, not merely cheaper: a target's hash covers its
  // resolved configuration, so entries produced by the previous plugin build are
  // unreachable rather than wrong once inference changes. Clearing the graph files
  // is what makes the new plugin take effect; clearing results was only collateral.
  //
  // The task database (`<machine>-v<schema>.db`, its SQLite `-wal`/`-shm`/`-journal`
  // and the `.lock` Nx opens it under) is the cache's index: Nx answers a hit only
  // for a `cache_outputs` row and evicts only by row, so deleting it leaves every
  // entry in `.nx/cache` unreachable and never evicted.
  //
  // Nothing else there is the old plugin's output either. Nx's locks and its
  // lockfile and file-hash caches do not depend on it, and the plugin's own state
  // (`cargo-resolve/`, `bounded-exec/`, producer receipts) is content-keyed, so a
  // rebuilt plugin finds it unreachable rather than wrong. A concurrent Nx process
  // writes that state under its own lock: emptying the directory raced it as
  // deleting `.nx/cache` did.
  //
  // Deliberately the workspace-local path, NOT NX_WORKSPACE_DATA_DIRECTORY. Host
  // CI runners redirect that to a shared per-lane tree which also holds the cache
  // provenance DB, and a fresh checkout has no build marker, so this runs on every
  // job: following the variable would clear that shared tree under every other job
  // of the lane. Nx's own graph cache accounts for changed plugin files, so the
  // redirected case needs no help from here.
  //
  // Through it, never the path itself: in a cowshed checkout the path is a link
  // onto the checkout's build volume, and removing it (as `rm -rf` of the path
  // did) left the next Nx run to make a real directory in its place, off the
  // volume, until cowshed's next refresh discarded it.
  const data = path.join(projectRoot, '.nx/workspace-data');
  await Promise.all(pluginDerivedNxState.map((entry) => rm(path.join(data, entry), { recursive: true, force: true })));
}

async function runQuietly(command: string, args: readonly string[], cwd: string): Promise<void> {
  const result = await $`${command} ${args}`.cwd(cwd).quiet(true).nothrow();
  if (result.exitCode === 0) {
    return;
  }
  if (result.stdout.length > 0) {
    process.stdout.write(result.stdout);
  }
  if (result.stderr.length > 0) {
    process.stderr.write(result.stderr);
  }
  throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.exitCode}`);
}

async function hasFileNewerThanMarker(
  directory: string,
  markerMtimeMs: number,
  configuration: 'tsconfig.lib.json' | 'tsconfig.lineage.json',
): Promise<boolean> {
  const sourcePaths = [path.join(directory, 'src'), path.join(directory, configuration)];

  for (const sourcePath of sourcePaths) {
    if (await pathHasFileNewerThanMarker(sourcePath, markerMtimeMs)) {
      return true;
    }
  }
  return false;
}

// `tsconfig.lib.json` excludes `src/**/*.test.ts` and `src/**/__tests__/**`, so those
// files cannot change dist. Counting them made editing any plugin test mark the
// plugin stale, which rebuilt it and cleared Nx's daemon state on every shell entry.

async function pathHasFileNewerThanMarker(sourcePath: string, markerMtimeMs: number): Promise<boolean> {
  const sourceStat = await stat(sourcePath);
  if (sourceStat.isFile()) {
    return sourceStat.mtimeMs > markerMtimeMs;
  }
  if (!sourceStat.isDirectory()) {
    return false;
  }

  for (const entry of await readdir(sourcePath, { withFileTypes: true })) {
    const childPath = path.join(sourcePath, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === '__tests__') {
        continue;
      }
      if (await pathHasFileNewerThanMarker(childPath, markerMtimeMs)) {
        return true;
      }
      continue;
    }
    if (entry.name.endsWith('.test.ts')) {
      continue;
    }
    if (entry.isFile() && (await stat(childPath)).mtimeMs > markerMtimeMs) {
      return true;
    }
  }
  return false;
}
