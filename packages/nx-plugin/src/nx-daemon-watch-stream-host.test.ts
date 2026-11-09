import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, readFile, rename, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { DAEMON_GLOB_PLUGIN, fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the watch-probe hunk of `patches/nx@24.0.3.patch` (`daemon/server/file-watching/watch-probe.js`). A daemon's
 * file watch can stop delivering without a word: an FSEventStream that fseventsd stopped serving (a restarted
 * fseventsd, a stream that never started) reports nothing, and Nx 24's watch settles by time, not by an event it waited
 * for (Nx 23.2.1's native watcher also dropped FSEvents' rescan flags; Nx 24 turns them into a rescan marker). The
 * daemon then hashed every task from the file state it last saw: a shed's daemon received no event for 33 minutes, and
 * `nx run-many` called a changed input's target "[existing outputs match the cache, left as is]", so its tests ran the
 * old build.
 *
 * One stream carries every change the daemon acts on, and on macOS a command's first request that answers from file
 * state first writes a probe and waits for it to come back through that stream, which delivers in order. A probe that
 * does not come back means the stream stopped: the daemon tells the client "watcher stream stopped", which goes on
 * without a daemon, says so in its log and stops, and the next client starts a daemon with a fresh stream. The probe is
 * made once per connection, by its first request answered from file state: a command sees every write made before it
 * started. A plugin worker is no command and makes none. Linux inotify has no such failure mode (no fseventsd between
 * the kernel and the watcher), so there the daemon only settles its watch before answering.
 *
 * The daemon's first probe also proves the files it booted with. Its boot walk can predate a write its stream never
 * reports: a daemon a shed's keeper starts while that shed's first check is writing an output booted without
 * `a/generated.txt`, and no event for it came before the probe of the next check came back, so `a:test` was keyed on
 * files that were not on disk. So the first probe of a daemon's life re-walks the files once it is back.
 *
 * A host test: the dead stream is a daemon started under a Seatbelt profile that denies FSEvents, which `sandbox_apply`
 * refuses inside another sandbox, such as a cowshed job's.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const DEAD_STREAM = '(version 1)(allow default)(deny mach-lookup (global-name "com.apple.FSEvents"))';
const RENAME_CASE = 'rebuilds a target whose input was replaced by rename, through the one stream';
const DOT_HIDDEN_CASE = 'proves its watcher current where .nxignore hides every dot path';

/**
 * The fixture's `node_modules` links the repository's. With `hidesDotPaths` the fixture ignores every dot path in its
 * `.nxignore`, as this repository does, and holds a real `node_modules` directory linking only `nx`. The daemon's watch
 * admits its probe by name (`alwaysWatch`), which wins over the ignore files, so the probe still comes back.
 */
async function fixtureWorkspace(workspace: string, hidesDotPaths = false): Promise<void> {
  if (hidesDotPaths) {
    await mkdir(join(workspace, 'node_modules'));
    await symlink(join(repositoryRoot, 'node_modules', 'nx'), join(workspace, 'node_modules', 'nx'), 'dir');
    await writeFile(join(workspace, '.nxignore'), '.*\n!/.gitignore\n!/.nxignore\n');
  } else {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  }
  await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\np/out.txt\n');
  await writeFile(join(workspace, 'nx.json'), JSON.stringify({ targetDefaults: { copy: { cache: true } } }));
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-watch-stream', private: true }));
  await mkdir(join(workspace, 'p'));
  await writeFile(
    join(workspace, 'p', 'project.json'),
    JSON.stringify({
      name: 'p',
      targets: { copy: { command: 'cat p/src.txt > p/out.txt', outputs: ['{projectRoot}/out.txt'] } },
    }),
  );
  await writeFile(join(workspace, 'p', 'src.txt'), 'one\n');
}

interface Ran {
  readonly status: number | null;
  readonly output: string;
}

function nx(workspace: string, env: Record<string, string>, launch: readonly string[], ...args: string[]): Ran {
  const [command, ...prefix] = [...launch, 'bun', nxEntry];
  if (command === undefined) throw new Error('a launch names its command');
  const ran = spawnSync(command, [...prefix, ...args], { cwd: workspace, env, encoding: 'utf8' });
  return { status: ran.status, output: `${ran.stdout}${ran.stderr}` };
}

/** Run `p:copy` and return what it printed and what it left in `p/out.txt`. */
async function copy(workspace: string, env: Record<string, string>): Promise<{ output: string; out: string }> {
  const ran = nx(workspace, env, [], 'run', 'p:copy', '--outputStyle=static');
  expect(ran.status, ran.output).toBe(0);
  return { output: ran.output, out: await readFile(join(workspace, 'p', 'out.txt'), 'utf8') };
}

describe('the daemon answers from the files as they are when a request arrives', () => {
  it('rebuilds a target whose input was replaced by rename, through the one stream, and proves its watcher current where .nxignore hides every dot path', async () => {
    // One healthy daemon walks both cases over the dot-hidden layout. Its probe must come back though .nxignore hides
    // every dot path, its workspace data directory included: the daemon's watch admits the probe by name.
    // A rename needs only the stream.
    await withNxFixture('nx-dot-hidden-', async ({ workspace, socketDirectory }) => {
      await fixtureWorkspace(workspace, true);
      const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_TUI: 'false' };
      const started = nx(workspace, env, [], 'daemon', '--start');
      expect(started.status, `${RENAME_CASE}, and ${DOT_HIDDEN_CASE}: the daemon starts\n${started.output}`).toBe(0);
      expect((await copy(workspace, env)).out, `${DOT_HIDDEN_CASE}: the first copy`).toBe('one\n');
      await writeFile(join(workspace, 'p', 'src.txt'), 'two\n');
      const second = await copy(workspace, env);
      expect(second.output, `${DOT_HIDDEN_CASE}: no stopped stream`).not.toContain('watcher stream stopped');
      expect(second.output, `${DOT_HIDDEN_CASE}: no stale cache hit`).not.toContain('existing outputs match the cache');
      expect(second.out, `${DOT_HIDDEN_CASE}: the rewritten input`).toBe('two\n');
      // An editor's or icm's publication: write beside, rename over.
      await writeFile(join(workspace, 'p', '.src.txt.tmp'), 'three\n');
      await rename(join(workspace, 'p', '.src.txt.tmp'), join(workspace, 'p', 'src.txt'));
      const renamed = await copy(workspace, env);
      expect(renamed.output, `${RENAME_CASE}: no stale cache hit`).not.toContain('existing outputs match the cache');
      expect(renamed.out, `${RENAME_CASE}: the renamed input`).toBe('three\n');
      if (process.platform === 'darwin') {
        const log = await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8');
        expect(log, `${DOT_HIDDEN_CASE}: the probe came back`).toContain('watcher current: probe came back');
        expect(log, `${DOT_HIDDEN_CASE}: no refused probe`).not.toContain('cannot be proven current');
      }
    });
  });

  if (process.platform === 'darwin') {
    it('proves the command current, never the plugin worker it asks for files', async () => {
      // A plugin worker's request for files nests in a daemon operation already current: the watcher's own recompute,
      // which must read its own snapshot, or a command whose connection made its probe. A probe per worker connection
      // cost every isolated graph plugin a round trip through the stream.
      await withNxFixture('nx-worker-probe-', async ({ workspace, socketDirectory }) => {
        await fixtureWorkspace(workspace, true);
        await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['./daemon-glob-plugin.js'] }));
        await writeFile(join(workspace, 'daemon-glob-plugin.js'), DAEMON_GLOB_PLUGIN);
        const env = {
          ...fixtureNxEnv(workspace, socketDirectory),
          NX_DAEMON: 'true',
          NX_TUI: 'false',
          NX_ISOLATE_PLUGINS: 'true',
        };
        const started = nx(workspace, env, [], 'daemon', '--start');
        expect(started.status, started.output).toBe(0);
        const shown = nx(workspace, env, [], 'show', 'projects', '--json');
        expect(shown.status, shown.output).toBe(0);
        const log = await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8');
        expect(log, 'the worker asked the daemon for files').toContain('Handled json message GLOB');
        expect(log.match(/watcher current: probe came back/g)?.length, log).toBe(1);
      });
    });
    it('re-walks the files it booted with once its first probe comes back', async () => {
      // A write the stream never reports, made after the daemon booted and before its first request: Nx's native watch
      // drops its transient files (`_tmp_*_*`, `TRANSIENT_FILE_GLOBS`), which its walk still lists as project files.
      // The cache holds the target's entry for the old bytes, from a run without a daemon, so a daemon answering from
      // its boot walk calls the target cached.
      await withNxFixture('nx-boot-walk-', async ({ workspace, socketDirectory }) => {
        await fixtureWorkspace(workspace);
        const unreported = join(workspace, 'p', '_tmp_input_1');
        await writeFile(unreported, 'one\n');
        const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_TUI: 'false' };
        expect((await copy(workspace, { ...env, NX_DAEMON: 'false' })).out, 'the run without a daemon').toBe('one\n');
        const started = nx(workspace, env, [], 'daemon', '--start');
        expect(started.status, started.output).toBe(0);
        await writeFile(unreported, 'two\n');
        const first = await copy(workspace, env);
        expect(first.output, 'the stream is live').not.toContain('watcher stream stopped');
        expect(first.output, 'no hit keyed on the boot walk').not.toContain('existing outputs match the cache');
        expect(first.output, 'no restore keyed on the boot walk').not.toContain('[local cache]');
      });
    });
  }
  if (process.platform === 'darwin') {
    // Each dead-stream case is its own daemon lifetime: its first request stops that daemon, and the layout decides
    // whether the watcher has a probe directory, which decides the message the stream gives.
    it('stops a daemon whose FSEvents stream delivers nothing, rather than hash from stale files', async () => {
      await withNxFixture('nx-dead-stream-', async ({ workspace, socketDirectory }) => {
        await fixtureWorkspace(workspace);
        const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_TUI: 'false' };
        const started = nx(workspace, env, ['sandbox-exec', '-p', DEAD_STREAM], 'daemon', '--start');
        expect(started.status, started.output).toBe(0);
        // The command's first request finds the stream dead: the daemon tells the client, which goes on without it.
        const first = await copy(workspace, env);
        expect(first.output).toContain('watcher stream stopped');
        expect(first.output).toContain('Continuing without the daemon.');
        expect(first.out).toBe('one\n');
        await writeFile(join(workspace, 'p', 'src.txt'), 'two\n');
        const second = await copy(workspace, env);
        expect(second.output).not.toContain('existing outputs match the cache');
        expect(second.out).toBe('two\n');
        const log = await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8');
        expect(log).toContain('watcher stream stopped');
      });
    });

    it('serves nothing from a watcher it has nowhere to prove current', async () => {
      await withNxFixture(
        'nx-no-probe-',
        async ({ root, workspace, socketDirectory }) => {
          // Workspace data on another directory, as a cowshed shed's is, node_modules linked from outside and no .git:
          // no directory inside the checkout takes a probe the watcher reports.
          await fixtureWorkspace(workspace);
          const data = join(root, 'elsewhere-data');
          await mkdir(data);
          const env = {
            ...fixtureNxEnv(workspace, socketDirectory),
            NX_WORKSPACE_DATA_DIRECTORY: data,
            NX_DAEMON: 'true',
            NX_TUI: 'false',
          };
          const started = nx(workspace, env, ['sandbox-exec', '-p', DEAD_STREAM], 'daemon', '--start');
          expect(started.status, started.output).toBe(0);
          const first = await copy(workspace, env);
          expect(first.output).toContain('cannot be proven current');
          expect(first.output).toContain('Continuing without the daemon.');
          expect(first.out).toBe('one\n');
          await writeFile(join(workspace, 'p', 'src.txt'), 'two\n');
          const second = await copy(workspace, env);
          expect(second.output).not.toContain('existing outputs match the cache');
          expect(second.out).toBe('two\n');
        },
        'ws',
      );
    });
  }
});
