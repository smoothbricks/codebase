import { afterEach, beforeEach, describe, expect, it, spyOn } from 'bun:test';
import { lstat, mkdir, mkdtemp, readdir, readlink, realpath, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { ownedInstallation } from './owned-installation.js';

/** The tool cache a census root keeps its copies in, a real directory as on a fresh CI checkout. */
const KEPT = join('node_modules', '.cache', 'smoothbricks-nx-plugin', 'owned-installations');

let censusRoot = '';

/**
 * A census root whose closure is three small packages: Nx, this plugin and the dependency the plugin declares,
 * installed in its own `node_modules` beside a real (not linked) `node_modules/.cache`. A copy kept in that cache lies
 * beneath this `node_modules`, so Node's ancestor lookup from the copy reaches every host package.
 */
beforeEach(async () => {
  censusRoot = await realpath(await mkdtemp(join(tmpdir(), 'owned-installation-')));
  await writeFile(join(censusRoot, 'package.json'), '{"name":"census-root","private":true}');
  const install = async (name: string, manifest: object) => {
    const root = join(censusRoot, 'node_modules', name);
    await mkdir(root, { recursive: true });
    await writeFile(join(root, 'package.json'), JSON.stringify({ name, version: '1.0.0', ...manifest }));
    await writeFile(join(root, 'index.js'), `module.exports = ${JSON.stringify(name)};\n`);
  };
  await install('nx', {});
  await install('@smoothbricks/nx-plugin', { dependencies: { dependency: '1.0.0' } });
  await install('dependency', {});
  await mkdir(join(censusRoot, KEPT), { recursive: true });
});
afterEach(async () => {
  await rm(censusRoot, { recursive: true, force: true });
});

/**
 * The argv of a process of its own that takes the copy kept as `name`, prints its `node_modules`, then runs `rest`. It
 * runs the module the shards' `build` dependency emitted, outside any bunfig, as the pool-silence cases do: loading the
 * source would load ttsc's runtime transform, and its plugin cache's notices (a build, an abandoned lock reclaimed) would
 * reach this process's stderr, which is the copy's to speak on.
 */
function taker(name: string, rest: string): string[] {
  return [
    process.execPath,
    '--config=/dev/null',
    '-e',
    `const { ownedInstallation } = await import(${JSON.stringify(join(import.meta.dir, '..', 'dist', 'owned-installation.js'))});
const installation = ownedInstallation(${JSON.stringify(censusRoot)}, ${JSON.stringify(name)});
process.stdout.write(await installation.nodeModules());
${rest}
await installation.retire();`,
  ];
}

/**
 * Take the copy kept as `name` and retire it in a process of its own, as one run of a test file does: a process
 * resolves each module once, so a take that shares this one's resolutions would not stand for the next run.
 */
function take(
  name: string,
  env: NodeJS.ProcessEnv = process.env,
): { readonly modules: string; readonly stderr: string } {
  const run = Bun.spawnSync(taker(name, ''), { cwd: dirname(import.meta.dir), env });
  const stderr = run.stderr.toString();
  expect(run.exitCode, stderr).toBe(0);
  return { modules: run.stdout.toString(), stderr };
}

/** Where `path`, read through every link, lies; asserted to be inside the copy whose `node_modules` is `modules`. */
async function inCopy(modules: string, path: string): Promise<void> {
  expect(await realpath(path)).toStartWith(`${dirname(modules)}/`);
}

describe('a kept fixture installation', () => {
  it('is made from an empty cache whose ancestors hold the host packages, and resolves nothing outside itself', async () => {
    const { modules, stderr } = take('fresh');
    expect(stderr).toBe('');
    expect(dirname(dirname(modules))).toBe(join(censusRoot, KEPT, 'fresh'));
    await inCopy(modules, join(modules, 'nx'));
    await inCopy(modules, join(modules, '@smoothbricks/nx-plugin'));
    await inCopy(modules, join(await realpath(join(modules, '@smoothbricks/nx-plugin')), '..', '..', 'dependency'));
    // Kept: the next run proves the same files and copies nothing.
    const index = join(modules, 'nx', 'index.js');
    const made = (await stat(index)).ino;
    expect(take('fresh')).toEqual({ modules, stderr: '' });
    expect((await stat(index)).ino).toBe(made);
  });

  /**
   * A take is about the copy, so its stderr is the copy's alone. A CI run on 2026-11-03 failed the case below with ttsc
   * reclaiming an abandoned plugin cache lock that a killed earlier run had left in the runner's shared cache: the taking
   * process loaded ttsc's runtime transform, and with it whatever state that cache was in. Here the cache is empty.
   */
  it("says nothing on stderr whatever state ttsc's plugin cache is in", async () => {
    const cache = await mkdtemp(join(tmpdir(), 'owned-installation-ttsc-'));
    try {
      expect(take('cacheless', { ...process.env, TTSC_CACHE_DIR: cache }).stderr).toBe('');
    } finally {
      await rm(cache, { recursive: true, force: true });
    }
  });

  it('makes again, elsewhere, a kept copy whose census would resolve a package from the host, and says why', async () => {
    const first = take('escaping');
    await rm(join(first.modules, 'nx'));
    const again = take('escaping');
    const host = join(censusRoot, 'node_modules', 'nx', 'package.json');
    expect(again.stderr).toContain('failed its proof, copying again');
    expect(again.stderr).toContain(`resolves nx from ${dirname(first.modules)} to ${host}, outside the copy`);
    expect(again.modules).not.toBe(first.modules);
    expect((await lstat(join(again.modules, 'nx'))).isSymbolicLink()).toBe(true);
    await inCopy(again.modules, join(again.modules, 'nx'));
    expect((await readdir(join(censusRoot, KEPT, 'escaping'))).sort()).toEqual(
      ['censuses.json', 'holders', 'lock', basename(dirname(again.modules))].sort(),
    );
    expect(await readlink(join(again.modules, 'nx'))).not.toStartWith('/');
  });

  it('copies again in place only the changed package, but never a copy another live process holds', async () => {
    const { modules } = take('changed');
    const kept = join(modules, '@smoothbricks/nx-plugin', 'index.js');
    const untouched = (await stat(kept)).ino;
    await writeFile(join(censusRoot, 'node_modules', 'nx', 'index.js'), 'module.exports = "changed";\n');
    const again = take('changed');
    expect(again.modules).toBe(modules);
    expect(again.stderr).toContain('1 of 3 packages no longer hold what the host closure holds');
    expect(again.stderr).toContain(join(censusRoot, 'node_modules', 'nx'));
    expect(await Bun.file(join(modules, 'nx', 'index.js')).text()).toBe('module.exports = "changed";\n');
    expect((await stat(kept)).ino).toBe(untouched);

    // A holder takes the copy, prints its `node_modules` and a newline, and retires once its stdin closes.
    const holder = Bun.spawn(
      taker('changed', "process.stdout.write('\\n');\nfor await (const _ of Bun.stdin.stream()) {}"),
      { cwd: dirname(import.meta.dir), stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' },
    );
    const reader = holder.stdout.getReader();
    const decoder = new TextDecoder();
    let printed = '';
    while (!printed.endsWith('\n')) {
      const next = await reader.read();
      if (next.done)
        throw new Error(`the holder ended before it took its copy: ${await new Response(holder.stderr).text()}`);
      printed += decoder.decode(next.value);
    }
    expect(printed.slice(0, -1)).toBe(modules);
    const heldIndex = join(modules, 'nx', 'index.js');
    const made = (await stat(heldIndex)).ino;
    await writeFile(join(censusRoot, 'node_modules', 'nx', 'index.js'), 'module.exports = "changed twice";\n');
    // This process takes it while the holder lives: the first take of this census root here, so nothing it resolves
    // was cached by an earlier take.
    const said: string[] = [];
    const write = spyOn(process.stderr, 'write').mockImplementation((chunk: string | Uint8Array) => {
      said.push(String(chunk));
      return true;
    });
    let elsewhere: string;
    try {
      const installation = ownedInstallation(censusRoot, 'changed');
      elsewhere = await installation.nodeModules();
      await installation.retire();
    } finally {
      write.mockRestore();
    }
    expect(said.join('')).toContain('1 of 3 packages no longer hold what the host closure holds');
    expect(said.join('')).toContain('another live process holds it, so it is made in a new directory');
    expect(elsewhere).not.toBe(modules);
    expect(await Bun.file(join(elsewhere, 'nx', 'index.js')).text()).toBe('module.exports = "changed twice";\n');
    // The holder's copy is as it took it, so its retirement proves it unchanged.
    expect(await Bun.file(heldIndex).text()).toBe('module.exports = "changed";\n');
    expect((await stat(heldIndex)).ino).toBe(made);
    holder.stdin.end();
    const exit = await holder.exited;
    expect(exit, await new Response(holder.stderr).text()).toBe(0);
    // Nothing alive holds the old copy now: the next take proves the new one, copies nothing and removes the old.
    expect(take('changed')).toEqual({ modules: elsewhere, stderr: '' });
    expect((await readdir(join(censusRoot, KEPT, 'changed'))).sort()).toEqual(
      ['censuses.json', 'holders', 'lock', basename(dirname(elsewhere))].sort(),
    );
  });

  it('copies only what a package publishes, never the tool state its own tests write beside it', async () => {
    // A workspace package's root holds its tests' scratch (cowshed's `.cache/api-generation-*`, made and removed
    // while another package's fixture copies the closure): content of no installation, and racy to read.
    const dependency = join(censusRoot, 'node_modules', 'dependency');
    await writeFile(
      join(dependency, 'package.json'),
      JSON.stringify({ name: 'dependency', version: '1.0.0', main: 'dist/index.js', files: ['dist'] }),
    );
    await mkdir(join(dependency, 'dist'), { recursive: true });
    await writeFile(join(dependency, 'dist', 'index.js'), 'module.exports = "dependency";\n');
    await mkdir(join(dependency, '.cache', 'api-generation-scratch'), { recursive: true });
    await writeFile(join(dependency, '.cache', 'api-generation-scratch', 'tsconfig.json'), '{}\n');
    const { modules, stderr } = take('published-only');
    expect(stderr).toBe('');
    const copied: string[] = [];
    for (const store of await readdir(join(modules, '.store'))) {
      const root = join(modules, '.store', store, 'node_modules', 'dependency');
      // A store that holds another package has no dependency entry: the one failure expected here.
      const entry = await lstat(root).catch((error: unknown) => {
        if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
        throw error;
      });
      if (entry?.isDirectory()) copied.push(...(await readdir(root, { recursive: true })));
    }
    expect(copied.sort()).toEqual(['dist', 'dist/index.js', 'package.json']);
  });
  it('copies a package’s own .nx directory, which its files select, so the next run proves and keeps the copy', async () => {
    // @nx/workspace 24.0.3 ships dist/src/generators/ci-workflow/files/ci-config/.nx/ci-config.yaml__tmpl__, under
    // `files` with a negation, which Nx's expandOutputs answers file by file.
    const dependency = join(censusRoot, 'node_modules', 'dependency');
    await writeFile(
      join(dependency, 'package.json'),
      JSON.stringify({
        name: 'dependency',
        version: '1.0.0',
        main: 'dist/index.js',
        files: ['dist', '!dist/skip.txt'],
      }),
    );
    await mkdir(join(dependency, 'dist', 'ci-config', '.nx'), { recursive: true });
    await writeFile(join(dependency, 'dist', 'index.js'), 'module.exports = "dependency";\n');
    await writeFile(join(dependency, 'dist', 'skip.txt'), 'not published\n');
    await writeFile(join(dependency, 'dist', 'ci-config', '.nx', 'ci-config.yaml__tmpl__'), 'ci: true\n');
    const first = take('dot-nx');
    expect(first.stderr).toBe('');
    const index = join(first.modules, 'nx', 'index.js');
    const made = (await stat(index)).ino;
    expect(take('dot-nx')).toEqual({ modules: first.modules, stderr: '' });
    expect((await stat(index)).ino).toBe(made);
  });
});
