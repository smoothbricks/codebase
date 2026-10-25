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

/** The argv of a process of its own that takes the copy kept as `name`, prints its `node_modules`, then runs `rest`. */
function taker(name: string, rest: string): string[] {
  return [
    process.execPath,
    '--preload',
    '@smoothbricks/validation/bun/preload',
    '-e',
    `const { ownedInstallation } = await import(${JSON.stringify(join(import.meta.dir, 'owned-installation.ts'))});
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
function take(name: string): { readonly modules: string; readonly stderr: string } {
  const run = Bun.spawnSync(taker(name, ''), { cwd: dirname(import.meta.dir) });
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
});
