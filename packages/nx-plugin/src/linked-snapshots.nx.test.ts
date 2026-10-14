// A linked-package snapshot vouched for by real Nx: the managed snapshot module's default probe, which loads the
// plugin product a consumer installs (this package's built `ensure-built`) and calls its read-only `probeBuilt`
// against a provider workspace whose nx.json enables this plugin's producer receipts. No receipt is injected: the
// production record, task hash, HEAD and every output byte come from the provider's own Nx run, and a provider no
// production vouches for is refused unwritten.
import { describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import typia from 'typia';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { managedAssetsRoot } from './managed-assets.js';

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');
const SNAPSHOTS = join(managedAssetsRoot, 'raw/tooling/direnv/linked-snapshots.ts');
const DECLARATION = { targets: ['linked-api:build'], artifactModules: [] };

/** One capture through the default probe, in a fresh Bun process as shell entry runs it: no plugins preloaded. */
const CAPTURE = `
const snapshots = await import(Bun.argv[1]);
const [consumer, source, declaration] = JSON.parse(Bun.argv[2]);
const binding = await snapshots.snapshotBinding(consumer, 'linked-api', source, declaration);
const receipt = binding.kind === 'snapshot' ? snapshots.verifySnapshot(binding.directory.slice(0, -'/packages/api'.length)) : null;
await new Promise<void>((resolve, reject) => process.stdout.write(JSON.stringify({ binding, receipt }), error => error ? reject(error) : resolve()));
`;

interface Captured {
  readonly binding: {
    readonly kind: string;
    readonly id?: string;
    readonly directory?: string;
    readonly reason?: string;
  };
  readonly receipt: {
    readonly id: string;
    readonly provenance: {
      readonly head: string;
      readonly headTree: string;
      readonly tasks: readonly { readonly id: string; readonly hash: string }[];
      readonly producers: readonly { readonly target: string; readonly basis: string; readonly sha256: string }[];
    };
    readonly producerReceipts: readonly {
      readonly version: number;
      readonly target: string;
      readonly argv: readonly string[];
      readonly trackedTreeClean: boolean;
    }[];
  } | null;
}

async function execute(file: string, args: string[], cwd: string, env = process.env) {
  const child = Bun.spawn([file, ...args], { cwd, env, stdout: 'pipe', stderr: 'pipe' });
  const [status, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { status, stdout, stderr };
}

async function git(workspace: string, ...args: string[]): Promise<string> {
  const run = await execute(
    'git',
    ['-C', workspace, '-c', 'user.name=Snapshot fixture', '-c', 'user.email=fixture@example.invalid', ...args],
    workspace,
  );
  expect(run.status, run.stdout + run.stderr).toBe(0);
  return run.stdout.trim();
}

/**
 * A provider Nx workspace whose `linked-api:build` copies its source into `dist`, committed. Its nx.json registers
 * this plugin with the producer option naming that target, so a successful run publishes its completion receipt.
 */
async function writeProvider(workspace: string): Promise<string> {
  await git(workspace, 'init', '--quiet');
  await writeFile(join(workspace, '.gitignore'), '.nx\nnode_modules\npackages/*/dist\n');
  // The repository's own node_modules, so the fixture resolves the Nx this package is written against.
  await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(
    join(workspace, 'nx.json'),
    JSON.stringify({
      useDaemonProcess: true,
      cacheDirectory: '.nx/cache',
      plugins: [{ plugin: '@smoothbricks/nx-plugin', options: { producerReceipts: { targets: DECLARATION.targets } } }],
    }),
  );
  await writeFile(
    join(workspace, 'package.json'),
    JSON.stringify({ name: 'provider', private: true, workspaces: ['packages/*'] }),
  );
  const api = join(workspace, 'packages/api');
  await mkdir(join(api, 'src'), { recursive: true });
  await writeFile(
    join(api, 'package.json'),
    JSON.stringify({
      name: 'linked-api',
      version: '1.0.0',
      type: 'module',
      files: ['dist', 'src'],
      exports: { '.': { default: './dist/index.js' } },
    }),
  );
  await writeFile(
    join(api, 'project.json'),
    JSON.stringify({
      name: 'linked-api',
      targets: {
        build: {
          executor: 'nx:run-commands',
          cache: true,
          inputs: ['{projectRoot}/src/**/*'],
          outputs: ['{projectRoot}/dist'],
          options: { command: 'mkdir -p dist && cp src/index.js dist/index.js', cwd: '{projectRoot}' },
        },
      },
    }),
  );
  await writeFile(join(api, 'src/index.js'), 'export const value = "first";\n');
  await git(workspace, 'add', '-A');
  await git(workspace, 'commit', '-qm', 'provider');
  return api;
}

describe('linked snapshots vouched for by the provider’s real Nx', () => {
  it('captures only what a real Nx production published, once per commit, and refuses stale output without writing it', async () => {
    const consumer = await mkdtemp(join(tmpdir(), 'linked-snapshot-consumer-'));
    try {
      await withNxFixture('linked-snapshot-', async ({ workspace, socketDirectory }) => {
        const source = await writeProvider(workspace);
        const daemonless = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'false' };
        const build = async () => {
          const built = await execute(
            'node',
            [nxEntry, 'run', 'linked-api:build', '--outputStyle=static'],
            workspace,
            daemonless,
          );
          expect(built.status, built.stdout + built.stderr).toBe(0);
        };

        // The consumer installs this package's built plugin product, as a workspace installs @smoothbricks/nx-plugin.
        await mkdir(join(consumer, 'node_modules/@smoothbricks'), { recursive: true });
        await writeFile(join(consumer, 'package.json'), JSON.stringify({ name: 'consumer', private: true }));
        await symlink(packageRoot, join(consumer, 'node_modules/@smoothbricks/nx-plugin'), 'dir');
        const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
        const capture = async (): Promise<Captured> => {
          const run = await execute(
            process.execPath,
            ['-e', CAPTURE, SNAPSHOTS, JSON.stringify([consumer, source, DECLARATION])],
            consumer,
            env,
          );
          expect(run.status, run.stderr).toBe(0);
          return typia.json.assertParse<Captured>(run.stdout);
        };

        // Nothing produced yet: no receipt vouches for the provider, so nothing is captured and nothing is built.
        const unproduced = await capture();
        expect(unproduced.binding.kind).toBe('refused');
        expect(existsSync(join(source, 'dist'))).toBe(false);

        await build();
        const first = await capture();
        expect(first.binding.kind, first.binding.reason).toBe('snapshot');
        expect(first.receipt?.provenance.head).toBe(await git(workspace, 'rev-parse', 'HEAD'));
        expect(first.receipt?.provenance.headTree).toBe(await git(workspace, 'rev-parse', 'HEAD^{tree}'));
        expect(first.receipt?.provenance.tasks.map(({ id }) => id)).toContain('linked-api:build');
        // The receipt the provider's own run published, retained verbatim and bound by the snapshot's name.
        expect(first.receipt?.provenance.producers.map(({ target }) => target)).toEqual(['linked-api:build']);
        const produced = first.receipt?.producerReceipts[0];
        expect(produced?.version).toBe(1);
        expect(produced?.target).toBe('linked-api:build');
        expect(produced?.trackedTreeClean).toBe(true);
        expect(produced?.argv.join(' ')).toContain('linked-api:build');
        expect(await readFile(join(first.binding.directory ?? '', 'dist/index.js'), 'utf8')).toBe(
          'export const value = "first";\n',
        );

        // The committed tree decides validity; an empty commit preserves the real original producer basis.
        const originalBasis = first.receipt?.provenance.head;
        if (originalBasis === undefined) throw new Error('the real producer omitted its basis');
        await git(workspace, 'commit', '-qm', 'same tree', '--allow-empty');
        expect(await git(workspace, 'rev-parse', 'HEAD')).not.toBe(originalBasis);
        const sameTree = await capture();
        expect(sameTree.binding.kind, sameTree.binding.reason).toBe('snapshot');
        expect(sameTree.binding.id).toBe(first.binding.id);
        expect(sameTree.receipt?.provenance.head).toBe(originalBasis);
        expect(sameTree.receipt?.provenance.producers[0]?.basis).toBe(originalBasis);

        // A genuinely changed committed tree cannot borrow the old production.
        await writeFile(join(source, 'src/index.js'), 'export const value = "second";\n');
        await git(workspace, 'add', '-A');
        await git(workspace, 'commit', '-qm', 'changed tree');
        expect((await capture()).binding.kind).toBe('refused');
        await build();
        const second = await capture();
        expect(second.binding.kind, second.binding.reason).toBe('snapshot');
        expect(second.binding.id).not.toBe(first.binding.id);
        expect(second.receipt?.provenance.head).toBe(await git(workspace, 'rev-parse', 'HEAD'));
        expect(await readFile(join(second.binding.directory ?? '', 'dist/index.js'), 'utf8')).toBe(
          'export const value = "second";\n',
        );

        // Output the producer did not record: refused as not current, and the provider's dist is left as it was.
        const output = join(source, 'dist/index.js');
        await writeFile(output, 'export const value = "tampered";\n');
        const before = await stat(output, { bigint: true });
        const stale = await capture();
        expect(stale.binding.kind).toBe('refused');
        expect(stale.binding.reason).toContain('Public product bytes differ from actual producer receipt');
        const after = await stat(output, { bigint: true });
        expect([after.ino, after.mtimeNs, after.ctimeNs]).toEqual([before.ino, before.mtimeNs, before.ctimeNs]);
        expect(await readFile(output, 'utf8')).toBe('export const value = "tampered";\n');
      });
    } finally {
      await rm(consumer, { recursive: true, force: true });
    }
  });
});
