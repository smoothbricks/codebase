import { expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { resetWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import { fixtureNxEnv } from './__tests__/fixture-nx-env.js';
import { CARGO_CLOSURE_INPUT } from './cargo-closure-input.js';
import { createNodesV2 } from './index.js';

const repositoryRoot = join(import.meta.dir, '../../..');

/**
 * An Nx workspace that is also the Cargo workspace. `app` reaches `leaf`
 * through `bridge`, and `external` — outside the Nx workspace — through
 * `bridge` too. `unrelated` is a member nothing depends on. `app` records
 * every real compile in `executions.log`, so a cache hit is an unchanged log.
 */
async function closureFixture(root: string, gitignore = 'node_modules/\ntarget/\ndist/\n.nx/\n') {
  const workspace = join(root, 'workspace');
  const executions = join(root, 'executions.log');
  const output = join(workspace, 'packages/app/dist/result.txt');
  const files: Record<string, string> = {
    'workspace/.gitignore': gitignore,
    'workspace/package.json':
      '{"name":"cargo-closure-fixture","private":true,"workspaces":["packages/*","crates/*"]}\n',
    'workspace/nx.json': JSON.stringify({
      plugins: ['@smoothbricks/nx-plugin'],
      namedInputs: { externalRustCrates: [] },
    }),
    'workspace/Cargo.toml':
      '[workspace]\nmembers=["packages/app","crates/bridge","crates/leaf","crates/unrelated"]\nresolver="2"\n',
    'workspace/packages/app/package.json': JSON.stringify({
      name: 'app',
      private: true,
      nx: {
        targets: {
          compile: {
            executor: 'nx:run-commands',
            cache: true,
            inputs: ['{projectRoot}/compile.ts', CARGO_CLOSURE_INPUT],
            outputs: ['{projectRoot}/dist'],
            options: { command: 'bun packages/app/compile.ts' },
          },
        },
      },
    }),
    'workspace/packages/app/compile.ts': `import { execFileSync } from 'node:child_process';
import { appendFile, mkdir, writeFile } from 'node:fs/promises';
const result = execFileSync('cargo', ['run', '--quiet', '--locked', '--offline', '--manifest-path', 'packages/app/Cargo.toml'], { encoding: 'utf8' });
await mkdir(${JSON.stringify(dirname(output))}, { recursive: true });
await writeFile(${JSON.stringify(output)}, result);
await appendFile(${JSON.stringify(executions)}, result);
`,
    'workspace/packages/app/Cargo.toml':
      '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nbridge={path="../../crates/bridge"}\n',
    'workspace/packages/app/src/main.rs': 'fn main() { println!("{}", bridge::answer()); }\n',
    'workspace/crates/bridge/Cargo.toml':
      '[package]\nname="bridge"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nleaf={path="../leaf"}\nexternal={path="../../../external"}\n',
    'workspace/crates/bridge/src/lib.rs': 'pub fn answer() -> u8 { leaf::answer() + external::answer() }\n',
    'workspace/crates/leaf/package.json': '{"name":"leaf","private":true}\n',
    'workspace/crates/leaf/Cargo.toml': '[package]\nname="leaf"\nversion="0.1.0"\nedition="2021"\n',
    'workspace/crates/leaf/src/lib.rs': 'pub fn answer() -> u8 { 1 }\n',
    'workspace/crates/unrelated/Cargo.toml': '[package]\nname="unrelated"\nversion="0.1.0"\nedition="2021"\n',
    'workspace/crates/unrelated/src/lib.rs': 'pub fn unrelated() -> u8 { 3 }\n',
    'external/Cargo.toml': '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n[workspace]\n',
    'external/src/lib.rs': 'pub fn answer() -> u8 { 10 }\n',
  };
  for (const [path, text] of Object.entries(files)) {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), text);
  }
  execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', join(workspace, 'Cargo.toml')], {
    stdio: 'pipe',
  });

  /** `nx run app:compile` in a fresh process, as CI runs it; the built plugin is the one Nx loads. */
  async function compile(): Promise<void> {
    const child = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'run', 'app:compile'], {
      cwd: workspace,
      env: fixtureNxEnv(workspace),
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect(exitCode, stdout + stderr).toBe(0);
  }
  const edit = (path: string, text: string) => writeFile(join(root, path), text);
  const log = () => readFile(executions, 'utf8');
  return { workspace, compile, edit, log };
}

it('keys a custom target on exactly its Cargo closure, inside the workspace and out', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-')));
  try {
    const { compile, edit, log } = await closureFixture(root);
    await compile();
    expect(await log()).toBe('11\n');
    await compile();
    expect(await log()).toBe('11\n');

    // Reached only through bridge: no path of app's own names it.
    await edit('workspace/crates/leaf/src/lib.rs', 'pub fn answer() -> u8 { 2 }\n');
    await compile();
    expect(await log()).toBe('11\n12\n');

    // A workspace member outside the closure is not an input.
    await edit('workspace/crates/unrelated/src/lib.rs', 'pub fn unrelated() -> u8 { 4 }\n');
    await compile();
    expect(await log()).toBe('11\n12\n');

    // Outside the Nx workspace, where no fileset reaches.
    await edit('external/src/lib.rs', 'pub fn answer() -> u8 { 20 }\n');
    await compile();
    expect(await log()).toBe('11\n12\n22\n');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);

it('hashes a closure member in an ignored directory, which no fileset can see', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-ignored-')));
  try {
    const { compile, edit, log } = await closureFixture(root, 'node_modules/\ntarget/\ndist/\n.nx/\ncrates/leaf/\n');
    await compile();
    expect(await log()).toBe('11\n');
    await edit('workspace/crates/leaf/src/lib.rs', 'pub fn answer() -> u8 { 2 }\n');
    await compile();
    expect(await log()).toBe('11\n12\n');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);

it('leaves a closure inside the workspace to Nx: no process runs when its tasks are hashed', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-inferred-')));
  try {
    const { workspace } = await closureFixture(root);
    resetWorkspaceContext();
    const [, infer] = createNodesV2;
    const inferred = await infer(['package.json', 'packages/app/package.json', 'crates/leaf/package.json'], undefined, {
      workspaceRoot: workspace,
      nxJsonConfiguration: { namedInputs: { externalRustCrates: [] } },
    });
    const closureOf = (projectRoot: string) =>
      inferred.find(([file]) => dirname(file) === projectRoot)?.[1].projects?.[projectRoot]?.namedInputs?.[
        CARGO_CLOSURE_INPUT
      ];
    const runtimeCommands = (projectRoot: string) =>
      (closureOf(projectRoot) ?? []).flatMap((input) =>
        typeof input === 'object' && 'runtime' in input ? [input.runtime] : [],
      );

    expect(closureOf('crates/leaf')).toContain('{workspaceRoot}/Cargo.lock');
    expect(runtimeCommands('crates/leaf')).toEqual([]);
    // app reaches `external`, the one member only a process can hash, and
    // only it: the in-workspace members stay filesets.
    expect(runtimeCommands('packages/app')).toEqual([
      'node node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-cargo-hash.js --closure packages/app Cargo.toml',
    ]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);
