import { execFileSync } from 'node:child_process';
import { mkdir, stat, symlink, utimes, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import type { CreateNodesResultV2 } from 'nx/src/devkit-exports.js';
import { CARGO_CLOSURE_INPUT } from '../cargo-closure-input.js';
import { createNodesV2 } from '../index.js';
import type { TestFileInstallation } from '../owned-installation.js';

/**
 * An Nx workspace that is also the Cargo workspace. `app` reaches `leaf`
 * through `bridge`, and `external` — outside the Nx workspace — through
 * `bridge` too. `unrelated` is a member nothing depends on. `app` declares a
 * custom cached target keyed on its closure.
 * `extraFiles` (paths relative to `root`) are written with the rest, last.
 * `installation` is the calling test file's own Nx installation.
 */
export async function closureFixture(
  installation: TestFileInstallation,
  root: string,
  gitignore = 'node_modules/\ntarget/\ndist/\n.nx/\n',
  extraFiles: Record<string, string> = {},
) {
  const workspace = join(root, 'workspace');
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
            inputs: [CARGO_CLOSURE_INPUT],
            options: { command: 'cargo build --quiet --locked --offline --manifest-path packages/app/Cargo.toml' },
          },
        },
      },
    }),
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
    ...extraFiles,
  };
  for (const [path, text] of Object.entries(files)) {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), text);
  }
  execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
  await symlink(await installation.nodeModules(), join(workspace, 'node_modules'), 'dir');
  execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', join(workspace, 'Cargo.toml')], {
    stdio: 'pipe',
  });

  /**
   * Rewrite a fixture file as an edit Nx can see. Nx keeps each file's hash
   * beside its mtime in whole seconds and reuses the hash until that second
   * changes, so a rewrite inside the second the file was last hashed in is
   * invisible to every fileset input. A fast host runs this whole fixture in
   * about a second; the edit is dated at least a second past the old mtime.
   */
  async function edit(path: string, text: string): Promise<void> {
    const file = join(root, path);
    const { mtimeMs } = await stat(file);
    await writeFile(file, text);
    const edited = new Date(Math.max(Date.now(), mtimeMs + 1000));
    await utimes(file, edited, edited);
  }
  return { workspace, edit };
}

/** One project-graph computation over the fixture: the plugin's `createNodes`, as Nx runs it for each recomputation. */
export function graphComputation(workspace: string) {
  const [, infer] = createNodesV2;
  return async () =>
    infer(['package.json', 'packages/app/package.json', 'crates/leaf/package.json'], undefined, {
      workspaceRoot: workspace,
      nxJsonConfiguration: { namedInputs: { externalRustCrates: [] } },
    });
}

/** The computation's nodes in file order: the plugin pushes them as their projects finish. */
export function byFile(graph: CreateNodesResultV2): CreateNodesResultV2 {
  return [...graph].sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
}

/** The `cargoClosure` definition the computation gave the project at `projectRoot`. */
export function closureOf(graph: CreateNodesResultV2, projectRoot: string) {
  return (
    graph.find(([file]) => dirname(file) === projectRoot)?.[1].projects?.[projectRoot]?.namedInputs?.[
      CARGO_CLOSURE_INPUT
    ] ?? []
  );
}
