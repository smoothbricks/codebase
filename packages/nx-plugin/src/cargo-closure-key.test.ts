import { afterAll, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import fc from 'fast-check';
import { WorkspaceContext } from 'nx/src/native/index.js';
import { resetWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import { byFile, closureFixture, closureOf, graphComputation } from './__tests__/cargo-closure-fixture.js';
import { useFixtureCargoHome } from './__tests__/fixture-cargo-home.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { testFileInstallation } from './__tests__/owned-installation.js';
import { runtimeInputValue } from './__tests__/runtime-input.js';
import { CARGO_CLOSURE_INPUT } from './cargo-closure-input.js';
import { isRecord } from './type-guards.js';

/**
 * The pre-push hook's key is the gate's key at the same tree (test-hardening #5).
 *
 * `check:linux` (the gate) writes its verdict under the key Nx computed from a project graph it
 * built before its own tasks ran; the pre-push probe (the hook) recomputes the graph afterwards
 * and reads under that key. A consumer's code generator, run by the gate, rewrites ignored,
 * content-named chunk files and the generated `.rs` sources that `include_str!` them. When the
 * Cargo closure named those files, the generator's own output moved the input list, so every
 * first push of a gated tree was refused (1f5c3a58e). The properties below hold the key to a
 * function of Nx's file index: no history of producer runs moves it, and the real Nx hook run
 * reads exactly the hash the real gate run wrote.
 */

useFixtureCargoHome();

/** The Nx installation this file's fixtures share; none may write into it. */
const installation = testFileInstallation('cargo-closure-key-installation');
afterAll(() => installation.retire());

/** The ignored directory the producer owns, inside `leaf`, which every closure of `app` reaches. */
const GENERATED = 'crates/leaf/src/generated';
const GITIGNORE = `node_modules/\ntarget/\ndist/\n.nx/\n.runs\n${GENERATED}/\n`;

/** `leaf` compiles an indexed file in, and the generated sources by `include!`, as a consumer's tracked sources do. */
const LEAF_FILES = {
  'workspace/crates/leaf/src/lib.rs': [
    'pub fn answer() -> u8 { 1 }',
    'pub const SKILL: &str = include_str!("../../../skills/SKILL.md");',
    'include!("generated/vocabulary.rs");',
  ].join('\n'),
  'workspace/skills/SKILL.md': 'skill-one',
};

/** One producer run: the chunk contents it emits, whether it clears the previous run's files, and where. */
interface ProducerRun {
  readonly chunks: readonly string[];
  readonly clean: boolean;
  readonly nested: boolean;
}

const producerRun: fc.Arbitrary<ProducerRun> = fc.record({
  chunks: fc.uniqueArray(fc.string({ minLength: 1, maxLength: 12 }), { minLength: 1, maxLength: 3 }),
  clean: fc.boolean(),
  nested: fc.boolean(),
});

/** A content-named chunk, as a bundler names it: the name moves whenever the content does. */
const chunkName = (content: string) => `chunk-${Bun.hash(content).toString(36)}.js`;

/** Write one run's ignored outputs: a table, its chunks, and the generated source including each. */
async function produce(workspace: string, { chunks, clean, nested }: ProducerRun): Promise<void> {
  const generated = join(workspace, GENERATED);
  if (clean) await rm(generated, { recursive: true, force: true });
  const directory = nested ? join(generated, 'shared') : generated;
  await mkdir(directory, { recursive: true });
  await writeFile(join(generated, 'table.bin'), chunks.join('\0'));
  const includes = chunks.map((content, index) => {
    const name = chunkName(content);
    return { name: `${nested ? 'shared/' : ''}${name}`, content, index };
  });
  for (const { name, content } of includes) await writeFile(join(generated, name), content);
  await writeFile(
    join(generated, 'vocabulary.rs'),
    [
      'pub const TABLE: &[u8] = include_bytes!("table.bin");',
      ...includes.map(({ name, index }) => `pub const CHUNK_${index}: &str = include_str!("${name}");`),
    ].join('\n'),
  );
}

let contexts = 0;

/**
 * `app`'s Cargo-closure key as a task hashes it: the input list the project graph names, the
 * bytes Nx's file index hashes for its filesets, and the stdout of its runtime entry. A fresh
 * graph computation and a fresh file context each time, as each `nx` process starts with.
 */
async function closureKey(workspace: string): Promise<string> {
  resetWorkspaceContext();
  const inputs = closureOf(byFile(await graphComputation(workspace)()), 'packages/app');
  // Every fileset the closure names is workspace-anchored; `!` entries exclude.
  const filesets = inputs.flatMap((input) => (typeof input === 'string' ? [input] : []));
  for (const input of filesets) expect(input).toMatch(/^!?\{workspaceRoot\}\//);
  const anchored = (input: string) => input.replace(/^!?\{workspaceRoot\}\//, '');
  const positive = filesets.filter((input) => !input.startsWith('!')).map(anchored);
  const negative = filesets.filter((input) => input.startsWith('!')).map(anchored);
  contexts += 1;
  const context = new WorkspaceContext(workspace, join(workspace, `.nx/closure-key-${contexts}`));
  const runtime = await Promise.all(
    inputs.flatMap((input) =>
      typeof input === 'object' && 'runtime' in input ? [runtimeInputValue(input.runtime, workspace)] : [],
    ),
  );
  return JSON.stringify({ inputs, files: context.hashFilesMatchingGlob(positive, negative), runtime });
}

it('no history of producer runs over ignored compiled-in files moves the Cargo closure key', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-key-')));
  try {
    const { workspace, edit } = await closureFixture(installation, root, GITIGNORE, LEAF_FILES);
    const gate = await closureKey(workspace);
    // The key really reaches leaf's compiled-in data, so an unmoved key is not an empty one.
    expect(gate).toContain('{workspaceRoot}/skills/SKILL.md');
    expect(gate).not.toContain('generated/');

    await fc.assert(
      fc.asyncProperty(fc.array(producerRun, { minLength: 1, maxLength: 4 }), async (history) => {
        await rm(join(workspace, GENERATED), { recursive: true, force: true });
        for (const run of history) await produce(workspace, run);
        expect(await closureKey(workspace)).toBe(gate);
      }),
      { numRuns: 15 },
    );

    // And an edit Nx's index sees still moves it: the property is not a key that never moves.
    await edit('workspace/skills/SKILL.md', 'skill-two');
    expect(await closureKey(workspace)).not.toBe(gate);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

const RECEIPT_ENV = 'COWSHED_NX_TASK_RECEIPT_ID';
const GATE_RECEIPT = '0123456789abcdef0123456789abcdef';
const HOOK_RECEIPT = 'fedcba9876543210fedcba9876543210';

/**
 * The gate's task: a producer like a consumer's runtime-vocabulary generator. Each execution writes a chunk
 * named for a new run number and the generated source including it, replacing the last run's,
 * and counts itself in `.runs`, which no input names.
 */
const PRODUCER = [
  'set -e',
  `d=${GENERATED}`,
  'n=$(( $(cat .runs 2>/dev/null || echo 0) + 1 ))',
  'echo "$n" > .runs',
  'rm -rf "$d"',
  'mkdir -p "$d"',
  `printf 'chunk %s' "$n" > "$d/chunk-$n.js"`,
  `printf 'pub const CHUNK: &str = include_str!("chunk-%s.js");\\n' "$n" > "$d/vocabulary.rs"`,
].join('\n');

/** The task hash and cache status the plugin's hint recorded for the one task a receipt's run ran. */
async function recorded(workspace: string, receipt: string): Promise<{ hash: string; status: string }> {
  const namespace = join(workspace, '.nx/cache/.cowshed-task-results', receipt);
  const files = await readdir(namespace);
  expect(files).toHaveLength(1);
  const [file] = files;
  if (file === undefined) throw new Error(`no hint in ${namespace}`);
  const hint: unknown = JSON.parse(await readFile(join(namespace, file), 'utf8'));
  const tasks = isRecord(hint) && Array.isArray(hint.tasks) ? hint.tasks : [];
  expect(tasks).toHaveLength(1);
  const [entry] = tasks;
  const task = isRecord(entry) && isRecord(entry.task) ? entry.task : undefined;
  expect(task?.id).toBe('app:gate');
  const hash = task?.hash;
  const status = isRecord(entry) ? entry.status : undefined;
  if (typeof hash !== 'string' || typeof status !== 'string') throw new Error(`malformed hint ${namespace}/${file}`);
  return { hash, status };
}

it('the hook run reads the key the gate run wrote, after the gate rewrote compiled-in outputs', async () => {
  await withNxFixture(
    'cargo-closure-key',
    async ({ root, workspace, socketDirectory }) => {
      await closureFixture(installation, root, GITIGNORE, {
        ...LEAF_FILES,
        'workspace/produce.sh': `${PRODUCER}\n`,
        'workspace/packages/app/package.json': JSON.stringify({
          name: 'app',
          private: true,
          nx: {
            targets: {
              gate: {
                executor: 'nx:run-commands',
                cache: true,
                inputs: [CARGO_CLOSURE_INPUT],
                options: { command: 'sh produce.sh', cwd: '{workspaceRoot}' },
              },
            },
          },
        }),
      });
      const { [RECEIPT_ENV]: _inherited, ...env } = fixtureNxEnv(workspace, socketDirectory);
      const nx = async (receipt: string) => {
        const argv = [
          process.execPath,
          join(workspace, 'node_modules/.bin/nx'),
          'run',
          'app:gate',
          '--outputStyle=static',
        ];
        const child = Bun.spawn(argv, {
          cwd: workspace,
          env: { ...env, [RECEIPT_ENV]: receipt },
          stdout: 'pipe',
          stderr: 'pipe',
        });
        const [exitCode, stdout, stderr] = await Promise.all([
          child.exited,
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ]);
        return { exitCode, log: `${stdout}${stderr}` };
      };

      const gate = await nx(GATE_RECEIPT);
      expect(gate.exitCode, gate.log).toBe(0);
      expect(await readFile(join(workspace, '.runs'), 'utf8')).toBe('1\n');
      // The gate's own execution left a compiled-in file its graph never saw. readdir's order is the filesystem's
      // (hash order on Linux), so the listing is compared as a set.
      expect((await readdir(join(workspace, GENERATED))).sort()).toEqual(['chunk-1.js', 'vocabulary.rs']);

      const hook = await nx(HOOK_RECEIPT);
      expect(hook.exitCode, hook.log).toBe(0);
      const [gateTask, hookTask] = [await recorded(workspace, GATE_RECEIPT), await recorded(workspace, HOOK_RECEIPT)];
      expect(gateTask.status).toBe('success');
      // The hook computed the gate's hash and read its entry (`-kept-existing`: the task declares no outputs).
      expect(hookTask).toEqual({ hash: gateTask.hash, status: 'local-cache-kept-existing' });
      expect(await readFile(join(workspace, '.runs'), 'utf8')).toBe('1\n');
    },
    'workspace',
  );
});
