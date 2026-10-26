import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { chmod, lstat, mkdir, mkdtemp, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Task } from 'nx/src/config/task-graph';
import { expandOutputs } from 'nx/src/native';
import typia from 'typia';
import { runInRepositoryNx } from './__tests__/repository-nx.js';
import { cargoRuntimeInput } from './cargo-toolchain-policy.js';
import type { EnsureBuiltProducerReceipt } from './ensure-built.js';
import {
  boundedReceiptRuntimes,
  captureProducerSource,
  dirtyProviderPaths,
  type ProducerSourceWitness,
  type ProviderInputProof,
  producerOutputPaths,
  producerSourceUnchanged,
  providerReceipt,
} from './ensure-built-receipt.js';
import { runtimeInput } from './runtime-input.js';

let root = '';
let source: ProducerSourceWitness;
let producer: Omit<EnsureBuiltProducerReceipt, 'outputSha256'>;
const task = {
  id: 'app:build',
  target: { project: 'app', target: 'build' },
  overrides: {},
  outputs: ['output'],
  cache: true,
  hash: 'current-key',
} satisfies Task;
const inputs = { files: ['source.txt'], runtime: [], environment: [], depOutputs: [], external: [] };
const proof: ProviderInputProof = {
  kind: 'exact-file-inputs',
  files: ['source.txt'],
  inputs: { [task.id]: inputs },
  runtimeSource: { kind: 'bounded' },
};
function git(...args: string[]): void {
  const result = Bun.spawnSync(['git', '-C', root, ...args], { stderr: 'pipe' });
  expect(result.exitCode, result.stderr.toString()).toBe(0);
}

beforeEach(async () => {
  root = await realpath(await mkdtemp(join(tmpdir(), 'ensure-built-receipt-')));
  git('init', '--quiet');
  await writeFile(join(root, '.gitignore'), 'output/\n');
  await writeFile(join(root, 'source.txt'), 'source');
  git('add', '.');
  git('-c', 'user.name=Receipt fixture', '-c', 'user.email=receipt@example.invalid', 'commit', '-qm', 'fixture');
  await mkdir(join(root, 'output/empty'), { recursive: true });
  await writeFile(join(root, 'output/file'), 'public bytes');
  await symlink('file', join(root, 'output/link'));
  await chmod(join(root, 'output/file'), 0o644);
  await chmod(join(root, 'output/empty'), 0o755);
  const captured = captureProducerSource(root, producerOutputPaths(root, [task], expandOutputs));
  if (!captured.ok) throw new Error(captured.detail);
  source = captured.value;
  producer = {
    version: 1,
    target: task.id,
    argv: ['nx', 'run', task.id],
    trackedTreeClean: true,
    environmentIdentity: 'unit-fixture-nx-hash-identity',
    tools: [],
    wallTimeMs: 0,
    inputs: [
      {
        taskId: task.id,
        overrides: task.overrides,
        declared: [],
        expanded: inputs,
        hashDetails: { command: 'unit-fixture', nodes: {} },
        files: source.files.filter(({ path }) => inputs.files.includes(path)),
      },
    ],
  };
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

describe('provider receipt', () => {
  it('does not borrow the caller Git directory/index/object namespace for provider provenance', () => {
    const saved = process.env.GIT_DIR;
    process.env.GIT_DIR = join(root, 'not-the-provider-git-directory');
    try {
      expect(providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer).disposition).toBe('hit');
    } finally {
      if (saved === undefined) delete process.env.GIT_DIR;
      else process.env.GIT_DIR = saved;
    }
  });
  it("reads provider provenance without the operator's global git configuration", async () => {
    // The cowshed sandbox denies the operator's global git configuration to a workspace's Nx daemon, which runs these
    // hooks under a host client's HOME; git then dies "unable to access". A self-referencing link fails git the same
    // way (ELOOP), where an unreadable file would only warn (EACCES).
    const home = join(root, 'operator-home');
    await mkdir(home);
    await symlink('.gitconfig', join(home, '.gitconfig'));
    const names = ['HOME', 'XDG_CONFIG_HOME', 'GIT_CONFIG_GLOBAL', 'GIT_CONFIG_NOSYSTEM'] as const;
    const saved = names.map((name) => [name, process.env[name]] as const);
    for (const name of names) delete process.env[name];
    process.env.HOME = home;
    try {
      const captured = captureProducerSource(root, producerOutputPaths(root, [task], expandOutputs));
      expect(captured.ok ? 'captured' : captured.detail).toBe('captured');
      expect(producerSourceUnchanged(root, source)).toEqual({ ok: true, value: true });
    } finally {
      for (const [name, value] of saved) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  });
  it('names current hashes, HEAD/tree, independent file digests, link text and empty directory modes', async () => {
    const result = providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer);
    expect(result.disposition).toBe('hit');
    if (result.disposition !== 'hit') throw new Error(result.reason.detail);
    expect(result.receipt.head).toMatch(/^[a-f0-9]{40,64}$/);
    expect(result.receipt.headTree).toMatch(/^[a-f0-9]{40,64}$/);
    expect(result.receipt.workspaceRoot).toBe(root);
    expect(result.receipt.runtimeEnvironmentProvenance).toBe('nx-task-hash-only');
    expect(result.receipt.tasks[0].id).toBe(task.id);
    expect(result.receipt.tasks[0].hash).toBe(task.hash);
    expect(result.receipt.tasks[0].outputs).toContainEqual({
      path: 'output/file',
      type: 'file',
      mode: 0o644,
      sha256: createHash('sha256').update('public bytes').digest('hex'),
    });
    expect(result.receipt.tasks[0].outputs).toContainEqual({
      path: 'output/link',
      type: 'symlink',
      mode: (await lstat(join(root, 'output/link'))).mode & 0o7777,
      linkText: 'file',
    });
    expect(result.receipt.tasks[0].outputs.find(({ path }) => path === 'output/empty')).toMatchObject({
      type: 'directory',
      mode: 0o755,
      entries: [],
    });
  });

  it('ignores unrelated untracked files while retaining tracked-tree production provenance', async () => {
    await writeFile(join(root, 'unrelated.ts'), 'unrelated');
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer).disposition).toBe('hit');
    expect(
      providerReceipt(
        root,
        task.id,
        [task],
        { ...proof, kind: 'conservative-provider-source' },
        expandOutputs,
        source,
        producer,
      ).disposition,
    ).toBe('hit');
  });

  it('refuses modified, deleted, newly matched and index-only dirty FILE inputs', async () => {
    await writeFile(join(root, 'source.txt'), 'changed');
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer).disposition).toBe('failed');
    git('add', 'source.txt');
    await writeFile(join(root, 'source.txt'), 'source');
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer).disposition).toBe('failed');
    git('restore', '--staged', 'source.txt');
    await writeFile(join(root, 'new-source.txt'), 'new');
    expect(
      providerReceipt(
        root,
        task.id,
        [task],
        { ...proof, files: [...proof.files, 'new-source.txt'] },
        expandOutputs,
        source,
        producer,
      ).disposition,
    ).toBe('failed');
    await rm(join(root, 'source.txt'));
    expect(
      providerReceipt(root, task.id, [task], { ...proof, files: [] }, expandOutputs, source, producer).disposition,
    ).toBe('failed');
  });

  it('recomputes output byte/directory identities after every public mutation', async () => {
    const first = providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer);
    if (first.disposition !== 'hit') throw new Error(first.reason.detail);
    await writeFile(join(root, 'output/file'), 'PUBLIC BYTES');
    await chmod(join(root, 'output/empty'), 0o700);
    const next = providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer);
    if (next.disposition !== 'hit') throw new Error(next.reason.detail);
    expect(next.receipt.tasks[0].outputs).not.toEqual(first.receipt.tasks[0].outputs);
    expect(next.receipt.tasks[0].outputs.find(({ path }) => path === 'output/empty')).toMatchObject({ mode: 0o700 });
  });

  it('accepts a new HEAD at the same committed tree without rewriting the producer basis', () => {
    const original = providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer);
    if (original.disposition !== 'hit') throw new Error(original.reason.detail);
    git(
      '-c',
      'user.name=Receipt fixture',
      '-c',
      'user.email=receipt@example.invalid',
      'commit',
      '--allow-empty',
      '-qm',
      'same tree',
    );
    expect(producerSourceUnchanged(root, source)).toEqual({ ok: true, value: true });
    const next = providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer);
    if (next.disposition !== 'hit') throw new Error(next.reason.detail);
    expect(next.receipt.head).toBe(original.receipt.head);
    expect(next.receipt.headTree).toBe(original.receipt.headTree);
    expect(next.receipt.producer.outputSha256).toBe(original.receipt.producer.outputSha256);
  });
  it('revokes a producer pre-source witness for a write/revert even when final bytes match HEAD', async () => {
    await writeFile(join(root, 'source.txt'), 'edited');
    await writeFile(join(root, 'source.txt'), 'source');
    expect(producerSourceUnchanged(root, source).ok).toBe(false);
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer).disposition).toBe('failed');
  });
  it('refuses unreadable public bytes, missing outputs and opaque runtime closure', async () => {
    await chmod(join(root, 'output/file'), 0);
    try {
      expect(providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer).disposition).toBe('failed');
    } finally {
      await chmod(join(root, 'output/file'), 0o600);
    }
    await rm(join(root, 'output'), { recursive: true });
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs, source, producer).disposition).toBe('failed');
    expect(
      providerReceipt(
        root,
        task.id,
        [task],
        { ...proof, runtimeSource: { kind: 'opaque', detail: 'unnameable command input closure' } },
        expandOutputs,
        source,
        producer,
      ),
    ).toMatchObject({
      disposition: 'failed',
      reason: { kind: 'provider-proof', detail: 'unnameable command input closure' },
    });
  });
});

describe('receipt runtime provenance', () => {
  it('admits only the existing generated pinned toolchain-version recipe, never a command containing the same words', () => {
    const recipe = cargoRuntimeInput('packages/app', 'rustc -vV && cargo -V').runtime;
    const planned = { [task.id]: { ...inputs, runtime: [recipe] } };
    expect(boundedReceiptRuntimes(planned, ['packages/app'], ['devenv.lock'], true)).toEqual({ kind: 'bounded' });
    expect(boundedReceiptRuntimes(planned, ['packages/app'], [], true).kind).toBe('opaque');
    expect(boundedReceiptRuntimes(planned, ['packages/app'], ['devenv.lock'], false).kind).toBe('opaque');
    expect(
      boundedReceiptRuntimes(
        { [task.id]: { ...inputs, runtime: [`${recipe}; cat /unproved/external`] } },
        ['packages/app'],
        ['devenv.lock'],
        true,
      ).kind,
    ).toBe('opaque');
    expect(
      boundedReceiptRuntimes(
        { [task.id]: { ...inputs, runtime: ['sh arbitrary-source-reader.sh'] } },
        ['packages/app'],
        ['devenv.lock'],
        true,
      ).kind,
    ).toBe('opaque');
  });

  it("refuses a declaration's own spelling of the toolchain versions: only the generator's recipe is one", () => {
    // The spelling cowshed's declared targets carried: the versions through the wrapper, with no directory to run in.
    const spelled = runtimeInput('rustc -vV').runtime;
    expect(
      boundedReceiptRuntimes(
        { [task.id]: { ...inputs, runtime: [spelled] } },
        ['packages/app', '.'],
        ['devenv.lock'],
        true,
      ),
    ).toEqual({ kind: 'opaque', detail: `Unbounded runtime source closure: ${spelled}` });
  });

  it('preserves whitespace, both sides of renames, deletion and index-only status paths', () => {
    expect(
      dirtyProviderPaths(' M source with spaces.ts\0R  new.ts\0old.ts\0D  deleted.ts\0?? new-source.ts\0'),
    ).toEqual(['source with spaces.ts', 'new.ts', 'old.ts', 'deleted.ts', 'new-source.ts']);
  });
});

describe('the producer receipts of this repository', () => {
  // A refusal is one stderr line beside a build that finished, so a declaration that makes a target's graph unprovable
  // breaks that target's receipts unseen. The repository's own declarations are judged here, on its own graph, by the
  // function the hooks call.
  it('prove a bounded runtime source: every runtime input their task graphs plan is a recipe the generator owns', async () => {
    const proofs = await repositoryReceiptProofs();
    expect(proofs.map(({ target }) => target)).not.toEqual([]);
    expect(proofs.filter(({ runtimeSource }) => runtimeSource.kind === 'opaque')).toEqual([]);
  });
});

/** What the producer hooks conclude about one receipt target of this repository, and the runtime inputs behind it. */
interface RepositoryReceiptProof {
  readonly target: string;
  readonly runtimeSource: ProviderInputProof['runtimeSource'];
  /** The runtime inputs each task of the target's graph plans, for the tasks that plan any. */
  readonly runtimes: Readonly<Record<string, readonly string[]>>;
}

const parseReceiptProofs = typia.json.createAssertParse<readonly RepositoryReceiptProof[]>();
const RECEIPT_PROOFS_SCRIPT = `const { createWorkspaceNxRequire, parseTargetSelector, providerInputProof } = await import(${JSON.stringify(
  fileURLToPath(new URL('./ensure-built.ts', import.meta.url)),
)});
const root = process.cwd();
const load = createWorkspaceNxRequire(root);
const nxJson = load('nx/src/config/nx-json').readNxJson(root);
const projectGraph = await load('nx/src/project-graph/project-graph').createProjectGraphAsync({ exitOnError: false });
const { createTaskGraph } = load('nx/src/tasks-runner/create-task-graph');
const targets = (nxJson.plugins ?? []).flatMap((entry) =>
  typeof entry === 'object' && entry.plugin === '@smoothbricks/nx-plugin' ? (entry.options?.producerReceipts?.targets ?? []) : [],
);
const proofs = targets.map((target) => {
  const selector = parseTargetSelector(target);
  const taskGraph = createTaskGraph(projectGraph, {}, [selector.project], [selector.target], selector.configuration, {}, false);
  const { inputs, runtimeSource } = providerInputProof(root, nxJson, projectGraph, taskGraph, load);
  const runtimes = Object.fromEntries(
    Object.entries(inputs).filter(([, input]) => input.runtime.length > 0).map(([task, input]) => [task, input.runtime]),
  );
  return { target, runtimeSource, runtimes };
});
process.stdout.write(JSON.stringify(proofs));`;

/** `providerInputProof` over each target nx.json declares for producer receipts, as the hooks call it. */
async function repositoryReceiptProofs(): Promise<readonly RepositoryReceiptProof[]> {
  return parseReceiptProofs(await runInRepositoryNx(RECEIPT_PROOFS_SCRIPT));
}
