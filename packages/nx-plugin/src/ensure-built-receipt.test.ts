import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { chmod, lstat, mkdir, mkdtemp, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Task } from 'nx/src/config/task-graph';
import { expandOutputs } from 'nx/src/native';
import { cargoRuntimeInput } from './cargo-toolchain-policy.js';
import {
  boundedReceiptRuntimes,
  dirtyProviderPaths,
  type ProviderInputProof,
  providerReceipt,
} from './ensure-built-receipt.js';

let root = '';
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
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

describe('provider receipt', () => {
  it('does not borrow the caller Git directory/index/object namespace for provider provenance', () => {
    const saved = process.env.GIT_DIR;
    process.env.GIT_DIR = join(root, 'not-the-provider-git-directory');
    try {
      expect(providerReceipt(root, task.id, [task], proof, expandOutputs).disposition).toBe('hit');
    } finally {
      if (saved === undefined) delete process.env.GIT_DIR;
      else process.env.GIT_DIR = saved;
    }
  });
  it('names current hashes, HEAD/tree, independent file digests, link text and empty directory modes', async () => {
    const result = providerReceipt(root, task.id, [task], proof, expandOutputs);
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

  it('does not refuse an unrelated untracked source when FILE closure is exactly proven', async () => {
    await writeFile(join(root, 'unrelated.ts'), 'unrelated');
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs).disposition).toBe('hit');
    expect(
      providerReceipt(root, task.id, [task], { ...proof, kind: 'conservative-provider-source' }, expandOutputs)
        .disposition,
    ).toBe('failed');
  });

  it('refuses modified, deleted, newly matched and index-only dirty FILE inputs', async () => {
    await writeFile(join(root, 'source.txt'), 'changed');
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs).disposition).toBe('failed');
    git('add', 'source.txt');
    await writeFile(join(root, 'source.txt'), 'source');
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs).disposition).toBe('failed');
    git('restore', '--staged', 'source.txt');
    await writeFile(join(root, 'new-source.txt'), 'new');
    expect(
      providerReceipt(root, task.id, [task], { ...proof, files: [...proof.files, 'new-source.txt'] }, expandOutputs)
        .disposition,
    ).toBe('failed');
    await rm(join(root, 'source.txt'));
    expect(providerReceipt(root, task.id, [task], { ...proof, files: [] }, expandOutputs).disposition).toBe('failed');
  });

  it('recomputes output byte/directory identities after every public mutation', async () => {
    const first = providerReceipt(root, task.id, [task], proof, expandOutputs);
    if (first.disposition !== 'hit') throw new Error(first.reason.detail);
    await writeFile(join(root, 'output/file'), 'PUBLIC BYTES');
    await chmod(join(root, 'output/empty'), 0o700);
    const next = providerReceipt(root, task.id, [task], proof, expandOutputs);
    if (next.disposition !== 'hit') throw new Error(next.reason.detail);
    expect(next.receipt.tasks[0].outputs).not.toEqual(first.receipt.tasks[0].outputs);
    expect(next.receipt.tasks[0].outputs.find(({ path }) => path === 'output/empty')).toMatchObject({ mode: 0o700 });
  });

  it('refuses unreadable public bytes, missing outputs and opaque runtime closure', async () => {
    await chmod(join(root, 'output/file'), 0);
    try {
      expect(providerReceipt(root, task.id, [task], proof, expandOutputs).disposition).toBe('failed');
    } finally {
      await chmod(join(root, 'output/file'), 0o600);
    }
    await rm(join(root, 'output'), { recursive: true });
    expect(providerReceipt(root, task.id, [task], proof, expandOutputs).disposition).toBe('failed');
    expect(
      providerReceipt(
        root,
        task.id,
        [task],
        { ...proof, runtimeSource: { kind: 'opaque', detail: 'unnameable command input closure' } },
        expandOutputs,
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
        { [task.id]: { ...inputs, runtime: [recipe + '; cat /unproved/external'] } },
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

  it('preserves whitespace, both sides of renames, deletion and index-only status paths', () => {
    expect(
      dirtyProviderPaths(' M source with spaces.ts\0R  new.ts\0old.ts\0D  deleted.ts\0?? new-source.ts\0'),
    ).toEqual(['source with spaces.ts', 'new.ts', 'old.ts', 'deleted.ts', 'new-source.ts']);
  });
});
