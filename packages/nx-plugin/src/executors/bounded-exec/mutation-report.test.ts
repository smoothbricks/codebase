import { expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rename, rm, symlink, truncate, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MAX_ARTIFACT_BYTES } from 'nx/src/tasks-runner/task-artifacts.js';
import {
  closeCapture,
  handOffMutationReport,
  MUTATION_REPORT_ARTIFACT,
  openCapture,
  TASK_ARTIFACTS_ENV,
} from './mutation-report.js';

/** A run's capture, Nx's artifact directory with the `mutationReport` file Nx makes, and a source file outside both. */
async function scene(options: { declared?: boolean } = {}) {
  const root = await mkdtemp(join(tmpdir(), 'mutation-report-'));
  const capture = await openCapture(root);
  const artifacts = join(root, 'artifacts');
  await mkdir(artifacts);
  const handed = join(artifacts, MUTATION_REPORT_ARTIFACT);
  if (options.declared ?? true) await writeFile(handed, '');
  await writeFile(join(root, 'source.ts'), 'secret source');
  return {
    root,
    capture,
    path: capture.path,
    env: { [TASK_ARTIFACTS_ENV]: artifacts },
    handed: async () => readFile(handed, 'utf8'),
    done: async () => {
      closeCapture(capture);
      await rm(root, { recursive: true, force: true });
    },
  };
}

it("hands the run's own Stryker report to Nx as the execution's artifact", async () => {
  const s = await scene();
  try {
    await writeFile(join(s.path, 'mutation.json'), '{"fresh":true}');
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toEqual({ kind: 'handed', bytes: 14 });
    expect(await s.handed()).toBe('{"fresh":true}');
  } finally {
    await s.done();
  }
});

it("hands cargo-mutants' outcomes from its --output directory", async () => {
  const s = await scene();
  try {
    await mkdir(join(s.path, 'mutants.out'));
    await writeFile(join(s.path, 'mutants.out/outcomes.json'), '{"outcomes":[]}');
    expect(await handOffMutationReport('cargo-mutants', s.capture, s.env)).toMatchObject({ kind: 'handed' });
    expect(await s.handed()).toBe('{"outcomes":[]}');
  } finally {
    await s.done();
  }
});

it('hands off nothing when the run wrote no report', async () => {
  const s = await scene();
  try {
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toEqual({ kind: 'absent' });
    expect(await s.handed()).toBe('');
  } finally {
    await s.done();
  }
});

it('reads beneath the capture it holds, never what is under its name when it reads', async () => {
  const s = await scene();
  try {
    await mkdir(join(s.path, 'mutants.out'));
    await writeFile(join(s.path, 'mutants.out/outcomes.json'), '{"outcomes":"this run"}');
    // The capture moved away and another directory put under its name, holding a forged report.
    await rename(s.path, `${s.path}-moved`);
    await mkdir(join(s.path, 'mutants.out'), { recursive: true });
    await writeFile(join(s.path, 'mutants.out/outcomes.json'), '{"outcomes":"forged"}');
    expect(await handOffMutationReport('cargo-mutants', s.capture, s.env)).toMatchObject({ kind: 'handed' });
    expect(await s.handed()).toBe('{"outcomes":"this run"}');
  } finally {
    await s.done();
  }
});

it('refuses a captured report that is a symlink or a FIFO, and hands off none of its bytes', async () => {
  const s = await scene();
  try {
    await symlink(join(s.root, 'source.ts'), join(s.path, 'mutation.json'));
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toEqual({
      kind: 'refused',
      reason: 'mutation.json is a link or not a file',
    });
    await rm(join(s.path, 'mutation.json'));
    expect(Bun.spawnSync(['mkfifo', join(s.path, 'mutation.json')]).exitCode).toBe(0);
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toEqual({
      kind: 'refused',
      reason: 'the report is not a regular file',
    });
    expect(await s.handed()).toBe('');
  } finally {
    await s.done();
  }
});

it("refuses by its size, before reading it, a report over Nx's artifact bound", async () => {
  const s = await scene();
  try {
    // Sparse: its size is over the bound, and reading it would allocate all of it.
    await writeFile(join(s.path, 'mutation.json'), '');
    await truncate(join(s.path, 'mutation.json'), MAX_ARTIFACT_BYTES + 1);
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toEqual({
      kind: 'refused',
      reason: `the report holds ${MAX_ARTIFACT_BYTES + 1} bytes, over Nx's ${MAX_ARTIFACT_BYTES}-byte artifact bound`,
    });
    expect(await s.handed()).toBe('');
  } finally {
    await s.done();
  }
});

it("refuses cargo-mutants' outcomes reached through a linked directory, and hands off none of its bytes", async () => {
  const s = await scene();
  try {
    const elsewhere = join(s.root, 'elsewhere');
    await mkdir(elsewhere);
    await writeFile(join(elsewhere, 'outcomes.json'), '{"outcomes":"not this run"}');
    await symlink(elsewhere, join(s.path, 'mutants.out'));
    expect(await handOffMutationReport('cargo-mutants', s.capture, s.env)).toEqual({
      kind: 'refused',
      reason: 'mutants.out is a link or not a directory',
    });
    expect(await s.handed()).toBe('');
  } finally {
    await s.done();
  }
});

it('refuses when Nx made no mutationReport file: the target declares no such artifact', async () => {
  const s = await scene({ declared: false });
  try {
    await writeFile(join(s.path, 'mutation.json'), '{"fresh":true}');
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toMatchObject({
      kind: 'refused',
      reason: expect.stringContaining('its target must declare `artifacts: ["mutationReport"]`'),
    });
  } finally {
    await s.done();
  }
});

it('refuses under an Nx that makes no task-artifact directory, so no report is handed to no result', async () => {
  const s = await scene();
  try {
    await writeFile(join(s.path, 'mutation.json'), '{"fresh":true}');
    expect(await handOffMutationReport('stryker', s.capture, {})).toMatchObject({
      kind: 'refused',
      reason: expect.stringContaining(`${TASK_ARTIFACTS_ENV} is unset`),
    });
  } finally {
    await s.done();
  }
});
