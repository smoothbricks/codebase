import { expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, symlink, truncate, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MAX_ARTIFACT_BYTES } from 'nx/src/tasks-runner/task-artifacts.js';
import { handOffMutationReport, MUTATION_REPORT_ARTIFACT, TASK_ARTIFACTS_ENV } from './mutation-report.js';

/** A run's capture, Nx's artifact directory for the execution, and a source file outside both. */
async function scene() {
  const root = await mkdtemp(join(tmpdir(), 'mutation-report-'));
  const capture = join(root, 'capture');
  const artifacts = join(root, 'artifacts');
  await mkdir(capture);
  await mkdir(artifacts);
  await writeFile(join(root, 'source.ts'), 'secret source');
  return {
    root,
    capture,
    artifacts,
    env: { [TASK_ARTIFACTS_ENV]: artifacts },
    handed: join(artifacts, MUTATION_REPORT_ARTIFACT),
    done: () => rm(root, { recursive: true, force: true }),
  };
}

it("hands the run's own Stryker report to Nx as the execution's artifact", async () => {
  const s = await scene();
  try {
    await writeFile(join(s.capture, 'mutation.json'), '{"fresh":true}');
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toEqual({ kind: 'handed', bytes: 14 });
    expect(await readFile(s.handed, 'utf8')).toBe('{"fresh":true}');
  } finally {
    await s.done();
  }
});

it("hands cargo-mutants' outcomes from its --output directory", async () => {
  const s = await scene();
  try {
    await mkdir(join(s.capture, 'mutants.out'));
    await writeFile(join(s.capture, 'mutants.out/outcomes.json'), '{"outcomes":[]}');
    expect(await handOffMutationReport('cargo-mutants', s.capture, s.env)).toMatchObject({ kind: 'handed' });
    expect(await readFile(s.handed, 'utf8')).toBe('{"outcomes":[]}');
  } finally {
    await s.done();
  }
});

it('hands off nothing when the run wrote no report', async () => {
  const s = await scene();
  try {
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toEqual({ kind: 'absent' });
    expect(await Bun.file(s.handed).exists()).toBe(false);
  } finally {
    await s.done();
  }
});

it('refuses a captured report that is a symlink or a FIFO, and hands off none of its bytes', async () => {
  const s = await scene();
  try {
    await symlink(join(s.root, 'source.ts'), join(s.capture, 'mutation.json'));
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toMatchObject({ kind: 'refused' });
    await rm(join(s.capture, 'mutation.json'));
    expect(Bun.spawnSync(['mkfifo', join(s.capture, 'mutation.json')]).exitCode).toBe(0);
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toMatchObject({
      kind: 'refused',
      reason: expect.stringContaining('not a regular file'),
    });
    expect(await Bun.file(s.handed).exists()).toBe(false);
  } finally {
    await s.done();
  }
});

it("refuses by its size, before reading it, a report over Nx's artifact bound", async () => {
  const s = await scene();
  try {
    // Sparse: its size is over the bound, and reading it would allocate all of it.
    await writeFile(join(s.capture, 'mutation.json'), '');
    await truncate(join(s.capture, 'mutation.json'), MAX_ARTIFACT_BYTES + 1);
    expect(await handOffMutationReport('stryker', s.capture, s.env)).toEqual({
      kind: 'refused',
      reason: `the captured report holds ${MAX_ARTIFACT_BYTES + 1} bytes, over Nx's ${MAX_ARTIFACT_BYTES}-byte artifact bound`,
    });
    expect(await Bun.file(s.handed).exists()).toBe(false);
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
    await symlink(elsewhere, join(s.capture, 'mutants.out'));
    expect(await handOffMutationReport('cargo-mutants', s.capture, s.env)).toEqual({
      kind: 'refused',
      reason: `${join(s.capture, 'mutants.out')} is not a directory but a link or another kind of file`,
    });
    expect(await Bun.file(s.handed).exists()).toBe(false);
  } finally {
    await s.done();
  }
});

it('refuses under an Nx that makes no task-artifact directory, so no report is handed to no result', async () => {
  const s = await scene();
  try {
    await writeFile(join(s.capture, 'mutation.json'), '{"fresh":true}');
    expect(await handOffMutationReport('stryker', s.capture, {})).toMatchObject({
      kind: 'refused',
      reason: expect.stringContaining(`${TASK_ARTIFACTS_ENV} is unset`),
    });
  } finally {
    await s.done();
  }
});
