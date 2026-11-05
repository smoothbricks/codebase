/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions cover literal GitHub Actions expressions. */
import { afterEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setupDevenvInputNames, setupDevenvSteps } from '../__tests__/setup-devenv-steps.js';

const locate = setupDevenvSteps().find((step) =>
  String(step.get('name')).includes('Locate the Nx cache on the shared bind'),
);

const binds: string[] = [];
/** Each run of the step gets its own GITHUB_ENV file, so a scope's answer is never an earlier scope's. */
let stepRuns = 0;
afterEach(() => {
  for (const bind of binds.splice(0)) rmSync(bind, { recursive: true, force: true });
});

function sharedBind(): string {
  const bind = mkdtempSync(join(tmpdir(), 'setup-devenv-records-'));
  binds.push(bind);
  return bind;
}

/**
 * A CI event: a push of `ref`, or a pull request numbered `ref` into `base`, run by `job` of `workflow` (CI's `main`
 * unless it says otherwise). `records` is what the workflow hands the action: the workflow and job whose pushes keep the
 * records its scope starts from, when they are not its own.
 */
interface Event {
  readonly ref: string;
  readonly base: string;
  readonly workflow?: string;
  readonly job?: string;
  readonly cacheScope?: string;
  readonly records?: { readonly workflow: string; readonly job?: string };
}

/**
 * Runs the step the way GitHub runs a bash step, its shared bind under `bind`, and answers the workspace data
 * directory it hands the job.
 */
function locateNxCache(bind: string, event: Event): string {
  if (!locate) throw new Error('setup-devenv has no step that locates the Nx cache on the shared bind');
  const workflow = event.workflow ?? 'CI';
  const githubEnv = join(bind, `github-env-${stepRuns++}`);
  const result = Bun.spawnSync(['bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', String(locate.get('run'))], {
    cwd: bind,
    env: {
      ...process.env,
      NX_CACHE_BIND: join(bind, 'nx'),
      NX_CACHE_REPO: 'example/app',
      NX_CACHE_LANE: workflow,
      SCOPE_REF: event.ref,
      BASE_REF: event.base,
      RECORDS_WORKFLOW: event.records?.workflow ?? '',
      RECORDS_JOB: event.records?.job ?? '',
      GITHUB_WORKFLOW: workflow,
      GITHUB_JOB: event.job ?? 'main',
      CACHE_SCOPE: event.cacheScope || event.job || 'main',
      RUNNER_OS: 'Linux',
      RUNNER_ARCH: 'X64',
      GITHUB_ENV: githubEnv,
    },
  });
  expect(result.exitCode, result.stderr.toString()).toBe(0);
  const workspaceData = /^NX_WORKSPACE_DATA_DIRECTORY=(.+)$/m.exec(readFileSync(githubEnv, 'utf8'))?.[1];
  if (workspaceData === undefined) throw new Error(`the step handed the job no workspace data: ${githubEnv}`);
  return workspaceData;
}

/** The record bounded-exec keeps for `task` under `workspaceData`, and a report beside it that is no record. */
function writeRecord(workspaceData: string, task: string, peakBytes: number): void {
  const directory = join(workspaceData, 'bounded-exec', encodeURIComponent(task));
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, 'verdict.json'), `${JSON.stringify({ task, hash: null, peakBytes })}\n`);
  writeFileSync(join(directory, 'report.xml'), '<testsuites/>\n');
}

function peakOf(workspaceData: string, task: string): unknown {
  const verdict = join(workspaceData, 'bounded-exec', encodeURIComponent(task), 'verdict.json');
  if (!existsSync(verdict)) return undefined;
  const record: unknown = JSON.parse(readFileSync(verdict, 'utf8'));
  return typeof record === 'object' && record !== null && 'peakBytes' in record ? record.peakBytes : undefined;
}

const push: Event = { ref: 'refs/heads/main', base: '' };
const pullRequest: Event = { ref: '7', base: 'main' };
/** A release workflow's run on the branch CI's pushes test: its tests are CI's, so its records start from CI's. */
const release: Event = {
  ref: 'refs/heads/main',
  base: '',
  workflow: 'Publish',
  job: 'publish',
  records: { workflow: 'CI', job: 'main' },
};
const GIB = 2 ** 30;

describe("a pull request's first run on the shared bind", () => {
  test("starts from each task record its base branch's runs left, and only the records", () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);
    writeRecord(main, 'lib:test', GIB);

    const scope = locateNxCache(bind, pullRequest);

    expect(scope).not.toBe(main);
    expect(peakOf(scope, 'app:test')).toBe(3 * GIB);
    expect(peakOf(scope, 'lib:test')).toBe(GIB);
    expect(existsSync(join(scope, 'bounded-exec', encodeURIComponent('app:test'), 'report.xml'))).toBe(false);
  });

  test('keeps its own records once it has run, whatever its base branch records since', () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);
    const scope = locateNxCache(bind, pullRequest);
    writeRecord(scope, 'app:test', 5 * GIB);
    writeRecord(main, 'app:test', 2 * GIB);

    expect(locateNxCache(bind, pullRequest)).toBe(scope);
    expect(peakOf(scope, 'app:test')).toBe(5 * GIB);
  });

  test('a push takes no records from another scope', () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);

    const branch = locateNxCache(bind, { ref: 'refs/heads/feature', base: '' });

    expect(existsSync(join(branch, 'bounded-exec'))).toBe(false);
  });
});

describe("a release workflow's first run on the shared bind", () => {
  test("starts from each task record CI's pushes to its ref left, and only the records", () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);
    writeRecord(main, 'lib:test', GIB);

    const scope = locateNxCache(bind, release);

    expect(scope).not.toBe(main);
    expect(peakOf(scope, 'app:test')).toBe(3 * GIB);
    expect(peakOf(scope, 'lib:test')).toBe(GIB);
    expect(existsSync(join(scope, 'bounded-exec', encodeURIComponent('app:test'), 'report.xml'))).toBe(false);
  });

  test("starts from the records of CI's pushes to its own ref, never another's", () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);

    const branch = locateNxCache(bind, { ...release, ref: 'refs/heads/feature' });

    expect(existsSync(join(branch, 'bounded-exec'))).toBe(false);
  });
});

describe('a later run on the shared bind', () => {
  test("takes the record of each task a release scope holds none of from CI's, and keeps its own", () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);
    writeRecord(main, 'lib:test', GIB);
    // The scope an earlier run left: a record of the one task it measured, none of the rest.
    const scope = locateNxCache(bind, { ...release, records: undefined });
    writeRecord(scope, 'app:test', 5 * GIB);

    expect(locateNxCache(bind, release)).toBe(scope);
    expect(peakOf(scope, 'app:test')).toBe(5 * GIB);
    expect(peakOf(scope, 'lib:test')).toBe(GIB);
  });

  test("takes the record of each task its base branch recorded since a pull request's last run, and keeps its own", () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);
    const scope = locateNxCache(bind, pullRequest);
    writeRecord(scope, 'app:test', 5 * GIB);
    writeRecord(main, 'app:test', 2 * GIB);
    writeRecord(main, 'lib:test', GIB);

    expect(locateNxCache(bind, pullRequest)).toBe(scope);
    expect(peakOf(scope, 'app:test')).toBe(5 * GIB);
    expect(peakOf(scope, 'lib:test')).toBe(GIB);
  });
});

describe('matrix starting-push task records', () => {
  test('each shard takes only its matching push records, then replaces them with its own measurements', () => {
    const bind = sharedBind();
    const firstPush = locateNxCache(bind, { ...push, job: 'matrix', cacheScope: 'test-first' });
    const secondPush = locateNxCache(bind, { ...push, job: 'matrix', cacheScope: 'test-second' });
    writeRecord(firstPush, 'app:test', GIB);
    writeRecord(secondPush, 'app:test', 3 * GIB);
    const first = locateNxCache(bind, { ...pullRequest, job: 'matrix', cacheScope: 'test-first' });
    const second = locateNxCache(bind, { ...pullRequest, job: 'matrix', cacheScope: 'test-second' });
    expect(first).not.toBe(second);
    expect(peakOf(first, 'app:test')).toBe(GIB);
    expect(peakOf(second, 'app:test')).toBe(3 * GIB);
    writeRecord(first, 'app:test', 5 * GIB);
    writeRecord(firstPush, 'app:test', 2 * GIB);
    writeRecord(firstPush, 'lib:test', GIB);
    expect(locateNxCache(bind, { ...pullRequest, job: 'matrix', cacheScope: 'test-first' })).toBe(first);
    expect(peakOf(first, 'app:test')).toBe(5 * GIB);
    expect(peakOf(first, 'lib:test')).toBe(GIB);
    expect(peakOf(second, 'app:test')).toBe(3 * GIB);
  });

  test('a records-workflow override keeps the shard identity unless records-job selects another one', () => {
    const bind = sharedBind();
    const matching = locateNxCache(bind, { ...push, cacheScope: 'test-first' });
    const other = locateNxCache(bind, { ...push, cacheScope: 'test-second' });
    writeRecord(matching, 'app:test', GIB);
    writeRecord(other, 'app:test', 3 * GIB);
    const own = { ...release, cacheScope: 'test-first', records: { workflow: 'CI' } };
    const seeded = locateNxCache(bind, own);
    expect(peakOf(seeded, 'app:test')).toBe(GIB);
    const selected = locateNxCache(bind, {
      ...release,
      cacheScope: 'test-third',
      records: { workflow: 'CI', job: 'test-second' },
    });
    expect(peakOf(selected, 'app:test')).toBe(3 * GIB);
    expect(locateNxCache(bind, { ...own, records: undefined })).toBe(seeded);
  });

  test('an explicit records-job still selects the original nonmatrix job scope', () => {
    const bind = sharedBind();
    const main = locateNxCache(bind, push);
    writeRecord(main, 'app:test', 3 * GIB);
    const shard = locateNxCache(bind, { ...release, cacheScope: 'test-first' });
    expect(peakOf(shard, 'app:test')).toBe(3 * GIB);
    expect(locateNxCache(bind, { ...push, cacheScope: '' })).toBe(main);
    expect(locateNxCache(bind, { ...push, cacheScope: 'main' })).toBe(main);
  });
});

describe('the inputs that name the scope a run starts from', () => {
  test('the action declares them and hands them to the step that copies the records', () => {
    expect(setupDevenvInputNames()).toEqual(['cache-scope', 'records-workflow', 'records-job']);
    expect(locate?.get('env')).toMatchObject({
      CACHE_SCOPE: '${{ inputs.cache-scope || github.job }}',
      RECORDS_WORKFLOW: '${{ inputs.records-workflow }}',
      RECORDS_JOB: '${{ inputs.records-job }}',
    });
  });
});
